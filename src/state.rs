use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::watch;

use crate::activity::Activity;
use crate::auth::AuthStore;

/// Discord accepts an activity array; clients render a handful at most.
const MAX_ACTIVITIES: usize = 5;

struct ClientPresence {
    activity: Option<Activity>,
    updated: Instant,
}

pub struct PresenceState {
    clients: Mutex<HashMap<u64, ClientPresence>>,
    next_id: AtomicU64,
    tx: watch::Sender<Vec<Activity>>,
    auth: Arc<AuthStore>,
}

impl PresenceState {
    pub fn new(auth: Arc<AuthStore>) -> (Arc<Self>, watch::Receiver<Vec<Activity>>) {
        let (tx, rx) = watch::channel(Vec::new());
        let state = Arc::new(Self {
            clients: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            tx,
            auth,
        });
        (state, rx)
    }

    pub fn next_client_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn auth(&self) -> &Arc<AuthStore> {
        &self.auth
    }

    /// Set or clear one client's activity.
    pub fn update(&self, client_id: u64, app_id: String, activity: Option<Activity>) {
        tracing::debug!(
            client_id,
            app_id,
            active = activity.is_some(),
            "client activity updated"
        );
        if let Ok(mut clients) = self.clients.lock() {
            clients.insert(
                client_id,
                ClientPresence {
                    activity,
                    updated: Instant::now(),
                },
            );
        }
        self.publish();
    }

    pub fn remove(&self, client_id: u64) {
        if let Ok(mut clients) = self.clients.lock() {
            clients.remove(&client_id);
        }
        self.publish();
    }

    pub fn clear(&self) {
        if let Ok(mut clients) = self.clients.lock() {
            clients.clear();
        }
        self.tx.send_replace(Vec::new());
    }

    /// Newest activity first, one per application, capped at five.
    fn publish(&self) {
        let activities = self
            .clients
            .lock()
            .ok()
            .map(|clients| {
                let mut active: Vec<(&Instant, &Activity)> = clients
                    .values()
                    .filter_map(|client| {
                        client
                            .activity
                            .as_ref()
                            .map(|activity| (&client.updated, activity))
                    })
                    .collect();
                active.sort_by(|left, right| right.0.cmp(left.0));
                let mut seen = HashSet::new();
                active
                    .into_iter()
                    .filter(|(_, activity)| seen.insert(activity.application_id.clone()))
                    .take(MAX_ACTIVITIES)
                    .map(|(_, activity)| activity.clone())
                    .collect()
            })
            .unwrap_or_default();
        self.tx.send_replace(activities);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activity::Activity;

    fn state() -> (
        Arc<PresenceState>,
        watch::Receiver<Vec<Activity>>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let auth = Arc::new(AuthStore::load(dir.path().join("auth.json")));
        let (state, rx) = PresenceState::new(auth);
        (state, rx, dir)
    }

    fn activity(app_id: &str, details: &str) -> Activity {
        Activity {
            name: "Test App".into(),
            application_id: app_id.into(),
            kind: 2,
            details: Some(details.into()),
            state: None,
            timestamps: None,
            assets: None,
            status_display_type: None,
            buttons: None,
            metadata: None,
            party: None,
        }
    }

    #[test]
    fn newest_activity_is_first() {
        let (state, rx, _dir) = state();
        state.update(1, "1".into(), Some(activity("1", "one")));
        state.update(2, "2".into(), Some(activity("2", "two")));
        let list = rx.borrow();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].details.as_deref(), Some("two"));
        assert_eq!(list[1].details.as_deref(), Some("one"));
    }

    #[test]
    fn same_application_keeps_the_newest() {
        let (state, rx, _dir) = state();
        state.update(1, "1".into(), Some(activity("1", "old")));
        state.update(2, "1".into(), Some(activity("1", "new")));
        let list = rx.borrow();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].details.as_deref(), Some("new"));
    }

    #[test]
    fn five_activities_are_kept_and_the_rest_dropped() {
        let (state, rx, _dir) = state();
        for index in 0..7 {
            state.update(
                index,
                index.to_string(),
                Some(activity(&index.to_string(), &format!("activity {index}"))),
            );
        }
        let list = rx.borrow();
        assert_eq!(list.len(), MAX_ACTIVITIES);
        assert_eq!(list[0].details.as_deref(), Some("activity 6"));
        assert_eq!(list[4].details.as_deref(), Some("activity 2"));
    }

    #[test]
    fn clearing_one_client_leaves_the_others() {
        let (state, rx, _dir) = state();
        state.update(1, "1".into(), Some(activity("1", "one")));
        state.update(2, "2".into(), Some(activity("2", "two")));
        state.update(2, "2".into(), None);
        let list = rx.borrow();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].details.as_deref(), Some("one"));
    }

    #[test]
    fn removing_the_last_client_clears_the_presence() {
        let (state, rx, _dir) = state();
        state.update(1, "1".into(), Some(activity("1", "one")));
        state.remove(1);
        assert!(rx.borrow().is_empty());
    }
}
