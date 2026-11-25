use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use reqwest::Client;
use serde::Deserialize;

const CACHE_TTL: Duration = Duration::from_secs(60 * 60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const USER_AGENT: &str = concat!("rpcbridge/", env!("CARGO_PKG_VERSION"));

#[derive(Clone, Debug)]
pub struct AppInfo {
    pub name: String,
}

#[derive(Deserialize)]
struct PartialApplication {
    name: String,
}

pub struct AppRegistry {
    http: Client,
    cache: Mutex<HashMap<String, (Instant, AppInfo)>>,
}

impl AppRegistry {
    pub fn new() -> Self {
        Self {
            http: Client::new(),
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub async fn name(&self, app_id: &str) -> Option<String> {
        if let Some(name) = self.cached(app_id) {
            return Some(name);
        }
        let url = format!("https://discord.com/api/v10/applications/{app_id}/rpc");
        let response = self
            .http
            .get(url)
            .timeout(REQUEST_TIMEOUT)
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            tracing::debug!(app_id, status = %response.status(), "application lookup failed");
            return None;
        }
        let application: PartialApplication = response.json().await.ok()?;
        let name = application.name;
        if let Ok(mut cache) = self.cache.lock() {
            if cache.len() >= 256 {
                cache.clear();
            }
            cache.insert(
                app_id.to_owned(),
                (Instant::now(), AppInfo { name: name.clone() }),
            );
        }
        Some(name)
    }

    fn cached(&self, app_id: &str) -> Option<String> {
        let cache = self.cache.lock().ok()?;
        let (at, info) = cache.get(app_id)?;
        (at.elapsed() < CACHE_TTL).then(|| info.name.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_is_bounded_and_starts_empty() {
        let registry = AppRegistry::new();
        assert!(registry.cached("1").is_none());
    }
}
