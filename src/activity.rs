use serde::Serialize;
use serde_json::Value;

const MAX_TEXT: usize = 128;
const MAX_ASSET_KEY: usize = 1024;
const MAX_BUTTON_LABEL: usize = 32;
const MAX_BUTTON_URL: usize = 512;
/// Values below this are seconds from older RPC clients. Gateway wants ms.
const SECONDS_CUTOFF: i64 = 1_000_000_000_000;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Activity {
    pub name: String,
    pub application_id: String,
    #[serde(rename = "type")]
    pub kind: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamps: Option<Timestamps>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assets: Option<Assets>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_display_type: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buttons: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ButtonMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub party: Option<Party>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Timestamps {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Assets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub large_image: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub large_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub small_image: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub small_text: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ButtonMetadata {
    pub button_urls: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Party {
    pub size: [u32; 2],
}

impl Activity {
    /// Turn the IPC `activity` object into the subset the Gateway accepts.
    /// `fallback_name` is the application name resolved from the client id.
    pub fn from_ipc(value: &Value, app_id: &str, fallback_name: &str) -> Option<Self> {
        let object = value.as_object()?;
        let kind = object
            .get("type")
            .and_then(Value::as_u64)
            .map(|kind| kind as u8)
            .filter(|kind| matches!(kind, 0 | 2 | 3 | 5))
            .unwrap_or(0);
        let name = if fallback_name.trim().is_empty() {
            text(object.get("name"), MAX_TEXT, true).unwrap_or_else(|| "Unknown".to_owned())
        } else {
            text_value(fallback_name, MAX_TEXT)
        };

        let details = text(object.get("details"), MAX_TEXT, true);
        let state = text(object.get("state"), MAX_TEXT, true);
        let timestamps = timestamps(object.get("timestamps"));
        let assets = assets(object.get("assets"));
        let status_display_type = object
            .get("status_display_type")
            .and_then(Value::as_u64)
            .filter(|value| *value <= 2)
            .map(|value| value as u8);
        let (buttons, metadata) = buttons(object);
        let party = party(object.get("party"));

        Some(Self {
            name,
            application_id: app_id.to_owned(),
            kind,
            details,
            state,
            timestamps,
            assets,
            status_display_type,
            buttons,
            metadata,
            party,
        })
    }
}

fn text(value: Option<&Value>, limit: usize, pad: bool) -> Option<String> {
    let raw = value?.as_str()?.trim();
    if raw.is_empty() {
        return None;
    }
    let mut cleaned: String = raw
        .chars()
        .filter(|ch| !ch.is_control())
        .take(limit)
        .collect();
    if cleaned.is_empty() {
        return None;
    }
    if pad && cleaned.chars().count() < 2 {
        cleaned.push(' ');
    }
    Some(cleaned)
}

fn text_value(raw: &str, limit: usize) -> String {
    let mut cleaned: String = raw
        .chars()
        .filter(|ch| !ch.is_control())
        .take(limit)
        .collect();
    if cleaned.is_empty() {
        cleaned.push(' ');
    }
    if cleaned.chars().count() < 2 {
        cleaned.push(' ');
    }
    cleaned
}

fn timestamps(value: Option<&Value>) -> Option<Timestamps> {
    let object = value?.as_object()?;
    let start = timestamp(object.get("start"));
    let end = timestamp(object.get("end")).filter(|end| start.is_none_or(|start| *end > start));
    if start.is_none() && end.is_none() {
        return None;
    }
    Some(Timestamps { start, end })
}

fn timestamp(value: Option<&Value>) -> Option<i64> {
    let mut value = value?.as_i64()?;
    if value <= 0 {
        return None;
    }
    if value < SECONDS_CUTOFF {
        value = value.saturating_mul(1000);
    }
    Some(value)
}

fn assets(value: Option<&Value>) -> Option<Assets> {
    let object = value?.as_object()?;
    let large_image = image(object.get("large_image"));
    let small_image = image(object.get("small_image"));
    if large_image.is_none() && small_image.is_none() {
        return None;
    }
    Some(Assets {
        large_text: text(object.get("large_text"), MAX_TEXT, false)
            .filter(|_| large_image.is_some()),
        small_text: text(object.get("small_text"), MAX_TEXT, false)
            .filter(|_| small_image.is_some()),
        large_image,
        small_image,
    })
}

fn image(value: Option<&Value>) -> Option<String> {
    let raw = value?.as_str()?.trim();
    if raw.is_empty() {
        return None;
    }
    let cleaned: String = raw
        .chars()
        .filter(|ch| !ch.is_control())
        .take(MAX_ASSET_KEY)
        .collect();
    (!cleaned.is_empty()).then_some(cleaned)
}

fn buttons(
    object: &serde_json::Map<String, Value>,
) -> (Option<Vec<String>>, Option<ButtonMetadata>) {
    let labels: Vec<String> = object
        .get("buttons")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(|label| text_value(label, MAX_BUTTON_LABEL))
                .take(2)
                .collect()
        })
        .unwrap_or_default();
    if labels.is_empty() {
        return (None, None);
    }
    let urls: Vec<String> = object
        .get("metadata")
        .and_then(|metadata| metadata.get("button_urls"))
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .filter(|url| valid_button_url(url))
                .map(str::to_owned)
                .take(2)
                .collect()
        })
        .unwrap_or_default();
    if urls.len() != labels.len() {
        return (None, None);
    }
    (Some(labels), Some(ButtonMetadata { button_urls: urls }))
}

fn valid_button_url(url: &str) -> bool {
    url.len() <= MAX_BUTTON_URL
        && url::Url::parse(url).is_ok_and(|url| {
            url.scheme() == "https"
                && url.has_host()
                && url.username().is_empty()
                && url.password().is_none()
        })
}

fn party(value: Option<&Value>) -> Option<Party> {
    let size = value?.get("size")?.as_array()?;
    let current = size.first()?.as_u64()? as u32;
    let max = size.get(1)?.as_u64()? as u32;
    (current > 0 && current <= max && max <= 9999).then_some(Party {
        size: [current, max],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maps_seconds_and_keeps_assets() {
        let value = json!({
            "type": 2,
            "details": "Song",
            "state": "Artist",
            "timestamps": {"start": 1_700_000_000, "end": 1_700_000_200},
            "assets": {
                "large_image": "https://img/x.jpg",
                "large_text": "Album",
                "small_image": "pause",
                "small_text": "Playing"
            },
            "status_display_type": 1
        });
        let activity = Activity::from_ipc(&value, "42", "Example App").unwrap();
        assert_eq!(activity.name, "Example App");
        assert_eq!(activity.application_id, "42");
        assert_eq!(activity.kind, 2);
        assert_eq!(activity.details.as_deref(), Some("Song"));
        assert_eq!(activity.timestamps.unwrap().start, Some(1_700_000_000_000));
        assert_eq!(
            activity.assets.unwrap().large_image.as_deref(),
            Some("https://img/x.jpg")
        );
        assert_eq!(activity.status_display_type, Some(1));
    }

    #[test]
    fn application_name_beats_a_client_supplied_name() {
        let value = json!({"name": "Some Launcher"});
        let activity = Activity::from_ipc(&value, "42", "Example App").unwrap();
        assert_eq!(activity.name, "Example App");
    }

    #[test]
    fn short_fields_are_padded_and_controls_removed() {
        let value = json!({"details": "X\u{0007}", "state": "ok", "name": "Y"});
        let activity = Activity::from_ipc(&value, "1", "").unwrap();
        assert_eq!(activity.details.as_deref(), Some("X "));
        assert_eq!(activity.name, "Y ");
    }

    #[test]
    fn buttons_need_matching_https_urls() {
        let bad = json!({"buttons": ["Open"], "metadata": {"button_urls": ["http://x"]}});
        let activity = Activity::from_ipc(&bad, "1", "App").unwrap();
        assert!(activity.buttons.is_none());
        let good = json!({"buttons": ["Open"], "metadata": {"button_urls": ["https://x.test/a"]}});
        let activity = Activity::from_ipc(&good, "1", "App").unwrap();
        assert_eq!(activity.buttons.unwrap(), vec!["Open"]);
        assert_eq!(
            activity.metadata.unwrap().button_urls,
            vec!["https://x.test/a"]
        );
    }

    #[test]
    fn invalid_party_is_dropped() {
        let value = json!({"party": {"size": [0, 4]}});
        assert!(
            Activity::from_ipc(&value, "1", "App")
                .unwrap()
                .party
                .is_none()
        );
        let value = json!({"party": {"size": [2, 4]}});
        assert_eq!(
            Activity::from_ipc(&value, "1", "App")
                .unwrap()
                .party
                .unwrap()
                .size,
            [2, 4]
        );
    }
}
