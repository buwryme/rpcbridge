use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::activity::Activity;
use crate::auth::AuthStore;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HELLO_TIMEOUT: Duration = Duration::from_secs(20);
const RECONNECT_BACKOFF: [u64; 5] = [3, 5, 10, 15, 30];
/// Discord allows five presence updates per 20 seconds.
const PRESENCE_INTERVAL: Duration = Duration::from_millis(4_250);
const OP_DISPATCH: u64 = 0;
const OP_HEARTBEAT: u64 = 1;
const OP_IDENTIFY: u64 = 2;
const OP_PRESENCE_UPDATE: u64 = 3;
const OP_RECONNECT: u64 = 7;
const OP_INVALID_SESSION: u64 = 9;
const OP_HELLO: u64 = 10;
const OP_HEARTBEAT_ACK: u64 = 11;
const USER_INTENTS: u64 =
    (1 << 12) | (1 << 18) | (1 << 19) | (1 << 22) | (1 << 23) | (1 << 27) | (1 << 28) | (1 << 29);
const CLIENT_CAPABILITIES: u64 = (1 << 4) | (1 << 5) | (1 << 12) | (1 << 16);

type WsWriter = SplitSink<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, Message>;

enum End {
    Reconnect,
    Auth,
    Stop,
    Fatal,
}

pub async fn run(
    auth: Arc<AuthStore>,
    gateway_url: String,
    status: &'static str,
    mut activity_rx: watch::Receiver<Vec<Activity>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut attempt = 0usize;
    let mut auth_retry = false;
    loop {
        if *shutdown.borrow() {
            return;
        }
        match session(
            &auth,
            &gateway_url,
            status,
            &mut activity_rx,
            &mut shutdown,
            &mut attempt,
            &mut auth_retry,
        )
        .await
        {
            End::Stop => return,
            End::Fatal => {
                tracing::warn!("discord session stopped; retrying in 30s");
                if !sleep_or_shutdown(&mut shutdown, 30).await {
                    return;
                }
            }
            End::Auth => match auth.refresh().await {
                Ok(_) => {
                    attempt = 0;
                    auth_retry = true;
                    tracing::info!("discord token refreshed");
                }
                Err(err) => {
                    tracing::warn!(%err, "discord token refresh failed");
                    if !sleep_or_shutdown(&mut shutdown, 30).await {
                        return;
                    }
                }
            },
            End::Reconnect => {
                let delay = RECONNECT_BACKOFF[attempt.min(RECONNECT_BACKOFF.len() - 1)];
                attempt += 1;
                tracing::info!(delay, "discord reconnecting");
                if !sleep_or_shutdown(&mut shutdown, delay).await {
                    return;
                }
            }
        }
    }
}

async fn session(
    auth: &AuthStore,
    gateway_url: &str,
    status: &'static str,
    activity_rx: &mut watch::Receiver<Vec<Activity>>,
    shutdown: &mut watch::Receiver<bool>,
    attempt: &mut usize,
    auth_retry: &mut bool,
) -> End {
    let token = match auth.token().await {
        Ok(token) => token,
        Err(err) => {
            tracing::warn!(%err, "discord authorization unavailable");
            return if auth.is_authorized() {
                End::Reconnect
            } else {
                End::Fatal
            };
        }
    };

    let connected = tokio::time::timeout(
        CONNECT_TIMEOUT,
        tokio_tungstenite::connect_async(gateway_url),
    )
    .await;
    let ws = match connected {
        Ok(Ok((ws, _))) => ws,
        Ok(Err(err)) => {
            tracing::debug!(%err, "discord gateway connection failed");
            return End::Reconnect;
        }
        Err(_) => {
            tracing::debug!("discord gateway connection timed out");
            return End::Reconnect;
        }
    };
    let (mut write, mut read) = ws.split();

    let hello = match tokio::time::timeout(HELLO_TIMEOUT, read.next()).await {
        Ok(Some(Ok(Message::Text(text)))) => serde_json::from_str::<Value>(&text).ok(),
        Ok(Some(Ok(Message::Close(frame)))) => return closed(frame, auth_retry),
        Ok(Some(Ok(_))) => None,
        Ok(Some(Err(err))) => {
            tracing::debug!(%err, "discord error before HELLO");
            None
        }
        Ok(None) | Err(_) => None,
    };
    let Some(hello) = hello else {
        tracing::debug!("discord sent no HELLO");
        return End::Reconnect;
    };
    let Some(interval) = hello
        .pointer("/d/heartbeat_interval")
        .and_then(Value::as_u64)
    else {
        tracing::debug!(?hello, "discord HELLO without an interval");
        return End::Reconnect;
    };
    let interval = interval.max(5_000);

    let identify = json!({
        "op": OP_IDENTIFY,
        "d": {
            "token": format!("Bearer {token}"),
            "capabilities": CLIENT_CAPABILITIES,
            "intents": USER_INTENTS,
            "properties": {
                "os": std::env::consts::OS,
                "browser": "rpcbridge",
                "device": "rpcbridge",
                "browser_version": env!("CARGO_PKG_VERSION"),
                "client_version": env!("CARGO_PKG_VERSION"),
            },
            "compress": false,
        }
    });
    if write
        .send(Message::Text(identify.to_string().into()))
        .await
        .is_err()
    {
        return End::Reconnect;
    }

    let jitter = heartbeat_jitter(interval);
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_millis(jitter),
        Duration::from_millis(interval),
    );
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut seq: Option<u64> = None;
    let mut awaiting_ack = false;
    let mut ready = false;
    let mut last_update: Option<Instant> = None;

    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    if ready {
                        let _ = send_presence(&mut write, &[], status).await;
                    }
                    let _ = write.send(Message::Close(None)).await;
                    return End::Stop;
                }
            }
            changed = activity_rx.changed() => {
                if changed.is_err() {
                    return End::Stop;
                }
                let current = activity_rx.borrow_and_update().clone();
                if ready {
                    match publish(&mut write, &current, status, &mut last_update).await {
                        Ok(()) => {}
                        Err(_) => return End::Reconnect,
                    }
                }
            }
            _ = heartbeat.tick() => {
                if awaiting_ack {
                    tracing::info!("discord heartbeat was not acknowledged");
                    return End::Reconnect;
                }
                let frame = json!({"op": OP_HEARTBEAT, "d": seq});
                if write.send(Message::Text(frame.to_string().into())).await.is_err() {
                    return End::Reconnect;
                }
                awaiting_ack = true;
            }
            message = read.next() => {
                let Some(message) = message else {
                    return End::Reconnect;
                };
                let message = match message {
                    Ok(message) => message,
                    Err(err) => {
                        tracing::debug!(%err, "discord gateway read failed");
                        return End::Reconnect;
                    }
                };
                match message {
                    Message::Text(text) => {
                        let Ok(frame) = serde_json::from_str::<Value>(&text) else {
                            continue;
                        };
                        if let Some(value) = frame.get("s").and_then(Value::as_u64) {
                            seq = Some(value);
                        }
                        match frame.get("op").and_then(Value::as_u64) {
                            Some(OP_DISPATCH) => match frame.get("t").and_then(Value::as_str) {
                                Some("READY") => {
                                    let user = frame
                                        .pointer("/d/user/username")
                                        .and_then(Value::as_str)
                                        .unwrap_or("?");
                                    tracing::info!(user, "discord presence connected");
                                    ready = true;
                                    *attempt = 0;
                                    *auth_retry = false;
                                    last_update = None;
                                    let current = activity_rx.borrow_and_update().clone();
                                    match publish(&mut write, &current, status, &mut last_update).await {
                                        Ok(()) => {}
                                        Err(_) => return End::Reconnect,
                                    }
                                }
                                Some("RESUMED") => {
                                    ready = true;
                                    *auth_retry = false;
                                    last_update = None;
                                    let current = activity_rx.borrow_and_update().clone();
                                    match publish(&mut write, &current, status, &mut last_update).await {
                                        Ok(()) => {}
                                        Err(_) => return End::Reconnect,
                                    }
                                }
                                Some("SESSIONS_REPLACE") => {
                                    tracing::debug!(?frame, "discord session presence replaced");
                                }
                                _ => {}
                            },
                            Some(OP_HEARTBEAT) => {
                                let frame = json!({"op": OP_HEARTBEAT, "d": seq});
                                let _ = write.send(Message::Text(frame.to_string().into())).await;
                                awaiting_ack = true;
                            }
                            Some(OP_RECONNECT) => return End::Reconnect,
                            Some(OP_INVALID_SESSION) => return End::Reconnect,
                            Some(OP_HEARTBEAT_ACK) => awaiting_ack = false,
                            Some(OP_HELLO) => {}
                            _ => {}
                        }
                    }
                    Message::Close(frame) => return closed(frame, auth_retry),
                    Message::Ping(data) => {
                        let _ = write.send(Message::Pong(data)).await;
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn publish(
    write: &mut WsWriter,
    activities: &[Activity],
    status: &'static str,
    last_update: &mut Option<Instant>,
) -> Result<()> {
    if let Some(remaining) = last_update.and_then(|at| PRESENCE_INTERVAL.checked_sub(at.elapsed()))
    {
        tokio::time::sleep(remaining).await;
    }
    send_presence(write, activities, status).await?;
    *last_update = Some(Instant::now());
    Ok(())
}

fn presence_payload(activities: &[Activity], status: &'static str) -> Value {
    json!({
        "op": OP_PRESENCE_UPDATE,
        "d": {
            "since": 0,
            "activities": activities,
            "status": status,
            "afk": false,
        }
    })
}

async fn send_presence(
    write: &mut WsWriter,
    activities: &[Activity],
    status: &'static str,
) -> Result<()> {
    let payload = presence_payload(activities, status);
    write
        .send(Message::Text(payload.to_string().into()))
        .await
        .context("send presence")?;
    if activities.is_empty() {
        tracing::debug!("presence cleared");
    } else {
        tracing::debug!(
            count = activities.len(),
            first = %activities[0].name,
            first_app = %activities[0].application_id,
            "presence sent"
        );
    }
    Ok(())
}

fn closed(frame: Option<CloseFrame>, auth_retry: &mut bool) -> End {
    let code = frame
        .as_ref()
        .map(|frame| u16::from(frame.code))
        .unwrap_or(1005);
    let reason = frame
        .as_ref()
        .map(|frame| frame.reason.as_str())
        .unwrap_or_default();
    tracing::debug!(code, reason, "discord gateway closed");
    match code {
        4004 => {
            if *auth_retry {
                End::Fatal
            } else {
                *auth_retry = true;
                End::Auth
            }
        }
        4010..=4014 => End::Fatal,
        _ => End::Reconnect,
    }
}

async fn sleep_or_shutdown(shutdown: &mut watch::Receiver<bool>, seconds: u64) -> bool {
    let sleep = tokio::time::sleep(Duration::from_secs(seconds));
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = &mut sleep => return true,
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return false;
                }
            }
        }
    }
}

fn heartbeat_jitter(interval: u64) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos())
        .unwrap_or_default();
    interval / 2 + u64::from(nanos) % (interval / 2).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    fn close(code: u16) -> Option<CloseFrame> {
        Some(CloseFrame {
            code: CloseCode::from(code),
            reason: "".into(),
        })
    }

    #[test]
    fn close_4004_requests_one_refresh() {
        let mut retried = false;
        assert!(matches!(closed(close(4004), &mut retried), End::Auth));
        assert!(retried);
        assert!(matches!(closed(close(4004), &mut retried), End::Fatal));
    }

    #[test]
    fn close_4014_is_fatal() {
        let mut retried = false;
        assert!(matches!(closed(close(4014), &mut retried), End::Fatal));
    }

    fn activity(app_id: &str, name: &str) -> Activity {
        Activity {
            name: name.into(),
            application_id: app_id.into(),
            kind: 2,
            details: Some(name.into()),
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
    fn presence_payload_keeps_the_whole_array() {
        let payload = presence_payload(&[activity("1", "One"), activity("2", "Two")], "online");
        let activities = payload["d"]["activities"].as_array().unwrap();
        assert_eq!(activities.len(), 2);
        assert_eq!(activities[0]["application_id"], "1");
        assert_eq!(activities[1]["application_id"], "2");
        assert_eq!(payload["d"]["status"], "online");
        assert_eq!(
            presence_payload(&[], "online")["d"]["activities"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }
}
