use std::io;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;

use crate::activity::Activity;
use crate::apps::AppRegistry;
use crate::assets::AssetResolver;
use crate::auth::Auth;
use crate::config;
use crate::state::PresenceState;

const HEADER_BYTES: usize = 8;
const MAX_FRAME_BYTES: usize = 64 * 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const OP_HANDSHAKE: u32 = 0;
pub(crate) const OP_FRAME: u32 = 1;
const OP_CLOSE: u32 = 2;
const OP_PING: u32 = 3;
const OP_PONG: u32 = 4;

#[derive(Deserialize)]
struct Handshake {
    v: u32,
    client_id: String,
}

#[derive(Deserialize)]
struct Command {
    cmd: String,
    #[serde(default)]
    nonce: Option<Value>,
    #[serde(default)]
    args: Option<Value>,
}

struct SocketFile {
    path: PathBuf,
}

impl Drop for SocketFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

struct ClientGuard {
    state: Arc<PresenceState>,
    client_id: u64,
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.state.remove(self.client_id);
    }
}

fn bind(preferred: Option<u8>) -> Result<(UnixListener, SocketFile)> {
    let directory = config::runtime_dir();
    let indices: Vec<u8> = preferred
        .map(|index| vec![index])
        .unwrap_or_else(|| (0..10).collect());
    let mut last = None;
    for index in indices {
        let path = directory.join(format!("discord-ipc-{index}"));
        match bind_one(&path) {
            Ok(listener) => {
                tracing::info!(path = %path.display(), "ipc listening");
                return Ok((listener, SocketFile { path }));
            }
            Err(err) if err.kind() == io::ErrorKind::AddrInUse => {
                tracing::debug!(path = %path.display(), "ipc slot busy");
                last = Some(err);
            }
            Err(err) => {
                return Err(err).with_context(|| format!("bind {}", path.display()));
            }
        }
    }
    match last {
        Some(err) => Err(err).context("no free discord-ipc slot"),
        None => bail!("no free discord-ipc slot in {}", directory.display()),
    }
}

fn bind_one(path: &std::path::Path) -> io::Result<UnixListener> {
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if !metadata.file_type().is_socket() {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "path exists and is not a socket",
            ));
        }
        // A datagram connect refuses a stale stream socket without entering
        // a live listener's accept queue.
        use std::os::unix::net::UnixDatagram;
        match UnixDatagram::unbound().and_then(|probe| probe.connect(path)) {
            Ok(()) => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "another process is listening",
                ));
            }
            Err(err) if err.kind() == io::ErrorKind::ConnectionRefused => {
                std::fs::remove_file(path)?;
            }
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "another process is listening",
                ));
            }
        }
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

pub async fn serve(
    state: Arc<PresenceState>,
    apps: Arc<AppRegistry>,
    assets: Arc<AssetResolver>,
    preferred: Option<u8>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let (listener, _socket) = bind(preferred)?;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted.context("ipc accept")?;
                if !same_user(&stream) {
                    tracing::warn!("refused an ipc client from another user");
                    continue;
                }
                let (state, apps, assets) = (state.clone(), apps.clone(), assets.clone());
                tokio::spawn(async move {
                    if let Err(err) = session(stream, state, apps, assets).await {
                        tracing::debug!(%err, "ipc client ended");
                    }
                });
            }
        }
    }
    Ok(())
}

fn same_user(stream: &UnixStream) -> bool {
    stream
        .peer_cred()
        .map(|credentials| credentials.uid() == unsafe { libc::geteuid() })
        .unwrap_or(false)
}

async fn session(
    mut stream: UnixStream,
    state: Arc<PresenceState>,
    apps: Arc<AppRegistry>,
    assets: Arc<AssetResolver>,
) -> Result<()> {
    let (opcode, body) = tokio::time::timeout(HANDSHAKE_TIMEOUT, read_frame(&mut stream))
        .await
        .context("ipc handshake timed out")??;
    if opcode != OP_HANDSHAKE {
        bail!("expected handshake, got opcode {opcode}");
    }
    let handshake: Handshake = serde_json::from_slice(&body).context("invalid handshake")?;
    if handshake.v != 1 {
        bail!("unsupported rpc version {}", handshake.v);
    }
    let app_id = handshake.client_id.trim().to_owned();
    if app_id.is_empty() || !app_id.chars().all(|ch| ch.is_ascii_digit()) {
        bail!("invalid client id");
    }
    state.auth().reload();
    let auth = state
        .auth()
        .current()
        .ok_or_else(|| anyhow::anyhow!("no authenticated account; run `rpcbridge login`"))?;

    write_json(&mut stream, OP_FRAME, &ready(&auth)).await?;
    let client_id = state.next_client_id();
    let _guard = ClientGuard {
        state: state.clone(),
        client_id,
    };
    tracing::info!(client_id, app_id, "ipc client connected");

    loop {
        let (opcode, body) = match read_frame(&mut stream).await {
            Ok(frame) => frame,
            Err(err) if is_disconnect(&err) => return Ok(()),
            Err(err) => return Err(err).context("ipc read"),
        };
        match opcode {
            OP_FRAME => {
                if let Err(err) = handle_command(
                    &mut stream,
                    &body,
                    &app_id,
                    client_id,
                    &state,
                    &apps,
                    &assets,
                )
                .await
                {
                    tracing::warn!(%err, "ipc command failed");
                }
            }
            OP_CLOSE => return Ok(()),
            OP_PING => write_frame(&mut stream, OP_PONG, &body).await?,
            _ => {}
        }
    }
}

async fn handle_command(
    stream: &mut UnixStream,
    body: &[u8],
    app_id: &str,
    client_id: u64,
    state: &Arc<PresenceState>,
    apps: &AppRegistry,
    assets: &AssetResolver,
) -> Result<()> {
    let command: Command = serde_json::from_slice(body).context("invalid command JSON")?;
    match command.cmd.as_str() {
        "SET_ACTIVITY" => {
            let activity_value = command
                .args
                .as_ref()
                .and_then(|args| args.get("activity"))
                .cloned();
            if matches!(activity_value.as_ref(), Some(Value::Object(_))) {
                let value = activity_value.clone().unwrap_or(Value::Null);
                let app_name = apps.name(app_id).await.unwrap_or_default();
                let Some(mut activity) = Activity::from_ipc(&value, app_id, &app_name) else {
                    reply_error(stream, &command.cmd, command.nonce, "Invalid activity").await?;
                    return Ok(());
                };
                if let Err(err) = assets.resolve(&mut activity).await {
                    tracing::debug!(%err, "asset resolution failed");
                }
                state.update(client_id, app_id.to_owned(), Some(activity));
            } else {
                state.update(client_id, app_id.to_owned(), None);
            }
            let ack = json!({
                "cmd": "SET_ACTIVITY",
                "evt": Value::Null,
                "data": activity_value.unwrap_or(Value::Null),
                "nonce": command.nonce.unwrap_or(Value::Null),
            });
            write_json(stream, OP_FRAME, &ack).await?;
        }
        "SUBSCRIBE" | "UNSUBSCRIBE" => {
            let ack = json!({
                "cmd": command.cmd,
                "evt": Value::Null,
                "data": {},
                "nonce": command.nonce.unwrap_or(Value::Null),
            });
            write_json(stream, OP_FRAME, &ack).await?;
        }
        "GET_SELECTED_VOICE_CHANNEL" => {
            let ack = json!({
                "cmd": command.cmd,
                "evt": Value::Null,
                "data": Value::Null,
                "nonce": command.nonce.unwrap_or(Value::Null),
            });
            write_json(stream, OP_FRAME, &ack).await?;
        }
        other => {
            reply_error(stream, other, command.nonce, "Unsupported command").await?;
        }
    }
    Ok(())
}

async fn reply_error(
    stream: &mut UnixStream,
    cmd: &str,
    nonce: Option<Value>,
    message: &str,
) -> Result<()> {
    let payload = json!({
        "cmd": cmd,
        "evt": "ERROR",
        "data": {"code": 4002, "message": message},
        "nonce": nonce.unwrap_or(Value::Null),
    });
    write_json(stream, OP_FRAME, &payload).await?;
    Ok(())
}

fn ready(auth: &Auth) -> Value {
    json!({
        "cmd": "DISPATCH",
        "evt": "READY",
        "data": {
            "v": 1,
            "config": {
                "cdn_host": "cdn.discordapp.com",
                "api_endpoint": "//discord.com/api",
                "environment": "production",
            },
            "user": {
                "id": auth.user_id,
                "username": auth.username,
                "discriminator": "0",
                "global_name": auth.display_name,
                "avatar": Value::Null,
            },
        }
    })
}

pub(crate) async fn read_frame(stream: &mut UnixStream) -> io::Result<(u32, Vec<u8>)> {
    let mut header = [0u8; HEADER_BYTES];
    stream.read_exact(&mut header).await?;
    let opcode = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let length = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "oversized rpc frame",
        ));
    }
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).await?;
    Ok((opcode, body))
}

pub(crate) async fn write_json(
    stream: &mut UnixStream,
    opcode: u32,
    value: &Value,
) -> io::Result<()> {
    let body = serde_json::to_vec(value).map_err(io::Error::other)?;
    write_frame(stream, opcode, &body).await
}

async fn write_frame(stream: &mut UnixStream, opcode: u32, body: &[u8]) -> io::Result<()> {
    let mut header = [0u8; HEADER_BYTES];
    header[..4].copy_from_slice(&opcode.to_le_bytes());
    header[4..].copy_from_slice(&(body.len() as u32).to_le_bytes());
    stream.write_all(&header).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

fn is_disconnect(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixStream;

    #[tokio::test]
    async fn frames_roundtrip() {
        let (mut left, mut right) = UnixStream::pair().unwrap();
        let payload = json!({"cmd": "SET_ACTIVITY"});
        write_json(&mut left, OP_FRAME, &payload).await.unwrap();
        let (opcode, body) = read_frame(&mut right).await.unwrap();
        assert_eq!(opcode, OP_FRAME);
        assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), payload);
    }

    #[tokio::test]
    async fn oversized_frame_is_refused() {
        let (mut left, mut right) = UnixStream::pair().unwrap();
        let mut header = [0u8; HEADER_BYTES];
        header[..4].copy_from_slice(&OP_FRAME.to_le_bytes());
        header[4..].copy_from_slice(&((MAX_FRAME_BYTES + 1) as u32).to_le_bytes());
        left.write_all(&header).await.unwrap();
        assert!(read_frame(&mut right).await.is_err());
    }

    #[test]
    fn ready_contains_the_authenticated_user() {
        let auth = Auth {
            access_token: "x".into(),
            refresh_token: "y".into(),
            expires_at_ms: 0,
            user_id: "1".into(),
            username: "user".into(),
            display_name: "User".into(),
        };
        let value = ready(&auth);
        assert_eq!(value["data"]["user"]["id"], "1");
        assert_eq!(value["data"]["user"]["global_name"], "User");
    }
}
