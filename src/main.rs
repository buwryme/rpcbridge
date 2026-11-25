mod activity;
mod apps;
mod assets;
mod auth;
mod config;
mod gateway;
mod ipc;
mod state;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use tokio::net::UnixStream;

use crate::apps::AppRegistry;
use crate::assets::AssetResolver;
use crate::auth::AuthStore;
use crate::config::Config;
use crate::state::PresenceState;

#[derive(Parser)]
#[command(
    name = "rpcbridge",
    version,
    about = "Bridge Discord RPC IPC clients to OAuth2 Social SDK presence"
)]
struct Cli {
    #[arg(long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Authorize rpcbridge with Discord and store the tokens.
    Login {
        /// Discord application id. Prompted for when omitted.
        #[arg(long)]
        client_id: Option<String>,
    },
    /// Remove the stored authorization.
    Logout,
    /// Run the IPC listener and Gateway session.
    Run,
    /// Show the account, config and live IPC sockets.
    Status,
    /// Connect to a running proxy as a test RPC client.
    Probe {
        /// Application id the probe pretends to be. Defaults to client_id.
        #[arg(long)]
        app_id: Option<String>,
        #[arg(long, default_value = "rpcbridge probe")]
        details: String,
        #[arg(long, default_value_t = 15)]
        seconds: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config_path = cli.config.unwrap_or_else(config::config_path);
    let config = Config::load(&config_path)?;
    match cli.command {
        Command::Login { client_id } => login(&config, &config_path, client_id).await,
        Command::Logout => {
            let store = auth_store(&config);
            store.clear();
            println!("signed out");
            println!("restart the daemon if it is running: systemctl --user restart rpcbridge");
            Ok(())
        }
        Command::Run => run(config).await,
        Command::Status => status(&config, &config_path),
        Command::Probe {
            app_id,
            details,
            seconds,
        } => probe(&config, app_id.as_deref(), &details, seconds).await,
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "rpcbridge=info".into());
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

fn auth_store(config: &Config) -> Arc<AuthStore> {
    Arc::new(
        AuthStore::load(config::auth_path())
            .with_client_id(config.client_id.clone())
            .with_client_secret(config.client_secret.clone()),
    )
}

fn prompt_client_id() -> Result<String> {
    use std::io::{self, Write};
    print!("Discord application id: ");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    validate_client_id(input.trim())
}

fn validate_client_id(id: &str) -> Result<String> {
    let id = id.trim();
    if id.is_empty() || !id.chars().all(|ch| ch.is_ascii_digit()) {
        bail!("Discord application id must be numeric");
    }
    Ok(id.to_owned())
}

async fn login(config: &Config, config_path: &Path, client_id: Option<String>) -> Result<()> {
    init_tracing();
    let client_id = match client_id {
        Some(id) => id,
        None if !config.client_id.trim().is_empty() => config.client_id.clone(),
        None => prompt_client_id()?,
    };
    let client_id = validate_client_id(&client_id)?;
    let mut config = config.clone();
    config.client_id = client_id.clone();
    config
        .save(config_path)
        .with_context(|| format!("save config {}", config_path.display()))?;
    println!(
        "application id {client_id} saved to {}",
        config_path.display()
    );
    let auth = auth::login(&client_id, &config.client_secret, &config.redirect_uri).await?;
    let store = auth_store(&config);
    store.store(auth.clone())?;
    println!(
        "signed in as {} ({})",
        auth.display_name.trim(),
        auth.username
    );
    println!("auth saved to {}", config::auth_path().display());
    println!("restart the daemon if it is running: systemctl --user restart rpcbridge");
    Ok(())
}

async fn run(config: Config) -> Result<()> {
    init_tracing();
    if config.client_id.trim().is_empty() {
        bail!("no Discord application id configured; run `rpcbridge login`");
    }
    let auth = auth_store(&config);
    if !auth.is_authorized() {
        bail!("not signed in; run `rpcbridge login` first");
    }
    let apps = Arc::new(AppRegistry::new());
    let assets = Arc::new(AssetResolver::new(config.client_id.clone(), auth.clone()));
    let (state, activity_rx) = PresenceState::new(auth.clone());
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    tokio::spawn(auth::renew_loop(auth.clone(), shutdown_rx.clone()));
    let gateway = tokio::spawn(gateway::run(
        auth,
        config.gateway_url.clone(),
        config.gateway_status(),
        activity_rx,
        shutdown_rx.clone(),
    ));
    let ipc = tokio::spawn(ipc::serve(
        state.clone(),
        apps,
        assets,
        config.socket_index,
        shutdown_rx,
    ));

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        result = ipc => {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => tracing::error!(%err, "ipc listener stopped"),
                Err(err) => tracing::error!(%err, "ipc task failed"),
            }
        }
        _ = gateway => {
            tracing::error!("gateway task stopped");
        }
    }

    let _ = shutdown_tx.send(true);
    state.clear();
    tokio::time::sleep(Duration::from_millis(400)).await;
    Ok(())
}

fn status(config: &Config, config_path: &Path) -> Result<()> {
    init_tracing();
    let store = auth_store(config);
    println!("config: {}", config_path.display());
    println!("auth:   {}", config::auth_path().display());
    if config.client_id.trim().is_empty() {
        println!("application id: not configured");
    } else {
        println!("application id: {}", config.client_id);
    }
    match store.current() {
        Some(auth) => {
            let days = (auth.expires_at_ms - auth::now_ms()) as f64 / 86_400_000.0;
            println!(
                "account: {} (@{}) id {}",
                auth.display_name.trim(),
                auth.username,
                auth.user_id
            );
            println!("token expires in {days:.1} days");
        }
        None => println!("account: not signed in"),
    }
    println!("gateway: {}", config.gateway_url);
    println!("status:  {}", config.gateway_status());
    let mut any = false;
    for (index, path) in config::socket_paths().iter().enumerate() {
        if socket_is_live(path) {
            any = true;
            println!("socket:  {} (live, slot {index})", path.display());
        }
    }
    if !any {
        println!("socket:  no live discord-ipc slot");
    }
    Ok(())
}

fn socket_is_live(path: &Path) -> bool {
    use std::os::unix::net::UnixDatagram;
    if !path.exists() {
        return false;
    }
    match UnixDatagram::unbound().and_then(|probe| probe.connect(path)) {
        Ok(()) => true,
        Err(err) => err.kind() != std::io::ErrorKind::ConnectionRefused,
    }
}

async fn probe(config: &Config, app_id: Option<&str>, details: &str, seconds: u64) -> Result<()> {
    init_tracing();
    let app_id = match app_id {
        Some(id) => id.to_owned(),
        None if !config.client_id.trim().is_empty() => config.client_id.clone(),
        None => bail!("no application id; pass --app-id or configure client_id"),
    };
    let app_id = validate_client_id(&app_id)?;
    let path = config::socket_paths()
        .into_iter()
        .find(|path| socket_is_live(path))
        .ok_or_else(|| anyhow::anyhow!("no live discord-ipc socket; start `rpcbridge run`"))?;
    println!("probing {}", path.display());
    let mut stream = UnixStream::connect(&path)
        .await
        .with_context(|| format!("connect {}", path.display()))?;
    write_handshake(&mut stream, &app_id).await?;
    read_until_ready(&mut stream).await?;

    let activity = json!({
        "type": 2,
        "details": details,
        "state": "rpcbridge probe",
    });
    let set = json!({
        "cmd": "SET_ACTIVITY",
        "args": {"pid": std::process::id(), "activity": activity},
        "nonce": "probe-set",
    });
    ipc::write_json(&mut stream, ipc::OP_FRAME, &set).await?;
    read_until_nonce(&mut stream, "probe-set").await?;
    println!("activity sent; keeping it for {seconds}s");
    tokio::time::sleep(Duration::from_secs(seconds)).await;

    let clear = json!({
        "cmd": "SET_ACTIVITY",
        "args": {"pid": std::process::id(), "activity": Value::Null},
        "nonce": "probe-clear",
    });
    ipc::write_json(&mut stream, ipc::OP_FRAME, &clear).await?;
    read_until_nonce(&mut stream, "probe-clear").await?;
    println!("activity cleared");
    Ok(())
}

async fn write_handshake(stream: &mut UnixStream, app_id: &str) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let body = serde_json::to_vec(&json!({"v": 1, "client_id": app_id}))?;
    let mut header = [0u8; 8];
    header[..4].copy_from_slice(&0u32.to_le_bytes());
    header[4..].copy_from_slice(&(body.len() as u32).to_le_bytes());
    stream.write_all(&header).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_until_ready(stream: &mut UnixStream) -> Result<()> {
    for _ in 0..8 {
        let (opcode, body) = ipc::read_frame(stream).await?;
        let value: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        if opcode == ipc::OP_FRAME && value.get("evt").and_then(Value::as_str) == Some("READY") {
            println!(
                "ready: {}",
                value
                    .pointer("/data/user/username")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
            );
            return Ok(());
        }
    }
    bail!("proxy did not send READY")
}

async fn read_until_nonce(stream: &mut UnixStream, nonce: &str) -> Result<()> {
    for _ in 0..8 {
        let (opcode, body) = ipc::read_frame(stream).await?;
        let value: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        if opcode == ipc::OP_FRAME && value.get("nonce").and_then(Value::as_str) == Some(nonce) {
            if value.get("evt").and_then(Value::as_str) == Some("ERROR") {
                bail!("proxy rejected {nonce}: {value}");
            }
            return Ok(());
        }
    }
    bail!("proxy did not acknowledge {nonce}")
}
