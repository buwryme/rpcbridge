use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex as AsyncMutex, watch};
use url::Url;

const AUTHORIZE_ENDPOINT: &str = "https://discord.com/oauth2/authorize";
const TOKEN_ENDPOINT: &str = "https://discord.com/api/oauth2/token";
const CURRENT_USER_ENDPOINT: &str = "https://discord.com/api/v10/users/@me";
const SCOPES: &str = "openid identify sdk.social_layer_presence";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const LOGIN_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const REFRESH_SKEW_MS: i64 = 5 * 60 * 1000;
/// Renew a day in advance, so a suspended laptop or a long Gateway session
/// never reaches the access token's expiry.
const RENEW_BEFORE_MS: i64 = 24 * 60 * 60 * 1000;
const RENEW_CHECK: Duration = Duration::from_secs(30 * 60);
const USER_AGENT: &str = concat!("rpcbridge/", env!("CARGO_PKG_VERSION"));

#[derive(Clone, Serialize, Deserialize)]
pub struct Auth {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at_ms: i64,
    pub user_id: String,
    pub username: String,
    pub display_name: String,
}

impl Auth {
    fn is_expiring(&self) -> bool {
        self.expires_at_ms > 0 && now_ms().saturating_add(REFRESH_SKEW_MS) >= self.expires_at_ms
    }

    fn needs_renewal(&self) -> bool {
        self.expires_at_ms > 0 && now_ms().saturating_add(RENEW_BEFORE_MS) >= self.expires_at_ms
    }
}

/// Keep the stored access token fresh while the daemon runs. The Gateway also
/// refreshes before it connects and after a 4004; this covers a session that
/// stays up for longer than the token's lifetime.
pub async fn renew_loop(auth: Arc<AuthStore>, mut shutdown: watch::Receiver<bool>) {
    loop {
        if !auth.is_authorized() {
            auth.reload();
        }
        if let Some(current) = auth.current()
            && current.needs_renewal()
        {
            match auth.refresh().await {
                Ok(_) => tracing::info!("discord token renewed"),
                Err(err) => tracing::warn!(%err, "discord token renewal failed"),
            }
        }
        let sleep = tokio::time::sleep(RENEW_CHECK);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = &mut sleep => break,
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: u64,
}

#[derive(Deserialize)]
struct UserResponse {
    id: String,
    username: String,
    #[serde(default)]
    global_name: Option<String>,
}

pub struct AuthStore {
    path: PathBuf,
    client_id: String,
    client_secret: String,
    auth: Mutex<Option<Auth>>,
    http: reqwest::Client,
    refresh: AsyncMutex<()>,
}

impl AuthStore {
    pub fn load(path: PathBuf) -> Self {
        let auth = std::fs::read_to_string(&path)
            .ok()
            .and_then(|text| serde_json::from_str::<Auth>(&text).ok())
            .filter(|auth| !auth.access_token.is_empty());
        Self {
            path,
            client_id: String::new(),
            client_secret: String::new(),
            auth: Mutex::new(auth),
            http: reqwest::Client::new(),
            refresh: AsyncMutex::new(()),
        }
    }

    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = client_id.into();
        self
    }

    pub fn with_client_secret(mut self, client_secret: impl Into<String>) -> Self {
        self.client_secret = client_secret.into();
        self
    }

    /// Pick up a login written by another process while this one is running.
    pub fn reload(&self) {
        let Ok(text) = std::fs::read_to_string(&self.path) else {
            return;
        };
        let Ok(auth) = serde_json::from_str::<Auth>(&text) else {
            return;
        };
        if auth.access_token.is_empty() {
            return;
        }
        if let Ok(mut current) = self.auth.lock() {
            *current = Some(auth);
        }
    }

    pub fn is_authorized(&self) -> bool {
        self.auth
            .lock()
            .map(|auth| {
                auth.as_ref()
                    .is_some_and(|auth| !auth.access_token.is_empty())
            })
            .unwrap_or(false)
    }

    pub fn current(&self) -> Option<Auth> {
        self.auth.lock().ok().and_then(|auth| auth.clone())
    }

    pub fn store(&self, auth: Auth) -> Result<()> {
        if let Ok(mut current) = self.auth.lock() {
            *current = Some(auth.clone());
        }
        write_auth(&self.path, &auth)
    }

    pub fn clear(&self) {
        if let Ok(mut auth) = self.auth.lock() {
            *auth = None;
        }
        match std::fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => tracing::warn!(%err, "could not delete auth file"),
        }
    }

    /// Access token safe for the Gateway. Refreshes close to expiry.
    pub async fn token(&self) -> Result<String> {
        if !self.is_authorized() {
            self.reload();
        }
        let auth = self
            .current()
            .ok_or_else(|| anyhow!("not signed in; run `rpcbridge login`"))?;
        if !auth.is_expiring() {
            return Ok(auth.access_token);
        }
        self.refresh().await
    }

    /// Force a refresh. Used when Discord answers 4004 even though the
    /// access token still looked valid.
    pub async fn refresh(&self) -> Result<String> {
        let _guard = self.refresh.lock().await;
        if self.client_id.trim().is_empty() {
            bail!("no Discord application id configured; run `rpcbridge login`");
        }
        let auth = self
            .current()
            .ok_or_else(|| anyhow!("not signed in; run `rpcbridge login`"))?;
        if auth.refresh_token.trim().is_empty() {
            self.clear();
            bail!("authorization expired; run `rpcbridge login`");
        }

        let mut params = vec![
            ("client_id", self.client_id.as_str()),
            ("grant_type", "refresh_token"),
            ("refresh_token", auth.refresh_token.as_str()),
        ];
        if !self.client_secret.trim().is_empty() {
            params.push(("client_secret", self.client_secret.as_str()));
        }
        let response = self
            .http
            .post(TOKEN_ENDPOINT)
            .timeout(REQUEST_TIMEOUT)
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .form(&params)
            .send()
            .await
            .context("discord token refresh request failed")?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            // invalid_client means the client type is wrong (usually Public
            // Client is off), not that the user revoked the token. Keep the
            // access token so it still works while the setting is fixed.
            if (status == reqwest::StatusCode::BAD_REQUEST
                || status == reqwest::StatusCode::UNAUTHORIZED)
                && body.contains("invalid_grant")
            {
                self.clear();
            }
            if body.contains("invalid_client") {
                bail!(
                    "discord refresh rejected with {status}: enable OAuth2 -> Public Client in the Discord Developer Portal, or set client_secret in the config ({body})"
                );
            }
            bail!("discord refresh rejected with {status}: {body}");
        }

        let token: TokenResponse = response
            .json()
            .await
            .context("discord refresh reply was not JSON")?;
        let refresh_token = token
            .refresh_token
            .filter(|token| !token.is_empty())
            .unwrap_or(auth.refresh_token);
        let updated = Auth {
            access_token: token.access_token,
            refresh_token,
            expires_at_ms: now_ms() + (token.expires_in.max(1) as i64).saturating_mul(1000),
            user_id: auth.user_id,
            username: auth.username,
            display_name: auth.display_name,
        };
        let access_token = updated.access_token.clone();
        self.store(updated)?;
        Ok(access_token)
    }
}

fn write_auth(path: &Path, auth: &Auth) -> Result<()> {
    let bytes = serde_json::to_vec(auth)?;
    let temporary = path.with_extension("json.tmp");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    #[cfg(not(unix))]
    std::fs::write(&temporary, &bytes)?;
    std::fs::rename(&temporary, path)?;
    Ok(())
}

pub struct LoginFlow {
    state: String,
    verifier: String,
}

impl LoginFlow {
    pub fn new() -> Self {
        Self {
            state: random_hex(16),
            verifier: random_hex(32),
        }
    }

    pub fn authorization_url(&self, client_id: &str, redirect_uri: &str) -> Result<String> {
        let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(Sha256::digest(self.verifier.as_bytes()));
        let mut url = Url::parse(AUTHORIZE_ENDPOINT)?;
        url.query_pairs_mut()
            .append_pair("client_id", client_id)
            .append_pair("response_type", "code")
            .append_pair("redirect_uri", redirect_uri)
            .append_pair("scope", SCOPES)
            .append_pair("state", &self.state)
            .append_pair("code_challenge", &challenge)
            .append_pair("code_challenge_method", "S256");
        Ok(url.to_string())
    }

    pub fn state(&self) -> &str {
        &self.state
    }

    fn verifier(&self) -> &str {
        &self.verifier
    }
}

impl Default for LoginFlow {
    fn default() -> Self {
        Self::new()
    }
}

fn random_hex(bytes: usize) -> String {
    let mut buffer = vec![0u8; bytes];
    getrandom::getrandom(&mut buffer).expect("system random source");
    hex::encode(buffer)
}

pub async fn login(client_id: &str, client_secret: &str, redirect_uri: &str) -> Result<Auth> {
    if client_id.trim().is_empty() || !client_id.chars().all(|ch| ch.is_ascii_digit()) {
        bail!("Discord application id must be numeric");
    }
    let redirect = Url::parse(redirect_uri).context("invalid redirect URI")?;
    if redirect.scheme() != "http" {
        bail!("redirect URI must use http on a loopback address");
    }
    let host = redirect
        .host_str()
        .filter(|host| *host == "127.0.0.1" || *host == "localhost")
        .ok_or_else(|| anyhow!("redirect URI must point at 127.0.0.1 or localhost"))?;
    let port = redirect.port_or_known_default().unwrap_or(80);
    let listener = TcpListener::bind((host, port))
        .await
        .with_context(|| format!("bind the OAuth callback on {host}:{port}"))?;
    let path = redirect.path().to_owned();
    let flow = LoginFlow::new();
    let url = flow.authorization_url(client_id, redirect_uri)?;

    println!("Open this URL to authorize rpcbridge:\n\n{url}\n");
    if !open_browser(&url) {
        println!("Could not open a browser automatically. Open the URL above by hand.");
    }

    let (code, mut stream) = wait_for_code(listener, &flow, &path).await?;
    match exchange_code(client_id, client_secret, redirect_uri, &flow, &code).await {
        Ok(auth) => {
            reply_login_success(&mut stream).await;
            Ok(auth)
        }
        Err(err) => {
            reply_login_error(&mut stream, &format!("{err:#}")).await;
            Err(err)
        }
    }
}

async fn wait_for_code(
    listener: TcpListener,
    flow: &LoginFlow,
    path: &str,
) -> Result<(String, TcpStream)> {
    let deadline = tokio::time::Instant::now() + LOGIN_TIMEOUT;
    loop {
        let (mut stream, _) = tokio::time::timeout_at(deadline, listener.accept())
            .await
            .map_err(|_| anyhow!("timed out waiting for Discord authorization"))?
            .context("OAuth callback listener failed")?;
        let request = match tokio::time::timeout(Duration::from_secs(10), read_request(&mut stream))
            .await
        {
            Ok(Ok(request)) => request,
            Ok(Err(err)) => {
                tracing::debug!(%err, "ignoring malformed OAuth callback");
                let _ = respond(&mut stream, "400 Bad Request", &error_page("Bad request.")).await;
                continue;
            }
            Err(_) => continue,
        };
        let target = request
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or("/");
        let Ok(url) = Url::parse(&format!("http://127.0.0.1{target}")) else {
            let _ = respond(&mut stream, "400 Bad Request", &error_page("Bad request.")).await;
            continue;
        };
        if url.path() != path {
            let _ = respond(&mut stream, "404 Not Found", "Not found.").await;
            continue;
        }
        let params: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        if params.get("state").map(String::as_str) != Some(flow.state()) {
            let _ = respond(
                &mut stream,
                "400 Bad Request",
                &error_page("The sign-in response did not match this request."),
            )
            .await;
            bail!("OAuth state mismatch");
        }
        if let Some(error) = params.get("error") {
            let reason = params
                .get("error_description")
                .cloned()
                .unwrap_or_else(|| error.clone());
            let _ = respond(&mut stream, "400 Bad Request", &error_page(&reason)).await;
            bail!("Discord authorization failed: {reason}");
        }
        let Some(code) = params.get("code") else {
            let _ = respond(
                &mut stream,
                "400 Bad Request",
                &error_page("Discord did not send an authorization code."),
            )
            .await;
            bail!("missing authorization code");
        };
        return Ok((code.clone(), stream));
    }
}

async fn read_request(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut buffer = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.windows(4).any(|window| window == b"\r\n\r\n") || buffer.len() >= 8192 {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&buffer).into_owned())
}

async fn respond(stream: &mut TcpStream, status: &str, body: &str) -> std::io::Result<()> {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.flush().await
}

async fn exchange_code(
    client_id: &str,
    client_secret: &str,
    redirect_uri: &str,
    flow: &LoginFlow,
    code: &str,
) -> Result<Auth> {
    let http = reqwest::Client::new();
    let mut params = vec![
        ("client_id", client_id),
        ("grant_type", "authorization_code"),
        ("code", code),
        ("redirect_uri", redirect_uri),
        ("code_verifier", flow.verifier()),
    ];
    if !client_secret.trim().is_empty() {
        params.push(("client_secret", client_secret));
    }
    let response = http
        .post(TOKEN_ENDPOINT)
        .timeout(REQUEST_TIMEOUT)
        .header(reqwest::header::USER_AGENT, USER_AGENT)
        .form(&params)
        .send()
        .await
        .context("discord token exchange failed")?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!("discord token exchange rejected with {status}: {body}");
    }
    let token: TokenResponse = response
        .json()
        .await
        .context("discord token reply was not JSON")?;
    let (user_id, username, display_name) = fetch_user(&http, &token.access_token)
        .await
        .unwrap_or_else(|err| {
            tracing::warn!(%err, "discord account lookup failed");
            (String::new(), "Discord".to_owned(), "Discord".to_owned())
        });

    Ok(Auth {
        access_token: token.access_token,
        refresh_token: token.refresh_token.unwrap_or_default(),
        expires_at_ms: now_ms() + (token.expires_in.max(1) as i64).saturating_mul(1000),
        user_id,
        username,
        display_name,
    })
}

async fn fetch_user(
    http: &reqwest::Client,
    access_token: &str,
) -> Result<(String, String, String)> {
    let response = http
        .get(CURRENT_USER_ENDPOINT)
        .timeout(REQUEST_TIMEOUT)
        .header(reqwest::header::USER_AGENT, USER_AGENT)
        .bearer_auth(access_token)
        .send()
        .await
        .context("discord account request failed")?;
    if !response.status().is_success() {
        bail!("discord account request returned {}", response.status());
    }
    let user: UserResponse = response
        .json()
        .await
        .context("discord account reply was not JSON")?;
    let display_name = user
        .global_name
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| user.username.clone());
    Ok((user.id, user.username, display_name))
}

fn open_browser(url: &str) -> bool {
    #[cfg(target_os = "linux")]
    let mut command = Command::new("xdg-open");
    #[cfg(target_os = "macos")]
    let mut command = Command::new("open");
    #[cfg(windows)]
    let mut command = {
        let mut command = Command::new("cmd");
        command.args(["/C", "start", ""]);
        command
    };
    command
        .arg(url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .is_ok()
}

fn success_page() -> String {
    format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>rpcbridge · connected</title><style>{}</style></head><body><main><div class="mark">✓</div><h1>Discord connected</h1><p>rpcbridge can now show activities on your profile.</p><p class="muted">Close this tab and leave the daemon running.</p></main></body></html>"#,
        PAGE_STYLE
    )
}

fn error_page(reason: &str) -> String {
    format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>rpcbridge · sign-in failed</title><style>{}</style></head><body><main class="failed"><div class="mark">✕</div><h1>Discord sign-in failed</h1><p class="muted">{}</p><p>Close this tab and try again.</p></main></body></html>"#,
        PAGE_STYLE,
        escape_html(reason)
    )
}

fn escape_html(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

const PAGE_STYLE: &str = r#"
:root{color-scheme:light dark}*{box-sizing:border-box}body{margin:0;min-height:100vh;display:grid;place-items:center;background:#f6f5f4;color:#2e3436;font-family:system-ui,-apple-system,"Cantarell","Segoe UI",sans-serif}main{max-width:30rem;padding:2rem;text-align:center}.mark{width:4.5rem;height:4.5rem;margin:0 auto 1.5rem;border-radius:50%;display:grid;place-items:center;font-size:2.4rem;font-weight:700;background:#3584e4;color:#fff}.failed .mark{background:#e01b24}h1{font-size:1.65rem;margin:0 0 .75rem}p{margin:0 0 .6rem;line-height:1.5}.muted{color:#5e5c64}@media(prefers-color-scheme:dark){body{background:#1e1e1e;color:#f6f5f4}.muted{color:#b0afb5}}
"#;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

pub async fn reply_login_success(stream: &mut TcpStream) {
    let _ = respond(stream, "200 OK", &success_page()).await;
}

pub async fn reply_login_error(stream: &mut TcpStream, reason: &str) {
    let _ = respond(stream, "400 Bad Request", &error_page(reason)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_url_has_pkce_and_loopback() {
        let flow = LoginFlow::new();
        let url = Url::parse(
            &flow
                .authorization_url("123", "http://127.0.0.1:8765/callback")
                .unwrap(),
        )
        .unwrap();
        let params: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(params["client_id"], "123");
        assert_eq!(params["code_challenge_method"], "S256");
        assert_eq!(params["state"], flow.state());
        assert!(params["scope"].contains("sdk.social_layer_presence"));
    }

    #[test]
    fn renewal_window_is_a_day() {
        let mut auth = Auth {
            access_token: "a".into(),
            refresh_token: "r".into(),
            expires_at_ms: now_ms() + 2 * 24 * 60 * 60 * 1000,
            user_id: "1".into(),
            username: "user".into(),
            display_name: "User".into(),
        };
        assert!(!auth.needs_renewal());
        auth.expires_at_ms = now_ms() + 60 * 60 * 1000;
        assert!(auth.needs_renewal());
    }

    #[test]
    fn auth_store_roundtrips_and_is_private() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        let store = AuthStore::load(path.clone());
        assert!(!store.is_authorized());
        store
            .store(Auth {
                access_token: "access".into(),
                refresh_token: "refresh".into(),
                expires_at_ms: 1,
                user_id: "1".into(),
                username: "user".into(),
                display_name: "User".into(),
            })
            .unwrap();
        assert!(store.is_authorized());
        let reloaded = AuthStore::load(path.clone());
        assert_eq!(reloaded.current().unwrap().username, "user");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        reloaded.clear();
        assert!(!path.exists());
    }
}
