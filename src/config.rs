use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub const DEFAULT_REDIRECT_URI: &str = "http://127.0.0.1:8765/callback";
pub const DEFAULT_GATEWAY_URL: &str = "wss://gateway.discord.gg/?v=10&encoding=json";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// The Discord application that owns the OAuth token. Required; entered
    /// during `login` when it is not already configured.
    pub client_id: String,
    /// Optional. Confidential clients need this to refresh; public clients
    /// leave it empty and rely on PKCE.
    pub client_secret: String,
    pub redirect_uri: String,
    /// None takes the first free `discord-ipc-N` slot.
    pub socket_index: Option<u8>,
    /// online, idle, dnd or invisible.
    pub status: String,
    /// Overridable for tests; production uses Discord's Gateway.
    pub gateway_url: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            client_id: String::new(),
            client_secret: String::new(),
            redirect_uri: DEFAULT_REDIRECT_URI.to_owned(),
            socket_index: None,
            status: "online".to_owned(),
            gateway_url: DEFAULT_GATEWAY_URL.to_owned(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read config {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parse config {}", path.display()))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let text = toml::to_string_pretty(self)?;
        std::fs::write(path, text).with_context(|| format!("write config {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }

    pub fn gateway_status(&self) -> &'static str {
        match self.status.to_ascii_lowercase().as_str() {
            "idle" => "idle",
            "dnd" => "dnd",
            "invisible" => "invisible",
            _ => "online",
        }
    }
}

pub fn config_dir() -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home().join(".config"))
        .join("rpcbridge")
}

pub fn data_dir() -> PathBuf {
    std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home().join(".local/share"))
        .join("rpcbridge")
}

pub fn home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn config_path() -> PathBuf {
    config_dir().join("config.toml")
}

pub fn auth_path() -> PathBuf {
    data_dir().join("auth.json")
}

pub fn runtime_dir() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|dir| dir.is_dir())
        .unwrap_or_else(std::env::temp_dir)
}

pub fn socket_paths() -> Vec<PathBuf> {
    (0..10)
        .map(|index| runtime_dir().join(format!("discord-ipc-{index}")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_status_falls_back_to_online() {
        let mut config = Config::default();
        assert_eq!(config.gateway_status(), "online");
        config.status = "DND".into();
        assert_eq!(config.gateway_status(), "dnd");
        config.status = "away".into();
        assert_eq!(config.gateway_status(), "online");
    }
}
