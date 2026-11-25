use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;

use crate::activity::Activity;
use crate::auth::AuthStore;

const ASSET_CACHE_LIMIT: usize = 512;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const USER_AGENT: &str = concat!("rpcbridge/", env!("CARGO_PKG_VERSION"));

#[derive(Deserialize)]
struct ExternalAsset {
    external_asset_path: String,
}

pub struct AssetResolver {
    http: Client,
    proxy_app_id: String,
    auth: Arc<AuthStore>,
    cache: Mutex<HashMap<String, Option<String>>>,
}

impl AssetResolver {
    pub fn new(proxy_app_id: impl Into<String>, auth: Arc<AuthStore>) -> Self {
        Self {
            http: Client::new(),
            proxy_app_id: proxy_app_id.into(),
            auth,
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub async fn resolve(&self, activity: &mut Activity) -> Result<()> {
        let Some(assets) = activity.assets.as_mut() else {
            return Ok(());
        };
        if let Some(image) = assets.large_image.clone() {
            match self.resolve_one(&image).await {
                Some(path) => assets.large_image = Some(path),
                None => {
                    assets.large_image = None;
                    assets.large_text = None;
                }
            }
        }
        if let Some(image) = assets.small_image.clone() {
            match self.resolve_one(&image).await {
                Some(path) => assets.small_image = Some(path),
                None => {
                    assets.small_image = None;
                    assets.small_text = None;
                }
            }
        }
        if assets.large_image.is_none() && assets.small_image.is_none() {
            activity.assets = None;
        }
        Ok(())
    }

    async fn resolve_one(&self, image: &str) -> Option<String> {
        if !is_external_url(image) {
            return Some(image.to_owned());
        }
        if let Ok(cache) = self.cache.lock() {
            if let Some(cached) = cache.get(image) {
                return cached.clone();
            }
        }
        let resolved = self.register(image).await;
        if let Ok(mut cache) = self.cache.lock() {
            if cache.len() >= ASSET_CACHE_LIMIT {
                cache.clear();
            }
            cache.insert(image.to_owned(), resolved.clone());
        }
        resolved
    }

    async fn register(&self, image: &str) -> Option<String> {
        let token = self.auth.token().await.ok()?;
        let url = format!(
            "https://discord.com/api/v10/applications/{}/external-assets",
            self.proxy_app_id
        );
        let response = self
            .http
            .post(url)
            .timeout(REQUEST_TIMEOUT)
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .bearer_auth(token)
            .json(&serde_json::json!({"urls": [image]}))
            .send()
            .await
            .context("external-assets request")
            .ok()?;
        if !response.status().is_success() {
            tracing::debug!(status = %response.status(), image, "external asset refused");
            return None;
        }
        let assets: Vec<ExternalAsset> = response.json().await.ok()?;
        let path = assets.first()?.external_asset_path.trim();
        (!path.is_empty()).then(|| format!("mp:{path}"))
    }
}

fn is_external_url(image: &str) -> bool {
    image.starts_with("https://") || image.starts_with("http://")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_http_images_are_registered() {
        assert!(is_external_url("https://x/y.png"));
        assert!(is_external_url("http://x/y.png"));
        assert!(!is_external_url("mp:external/x.png"));
        assert!(!is_external_url("pause"));
    }
}
