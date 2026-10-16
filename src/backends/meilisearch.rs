use crate::classify::Classification;
use crate::config::MeilisearchConfig;
use crate::search::SearchHit;
use crate::{InfoHash, TorrentMetadata};
use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::json;

pub struct MeilisearchMirror {
    client: Client,
    config: MeilisearchConfig,
}

impl MeilisearchMirror {
    pub fn new(config: MeilisearchConfig) -> Result<Self> {
        Ok(Self {
            client: Client::new(),
            config,
        })
    }

    pub async fn upsert_document(
        &self,
        info_hash: &InfoHash,
        metadata: &TorrentMetadata,
        classification: &Classification,
        seeders: u64,
        leechers: u64,
        enriched_at: u64,
    ) -> Result<()> {
        let url = format!(
            "{}/indexes/{}/documents",
            self.config.url.trim_end_matches('/'),
            self.config.index
        );
        let document = json!({
            "info_hash": info_hash.to_hex(),
            "name": metadata.name,
            "content_type": classification.content_type,
            "tags": classification.tags,
            "total_length": metadata.total_length,
            "seeders": seeders,
            "leechers": leechers,
            "enriched_at": enriched_at,
        });
        let mut request = self.client.post(url).json(&[document]);
        if !self.config.api_key.is_empty() {
            request = request.header("Authorization", format!("Bearer {}", self.config.api_key));
        }
        let response = request.send().await.context("meilisearch upsert")?;
        if !response.status().is_success() {
            anyhow::bail!("meilisearch upsert failed: {}", response.status());
        }
        Ok(())
    }

    pub async fn search(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<SearchHit>> {
        let url = format!(
            "{}/indexes/{}/search",
            self.config.url.trim_end_matches('/'),
            self.config.index
        );
        let body = json!({
            "q": query,
            "limit": limit,
            "offset": offset,
        });
        let mut request = self.client.post(url).json(&body);
        if !self.config.api_key.is_empty() {
            request = request.header("Authorization", format!("Bearer {}", self.config.api_key));
        }
        let response = request.send().await.context("meilisearch search")?;
        let payload: serde_json::Value = response.json().await?;
        let hits = payload
            .get("hits")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        let mut results = Vec::new();
        for hit in hits {
            results.push(SearchHit {
                info_hash: hit
                    .get("info_hash")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                name: hit
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                content_type: hit
                    .get("content_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                tags: hit
                    .get("tags")
                    .and_then(|v| v.as_array())
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default(),
                total_length: hit
                    .get("total_length")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                seeders: hit
                    .get("seeders")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                leechers: hit
                    .get("leechers")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                enriched_at: hit
                    .get("enriched_at")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                score: 1.0,
            });
        }
        Ok(results)
    }
}
