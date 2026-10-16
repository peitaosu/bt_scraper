use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub dht: DhtConfig,
    pub enrichment: EnrichmentConfig,
    pub tracker: TrackerConfig,
    pub store: StoreConfig,
    pub index: IndexConfig,
    pub search: SearchConfig,
    pub metrics: MetricsConfig,
    pub classification: ClassificationSettings,
    pub postgres: PostgresConfig,
    pub meilisearch: MeilisearchConfig,
    pub api: ApiConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DhtConfig {
    pub bind: String,
    pub bootstrap: Vec<String>,
    pub bep51_interval_secs: u64,
    pub bep51_batch: usize,
    pub bep51_concurrency: usize,
    pub find_node_burst_interval_secs: u64,
    pub find_node_burst_size: usize,
    pub routing_table_path: String,
    pub routing_save_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EnrichmentConfig {
    pub concurrency: usize,
    pub queue_capacity: usize,
    pub peers_per_hash: usize,
    pub peer_timeout_secs: u64,
    pub retry_base_secs: u64,
    pub retry_max_secs: u64,
    pub retry_max_attempts: u32,
    pub retry_poll_interval_secs: u64,
    pub scheduler_batch: usize,
    pub failed_retry_after_secs: u64,
    pub discovery_batch_size: usize,
    pub discovery_flush_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TrackerConfig {
    pub peers: Vec<String>,
    pub scrape_urls: Vec<String>,
    pub scrape_interval_secs: u64,
    pub scrape_batch: usize,
    pub scrape_timeout_secs: u64,
    pub scrape_stale_secs: u64,
}

impl TrackerConfig {
    pub fn peer_urls(&self) -> &[String] {
        &self.peers
    }

    pub fn scrape_urls(&self) -> Vec<String> {
        if self.scrape_urls.is_empty() {
            return self.peers.clone();
        }
        self.scrape_urls.clone()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StoreConfig {
    pub path: String,
    pub peer_ttl_secs: u64,
    pub write_batch_size: usize,
    pub write_flush_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndexConfig {
    pub path: String,
    pub batch_size: usize,
    pub poll_interval_secs: u64,
    pub commit_batch: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchConfig {
    pub default_limit: usize,
    pub max_limit: usize,
    pub default_sort: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClassificationSettings {
    pub config_path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    pub enabled: bool,
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MeilisearchConfig {
    pub enabled: bool,
    pub url: String,
    pub api_key: String,
    pub index: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    pub bind: String,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)?;
        Ok(toml::from_str(&content)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_default_config() {
        let config = Config::load("config/default.toml").unwrap();
        assert_eq!(config.dht.bind, "0.0.0.0:6881");
        assert_eq!(config.enrichment.peers_per_hash, 8);
        assert!(!config.tracker.peers.is_empty());
        assert_eq!(config.index.path, "data/search");
        assert_eq!(config.classification.config_path, "config/classification.toml");
        assert!(config.dht.bep51_concurrency >= 1);
        assert!(config.enrichment.discovery_batch_size >= 1);
        assert!(config.store.write_batch_size >= 1);
    }

    #[test]
    fn loads_test_config() {
        let config = Config::load("config/test.toml").unwrap();
        assert_eq!(config.api.bind, "127.0.0.1:18080");
        assert_eq!(config.dht.bep51_interval_secs, 2);
    }
}
