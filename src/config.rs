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
    #[serde(default = "default_bep51_sample_timeout_secs")]
    pub bep51_sample_timeout_secs: u64,
    #[serde(default = "default_udp_pool_size")]
    pub udp_pool_size: usize,
    pub find_node_burst_interval_secs: u64,
    pub find_node_burst_size: usize,
    pub routing_table_path: String,
    pub routing_save_interval_secs: u64,
    #[serde(default = "default_dht_lookup_timeout_secs")]
    pub lookup_timeout_secs: u64,
    #[serde(default)]
    pub discovery_queue_capacity: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EnrichmentConfig {
    pub concurrency: usize,
    pub queue_capacity: usize,
    #[serde(default)]
    pub enrich_queue_capacity: Option<usize>,
    pub peers_per_hash: usize,
    pub peer_timeout_secs: u64,
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    pub retry_base_secs: u64,
    pub retry_max_secs: u64,
    pub retry_max_attempts: u32,
    pub retry_poll_interval_secs: u64,
    pub scheduler_batch: usize,
    #[serde(default = "default_scheduler_lookup_concurrency")]
    pub scheduler_lookup_concurrency: usize,
    #[serde(default = "default_scheduler_batch_max")]
    pub scheduler_batch_max: usize,
    pub failed_retry_after_secs: u64,
    pub discovery_batch_size: usize,
    pub discovery_flush_ms: u64,
    #[serde(default = "default_true")]
    pub skip_dht_if_cached_peers: bool,
    #[serde(default = "default_tracker_urls_per_lookup")]
    pub tracker_urls_per_lookup: usize,
    #[serde(default = "default_peer_fail_threshold")]
    pub peer_fail_threshold: u32,
    #[serde(default = "default_global_tcp_limit")]
    pub global_tcp_limit: usize,
    #[serde(default = "default_stats_interval_secs")]
    pub stats_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TrackerConfig {
    pub peers: Vec<String>,
    pub scrape_urls: Vec<String>,
    pub scrape_interval_secs: u64,
    pub scrape_batch: usize,
    pub scrape_timeout_secs: u64,
    pub scrape_stale_secs: u64,
    #[serde(default = "default_http_timeout_secs")]
    pub http_timeout_secs: u64,
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

impl Config {
    pub fn discovery_channel_capacity(&self) -> usize {
        self.dht
            .discovery_queue_capacity
            .unwrap_or(self.enrichment.queue_capacity)
    }

    pub fn enrich_channel_capacity(&self) -> usize {
        self.enrichment
            .enrich_queue_capacity
            .unwrap_or(self.enrichment.queue_capacity)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StoreConfig {
    pub path: String,
    pub peer_ttl_secs: u64,
    pub write_batch_size: usize,
    pub write_flush_ms: u64,
    #[serde(default = "default_read_pool_size")]
    pub read_pool_size: u32,
    #[serde(default = "default_sqlite_cache_kb")]
    pub sqlite_cache_size_kb: i32,
    #[serde(default = "default_sqlite_mmap_mb")]
    pub sqlite_mmap_size_mb: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndexConfig {
    pub path: String,
    pub batch_size: usize,
    pub poll_interval_secs: u64,
    pub commit_batch: usize,
    #[serde(default = "default_index_writer_heap_mb")]
    pub writer_heap_mb: usize,
    #[serde(default = "default_true")]
    pub reload_reader_on_commit: bool,
    #[serde(default = "default_true")]
    pub patch_swarm_on_scrape: bool,
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
    #[serde(default = "default_search_cache_secs")]
    pub search_cache_secs: u64,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)?;
        Ok(toml::from_str(&content)?)
    }
}

fn default_true() -> bool {
    true
}

fn default_bep51_sample_timeout_secs() -> u64 {
    8
}

fn default_udp_pool_size() -> usize {
    32
}

fn default_dht_lookup_timeout_secs() -> u64 {
    5
}

fn default_connect_timeout_secs() -> u64 {
    8
}

fn default_scheduler_lookup_concurrency() -> usize {
    16
}

fn default_scheduler_batch_max() -> usize {
    128
}

fn default_tracker_urls_per_lookup() -> usize {
    2
}

fn default_peer_fail_threshold() -> u32 {
    3
}

fn default_global_tcp_limit() -> usize {
    512
}

fn default_stats_interval_secs() -> u64 {
    30
}

fn default_read_pool_size() -> u32 {
    4
}

fn default_sqlite_cache_kb() -> i32 {
    -64000
}

fn default_sqlite_mmap_mb() -> i64 {
    256
}

fn default_index_writer_heap_mb() -> usize {
    50
}

fn default_http_timeout_secs() -> u64 {
    15
}

fn default_search_cache_secs() -> u64 {
    0
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
        assert!(config.dht.lookup_timeout_secs >= 1);
        assert!(config.enrichment.skip_dht_if_cached_peers);
    }

    #[test]
    fn loads_test_config() {
        let config = Config::load("config/test.toml").unwrap();
        assert_eq!(config.api.bind, "127.0.0.1:18080");
        assert_eq!(config.dht.bep51_interval_secs, 2);
    }

    #[test]
    fn loads_b550_config() {
        let config = Config::load("config/b550.toml").unwrap();
        assert_eq!(config.api.bind, "0.0.0.0:18080");
        assert!(config.store.read_pool_size >= 1);
        assert!(config.enrichment.scheduler_lookup_concurrency >= 1);
    }
}
