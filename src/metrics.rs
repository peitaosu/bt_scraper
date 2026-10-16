use metrics_exporter_prometheus::PrometheusBuilder;

pub fn init_metrics(bind: &str) -> anyhow::Result<metrics_exporter_prometheus::PrometheusHandle> {
    let handle = PrometheusBuilder::new().install_recorder()?;
    let _ = bind;
    Ok(handle)
}

pub fn register_metrics() {
    metrics::describe_counter!(
        "hashes_discovered_total",
        "Infohashes inserted into the store"
    );
    metrics::describe_counter!(
        "discovery_dropped_total",
        "Inbound discovery events dropped when the channel is full"
    );
    metrics::describe_counter!(
        "metadata_success_total",
        "Torrent metadata fetches that succeeded"
    );
    metrics::describe_counter!(
        "bep51_samples_total",
        "Infohashes received from BEP51 sample responses"
    );
    metrics::describe_counter!(
        "enrichment_no_peers_total",
        "Enrichment attempts skipped due to no peers"
    );
    metrics::describe_counter!(
        "metadata_blocked_total",
        "Torrent metadata rejected at ingest due to blocklist"
    );
    metrics::describe_counter!(
        "metadata_failure_total",
        "Torrent metadata fetches that failed"
    );
    metrics::describe_counter!("index_success_total", "Torrents indexed into Tantivy");
    metrics::describe_counter!("index_failure_total", "Torrent index operations that failed");
    metrics::describe_counter!(
        "index_skipped_blocked_total",
        "Torrents skipped from index due to blocklist"
    );
    metrics::describe_counter!("scrape_batch_total", "Tracker scrape batches completed");
    metrics::describe_counter!(
        "scrape_tracker_success_total",
        "Successful tracker scrape requests"
    );
    metrics::describe_counter!(
        "scrape_tracker_failure_total",
        "Failed tracker scrape requests"
    );
    metrics::describe_gauge!("retry_queue_depth", "Hashes waiting for enrichment retry");
    metrics::describe_gauge!("pipeline_enriched", "Enriched torrent count");
    metrics::describe_gauge!("pipeline_pending", "Pending torrent count");
    metrics::describe_gauge!("pipeline_indexed", "Indexed torrent count");
    metrics::describe_histogram!(
        "metadata_latency_seconds",
        "Metadata fetch latency in seconds"
    );
}
