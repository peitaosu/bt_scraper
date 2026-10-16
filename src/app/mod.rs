use anyhow::Result;
use crate::api::AppState;
use crate::classification_config::ClassificationRules;
use crate::config::Config;
use crate::dht::{
    run_bep51_sampler, run_find_node_burst, run_routing_table_persistence, DhtEngine,
};
use crate::enrich::{
    run_discovery_batcher, run_enrichment_pool, run_enrichment_scheduler,
    run_failed_requeue_loop, run_peer_prune_loop, DiscoverySettings, EnrichSettings,
};
use crate::workers::run_indexer_loop;
use crate::metrics::{init_metrics, register_metrics};
use crate::ports::DhtPeerLookup;
use crate::routing::load_routing_table;
use crate::workers::run_tracker_scrape_loop;
use crate::search::SearchIndex;
use crate::store::Store;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub async fn run(config: Config) -> Result<()> {
    register_metrics();
    let metrics_handle = if config.metrics.enabled {
        Some(init_metrics("")?)
    } else {
        None
    };

    let shutdown = CancellationToken::new();
    let worker_cancel = shutdown.child_token();

    let rules = ClassificationRules::load(&config.classification.config_path)?;
    let backends = Arc::new(crate::backends::BackendContext::from_config(&config).await?);
    info!("opening store");
    let store = Store::open(
        &config.store.path,
        config.store.write_batch_size,
        config.store.write_flush_ms,
    )?;
    info!("opening search index");
    let search = Arc::new(SearchIndex::open(&config.index.path)?);
    let (discovery_tx, discovery_rx) = mpsc::channel(config.enrichment.queue_capacity);
    let (enrich_tx, enrich_rx) = mpsc::channel(config.enrichment.queue_capacity);

    let persisted = load_routing_table(&config.dht.routing_table_path);
    info!(bind = %config.dht.bind, "starting dht");
    let dht = DhtEngine::start(
        &config.dht.bind,
        &config.dht.bootstrap,
        &persisted,
        discovery_tx.clone(),
    )
    .await?;

    let cancel = worker_cancel.clone();
    let dht_bootstrap = dht.clone();
    let async_dht = dht.async_dht();
    let bep51_interval_secs = config.dht.bep51_interval_secs;
    let bep51_batch = config.dht.bep51_batch;
    let bep51_concurrency = config.dht.bep51_concurrency;
    tokio::spawn(async move {
        dht_bootstrap.wait_bootstrapped().await;
        run_bep51_sampler(
            async_dht,
            discovery_tx,
            dht_bootstrap.sample_store(),
            bep51_interval_secs,
            bep51_batch,
            bep51_concurrency,
            cancel,
        )
        .await;
    });

    tokio::spawn(run_find_node_burst(
        dht.async_dht(),
        config.dht.find_node_burst_interval_secs,
        config.dht.find_node_burst_size,
        worker_cancel.clone(),
    ));
    tokio::spawn(run_routing_table_persistence(
        dht.async_dht(),
        config.dht.routing_table_path.clone(),
        config.dht.routing_save_interval_secs,
        worker_cancel.clone(),
    ));

    let discovery_settings = DiscoverySettings {
        batch_size: config.enrichment.discovery_batch_size,
        flush_ms: config.enrichment.discovery_flush_ms,
    };
    tokio::spawn(run_discovery_batcher(
        discovery_rx,
        store.clone(),
        enrich_tx.clone(),
        discovery_settings,
        worker_cancel.clone(),
    ));

    let enrichment_settings = EnrichSettings {
        concurrency: config.enrichment.concurrency,
        peers_per_hash: config.enrichment.peers_per_hash,
        peer_timeout_secs: config.enrichment.peer_timeout_secs,
        retry_base_secs: config.enrichment.retry_base_secs,
        retry_max_secs: config.enrichment.retry_max_secs,
        retry_max_attempts: config.enrichment.retry_max_attempts,
        tracker_peer_urls: config.tracker.peer_urls().to_vec(),
        rules: rules.clone(),
    };
    let dht_peers: Arc<dyn DhtPeerLookup> = dht.clone();
    tokio::spawn(run_enrichment_pool(
        enrich_rx,
        store.clone(),
        dht_peers.clone(),
        enrichment_settings.clone(),
    ));
    tokio::spawn(run_enrichment_scheduler(
        store.clone(),
        dht_peers,
        enrich_tx,
        enrichment_settings,
        config.enrichment.retry_poll_interval_secs,
        config.enrichment.scheduler_batch,
        worker_cancel.clone(),
    ));
    tokio::spawn(run_failed_requeue_loop(
        store.clone(),
        config.enrichment.retry_poll_interval_secs,
        config.enrichment.failed_retry_after_secs,
        config.enrichment.retry_max_attempts,
        worker_cancel.clone(),
    ));
    tokio::spawn(run_peer_prune_loop(
        store.clone(),
        config.store.peer_ttl_secs,
        config.enrichment.retry_poll_interval_secs,
        worker_cancel.clone(),
    ));

    tokio::spawn(run_indexer_loop(
        store.clone(),
        search.clone(),
        rules.clone(),
        backends.clone(),
        config.index.clone(),
        worker_cancel.clone(),
    ));

    tokio::spawn(run_tracker_scrape_loop(
        store.clone(),
        config.tracker.clone(),
        worker_cancel.clone(),
    ));

    let store_stats = store.clone();
    let stats_cancel = worker_cancel.clone();
    tokio::spawn(async move {
        loop {
            if stats_cancel.is_cancelled() {
                return;
            }
            if let Ok((total, enriched, pending)) = store_stats.count_by_status().await {
                let indexed = store_stats.count_indexed().await.unwrap_or(0);
                let retry_depth = store_stats
                    .count_retry_queue(crate::unix_now())
                    .await
                    .unwrap_or(0);
                let peers = store_stats.peer_breakdown().await.unwrap_or_default();
                let torrents = store_stats.torrent_breakdown().await.unwrap_or_default();
                metrics::gauge!("retry_queue_depth").set(retry_depth as f64);
                metrics::gauge!("pipeline_enriched").set(enriched as f64);
                metrics::gauge!("pipeline_pending").set(pending as f64);
                metrics::gauge!("pipeline_indexed").set(indexed as f64);
                info!(
                    total,
                    enriched,
                    pending,
                    indexed,
                    retry_depth,
                    peers_announce = peers.announce,
                    peers_dht = peers.dht,
                    peers_tracker = peers.tracker,
                    status_discovered = torrents.discovered,
                    status_enriching = torrents.enriching,
                    status_failed = torrents.failed,
                    status_blocked = torrents.blocked,
                    "stats"
                );
            }
            tokio::select! {
                _ = stats_cancel.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(30)) => {}
            }
        }
    });

    let app_state = AppState {
        store,
        search,
        search_config: config.search.clone(),
        metrics: metrics_handle,
        backends,
        rules,
    };

    let mut router = axum::Router::new()
        .route("/health", axum::routing::get(crate::api::health))
        .route("/stats", axum::routing::get(crate::api::stats))
        .route("/search", axum::routing::get(crate::api::search))
        .route("/torrent/{info_hash}", axum::routing::get(crate::api::get_torrent));

    if config.metrics.enabled {
        router = router.route("/metrics", axum::routing::get(crate::api::metrics));
    }

    let router = router.with_state(app_state);
    let listener = tokio::net::TcpListener::bind(&config.api.bind).await?;
    info!(bind = %config.api.bind, "api listening");

    axum::serve(listener, router.into_make_service())
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("shutdown signal received");
            shutdown.cancel();
        })
        .await?;
    Ok(())
}
