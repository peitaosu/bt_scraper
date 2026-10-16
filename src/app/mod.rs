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
use crate::infra::RuntimeServices;
use crate::metrics::{init_metrics, register_metrics};
use crate::ports::DhtPeerLookup;
use crate::routing::load_routing_table;
use crate::search::SearchIndex;
use crate::store::Store;
use crate::workers::run_indexer_loop;
use crate::workers::run_tracker_scrape_loop;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
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

    let runtime = RuntimeServices::new(
        config.tracker.http_timeout_secs,
        config.dht.udp_pool_size,
    );

    let rules = ClassificationRules::load(&config.classification.config_path)?;
    let backends = Arc::new(crate::backends::BackendContext::from_config(&config).await?);
    info!("opening store");
    let store = Store::open_from_config(
        &config.store,
        config.enrichment.peer_fail_threshold,
    )?;
    info!("opening search index");
    let search = Arc::new(SearchIndex::open_with_options(
        &config.index.path,
        config.index.writer_heap_mb,
        config.index.reload_reader_on_commit,
    )?);
    let (discovery_tx, discovery_rx) = mpsc::channel(config.discovery_channel_capacity());
    let (enrich_tx, enrich_rx) = mpsc::channel(config.enrich_channel_capacity());

    let persisted = load_routing_table(&config.dht.routing_table_path);
    info!(bind = %config.dht.bind, "starting dht");
    let dht = DhtEngine::start(
        &config.dht.bind,
        &config.dht.bootstrap,
        &persisted,
        discovery_tx.clone(),
        config.dht.lookup_timeout_secs,
    )
    .await?;

    let cancel = worker_cancel.clone();
    let dht_bootstrap = dht.clone();
    let async_dht = dht.async_dht();
    let bep51_interval_secs = config.dht.bep51_interval_secs;
    let bep51_batch = config.dht.bep51_batch;
    let bep51_concurrency = config.dht.bep51_concurrency;
    let bep51_sample_timeout_secs = config.dht.bep51_sample_timeout_secs;
    let udp = runtime.udp.clone();
    tokio::spawn(async move {
        dht_bootstrap.wait_bootstrapped().await;
        run_bep51_sampler(
            async_dht,
            discovery_tx,
            dht_bootstrap.sample_store(),
            udp,
            bep51_interval_secs,
            bep51_batch,
            bep51_concurrency,
            bep51_sample_timeout_secs,
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
        connect_timeout_secs: config.enrichment.connect_timeout_secs,
        retry_base_secs: config.enrichment.retry_base_secs,
        retry_max_secs: config.enrichment.retry_max_secs,
        retry_max_attempts: config.enrichment.retry_max_attempts,
        tracker_peer_urls: config.tracker.peer_urls().to_vec(),
        rules: rules.clone(),
        skip_dht_if_cached_peers: config.enrichment.skip_dht_if_cached_peers,
        tracker_urls_per_lookup: config.enrichment.tracker_urls_per_lookup,
        peer_fail_threshold: config.enrichment.peer_fail_threshold,
    };
    let tcp_limit = Arc::new(Semaphore::new(config.enrichment.global_tcp_limit.max(1)));
    let dht_peers: Arc<dyn DhtPeerLookup> = dht.clone();
    tokio::spawn(run_enrichment_pool(
        enrich_rx,
        store.clone(),
        dht_peers.clone(),
        runtime.trackers.clone(),
        enrichment_settings.clone(),
        tcp_limit,
    ));
    tokio::spawn(run_enrichment_scheduler(
        store.clone(),
        dht_peers,
        runtime.trackers.clone(),
        enrich_tx,
        enrichment_settings,
        config.enrichment.retry_poll_interval_secs,
        config.enrichment.scheduler_batch,
        config.enrichment.scheduler_lookup_concurrency,
        config.enrichment.scheduler_batch_max,
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
        search.clone(),
        runtime.trackers.clone(),
        config.tracker.clone(),
        config.index.patch_swarm_on_scrape,
        worker_cancel.clone(),
    ));

    let store_stats = store.clone();
    let stats_cancel = worker_cancel.clone();
    let stats_interval_secs = config.enrichment.stats_interval_secs;
    tokio::spawn(async move {
        loop {
            if stats_cancel.is_cancelled() {
                return;
            }
            let now = crate::unix_now();
            if let Ok(snapshot) = store_stats.pipeline_snapshot(now).await {
                metrics::gauge!("retry_queue_depth").set(snapshot.retry_queue as f64);
                metrics::gauge!("pipeline_enriched").set(snapshot.enriched as f64);
                metrics::gauge!("pipeline_pending").set(snapshot.pending as f64);
                metrics::gauge!("pipeline_indexed").set(snapshot.indexed as f64);
                info!(
                    total = snapshot.total,
                    enriched = snapshot.enriched,
                    pending = snapshot.pending,
                    indexed = snapshot.indexed,
                    retry_depth = snapshot.retry_queue,
                    peers_announce = snapshot.peers.announce,
                    peers_dht = snapshot.peers.dht,
                    peers_tracker = snapshot.peers.tracker,
                    status_discovered = snapshot.torrents.discovered,
                    status_enriching = snapshot.torrents.enriching,
                    status_failed = snapshot.torrents.failed,
                    status_blocked = snapshot.torrents.blocked,
                    "stats"
                );
            }
            tokio::select! {
                _ = stats_cancel.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(stats_interval_secs)) => {}
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
