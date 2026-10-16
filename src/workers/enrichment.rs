use anyhow::Result;
use crate::classify::classify;
use crate::domain::pipeline::may_schedule_enrichment;
use crate::classification_config::ClassificationRules;
use crate::infra::TrackerGateway;
use crate::peer_policy::is_storable_peer;
use crate::ports::DhtPeerLookup;
use crate::store::Store;
use crate::wire::fetch_metadata_with_connect_timeout;
use crate::{InfoHash, Peer, TorrentMetadata, unix_now};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::peer_collect::{collect_peers, PeerCollectConfig};

#[derive(Clone)]
pub struct EnrichSettings {
    pub concurrency: usize,
    pub peers_per_hash: usize,
    pub peer_timeout_secs: u64,
    pub connect_timeout_secs: u64,
    pub retry_base_secs: u64,
    pub retry_max_secs: u64,
    pub retry_max_attempts: u32,
    pub tracker_peer_urls: Vec<String>,
    pub rules: Arc<ClassificationRules>,
    pub skip_dht_if_cached_peers: bool,
    pub tracker_urls_per_lookup: usize,
    pub peer_fail_threshold: u32,
}

pub async fn run_enrichment_pool(
    rx: mpsc::Receiver<InfoHash>,
    store: Arc<Store>,
    dht: Arc<dyn DhtPeerLookup>,
    trackers: Arc<TrackerGateway>,
    config: EnrichSettings,
    tcp_limit: Arc<Semaphore>,
) {
    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let mut rx = rx;
    loop {
        let info_hash = match rx.recv().await {
            Some(hash) => hash,
            None => break,
        };
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break,
        };
        let tcp_permit = match tcp_limit.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break,
        };
        let store = store.clone();
        let dht = dht.clone();
        let trackers = trackers.clone();
        let config = config.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _tcp = tcp_permit;
            if let Err(error) = enrich_one(&store, dht.as_ref(), &trackers, &info_hash, &config).await
            {
                debug!(hash = %info_hash, error = %error, "enrichment failed");
            }
        });
    }
}

pub async fn run_enrichment_scheduler(
    store: Arc<Store>,
    dht: Arc<dyn DhtPeerLookup>,
    trackers: Arc<TrackerGateway>,
    enrich_tx: mpsc::Sender<InfoHash>,
    config: EnrichSettings,
    interval_secs: u64,
    batch: usize,
    lookup_concurrency: usize,
    batch_max: usize,
    cancel: CancellationToken,
) {
    let batch = batch.clamp(1, batch_max);
    let semaphore = Arc::new(Semaphore::new(lookup_concurrency.max(1)));
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let now = unix_now();
        let candidates = match store.get_retry_candidates(batch, now).await {
            Ok(candidates) => candidates,
            Err(error) => {
                warn!(%error, "enrichment scheduler query failed");
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(interval_secs)) => {}
                }
                continue;
            }
        };
        if candidates.is_empty() {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(interval_secs)) => {}
            }
            continue;
        }
        let candidate_count = candidates.len();
        metrics::gauge!("retry_queue_depth")
            .set(store.count_retry_queue(now).await.unwrap_or(0) as f64);
        info!(count = candidate_count, "enrichment scheduler tick");
        let mut enqueued = 0usize;
        let mut cached_quality = 0usize;
        let mut found_quality = 0usize;
        let mut set = tokio::task::JoinSet::new();
        for hash in candidates {
            let store = store.clone();
            let dht = dht.clone();
            let trackers = trackers.clone();
            let config = config.clone();
            let semaphore = semaphore.clone();
            set.spawn(async move {
                let _permit = semaphore.acquire_owned().await.ok()?;
                let decision = should_enqueue(&store, dht.as_ref(), &trackers, &hash, &config).await;
                Some((hash, decision))
            });
        }
        while let Some(result) = set.join_next().await {
            let Some((hash, decision)) = result.ok().flatten() else {
                continue;
            };
            let enqueue = matches!(
                decision,
                EnqueueDecision::CachedQuality | EnqueueDecision::FoundQuality
            );
            match decision {
                EnqueueDecision::CachedQuality => cached_quality += 1,
                EnqueueDecision::FoundQuality => found_quality += 1,
                EnqueueDecision::Skip => {}
            }
            if enqueue {
                if enrich_tx.try_send(hash).is_err() {
                    metrics::counter!("enrich_queue_dropped_total").increment(1);
                    return;
                }
                enqueued += 1;
            }
        }
        info!(
            candidates = candidate_count,
            enqueued,
            cached_quality,
            found_quality,
            skipped = candidate_count.saturating_sub(enqueued),
            "enrichment scheduler"
        );
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(interval_secs)) => {}
        }
    }
}

enum EnqueueDecision {
    CachedQuality,
    FoundQuality,
    Skip,
}

async fn should_enqueue(
    store: &Store,
    dht: &dyn DhtPeerLookup,
    trackers: &TrackerGateway,
    info_hash: &InfoHash,
    config: &EnrichSettings,
) -> EnqueueDecision {
    let record = store.get_record(info_hash).await.ok().flatten();
    let Some(record) = record else {
        return EnqueueDecision::Skip;
    };
    if !may_schedule_enrichment(record.status) {
        return EnqueueDecision::Skip;
    }
    if store.has_quality_peer(info_hash).await.unwrap_or(false) {
        debug!(hash = %info_hash, "scheduler enqueued: cached quality peer");
        return EnqueueDecision::CachedQuality;
    }
    let peers = collect_peers_for_config(store, dht, trackers, info_hash, config).await;
    let _ = store.touch_peer_lookup(info_hash).await;
    if peers.iter().any(|p| is_storable_peer(p)) {
        debug!(hash = %info_hash, "scheduler enqueued: found quality peers");
        return EnqueueDecision::FoundQuality;
    }
    debug!(hash = %info_hash, "scheduler skipped: no quality peers");
    EnqueueDecision::Skip
}

pub async fn run_failed_requeue_loop(
    store: Arc<Store>,
    interval_secs: u64,
    failed_after_secs: u64,
    max_attempts: u32,
    cancel: CancellationToken,
) {
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let now = unix_now();
        if let Ok(count) = store
            .requeue_failed(now, max_attempts, failed_after_secs)
            .await
        {
            if count > 0 {
                info!(count, "requeued exhausted failed torrents");
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(interval_secs)) => {}
        }
    }
}

pub async fn run_peer_prune_loop(
    store: Arc<Store>,
    peer_ttl_secs: u64,
    interval_secs: u64,
    cancel: CancellationToken,
) {
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let cutoff = unix_now().saturating_sub(peer_ttl_secs);
        if let Ok(count) = store.prune_stale_peers(cutoff).await {
            if count > 0 {
                debug!(count, "pruned stale peers");
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(interval_secs)) => {}
        }
    }
}

async fn enrich_one(
    store: &Store,
    dht: &dyn DhtPeerLookup,
    trackers: &TrackerGateway,
    info_hash: &InfoHash,
    config: &EnrichSettings,
) -> Result<()> {
    let record = store.get_record(info_hash).await?;
    let Some(record) = record else {
        return Ok(());
    };
    if record.status == crate::EnrichmentStatus::Enriching {
        return Ok(());
    }
    if !may_schedule_enrichment(record.status) {
        return Ok(());
    }
    if record.attempt_count >= config.retry_max_attempts {
        return Ok(());
    }
    store.mark_enriching(info_hash).await?;
    let attempt = store
        .get_record(info_hash)
        .await?
        .map(|r| r.attempt_count)
        .unwrap_or(1);

    let started = Instant::now();
    let peers = collect_peers_for_config(store, dht, trackers, info_hash, config).await;
    let peers: Vec<Peer> = peers
        .into_iter()
        .filter(|peer| is_storable_peer(peer))
        .take(config.peers_per_hash)
        .collect();
    if peers.is_empty() {
        schedule_retry(store, info_hash, config, attempt).await?;
        metrics::counter!("enrichment_no_peers_total").increment(1);
        return Ok(());
    }

    let timeout = Duration::from_secs(config.peer_timeout_secs);
    let connect_timeout = Duration::from_secs(config.connect_timeout_secs);
    let peer_count = peers.len();
    let tried_addrs: Vec<SocketAddr> = peers.iter().map(|p| p.addr).collect();
    let (metadata, last_error) =
        race_metadata(info_hash, peers, timeout, connect_timeout).await;
    metrics::histogram!("metadata_latency_seconds").record(started.elapsed().as_secs_f64());

    if let Some(metadata) = metadata {
        let classification = classify(&metadata, &config.rules);
        if classification.blocked {
            store.mark_blocked(info_hash).await?;
            metrics::counter!("metadata_blocked_total").increment(1);
            info!(hash = %info_hash, "blocked at ingest");
            return Ok(());
        }
        store
            .save_enriched(
                info_hash,
                &metadata,
                Some(&classification.content_type),
                Some(&classification.tags),
            )
            .await?;
        metrics::counter!("metadata_success_total").increment(1);
        info!(hash = %info_hash, name = %metadata.name, "enriched");
        return Ok(());
    }

    let _ = store.record_peer_failures(info_hash, &tried_addrs).await;
    metrics::counter!("metadata_failure_total").increment(1);
    schedule_retry(store, info_hash, config, attempt).await?;
    warn!(
        hash = %info_hash,
        attempt,
        peers = peer_count,
        last_error = last_error.as_deref().unwrap_or("unknown"),
        "enrichment failed: metadata fetch"
    );
    Ok(())
}

fn collect_config(config: &EnrichSettings) -> PeerCollectConfig {
    PeerCollectConfig {
        skip_dht_if_cached: config.skip_dht_if_cached_peers,
        tracker_urls: config.tracker_peer_urls.clone(),
        tracker_urls_per_lookup: config.tracker_urls_per_lookup,
        peer_timeout: Duration::from_secs(config.peer_timeout_secs),
        cached_peer_limit: 64,
    }
}

async fn collect_peers_for_config(
    store: &Store,
    dht: &dyn DhtPeerLookup,
    trackers: &TrackerGateway,
    info_hash: &InfoHash,
    config: &EnrichSettings,
) -> Vec<Peer> {
    collect_peers(
        store,
        dht,
        trackers,
        info_hash,
        &collect_config(config),
    )
    .await
}

async fn race_metadata(
    info_hash: &InfoHash,
    peers: Vec<Peer>,
    timeout: Duration,
    connect_timeout: Duration,
) -> (Option<TorrentMetadata>, Option<String>) {
    let mut set = tokio::task::JoinSet::new();
    for peer in peers {
        let hash = *info_hash;
        let addr = peer.addr;
        let source = peer.source;
        set.spawn(async move {
            fetch_metadata_with_connect_timeout(&hash, addr, timeout, connect_timeout)
                .await
                .map_err(|error| (addr, source, error))
        });
    }
    let mut last_error = None;
    while let Some(result) = set.join_next().await {
        match result {
            Ok(Ok(metadata)) => {
                set.abort_all();
                return (Some(metadata), None);
            }
            Ok(Err((addr, source, error))) => {
                last_error = Some(format!("{addr} ({source:?}): {error:#}"));
            }
            Err(error) => {
                last_error = Some(format!("task: {error:#}"));
            }
        }
    }
    (None, last_error)
}

async fn schedule_retry(
    store: &Store,
    info_hash: &InfoHash,
    config: &EnrichSettings,
    attempt: u32,
) -> Result<()> {
    let delay = config.retry_base_secs.saturating_mul(1u64 << attempt.min(10));
    let delay = delay.min(config.retry_max_secs);
    let next = unix_now() + delay;
    store.mark_failed(info_hash, next).await?;
    Ok(())
}
