use anyhow::Result;
use crate::classify::classify;
use crate::domain::pipeline::may_schedule_enrichment;
use crate::classification_config::ClassificationRules;
use crate::ports::DhtPeerLookup;
use crate::peer_policy::{is_quality_peer, peer_source_rank};
use crate::store::Store;
use crate::tracker::fetch_peers;
use crate::wire::fetch_metadata;
use crate::{InfoHash, Peer, PeerSource, TorrentMetadata, unix_now};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct EnrichSettings {
    pub concurrency: usize,
    pub peers_per_hash: usize,
    pub peer_timeout_secs: u64,
    pub retry_base_secs: u64,
    pub retry_max_secs: u64,
    pub retry_max_attempts: u32,
    pub tracker_peer_urls: Vec<String>,
    pub rules: Arc<ClassificationRules>,
}

fn quality_peers(peers: &[Peer]) -> Vec<Peer> {
    peers.iter().copied().filter(|p| is_quality_peer(p)).collect()
}

fn peer_counts(peers: &[Peer]) -> (usize, usize, usize) {
    let mut announce = 0usize;
    let mut dht = 0usize;
    let mut tracker = 0usize;
    for peer in peers {
        match peer.source {
            PeerSource::Announce => announce += 1,
            PeerSource::Dht => dht += 1,
            PeerSource::Tracker => tracker += 1,
        }
    }
    (announce, dht, tracker)
}

pub async fn run_enrichment_pool(
    rx: mpsc::Receiver<InfoHash>,
    store: Arc<Store>,
    dht: Arc<dyn DhtPeerLookup>,
    config: EnrichSettings,
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
        let store = store.clone();
        let dht = dht.clone();
        let config = config.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = enrich_one(&store, dht.as_ref(), &info_hash, &config).await {
                debug!(hash = %info_hash, error = %error, "enrichment failed");
            }
        });
    }
}

pub async fn run_enrichment_scheduler(
    store: Arc<Store>,
    dht: Arc<dyn DhtPeerLookup>,
    enrich_tx: mpsc::Sender<InfoHash>,
    config: EnrichSettings,
    interval_secs: u64,
    batch: usize,
    cancel: CancellationToken,
) {
    let batch = batch.clamp(1, 64);
    let lookup_concurrency = 8;
    let semaphore = Arc::new(Semaphore::new(lookup_concurrency));
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
            let config = config.clone();
            let semaphore = semaphore.clone();
            set.spawn(async move {
                let _permit = semaphore.acquire_owned().await.ok()?;
                let decision = should_enqueue(&store, dht.as_ref(), &hash, &config).await;
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
                if enrich_tx.send(hash).await.is_err() {
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
    let peers = collect_peers(store, dht, info_hash, config).await;
    let _ = store.touch_peer_lookup(info_hash).await;
    let (announce, dht_count, tracker) = peer_counts(&peers);
    if quality_peers(&peers).is_empty() {
        debug!(
            hash = %info_hash,
            announce,
            dht = dht_count,
            tracker,
            "scheduler skipped: no quality peers"
        );
        return EnqueueDecision::Skip;
    }
    debug!(
        hash = %info_hash,
        announce,
        dht = dht_count,
        tracker,
        "scheduler enqueued: found quality peers"
    );
    EnqueueDecision::FoundQuality
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
    let peers = collect_peers(store, dht, info_hash, config).await;
    let (announce, dht_count, tracker) = peer_counts(&peers);
    let mut peers = quality_peers(&peers);
    peers.sort_by_key(|peer| peer_source_rank(peer.source));
    peers.truncate(config.peers_per_hash);
    if peers.is_empty() {
        schedule_retry(store, info_hash, config, attempt).await?;
        metrics::counter!("enrichment_no_peers_total").increment(1);
        debug!(
            hash = %info_hash,
            attempt,
            announce,
            dht = dht_count,
            tracker,
            "enrichment deferred: no quality peers"
        );
        return Ok(());
    }

    let peer_addrs: Vec<_> = peers.iter().map(|p| format!("{} ({:?})", p.addr, p.source)).collect();
    debug!(
        hash = %info_hash,
        attempt,
        peers = %peer_addrs.join(", "),
        "enrichment trying metadata fetch"
    );

    let timeout = Duration::from_secs(config.peer_timeout_secs);
    let peer_count = peers.len();
    let (metadata, last_error) = race_metadata(info_hash, peers, timeout).await;
    metrics::histogram!("metadata_latency_seconds").record(started.elapsed().as_secs_f64());

    if let Some(metadata) = metadata {
        if classify(&metadata, &config.rules).blocked {
            store.mark_blocked(info_hash).await?;
            metrics::counter!("metadata_blocked_total").increment(1);
            info!(hash = %info_hash, "blocked at ingest");
            return Ok(());
        }
        store.save_enriched(info_hash, &metadata).await?;
        metrics::counter!("metadata_success_total").increment(1);
        info!(hash = %info_hash, name = %metadata.name, "enriched");
        return Ok(());
    }

    metrics::counter!("metadata_failure_total").increment(1);
    schedule_retry(store, info_hash, config, attempt).await?;
    warn!(
        hash = %info_hash,
        attempt,
        peers = peer_count,
        announce,
        dht = dht_count,
        tracker,
        last_error = last_error.as_deref().unwrap_or("unknown"),
        "enrichment failed: metadata fetch"
    );
    Ok(())
}

async fn race_metadata(
    info_hash: &InfoHash,
    peers: Vec<Peer>,
    timeout: Duration,
) -> (Option<TorrentMetadata>, Option<String>) {
    let mut set = tokio::task::JoinSet::new();
    for peer in peers {
        let hash = *info_hash;
        let addr = peer.addr;
        let source = peer.source;
        set.spawn(async move {
            fetch_metadata(&hash, addr, timeout)
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
                debug!(
                    hash = %info_hash,
                    %addr,
                    ?source,
                    error = %error,
                    "metadata peer failed"
                );
            }
            Err(error) => {
                last_error = Some(format!("task: {error:#}"));
                debug!(hash = %info_hash, %error, "metadata task failed");
            }
        }
    }
    (None, last_error)
}

async fn collect_peers(
    store: &Store,
    dht: &dyn DhtPeerLookup,
    info_hash: &InfoHash,
    config: &EnrichSettings,
) -> Vec<Peer> {
    let hash = *info_hash;
    let store_cached = store;
    let dht_lookup = dht.lookup_peers(info_hash);
    let tracker_urls = config.tracker_peer_urls.clone();
    let tracker_timeout = Duration::from_secs(config.peer_timeout_secs);

    let (cached, dht_peers, tracker_peers) = tokio::join!(
        async move { store_cached.get_peers(&hash, 64).await.unwrap_or_default() },
        dht_lookup,
        fetch_tracker_peers(&hash, &tracker_urls, tracker_timeout),
    );

    let mut set = HashSet::new();
    let mut peers = Vec::new();
    let mut pending_upsert = Vec::new();

    for peer in cached {
        if set.insert(peer.addr) {
            peers.push(peer);
        }
    }
    for peer in dht_peers {
        pending_upsert.push(peer.clone());
        if set.insert(peer.addr) {
            peers.push(peer);
        }
    }
    for peer in tracker_peers {
        pending_upsert.push(peer.clone());
        if set.insert(peer.addr) {
            peers.push(peer);
        }
    }

    if !pending_upsert.is_empty() {
        let _ = store.upsert_peers(info_hash, &pending_upsert).await;
    }

    peers.sort_by_key(|peer| peer_source_rank(peer.source));

    peers
}

async fn fetch_tracker_peers(
    info_hash: &InfoHash,
    tracker_urls: &[String],
    timeout: Duration,
) -> Vec<Peer> {
    let mut set = tokio::task::JoinSet::new();
    for url in tracker_urls {
        let url = url.clone();
        let hash = *info_hash;
        set.spawn(async move { fetch_peers(&url, &hash, timeout).await });
    }
    let mut peers = Vec::new();
    while let Some(result) = set.join_next().await {
        if let Ok(Ok(mut tracker_peers)) = result {
            peers.append(&mut tracker_peers);
        }
    }
    peers
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
