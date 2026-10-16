use crate::store::Store;
use crate::{DiscoveredHash, DiscoverySource, InfoHash, Peer};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

#[derive(Clone)]
pub struct DiscoverySettings {
    pub batch_size: usize,
    pub flush_ms: u64,
}

pub async fn run_discovery_batcher(
    mut rx: mpsc::Receiver<DiscoveredHash>,
    store: Arc<Store>,
    enrich_tx: mpsc::Sender<InfoHash>,
    settings: DiscoverySettings,
    cancel: CancellationToken,
) {
    let mut buffer: Vec<DiscoveredHash> = Vec::with_capacity(settings.batch_size);
    let mut interval = tokio::time::interval(Duration::from_millis(settings.flush_ms.max(1)));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        if cancel.is_cancelled() {
            break;
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            item = rx.recv() => {
                match item {
                    Some(item) => {
                        let immediate_announce = matches!(
                            item.source,
                            DiscoverySource::AnnouncePeer
                        ) && item.peer.is_some();
                        if immediate_announce {
                            flush_discovery_batch(&store, &enrich_tx, &mut vec![item]).await;
                        } else {
                            buffer.push(item);
                            if buffer.len() >= settings.batch_size {
                                flush_discovery_batch(&store, &enrich_tx, &mut buffer).await;
                            }
                        }
                    }
                    None => {
                        if !buffer.is_empty() {
                            flush_discovery_batch(&store, &enrich_tx, &mut buffer).await;
                        }
                        break;
                    }
                }
            }
            _ = interval.tick() => {
                if !buffer.is_empty() {
                    flush_discovery_batch(&store, &enrich_tx, &mut buffer).await;
                }
            }
        }
    }
}

async fn flush_discovery_batch(
    store: &Store,
    enrich_tx: &mpsc::Sender<InfoHash>,
    buffer: &mut Vec<DiscoveredHash>,
) {
    if buffer.is_empty() {
        return;
    }
    let mut bep51 = 0usize;
    let mut get_peers = 0usize;
    let mut announce = 0usize;
    let items: Vec<(InfoHash, Option<Peer>)> = buffer
        .drain(..)
        .map(|item| {
            match item.source {
                DiscoverySource::Bep51 => bep51 += 1,
                DiscoverySource::GetPeers => get_peers += 1,
                DiscoverySource::AnnouncePeer => announce += 1,
            }
            (item.info_hash, item.peer)
        })
        .collect();
    let batch_size = items.len();
    match store.process_discovery_batch(items).await {
        Ok(result) => {
            metrics::counter!("hashes_discovered_total")
                .increment(result.newly_discovered as u64);
            let enqueued = result.to_enrich.len();
            info!(
                batch_size,
                new = result.newly_discovered,
                enqueued,
                bep51,
                get_peers,
                announce,
                "discovery batch"
            );
            for hash in result.to_enrich {
                debug!(hash = %hash, "discovery enqueued for enrichment");
                if enrich_tx.send(hash).await.is_err() {
                    return;
                }
            }
        }
        Err(error) => {
            debug!(%error, "discovery batch write failed");
        }
    }
}
