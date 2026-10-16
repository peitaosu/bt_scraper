use crate::config::TrackerConfig;
use crate::store::Store;
use crate::tracker::scrape_counts;
use crate::unix_now;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

pub async fn run_tracker_scrape_loop(
    store: Arc<Store>,
    config: TrackerConfig,
    cancel: CancellationToken,
) {
    let timeout = Duration::from_secs(config.scrape_timeout_secs);
    let scrape_urls = config.scrape_urls();
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let now = unix_now();
        let stale_before = now.saturating_sub(config.scrape_stale_secs);
        let candidates = match store.get_scrape_candidates(config.scrape_batch, stale_before).await
        {
            Ok(candidates) => candidates,
            Err(error) => {
                warn!(%error, "scrape candidate query failed");
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(config.scrape_interval_secs)) => {}
                }
                continue;
            }
        };

        if candidates.is_empty() {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(config.scrape_interval_secs)) => {}
            }
            continue;
        }

        let mut merged: HashMap<crate::InfoHash, (u32, u32)> = HashMap::new();
        let mut set = tokio::task::JoinSet::new();
        for tracker_url in &scrape_urls {
            let tracker_url = tracker_url.clone();
            let candidates = candidates.clone();
            set.spawn(async move { scrape_counts(&tracker_url, &candidates, timeout).await });
        }
        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok(results)) => {
                    for (hash, seeders, leechers) in results {
                        let entry = merged.entry(hash).or_insert((0, 0));
                        if seeders > entry.0 {
                            entry.0 = seeders;
                        }
                        if leechers > entry.1 {
                            entry.1 = leechers;
                        }
                    }
                    metrics::counter!("scrape_tracker_success_total").increment(1);
                }
                Ok(Err(error)) => {
                    debug!(%error, "tracker scrape failed");
                    metrics::counter!("scrape_tracker_failure_total").increment(1);
                }
                Err(error) => {
                    debug!(%error, "tracker scrape task failed");
                    metrics::counter!("scrape_tracker_failure_total").increment(1);
                }
            }
        }

        let updates: Vec<_> = candidates
            .iter()
            .map(|hash| {
                let (seeders, leechers) = merged.get(hash).copied().unwrap_or((0, 0));
                (*hash, seeders, leechers, now)
            })
            .collect();
        if let Err(error) = store.update_tracker_stats_batch(updates).await {
            warn!(%error, "batch tracker stats update failed");
        }

        metrics::counter!("scrape_batch_total").increment(1);
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(config.scrape_interval_secs)) => {}
        }
    }
}
