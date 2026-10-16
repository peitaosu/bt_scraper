use anyhow::Result;
use crate::backends::BackendContext;
use crate::classify::{classify, Classification};
use crate::classification_config::ClassificationRules;
use crate::config::IndexConfig;
use crate::search::SearchIndex;
use crate::store::{IndexCandidate, IndexMark, Store};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

pub async fn run_indexer_loop(
    store: Arc<Store>,
    search: Arc<SearchIndex>,
    rules: Arc<ClassificationRules>,
    backends: Arc<BackendContext>,
    config: IndexConfig,
    cancel: CancellationToken,
) {
    loop {
        if cancel.is_cancelled() {
            return;
        }
        match process_index_batch(&store, &search, &rules, &backends, &config).await {
            Ok(0) => {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(config.poll_interval_secs)) => {}
                }
            }
            Ok(count) => {
                info!(count, "indexed batch");
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
            Err(error) => {
                warn!(%error, "index batch failed");
                metrics::counter!("index_failure_total").increment(1);
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(config.poll_interval_secs)) => {}
                }
            }
        }
    }
}

pub async fn index_batch(
    store: &Store,
    search: &Arc<SearchIndex>,
    rules: &ClassificationRules,
    backends: &BackendContext,
    config: &IndexConfig,
    limit: usize,
) -> Result<usize> {
    let mut config = config.clone();
    config.batch_size = limit;
    process_index_batch(store, search, rules, backends, &config).await
}

async fn process_index_batch(
    store: &Store,
    search: &Arc<SearchIndex>,
    rules: &ClassificationRules,
    backends: &BackendContext,
    config: &IndexConfig,
) -> Result<usize> {
    let candidates = store.get_index_candidates(config.batch_size).await?;
    if candidates.is_empty() {
        return Ok(0);
    }

    let mut classified = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let classification = classification_for_candidate(&candidate, rules);
        classified.push((candidate, classification));
    }

    let tantivy_items: Vec<(IndexCandidate, Classification)> = classified
        .iter()
        .filter(|(_, c)| !c.blocked)
        .map(|(candidate, c)| (candidate.clone(), c.clone()))
        .collect();

    if !tantivy_items.is_empty() {
        let search = search.clone();
        let commit_batch = config.commit_batch;
        let reload_on_commit = config.reload_reader_on_commit;
        tokio::task::spawn_blocking(move || {
            write_tantivy_batch(&search, &tantivy_items, commit_batch, reload_on_commit)
        })
        .await
        .map_err(|_| anyhow::anyhow!("index write task panicked"))??;
    }

    let mut marks = Vec::new();
    let mut indexed = 0usize;
    for (candidate, classification) in classified {
        if classification.blocked {
            debug!(hash = %candidate.info_hash, "skipping blocked torrent for index");
            marks.push(IndexMark {
                info_hash: candidate.info_hash,
                content_type: classification.content_type,
                tags: classification.tags,
            });
            metrics::counter!("index_skipped_blocked_total").increment(1);
            continue;
        }

        marks.push(IndexMark {
            info_hash: candidate.info_hash,
            content_type: classification.content_type.clone(),
            tags: classification.tags.clone(),
        });
        backends
            .on_indexed(
                &candidate.info_hash,
                &candidate.metadata,
                &classification,
                candidate.seeders as u64,
                candidate.leechers as u64,
                candidate.enriched_at,
            )
            .await?;
        metrics::counter!("index_success_total").increment(1);
        indexed += 1;
    }
    store.mark_indexed_batch(marks).await?;

    Ok(indexed)
}

fn classification_for_candidate(
    candidate: &IndexCandidate,
    rules: &ClassificationRules,
) -> Classification {
    let mut classification = classify(&candidate.metadata, rules);
    if !classification.blocked {
        if let Some(content_type) = &candidate.content_type {
            if !candidate.tags.is_empty() {
                classification.content_type = content_type.clone();
                classification.tags = candidate.tags.clone();
            }
        }
    }
    classification
}

fn write_tantivy_batch(
    search: &Arc<SearchIndex>,
    items: &[(IndexCandidate, Classification)],
    commit_batch: usize,
    reload_on_commit: bool,
) -> Result<()> {
    let mut pending_commits = 0usize;
    search.with_writer(|writer| {
        for (candidate, classification) in items {
            search.upsert(
                writer,
                &candidate.info_hash,
                &candidate.metadata,
                classification,
                candidate.seeders as u64,
                candidate.leechers as u64,
                candidate.enriched_at,
                candidate.discovered_at,
            )?;
            pending_commits += 1;
            if pending_commits >= commit_batch {
                writer.commit()?;
                pending_commits = 0;
            }
        }
        if pending_commits > 0 {
            writer.commit()?;
        }
        Ok(())
    })?;
    if reload_on_commit {
        let _ = search.reload();
    }
    Ok(())
}
