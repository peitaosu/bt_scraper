use anyhow::{Context, Result};
use crate::classification_config::ClassificationRules;
use crate::classify::classify;
use crate::config::Config;
use crate::workers::index_batch;
use crate::search::SearchIndex;
use crate::store::Store;
use std::path::PathBuf;
use std::sync::Arc;

pub async fn stats(config_path: &PathBuf) -> Result<()> {
    let config = Config::load(config_path)?;
    let store = Store::open_from_config(&config.store, config.enrichment.peer_fail_threshold)?;
    let (total, enriched, pending) = store.count_by_status().await?;
    let indexed = store.count_indexed().await?;
    let payload = serde_json::json!({
        "total": total,
        "enriched": enriched,
        "pending": pending,
        "indexed": indexed,
    });
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

pub async fn backfill(config_path: &PathBuf) -> Result<()> {
    let config = Config::load(config_path)?;
    let store = Store::open_from_config(&config.store, config.enrichment.peer_fail_threshold)?;
    let count = store.clear_all_indexed_flags().await?;
    println!("cleared indexed_at on {count} torrents");
    Ok(())
}

pub async fn reindex(config_path: &PathBuf) -> Result<()> {
    let config = Config::load(config_path)?;
    let store = Store::open_from_config(&config.store, config.enrichment.peer_fail_threshold)?;
    store.clear_all_indexed_flags().await?;
    let search = Arc::new(SearchIndex::open_with_options(
        &config.index.path,
        config.index.writer_heap_mb,
        config.index.reload_reader_on_commit,
    )?);
    let rules = ClassificationRules::load(&config.classification.config_path)?;
    let backends = crate::backends::BackendContext::from_config(&config).await?;
    let indexed = index_batch(&store, &search, &rules, &backends, &config.index, usize::MAX).await?;
    println!("reindexed {indexed} torrents");
    Ok(())
}

pub async fn export(config_path: &PathBuf, output: &PathBuf) -> Result<()> {
    let config = Config::load(config_path)?;
    let store = Store::open_from_config(&config.store, config.enrichment.peer_fail_threshold)?;
    let rules = ClassificationRules::load(&config.classification.config_path)?;
    let mut file = std::fs::File::create(output).context("create export file")?;
    use std::io::Write;
    for record in store.export_enriched().await? {
        let metadata = record
            .metadata
            .as_ref()
            .context("enriched torrent missing metadata")?;
        let classification = classify(metadata, &rules);
        let line = serde_json::json!({
            "info_hash": record.info_hash.to_hex(),
            "name": metadata.name,
            "content_type": classification.content_type,
            "tags": classification.tags,
            "seeders": record.seeders,
            "leechers": record.leechers,
            "discovered_at": record.discovered_at,
            "enriched_at": record.enriched_at,
        });
        writeln!(file, "{}", line)?;
    }
    Ok(())
}
