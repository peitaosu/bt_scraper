use bt_scraper::backends::BackendContext;
use bt_scraper::classification_config::ClassificationRules;
use bt_scraper::config::IndexConfig;
use bt_scraper::indexer::index_batch;
use bt_scraper::search::{SearchIndex, SearchQuery};
use bt_scraper::store::Store;
use bt_scraper::{InfoHash, TorrentMetadata};
use std::sync::Arc;
use tempfile::{NamedTempFile, TempDir};

#[tokio::test]
async fn index_batch_indexes_enriched_torrents() {
    let db = NamedTempFile::new().unwrap();
    let index_dir = TempDir::new().unwrap();
    let store = Store::open_for_tests(db.path()).unwrap();
    let search = Arc::new(SearchIndex::open(index_dir.path()).unwrap());
    let rules = ClassificationRules::load("config/classification.toml").unwrap();
    let backends = Arc::new(BackendContext {
        postgres: None,
        meilisearch: None,
    });

    let hash = InfoHash([20u8; 20]);
    store
        .process_discovery_batch(vec![(hash, None)])
        .await
        .unwrap();
    store
        .save_enriched(
            &hash,
            &TorrentMetadata {
                name: "Ubuntu 24.04 Desktop".into(),
                files: vec![],
                total_length: 5_000_000_000,
            },
            None,
            None,
        )
        .await
        .unwrap();

    let config = IndexConfig {
        path: index_dir.path().to_string_lossy().into_owned(),
        batch_size: 10,
        poll_interval_secs: 1,
        commit_batch: 5,
        writer_heap_mb: 50,
        reload_reader_on_commit: true,
        patch_swarm_on_scrape: true,
    };

    let indexed = index_batch(&store, &search, &rules, &backends, &config, 10)
        .await
        .unwrap();
    assert_eq!(indexed, 1);
    assert_eq!(store.count_indexed().await.unwrap(), 1);

    let result = search
        .search(&SearchQuery {
            q: "Ubuntu".into(),
            content_type: None,
            tags: vec![],
            offset: 0,
            limit: 10,
            sort: "score".into(),
        })
        .unwrap();
    assert_eq!(result.total, 1);
    assert_eq!(result.hits[0].info_hash, hash.to_hex());
}

#[tokio::test]
async fn tracker_scrape_update_keeps_index_queue_empty() {
    let db = NamedTempFile::new().unwrap();
    let store = Store::open_for_tests(db.path()).unwrap();
    let hash = InfoHash([21u8; 20]);
    store
        .process_discovery_batch(vec![(hash, None)])
        .await
        .unwrap();
    store
        .save_enriched(
            &hash,
            &TorrentMetadata {
                name: "reindex-me.mkv".into(),
                files: vec![],
                total_length: 100,
            },
            None,
            None,
        )
        .await
        .unwrap();
    store
        .mark_indexed(&hash, "video", &["hd".to_string()])
        .await
        .unwrap();
    assert_eq!(store.count_indexed().await.unwrap(), 1);

    store
        .update_tracker_stats_batch(vec![(hash, 99, 1, bt_scraper::unix_now())])
        .await
        .unwrap();

    let candidates = store.get_index_candidates(10).await.unwrap();
    assert!(candidates.is_empty());
    let record = store.get_record(&hash).await.unwrap().unwrap();
    assert_eq!(record.seeders, 99);
}

#[tokio::test]
async fn blocked_torrents_never_reach_index() {
    let db = NamedTempFile::new().unwrap();
    let store = Store::open_for_tests(db.path()).unwrap();
    let hash = InfoHash([22u8; 20]);
    store
        .process_discovery_batch(vec![(hash, None)])
        .await
        .unwrap();
    store.mark_blocked(&hash).await.unwrap();

    let candidates = store.get_index_candidates(10).await.unwrap();
    assert!(candidates.is_empty());
    let record = store.get_record(&hash).await.unwrap().unwrap();
    assert_eq!(record.status, bt_scraper::EnrichmentStatus::Blocked);
}

#[tokio::test]
async fn store_handles_concurrent_reads() {
    let db = NamedTempFile::new().unwrap();
    let store = Store::open_for_tests(db.path()).unwrap();
    for i in 0u8..20 {
        store
            .process_discovery_batch(vec![(InfoHash([i; 20]), None)])
            .await
            .unwrap();
    }

    let mut handles = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            store.count_by_status().await.unwrap()
        }));
    }
    for handle in handles {
        let (total, _, _) = handle.await.unwrap();
        assert_eq!(total, 20);
    }
}
