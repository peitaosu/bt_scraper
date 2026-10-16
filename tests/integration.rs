use bt_scraper::classification_config::ClassificationRules;
use bt_scraper::classify::classify;
use bt_scraper::config::Config;
use bt_scraper::search::{SearchIndex, SearchQuery};
use bt_scraper::store::Store;
use bt_scraper::{InfoHash, TorrentMetadata};
use std::time::Duration;
use tempfile::{NamedTempFile, TempDir};

#[test]
fn pipeline_config_files_are_valid() {
    let _ = Config::load("config/default.toml").unwrap();
    let _ = Config::load("config/test.toml").unwrap();
    let _ = Config::load("config/b550.toml").unwrap();
    let _ = ClassificationRules::load("config/classification.toml").unwrap();
}

#[tokio::test]
async fn store_survives_reopen() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_path_buf();
    {
        let store = Store::open_for_tests(&path).unwrap();
        store
            .process_discovery_batch(vec![(InfoHash([1u8; 20]), None)])
            .await
            .unwrap();
    }
    {
        let store = Store::open_for_tests(&path).unwrap();
        let (total, _, _) = store.count_by_status().await.unwrap();
        assert_eq!(total, 1);
    }
}

#[test]
fn search_index_roundtrip() {
    let dir = TempDir::new().unwrap();
    let search = SearchIndex::open(dir.path()).unwrap();
    let rules = ClassificationRules::load("config/classification.toml").unwrap();
    let hash = InfoHash([2u8; 20]);
    let metadata = TorrentMetadata {
        name: "Debian ISO".into(),
        files: vec![],
        total_length: 500,
    };
    let classification = classify(&metadata, &rules);
    search
        .with_writer(|writer| {
            search.upsert(
                writer,
                &hash,
                &metadata,
                &classification,
                1,
                0,
                1,
                1,
            )?;
            writer.commit()?;
            Ok(())
        })
        .unwrap();
    search.reload().unwrap();

    let result = search
        .search(&SearchQuery {
            q: "Debian".into(),
            content_type: None,
            tags: vec![],
            offset: 0,
            limit: 5,
            sort: "score".into(),
        })
        .unwrap();
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].info_hash, hash.to_hex());
}

#[tokio::test]
async fn store_mark_indexed_batch_and_snapshot() {
    let file = NamedTempFile::new().unwrap();
    let store = Store::open_for_tests(file.path()).unwrap();
    let hash = InfoHash([12u8; 20]);
    store
        .process_discovery_batch(vec![(hash, None)])
        .await
        .unwrap();
    store
        .save_enriched(
            &hash,
            &TorrentMetadata {
                name: "integration.mkv".into(),
                files: vec![],
                total_length: 42,
            },
            Some("video"),
            Some(&["show".to_string()]),
        )
        .await
        .unwrap();
    store
        .mark_indexed_batch(vec![bt_scraper::store::IndexMark {
            info_hash: hash,
            content_type: "video".into(),
            tags: vec!["show".into()],
        }])
        .await
        .unwrap();
    let snapshot = store.pipeline_snapshot(bt_scraper::unix_now()).await.unwrap();
    assert_eq!(snapshot.indexed, 1);
    assert_eq!(snapshot.enriched, 1);
}

#[tokio::test]
#[ignore = "requires outbound UDP to the public DHT; run: cargo test -- --ignored dht_bootstrap"]
async fn dht_bootstrap_and_sample() {
    use bt_scraper::dht::{build_ephemeral_dht_node, discover_sample_hashes};
    use bt_scraper::routing::load_routing_table;

    let bootstrap = vec![
        "router.bittorrent.com:6881".to_string(),
        "dht.transmissionbt.com:6881".to_string(),
    ];
    let persisted = load_routing_table("data/routing.json");
    let dht = build_ephemeral_dht_node(&bootstrap, &persisted).expect("dht node");

    let bootstrap_timeout = Duration::from_secs(120);
    let started = std::time::Instant::now();
    while started.elapsed() < bootstrap_timeout {
        if dht.bootstrapped().await {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    assert!(
        dht.bootstrapped().await,
        "dht failed to bootstrap within {bootstrap_timeout:?}"
    );

    let hashes = discover_sample_hashes(&dht, Duration::from_secs(120), 1)
        .await
        .expect("bep51 sample discovery");
    assert!(!hashes.is_empty());
}
