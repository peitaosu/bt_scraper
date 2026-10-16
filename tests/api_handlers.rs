use bt_scraper::backends::BackendContext;
use bt_scraper::classification_config::ClassificationRules;
use bt_scraper::config::SearchConfig;
use bt_scraper::search::SearchIndex;
use bt_scraper::store::Store;
use bt_scraper::{api, InfoHash};
use std::sync::Arc;
use tempfile::{NamedTempFile, TempDir};

fn empty_backends() -> Arc<BackendContext> {
    Arc::new(BackendContext {
        postgres: None,
        meilisearch: None,
    })
}

async fn app_state(store: Arc<Store>) -> api::AppState {
    let search_dir = TempDir::new().unwrap();
    api::AppState {
        store,
        search: Arc::new(SearchIndex::open(search_dir.path()).unwrap()),
        search_config: SearchConfig {
            default_limit: 20,
            max_limit: 100,
            default_sort: "score".into(),
        },
        metrics: None,
        backends: empty_backends(),
        rules: ClassificationRules::load("config/classification.toml").unwrap(),
    }
}

#[tokio::test]
async fn health_returns_ok() {
    assert_eq!(api::health().await, "ok");
}

#[tokio::test]
async fn stats_reflects_store_counts() {
    let file = NamedTempFile::new().unwrap();
    let store = Store::open(file.path(), 16, 10).unwrap();
    store
        .process_discovery_batch(vec![
            (InfoHash([1u8; 20]), None),
            (InfoHash([2u8; 20]), None),
        ])
        .await
        .unwrap();
    let state = app_state(store).await;
    let json = api::stats(axum::extract::State(state)).await.0;
    assert_eq!(json["total"], 2);
    assert_eq!(json["enriched"], 0);
    assert_eq!(json["pending"], 2);
    assert_eq!(json["indexed"], 0);
}

#[tokio::test]
async fn search_empty_index_returns_zero_total() {
    let file = NamedTempFile::new().unwrap();
    let store = Store::open(file.path(), 16, 10).unwrap();
    let state = app_state(store).await;
    let response = api::search(
        axum::extract::State(state),
        axum::extract::Query(api::SearchParams {
            q: None,
            content_type: None,
            tags: None,
            offset: None,
            limit: Some(5),
            sort: Some("seeders".into()),
        }),
    )
    .await
    .unwrap()
    .0;
    assert_eq!(response["total"], 0);
    assert!(response["results"].as_array().unwrap().is_empty());
}
