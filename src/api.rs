use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use crate::backends::BackendContext;
use crate::classification_config::ClassificationRules;
use crate::config::SearchConfig;
use crate::search::{SearchHit, SearchIndex, SearchQuery};
use crate::store::Store;
use crate::InfoHash;
use metrics_exporter_prometheus::PrometheusHandle;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub search: Arc<SearchIndex>,
    pub search_config: SearchConfig,
    pub metrics: Option<PrometheusHandle>,
    pub backends: Arc<BackendContext>,
    pub rules: Arc<ClassificationRules>,
}

pub async fn health() -> &'static str {
    "ok"
}

pub async fn stats(State(state): State<AppState>) -> Json<serde_json::Value> {
    let (total, enriched, pending) = state.store.count_by_status().await.unwrap_or((0, 0, 0));
    let indexed = state.store.count_indexed().await.unwrap_or(0);
    let retry_queue = state
        .store
        .count_retry_queue(crate::unix_now())
        .await
        .unwrap_or(0);
    Json(json!({
        "total": total,
        "enriched": enriched,
        "pending": pending,
        "indexed": indexed,
        "retry_queue": retry_queue,
    }))
}

#[derive(Debug, Deserialize)]
pub struct SearchParams {
    pub q: Option<String>,
    #[serde(rename = "type")]
    pub content_type: Option<String>,
    pub tags: Option<String>,
    pub offset: Option<usize>,
    pub limit: Option<usize>,
    pub sort: Option<String>,
}

pub async fn search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let limit = params
        .limit
        .unwrap_or(state.search_config.default_limit)
        .min(state.search_config.max_limit)
        .max(1);
    let sort = params
        .sort
        .unwrap_or_else(|| state.search_config.default_sort.clone());
    let query = SearchQuery {
        q: params.q.unwrap_or_default(),
        content_type: params.content_type,
        tags: params
            .tags
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|tag| !tag.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        offset: params.offset.unwrap_or(0),
        limit,
        sort: sort.clone(),
    };

    if state.backends.meilisearch.is_some() && !query.q.is_empty() {
        if let Ok(Some(hits)) = state
            .backends
            .search_meilisearch(&query.q, limit, query.offset)
            .await
        {
            return Ok(Json(json!({
                "query": query.q,
                "offset": query.offset,
                "limit": limit,
                "sort": sort,
                "total": hits.len(),
                "results": hits,
                "engine": "meilisearch",
            })));
        }
    }

    let search = state.search.clone();
    let search_query = query.clone();
    let result = tokio::task::spawn_blocking(move || search.search(&search_query))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json!({
        "query": query.q,
        "offset": query.offset,
        "limit": limit,
        "sort": sort,
        "total": result.total,
        "results": result.hits,
        "engine": "tantivy",
    })))
}

pub async fn get_torrent(
    State(state): State<AppState>,
    Path(info_hash): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let hash = InfoHash::from_hex(&info_hash).ok_or(StatusCode::BAD_REQUEST)?;
    let record = state
        .store
        .get_record(&hash)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    if let Ok(Some(hit)) = state.search.get_by_hash(&hash) {
        let files = record
            .metadata
            .as_ref()
            .map(|metadata| json!(metadata.files));
        return Ok(Json(torrent_json(&hit, files)));
    }

    Ok(Json(json!({
        "info_hash": hash.to_hex(),
        "status": record.status,
        "seeders": record.seeders,
        "leechers": record.leechers,
        "discovered_at": record.discovered_at,
        "enriched_at": record.enriched_at,
        "metadata": record.metadata,
    })))
}

pub async fn metrics(State(state): State<AppState>) -> Result<impl IntoResponse, StatusCode> {
    let handle = state.metrics.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    Ok(handle.render())
}

fn torrent_json(hit: &SearchHit, files: Option<serde_json::Value>) -> serde_json::Value {
    json!({
        "info_hash": hit.info_hash,
        "name": hit.name,
        "content_type": hit.content_type,
        "tags": hit.tags,
        "total_length": hit.total_length,
        "seeders": hit.seeders,
        "leechers": hit.leechers,
        "enriched_at": hit.enriched_at,
        "score": hit.score,
        "files": files,
    })
}
