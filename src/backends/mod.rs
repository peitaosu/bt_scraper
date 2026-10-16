pub mod meilisearch;
pub mod postgres;

use crate::classify::Classification;
use crate::search::SearchHit;
use crate::{InfoHash, TorrentMetadata};
use anyhow::Result;
use std::sync::Arc;

pub struct BackendContext {
    pub postgres: Option<Arc<postgres::PostgresMirror>>,
    pub meilisearch: Option<Arc<meilisearch::MeilisearchMirror>>,
}

impl BackendContext {
    pub async fn from_config(config: &crate::config::Config) -> Result<Self> {
        let postgres = if config.postgres.enabled {
            Some(Arc::new(
                postgres::PostgresMirror::connect(&config.postgres.url).await?,
            ))
        } else {
            None
        };
        let meilisearch = if config.meilisearch.enabled {
            Some(Arc::new(meilisearch::MeilisearchMirror::new(
                config.meilisearch.clone(),
            )?))
        } else {
            None
        };
        Ok(Self {
            postgres,
            meilisearch,
        })
    }

    pub async fn on_indexed(
        &self,
        info_hash: &InfoHash,
        metadata: &TorrentMetadata,
        classification: &Classification,
        seeders: u64,
        leechers: u64,
        enriched_at: u64,
    ) -> Result<()> {
        if let Some(postgres) = &self.postgres {
            postgres
                .upsert_torrent(
                    info_hash,
                    metadata,
                    classification,
                    seeders,
                    leechers,
                    enriched_at,
                )
                .await?;
        }
        if let Some(meilisearch) = &self.meilisearch {
            meilisearch
                .upsert_document(
                    info_hash,
                    metadata,
                    classification,
                    seeders,
                    leechers,
                    enriched_at,
                )
                .await?;
        }
        Ok(())
    }

    pub async fn search_meilisearch(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Option<Vec<SearchHit>>> {
        if let Some(meilisearch) = &self.meilisearch {
            return Ok(Some(
                meilisearch.search(query, limit, offset).await?,
            ));
        }
        Ok(None)
    }
}
