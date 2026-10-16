use crate::classify::Classification;
use crate::{InfoHash, TorrentMetadata};
use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

pub struct PostgresMirror {
    pool: PgPool,
}

impl PostgresMirror {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .context("connect postgres")?;
        let mirror = Self { pool };
        mirror.init_schema().await?;
        Ok(mirror)
    }

    async fn init_schema(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS torrents (
                info_hash      BYTEA PRIMARY KEY,
                name           TEXT NOT NULL,
                files_json     TEXT,
                total_length   BIGINT,
                content_type   TEXT,
                tags_json      TEXT,
                seeders        BIGINT NOT NULL DEFAULT 0,
                leechers       BIGINT NOT NULL DEFAULT 0,
                enriched_at    BIGINT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_torrent(
        &self,
        info_hash: &InfoHash,
        metadata: &TorrentMetadata,
        classification: &Classification,
        seeders: u64,
        leechers: u64,
        enriched_at: u64,
    ) -> Result<()> {
        let files_json = serde_json::to_string(&metadata.files)?;
        let tags_json = serde_json::to_string(&classification.tags)?;
        sqlx::query(
            "INSERT INTO torrents (
                info_hash, name, files_json, total_length, content_type, tags_json,
                seeders, leechers, enriched_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (info_hash) DO UPDATE SET
                name = EXCLUDED.name,
                files_json = EXCLUDED.files_json,
                total_length = EXCLUDED.total_length,
                content_type = EXCLUDED.content_type,
                tags_json = EXCLUDED.tags_json,
                seeders = EXCLUDED.seeders,
                leechers = EXCLUDED.leechers,
                enriched_at = EXCLUDED.enriched_at",
        )
        .bind(&info_hash.0[..])
        .bind(&metadata.name)
        .bind(files_json)
        .bind(metadata.total_length as i64)
        .bind(&classification.content_type)
        .bind(tags_json)
        .bind(seeders as i64)
        .bind(leechers as i64)
        .bind(enriched_at as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
