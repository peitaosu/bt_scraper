mod sql;
mod writer;

use crate::{InfoHash, Peer, TorrentMetadata, TorrentRecord};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use writer::{StoreWriter, WriteOp, WriteReply};

pub use sql::DiscoveryBatchResult;
pub use sql::{PeerBreakdown, TorrentBreakdown};

#[derive(Clone)]
pub struct IndexCandidate {
    pub info_hash: InfoHash,
    pub metadata: TorrentMetadata,
    pub seeders: u32,
    pub leechers: u32,
    pub enriched_at: u64,
    pub discovered_at: u64,
}

pub struct Store {
    path: Arc<PathBuf>,
    writer: StoreWriter,
}

impl Store {
    pub fn open(path: impl AsRef<Path>, write_batch_size: usize, write_flush_ms: u64) -> Result<Arc<Self>> {
        let path = Arc::new(path.as_ref().to_path_buf());
        {
            let conn = sql::open_write(&path)?;
            sql::initialize_store(&conn)?;
        }
        let writer = StoreWriter::spawn(path.as_ref().clone(), write_batch_size, write_flush_ms)?;
        Ok(Arc::new(Self { path, writer }))
    }

    async fn read<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&rusqlite::Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = sql::open_read(&path)?;
            f(&conn)
        })
        .await
        .context("store read task panicked")?
    }

    pub async fn process_discovery_batch(
        &self,
        items: Vec<(InfoHash, Option<Peer>)>,
    ) -> Result<DiscoveryBatchResult> {
        match self
            .writer
            .write(WriteOp::ProcessDiscoveryBatch { items })
            .await?
        {
            WriteReply::DiscoveryBatch(result) => Ok(result),
            _ => anyhow::bail!("unexpected store reply"),
        }
    }

    pub async fn upsert_peers(&self, info_hash: &InfoHash, peers: &[Peer]) -> Result<()> {
        if peers.is_empty() {
            return Ok(());
        }
        self.writer
            .write(WriteOp::UpsertPeers {
                info_hash: *info_hash,
                peers: peers.to_vec(),
            })
            .await?;
        Ok(())
    }

    pub async fn is_enriched(&self, info_hash: &InfoHash) -> Result<bool> {
        let hash = *info_hash;
        self.read(move |conn| sql::is_enriched(conn, &hash)).await
    }

    pub async fn get_peers(&self, info_hash: &InfoHash, limit: usize) -> Result<Vec<Peer>> {
        let hash = *info_hash;
        self.read(move |conn| sql::get_peers(conn, &hash, limit))
            .await
    }

    pub async fn mark_enriching(&self, info_hash: &InfoHash) -> Result<()> {
        self.writer
            .write(WriteOp::MarkEnriching {
                info_hash: *info_hash,
            })
            .await?;
        Ok(())
    }

    pub async fn save_enriched(&self, info_hash: &InfoHash, metadata: &TorrentMetadata) -> Result<()> {
        self.writer
            .write(WriteOp::SaveEnriched {
                info_hash: *info_hash,
                metadata: metadata.clone(),
            })
            .await?;
        Ok(())
    }

    pub async fn mark_failed(&self, info_hash: &InfoHash, next_retry_at: u64) -> Result<()> {
        self.writer
            .write(WriteOp::MarkFailed {
                info_hash: *info_hash,
                next_retry_at,
            })
            .await?;
        Ok(())
    }

    pub async fn mark_blocked(&self, info_hash: &InfoHash) -> Result<()> {
        self.writer
            .write(WriteOp::MarkBlocked {
                info_hash: *info_hash,
            })
            .await?;
        Ok(())
    }

    pub async fn is_blocked(&self, info_hash: &InfoHash) -> Result<bool> {
        let hash = *info_hash;
        self.read(move |conn| sql::is_blocked(conn, &hash)).await
    }

    pub async fn get_retry_candidates(&self, limit: usize, now: u64) -> Result<Vec<InfoHash>> {
        self.read(move |conn| sql::get_retry_candidates(conn, limit, now))
            .await
    }

    pub async fn has_quality_peer(&self, info_hash: &InfoHash) -> Result<bool> {
        let peers = self.get_peers(info_hash, 64).await?;
        Ok(peers.iter().any(|peer| crate::peer_policy::is_quality_peer(peer)))
    }

    pub async fn touch_peer_lookup(&self, info_hash: &InfoHash) -> Result<()> {
        let _ = self
            .writer
            .write_fire_and_forget(WriteOp::TouchPeerLookup {
                info_hash: *info_hash,
            })?;
        Ok(())
    }

    pub async fn get_record(&self, info_hash: &InfoHash) -> Result<Option<TorrentRecord>> {
        let hash = *info_hash;
        self.read(move |conn| sql::get_record(conn, &hash)).await
    }

    pub async fn count_by_status(&self) -> Result<(u64, u64, u64)> {
        self.read(sql::count_by_status).await
    }

    pub async fn peer_breakdown(&self) -> Result<PeerBreakdown> {
        self.read(sql::peer_breakdown).await
    }

    pub async fn torrent_breakdown(&self) -> Result<TorrentBreakdown> {
        self.read(sql::torrent_breakdown).await
    }

    pub async fn count_indexed(&self) -> Result<u64> {
        self.read(sql::count_indexed).await
    }

    pub async fn get_index_candidates(&self, limit: usize) -> Result<Vec<IndexCandidate>> {
        self.read(move |conn| sql::get_index_candidates(conn, limit))
            .await
    }

    pub async fn mark_indexed(
        &self,
        info_hash: &InfoHash,
        content_type: &str,
        tags: &[String],
    ) -> Result<()> {
        self.writer
            .write(WriteOp::MarkIndexed {
                info_hash: *info_hash,
                content_type: content_type.to_string(),
                tags: tags.to_vec(),
            })
            .await?;
        Ok(())
    }

    pub async fn get_scrape_candidates(
        &self,
        limit: usize,
        stale_before: u64,
    ) -> Result<Vec<InfoHash>> {
        self.read(move |conn| sql::get_scrape_candidates(conn, limit, stale_before))
            .await
    }

    pub async fn update_tracker_stats_batch(
        &self,
        updates: Vec<(InfoHash, u32, u32, u64)>,
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        self.writer
            .write(WriteOp::UpdateTrackerStatsBatch { updates })
            .await?;
        Ok(())
    }

    pub async fn clear_all_indexed_flags(&self) -> Result<u64> {
        match self.writer.write(WriteOp::ClearIndexedFlags).await? {
            WriteReply::U64(count) => Ok(count),
            _ => Ok(0),
        }
    }

    pub async fn prune_stale_peers(&self, older_than: u64) -> Result<u64> {
        match self
            .writer
            .write(WriteOp::PruneStalePeers { older_than })
            .await?
        {
            WriteReply::U64(count) => Ok(count),
            _ => Ok(0),
        }
    }

    pub async fn requeue_failed(
        &self,
        now: u64,
        max_attempts: u32,
        retry_after_secs: u64,
    ) -> Result<u64> {
        match self
            .writer
            .write(WriteOp::RequeueFailed {
                now,
                max_attempts,
                retry_after_secs,
            })
            .await?
        {
            WriteReply::U64(count) => Ok(count),
            _ => Ok(0),
        }
    }

    pub async fn count_retry_queue(&self, now: u64) -> Result<u64> {
        self.read(move |conn| sql::count_retry_queue(conn, now))
            .await
    }

    pub async fn export_enriched(&self) -> Result<Vec<TorrentRecord>> {
        let hashes = self.read(sql::export_enriched_hashes).await?;
        let mut records = Vec::new();
        for hash in hashes {
            if let Some(record) = self.get_record(&hash).await? {
                records.push(record);
            }
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PeerSource, unix_now};
    use std::net::{Ipv4Addr, SocketAddr};
    use tempfile::NamedTempFile;

    async fn test_store(path: &Path) -> Arc<Store> {
        Store::open(path, 32, 10).unwrap()
    }

    #[tokio::test]
    async fn discovers_and_enriches_torrent() {
        let file = NamedTempFile::new().unwrap();
        let store = test_store(file.path()).await;
        let hash = InfoHash([7u8; 20]);

        let batch = store
            .process_discovery_batch(vec![(hash, None)])
            .await
            .unwrap();
        assert_eq!(batch.newly_discovered, 1);
        assert!(!store.is_enriched(&hash).await.unwrap());

        store
            .save_enriched(
                &hash,
                &TorrentMetadata {
                    name: "example".into(),
                    files: vec![],
                    total_length: 1024,
                },
            )
            .await
            .unwrap();

        assert!(store.is_enriched(&hash).await.unwrap());
        let (total, enriched, pending) = store.count_by_status().await.unwrap();
        assert_eq!(total, 1);
        assert_eq!(enriched, 1);
        assert_eq!(pending, 0);

        let peer = Peer::new(
            SocketAddr::from((Ipv4Addr::new(1, 2, 3, 4), 6881)),
            PeerSource::Announce,
        );
        store.upsert_peers(&hash, &[peer]).await.unwrap();
        let peers = store.get_peers(&hash, 10).await.unwrap();
        assert_eq!(peers.len(), 1);
    }

    #[tokio::test]
    async fn index_and_scrape_lifecycle() {
        let file = NamedTempFile::new().unwrap();
        let store = test_store(file.path()).await;
        let hash = InfoHash([3u8; 20]);
        store
            .process_discovery_batch(vec![(hash, None)])
            .await
            .unwrap();
        store
            .save_enriched(
                &hash,
                &TorrentMetadata {
                    name: "sample.mkv".into(),
                    files: vec![],
                    total_length: 100,
                },
            )
            .await
            .unwrap();

        let candidates = store.get_index_candidates(10).await.unwrap();
        assert_eq!(candidates.len(), 1);

        store
            .mark_indexed(&hash, "video", &["hd".to_string()])
            .await
            .unwrap();
        assert_eq!(store.count_indexed().await.unwrap(), 1);

        store
            .update_tracker_stats_batch(vec![(hash, 5, 2, unix_now())])
            .await
            .unwrap();
        let stale_before = unix_now().saturating_sub(300);
        let scrape_candidates = store
            .get_scrape_candidates(10, stale_before)
            .await
            .unwrap();
        assert!(scrape_candidates.is_empty());

        let reindex = store.get_index_candidates(10).await.unwrap();
        assert_eq!(reindex.len(), 1);
    }

    #[tokio::test]
    async fn reset_stuck_enriching_on_open() {
        let file = NamedTempFile::new().unwrap();
        {
            let store = test_store(file.path()).await;
            let hash = InfoHash([4u8; 20]);
            store
                .process_discovery_batch(vec![(hash, None)])
                .await
                .unwrap();
            store.mark_enriching(&hash).await.unwrap();
        }
        {
            let store = test_store(file.path()).await;
            let record = store.get_record(&InfoHash([4u8; 20])).await.unwrap().unwrap();
            assert_eq!(record.status, crate::EnrichmentStatus::Discovered);
        }
    }
}
