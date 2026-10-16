mod read_pool;
mod sql;
mod writer;

use crate::{InfoHash, Peer, TorrentMetadata, TorrentRecord};
use anyhow::{Context, Result};
use read_pool::ReadPool;
use sql::schema::SqlitePragmas;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use writer::{StoreWriter, WriteOp, WriteReply};

pub use sql::DiscoveryBatchResult;
pub use sql::{IndexMark, PeerBreakdown, PipelineSnapshot, TorrentBreakdown};

#[derive(Clone)]
pub struct IndexCandidate {
    pub info_hash: InfoHash,
    pub metadata: TorrentMetadata,
    pub seeders: u32,
    pub leechers: u32,
    pub enriched_at: u64,
    pub discovered_at: u64,
    pub content_type: Option<String>,
    pub tags: Vec<String>,
}

pub struct Store {
    path: Arc<PathBuf>,
    writer: StoreWriter,
    reads: Arc<ReadPool>,
    peer_fail_threshold: u32,
}

impl Store {
    pub fn open(
        path: impl AsRef<Path>,
        write_batch_size: usize,
        write_flush_ms: u64,
        read_pool_size: u32,
        pragmas: SqlitePragmas,
        peer_fail_threshold: u32,
    ) -> Result<Arc<Self>> {
        let path = Arc::new(path.as_ref().to_path_buf());
        {
            let conn = sql::open_write_with_pragmas(&path, &pragmas)?;
            sql::initialize_store(&conn)?;
        }
        let reads = ReadPool::open(&path, read_pool_size)?;
        let writer = StoreWriter::spawn(
            path.as_ref().clone(),
            write_batch_size,
            write_flush_ms,
            pragmas,
        )?;
        Ok(Arc::new(Self {
            path,
            writer,
            reads,
            peer_fail_threshold,
        }))
    }

    async fn read<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&rusqlite::Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let reads = self.reads.clone();
        tokio::task::spawn_blocking(move || reads.with_conn(f))
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
        let _ = self
            .writer
            .write(WriteOp::UpsertPeers {
                info_hash: *info_hash,
                peers: peers.to_vec(),
            })
            .await?;
        Ok(())
    }

    pub async fn record_peer_failures(
        &self,
        info_hash: &InfoHash,
        addrs: &[std::net::SocketAddr],
    ) -> Result<()> {
        if addrs.is_empty() {
            return Ok(());
        }
        let _ = self.writer.write_fire_and_forget(WriteOp::RecordPeerFailures {
            info_hash: *info_hash,
            addrs: addrs.to_vec(),
        })?;
        Ok(())
    }

    pub fn open_from_config(
        store: &crate::config::StoreConfig,
        peer_fail_threshold: u32,
    ) -> Result<Arc<Self>> {
        Self::open(
            &store.path,
            store.write_batch_size,
            store.write_flush_ms,
            store.read_pool_size,
            sqlite_pragmas_from_config(store),
            peer_fail_threshold,
        )
    }

    pub async fn is_enriched(&self, info_hash: &InfoHash) -> Result<bool> {
        let hash = *info_hash;
        self.read(move |conn| sql::is_enriched(conn, &hash)).await
    }

    pub async fn get_peers(&self, info_hash: &InfoHash, limit: usize) -> Result<Vec<Peer>> {
        let hash = *info_hash;
        let threshold = self.peer_fail_threshold;
        self.read(move |conn| {
            Ok(sql::get_peers(conn, &hash, limit, threshold)?
                .into_iter()
                .map(|stored| stored.peer)
                .collect())
        })
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

    pub async fn save_enriched(
        &self,
        info_hash: &InfoHash,
        metadata: &TorrentMetadata,
        content_type: Option<&str>,
        tags: Option<&[String]>,
    ) -> Result<()> {
        let tags_json = tags.map(serde_json::to_string).transpose()?;
        self.writer
            .write(WriteOp::SaveEnriched {
                info_hash: *info_hash,
                metadata: metadata.clone(),
                content_type: content_type.map(str::to_string),
                tags_json,
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
        let hash = *info_hash;
        let threshold = self.peer_fail_threshold;
        self.read(move |conn| sql::has_quality_peer(conn, &hash, threshold))
            .await
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

    pub async fn pipeline_snapshot(&self, now: u64) -> Result<PipelineSnapshot> {
        self.read(move |conn| sql::pipeline_snapshot(conn, now)).await
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
        let _ = self
            .writer
            .write(WriteOp::MarkIndexed {
                info_hash: *info_hash,
                content_type: content_type.to_string(),
                tags: tags.to_vec(),
            })
            .await?;
        Ok(())
    }

    pub async fn mark_indexed_batch(&self, marks: Vec<IndexMark>) -> Result<()> {
        if marks.is_empty() {
            return Ok(());
        }
        self.writer
            .write(WriteOp::MarkIndexedBatch { marks })
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
        self.read(|conn| {
            let hashes = sql::export_enriched_hashes(conn)?;
            let mut records = Vec::with_capacity(hashes.len());
            for hash in hashes {
                if let Some(record) = sql::get_record(conn, &hash)? {
                    records.push(record);
                }
            }
            Ok(records)
        })
        .await
    }

    pub fn open_for_tests(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        Self::open(
            path,
            16,
            10,
            2,
            SqlitePragmas::default(),
            3,
        )
    }
}

pub fn sqlite_pragmas_from_config(config: &crate::config::StoreConfig) -> SqlitePragmas {
    SqlitePragmas {
        cache_size_kb: config.sqlite_cache_size_kb,
        mmap_size_bytes: config.sqlite_mmap_size_mb * 1024 * 1024,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PeerSource, unix_now};
    use std::net::{Ipv4Addr, SocketAddr};
    use tempfile::NamedTempFile;

    async fn test_store(path: &Path) -> Arc<Store> {
        Store::open(path, 32, 10, 2, SqlitePragmas::default(), 3).unwrap()
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
                None,
                None,
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
                None,
                None,
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
        assert_eq!(reindex.len(), 0);
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

    #[tokio::test]
    async fn record_peer_failures_updates_quality_gate() {
        let file = NamedTempFile::new().unwrap();
        let store = test_store(file.path()).await;
        let hash = InfoHash([8u8; 20]);
        store
            .process_discovery_batch(vec![(hash, None)])
            .await
            .unwrap();
        let addr = SocketAddr::from((Ipv4Addr::new(1, 2, 3, 8), 6888));
        let peer = Peer::new(addr, PeerSource::Dht);
        store.upsert_peers(&hash, &[peer]).await.unwrap();
        assert!(store.has_quality_peer(&hash).await.unwrap());
        for _ in 0..3 {
            store.record_peer_failures(&hash, &[addr]).await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!store.has_quality_peer(&hash).await.unwrap());
    }

    #[tokio::test]
    async fn mark_indexed_batch_via_writer() {
        let file = NamedTempFile::new().unwrap();
        let store = test_store(file.path()).await;
        let first = InfoHash([9u8; 20]);
        let second = InfoHash([10u8; 20]);
        for hash in [first, second] {
            store
                .process_discovery_batch(vec![(hash, None)])
                .await
                .unwrap();
            store
                .save_enriched(
                    &hash,
                    &TorrentMetadata {
                        name: "batch".into(),
                        files: vec![],
                        total_length: 1,
                    },
                    None,
                    None,
                )
                .await
                .unwrap();
        }
        store
            .mark_indexed_batch(vec![
                IndexMark {
                    info_hash: first,
                    content_type: "video".into(),
                    tags: vec!["tag".into()],
                },
                IndexMark {
                    info_hash: second,
                    content_type: "audio".into(),
                    tags: vec![],
                },
            ])
            .await
            .unwrap();
        assert_eq!(store.count_indexed().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn pipeline_snapshot_via_read_pool() {
        let file = NamedTempFile::new().unwrap();
        let store = test_store(file.path()).await;
        let hash = InfoHash([11u8; 20]);
        store
            .process_discovery_batch(vec![(hash, None)])
            .await
            .unwrap();
        let now = unix_now();
        store.mark_failed(&hash, now + 7200).await.unwrap();
        let snapshot = store.pipeline_snapshot(now).await.unwrap();
        assert_eq!(snapshot.total, 1);
        assert_eq!(snapshot.retry_queue, 0);
        assert_eq!(snapshot.torrents.failed, 1);
    }

    #[test]
    fn sqlite_pragmas_from_config_scales_mmap() {
        let store = crate::config::Config::load("config/default.toml")
            .unwrap()
            .store;
        let pragmas = sqlite_pragmas_from_config(&store);
        assert_eq!(pragmas.cache_size_kb, store.sqlite_cache_size_kb);
        assert_eq!(
            pragmas.mmap_size_bytes,
            store.sqlite_mmap_size_mb * 1024 * 1024
        );
    }
}
