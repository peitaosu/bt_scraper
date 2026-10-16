use super::sql;
use crate::{InfoHash, Peer, TorrentMetadata};
use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::Duration;
use tokio::sync::oneshot;

pub enum WriteOp {
    ProcessDiscoveryBatch {
        items: Vec<(InfoHash, Option<Peer>)>,
    },
    UpsertPeers {
        info_hash: InfoHash,
        peers: Vec<Peer>,
    },
    MarkEnriching {
        info_hash: InfoHash,
    },
    SaveEnriched {
        info_hash: InfoHash,
        metadata: TorrentMetadata,
    },
    MarkFailed {
        info_hash: InfoHash,
        next_retry_at: u64,
    },
    MarkBlocked {
        info_hash: InfoHash,
    },
    MarkIndexed {
        info_hash: InfoHash,
        content_type: String,
        tags: Vec<String>,
    },
    UpdateTrackerStatsBatch {
        updates: Vec<(InfoHash, u32, u32, u64)>,
    },
    PruneStalePeers {
        older_than: u64,
    },
    RequeueFailed {
        now: u64,
        max_attempts: u32,
        retry_after_secs: u64,
    },
    TouchPeerLookup {
        info_hash: InfoHash,
    },
    ClearIndexedFlags,
}

pub enum WriteReply {
    Unit,
    U64(u64),
    DiscoveryBatch(sql::DiscoveryBatchResult),
}

struct WriteRequest {
    op: WriteOp,
    reply: Option<oneshot::Sender<Result<WriteReply>>>,
}

pub struct StoreWriter {
    _join: thread::JoinHandle<()>,
    tx: mpsc::Sender<WriteRequest>,
}

impl StoreWriter {
    pub fn spawn(path: PathBuf, batch_size: usize, flush_ms: u64) -> Result<Self> {
        let (tx, rx) = mpsc::channel();
        let join = thread::Builder::new()
            .name("store-writer".into())
            .spawn(move || writer_loop(path, rx, batch_size, flush_ms.max(1)))
            .context("spawn store writer thread")?;
        Ok(Self { _join: join, tx })
    }

    pub async fn write(&self, op: WriteOp) -> Result<WriteReply> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WriteRequest {
                op,
                reply: Some(reply_tx),
            })
            .map_err(|_| anyhow::anyhow!("store writer stopped"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("store writer reply dropped"))?
    }

    pub fn write_fire_and_forget(&self, op: WriteOp) -> Result<()> {
        self.tx
            .send(WriteRequest { op, reply: None })
            .map_err(|_| anyhow::anyhow!("store writer stopped"))?;
        Ok(())
    }
}

fn writer_loop(
    path: PathBuf,
    rx: mpsc::Receiver<WriteRequest>,
    batch_size: usize,
    flush_ms: u64,
) {
    let conn = match sql::open_write(&path) {
        Ok(conn) => conn,
        Err(error) => {
            tracing::error!(%error, "store writer failed to open database");
            return;
        }
    };
    if let Err(error) = sql::initialize_store(&conn) {
        tracing::error!(%error, "store initialization failed");
        return;
    }

    let flush_interval = Duration::from_millis(flush_ms);
    let mut deferred: Vec<WriteRequest> = Vec::new();

    loop {
        let timeout = if deferred.is_empty() {
            None
        } else {
            Some(flush_interval)
        };

        let request = match timeout {
            Some(wait) => match rx.recv_timeout(wait) {
                Ok(req) => Some(req),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => {
                    flush_deferred(&conn, &mut deferred);
                    break;
                }
            },
            None => rx.recv().ok(),
        };

        if let Some(req) = request {
            if req.reply.is_some() {
                flush_deferred(&conn, &mut deferred);
                let response = execute_op(&conn, req.op);
                if let Some(reply) = req.reply {
                    let _ = reply.send(response);
                }
            } else {
                deferred.push(req);
                if deferred.len() >= batch_size {
                    flush_deferred(&conn, &mut deferred);
                }
            }
        } else {
            flush_deferred(&conn, &mut deferred);
        }
    }
}

fn flush_deferred(conn: &rusqlite::Connection, deferred: &mut Vec<WriteRequest>) {
    if deferred.is_empty() {
        return;
    }
    let batch: Vec<WriteRequest> = deferred.drain(..).collect();
    for req in batch {
        if let Err(error) = execute_op(conn, req.op) {
            tracing::warn!(%error, "deferred store write failed");
        }
    }
}

fn execute_op(conn: &rusqlite::Connection, op: WriteOp) -> Result<WriteReply> {
    match op {
        WriteOp::ProcessDiscoveryBatch { items } => {
            let result = sql::process_discovery_batch(conn, &items)?;
            Ok(WriteReply::DiscoveryBatch(result))
        }
        WriteOp::UpsertPeers { info_hash, peers } => {
            sql::upsert_peers(conn, &info_hash, &peers)?;
            Ok(WriteReply::Unit)
        }
        WriteOp::MarkEnriching { info_hash } => {
            sql::mark_enriching(conn, &info_hash)?;
            Ok(WriteReply::Unit)
        }
        WriteOp::SaveEnriched { info_hash, metadata } => {
            sql::save_enriched(conn, &info_hash, &metadata)?;
            Ok(WriteReply::Unit)
        }
        WriteOp::MarkFailed { info_hash, next_retry_at } => {
            sql::mark_failed(conn, &info_hash, next_retry_at)?;
            Ok(WriteReply::Unit)
        }
        WriteOp::MarkBlocked { info_hash } => {
            sql::mark_blocked(conn, &info_hash)?;
            Ok(WriteReply::Unit)
        }
        WriteOp::MarkIndexed {
            info_hash,
            content_type,
            tags,
        } => {
            sql::mark_indexed(conn, &info_hash, &content_type, &tags)?;
            Ok(WriteReply::Unit)
        }
        WriteOp::UpdateTrackerStatsBatch { updates } => {
            sql::update_tracker_stats_batch(conn, &updates)?;
            Ok(WriteReply::Unit)
        }
        WriteOp::PruneStalePeers { older_than } => {
            let count = sql::prune_stale_peers(conn, older_than)?;
            Ok(WriteReply::U64(count))
        }
        WriteOp::RequeueFailed {
            now,
            max_attempts,
            retry_after_secs,
        } => {
            let count = sql::requeue_failed(conn, now, max_attempts, retry_after_secs)?;
            Ok(WriteReply::U64(count))
        }
        WriteOp::TouchPeerLookup { info_hash } => {
            sql::touch_peer_lookup(conn, &info_hash)?;
            Ok(WriteReply::Unit)
        }
        WriteOp::ClearIndexedFlags => {
            let count = sql::clear_all_indexed_flags(conn)?;
            Ok(WriteReply::U64(count))
        }
    }
}
