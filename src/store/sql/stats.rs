use anyhow::Result;
use rusqlite::{params, Connection};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerBreakdown {
    pub announce: u64,
    pub dht: u64,
    pub tracker: u64,
}

impl PeerBreakdown {
    pub fn total(self) -> u64 {
        self.announce + self.dht + self.tracker
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TorrentBreakdown {
    pub discovered: u64,
    pub enriching: u64,
    pub failed: u64,
    pub blocked: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PipelineSnapshot {
    pub total: u64,
    pub enriched: u64,
    pub pending: u64,
    pub indexed: u64,
    pub retry_queue: u64,
    pub peers: PeerBreakdown,
    pub torrents: TorrentBreakdown,
}

pub fn count_by_status(conn: &Connection) -> Result<(u64, u64, u64)> {
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM torrents", [], |row| row.get(0))?;
    let enriched: i64 = conn.query_row(
        "SELECT COUNT(*) FROM torrents WHERE status = 'enriched'",
        [],
        |row| row.get(0),
    )?;
    let pending: i64 = conn.query_row(
        "SELECT COUNT(*) FROM torrents
         WHERE status IN ('discovered', 'failed', 'enriching')",
        [],
        |row| row.get(0),
    )?;
    Ok((total as u64, enriched as u64, pending as u64))
}

pub fn pipeline_snapshot(conn: &Connection, now: u64) -> Result<PipelineSnapshot> {
    let row: (i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64) = conn.query_row(
        "SELECT
            (SELECT COUNT(*) FROM torrents),
            (SELECT COUNT(*) FROM torrents WHERE status = 'enriched'),
            (SELECT COUNT(*) FROM torrents WHERE status IN ('discovered', 'failed', 'enriching')),
            (SELECT COUNT(*) FROM torrents WHERE indexed_at IS NOT NULL),
            (SELECT COUNT(*) FROM torrents
                WHERE status IN ('discovered', 'failed')
                  AND (next_retry_at IS NULL OR next_retry_at <= ?1)),
            (SELECT COUNT(*) FROM peers WHERE source = 'announce'),
            (SELECT COUNT(*) FROM peers WHERE source = 'dht'),
            (SELECT COUNT(*) FROM peers WHERE source = 'tracker'),
            (SELECT COUNT(*) FROM torrents WHERE status = 'discovered'),
            (SELECT COUNT(*) FROM torrents WHERE status = 'enriching'),
            (SELECT COUNT(*) FROM torrents WHERE status = 'failed'),
            (SELECT COUNT(*) FROM torrents WHERE status = 'blocked')",
        params![now as i64],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
                row.get(9)?,
                row.get(10)?,
                row.get(11)?,
            ))
        },
    )?;
    Ok(PipelineSnapshot {
        total: row.0 as u64,
        enriched: row.1 as u64,
        pending: row.2 as u64,
        indexed: row.3 as u64,
        retry_queue: row.4 as u64,
        peers: PeerBreakdown {
            announce: row.5 as u64,
            dht: row.6 as u64,
            tracker: row.7 as u64,
        },
        torrents: TorrentBreakdown {
            discovered: row.8 as u64,
            enriching: row.9 as u64,
            failed: row.10 as u64,
            blocked: row.11 as u64,
        },
    })
}

pub fn peer_breakdown(conn: &Connection) -> Result<PeerBreakdown> {
    let mut breakdown = PeerBreakdown::default();
    let mut stmt = conn.prepare("SELECT source, COUNT(*) FROM peers GROUP BY source")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    for row in rows {
        let (source, count) = row?;
        let count = count as u64;
        match source.as_str() {
            "announce" => breakdown.announce = count,
            "dht" => breakdown.dht = count,
            "tracker" => breakdown.tracker = count,
            _ => {}
        }
    }
    Ok(breakdown)
}

pub fn torrent_breakdown(conn: &Connection) -> Result<TorrentBreakdown> {
    let mut breakdown = TorrentBreakdown::default();
    let mut stmt = conn.prepare("SELECT status, COUNT(*) FROM torrents GROUP BY status")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    for row in rows {
        let (status, count) = row?;
        let count = count as u64;
        match status.as_str() {
            "discovered" => breakdown.discovered = count,
            "enriching" => breakdown.enriching = count,
            "failed" => breakdown.failed = count,
            "blocked" => breakdown.blocked = count,
            _ => {}
        }
    }
    Ok(breakdown)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InfoHash;

    #[test]
    fn wal_allows_read_while_write_connection_open() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let write_conn = super::super::schema::open_write(file.path()).unwrap();
        super::super::torrent::insert_discovered(&write_conn, &InfoHash([11u8; 20])).unwrap();
        let read_conn = super::super::schema::open_read(file.path()).unwrap();
        let (total, _, _) = count_by_status(&read_conn).unwrap();
        assert_eq!(total, 1);
    }

    #[test]
    fn pipeline_snapshot_aggregates_status_and_peers() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let conn = super::super::schema::open_write(file.path()).unwrap();
        let now = crate::unix_now();
        let ready = InfoHash([40u8; 20]);
        let queued = InfoHash([41u8; 20]);
        let deferred = InfoHash([42u8; 20]);
        super::super::torrent::insert_discovered(&conn, &ready).unwrap();
        super::super::torrent::insert_discovered(&conn, &queued).unwrap();
        super::super::torrent::insert_discovered(&conn, &deferred).unwrap();
        super::super::torrent::mark_failed(&conn, &deferred, now + 3600).unwrap();
        super::super::torrent::save_enriched(
            &conn,
            &ready,
            &crate::TorrentMetadata {
                name: "indexed.mkv".into(),
                files: vec![],
                total_length: 1,
            },
            None,
            None,
        )
        .unwrap();
        super::super::index::mark_indexed(&conn, &ready, "video", &[]).unwrap();
        let peer = crate::Peer::new(
            std::net::SocketAddr::from((std::net::Ipv4Addr::new(1, 2, 3, 5), 6885)),
            crate::PeerSource::Announce,
        );
        super::super::peers::upsert_peer(&conn, &ready, &peer).unwrap();

        let snapshot = pipeline_snapshot(&conn, now).unwrap();
        assert_eq!(snapshot.total, 3);
        assert_eq!(snapshot.enriched, 1);
        assert_eq!(snapshot.indexed, 1);
        assert_eq!(snapshot.retry_queue, 1);
        assert_eq!(snapshot.peers.announce, 1);
        assert_eq!(snapshot.torrents.discovered, 1);
        assert_eq!(snapshot.torrents.failed, 1);
    }
}
