use anyhow::Result;
use rusqlite::Connection;

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
}
