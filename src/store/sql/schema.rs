use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

pub fn open_write(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(path).context("open database")?;
    init_schema(&conn)?;
    Ok(conn)
}

pub fn open_read(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .context("open read database")?;
    Ok(conn)
}

pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA busy_timeout = 5000;

        CREATE TABLE IF NOT EXISTS torrents (
            info_hash      BLOB PRIMARY KEY,
            status         TEXT NOT NULL,
            name           TEXT,
            files_json     TEXT,
            total_length   INTEGER,
            content_type   TEXT,
            tags_json      TEXT,
            seeders        INTEGER NOT NULL DEFAULT 0,
            leechers       INTEGER NOT NULL DEFAULT 0,
            attempt_count  INTEGER NOT NULL DEFAULT 0,
            discovered_at  INTEGER NOT NULL,
            enriched_at    INTEGER,
            next_retry_at  INTEGER,
            tracker_at     INTEGER,
            indexed_at     INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_torrents_status ON torrents(status);
        CREATE INDEX IF NOT EXISTS idx_torrents_retry ON torrents(next_retry_at)
            WHERE status IN ('discovered', 'failed');

        CREATE TABLE IF NOT EXISTS peers (
            info_hash   BLOB NOT NULL,
            ip          BLOB NOT NULL,
            port        INTEGER NOT NULL,
            source      TEXT NOT NULL,
            seen_at     INTEGER NOT NULL,
            PRIMARY KEY (info_hash, ip, port)
        );

        CREATE INDEX IF NOT EXISTS idx_peers_hash ON peers(info_hash);
        ",
    )?;
    let _ = conn.execute(
        "ALTER TABLE torrents ADD COLUMN peer_lookup_at INTEGER",
        [],
    );
    Ok(())
}

pub fn reset_stuck_enriching(conn: &Connection) -> Result<()> {
    conn.execute(
        "UPDATE torrents SET status = 'discovered'
         WHERE status = 'enriching'",
        [],
    )?;
    Ok(())
}

pub fn initialize_store(conn: &Connection) -> Result<()> {
    reset_stuck_enriching(conn)?;
    if let Ok(count) = super::torrent::reset_failed_without_quality_peers(conn) {
        if count > 0 {
            tracing::info!(count, "reset failed torrents without quality peers");
        }
    }
    Ok(())
}
