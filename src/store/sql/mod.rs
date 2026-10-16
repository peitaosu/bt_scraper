mod discovery;
mod index;
mod peers;
mod row;
pub mod schema;
mod stats;
mod swarm;
mod torrent;

pub use discovery::{DiscoveryBatchResult, process_discovery_batch};
pub use index::{
    clear_all_indexed_flags, count_indexed, get_index_candidates, mark_indexed,
    mark_indexed_batch, IndexMark,
};
pub use peers::{
    get_peers, has_quality_peer, prune_stale_peers, record_peer_failures, upsert_peers,
};
pub use schema::{initialize_store, open_read, open_write, open_write_with_pragmas, SqlitePragmas};
pub use stats::{
    count_by_status, peer_breakdown, pipeline_snapshot, torrent_breakdown, PeerBreakdown,
    PipelineSnapshot, TorrentBreakdown,
};
pub use swarm::{get_scrape_candidates, update_tracker_stats_batch};
pub use torrent::{
    count_retry_queue, export_enriched_hashes, get_record, get_retry_candidates, is_blocked,
    is_enriched, mark_blocked, mark_enriching, mark_failed, requeue_failed, save_enriched,
    touch_peer_lookup,
};
