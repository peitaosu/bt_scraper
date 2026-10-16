# Performance architecture (sections 1–10)

Unified layers shared by discovery, enrichment, scrape, and API.

## Layer map

```text
RuntimeServices (infra)
  ├── SharedHttp          → HTTP trackers, scrape, API backends
  ├── UdpPool             → BEP51 samples, UDP trackers (reuse sockets)
  ├── DnsCache            → tracker host resolution
  └── TrackerGateway      → fetch_peers / scrape_counts (single entry)

Store
  ├── Write thread        → batched ops, coalesced fire-and-forget
  ├── Read pool (r2d2)    → no per-query Connection::open
  └── Schema              → peer fail_count, torrent.quality_peer_count, indexes

Pipeline workers
  ├── discovery           → batch SQL, try_send enrich burst
  ├── enrichment          → PeerCollector (skip DHT if cached), TrackerGateway
  ├── indexer             → long-lived Tantivy writer, batch mark_indexed
  └── scrape              → shared tracker I/O, Tantivy swarm patch (no reindex flag)

Cross-cutting
  ├── peer_policy         → failure-aware ranking
  ├── classify            → Aho–Corasick blocklist; classify once at enrich
  └── metrics             → drops, failures, pool depth
```

## Config

All new knobs use `serde(default)` so existing TOML keeps working. Tunables live under `[dht]`, `[enrichment]`, `[store]`, `[index]`, `[api]`, `[tracker]`.

| Area | Examples (defaults apply if omitted) |
|------|--------------------------------------|
| `[dht]` | `lookup_timeout_secs`, `bep51_sample_timeout_secs`, `udp_pool_size`, `discovery_queue_capacity` |
| `[enrichment]` | `skip_dht_if_cached_peers`, `scheduler_lookup_concurrency`, `scheduler_batch_max`, `tracker_urls_per_lookup`, `peer_fail_threshold`, `global_tcp_limit`, `connect_timeout_secs`, `enrich_queue_capacity`, `stats_interval_secs` |
| `[store]` | `read_pool_size`, `sqlite_cache_size_kb`, `sqlite_mmap_size_mb` |
| `[index]` | `writer_heap_mb`, `reload_reader_on_commit`, `patch_swarm_on_scrape` |
| `[tracker]` | `http_timeout_secs` |
| `[api]` | `search_cache_secs` (reserved) |

## Data model

- `peers.fail_count`, `peers.last_fail_at` — deprioritize stale endpoints.
- `torrents.quality_peer_count` — denormalized; maintained on peer upsert/prune/fail reset.
- Scrape updates `seeders`/`leechers`/`tracker_at` only; search index gets optional swarm patch without clearing `indexed_at`.

## I/O patterns (DRY)

| Pattern | Used by |
|---------|---------|
| `UdpPool::acquire()` | BEP51 sampler, UDP tracker |
| `TrackerGateway::announce_peers` | enrichment scheduler, enrich_one |
| `TrackerGateway::scrape` | scrape worker |
| `Store::read_pool` | all `Store::read` |
| `WriteOp::*Batch` | discovery, index marks, scrape stats |
