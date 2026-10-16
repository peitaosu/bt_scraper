# Architecture

BitTorrent DHT indexer: one [mainline](https://docs.rs/mainline) DHT node (BEP 5 server on `dht.bind`), BEP 51 outbound sampling, metadata via `ut_metadata`, SQLite persistence, Tantivy search, optional Postgres/Meilisearch mirrors.

## Pipeline

```text
BEP51 / inbound get_peers / announce_peer
        → discovery batcher → SQLite (discovered)
        → enrichment scheduler + queue → metadata fetch → enriched | failed | blocked
        → indexer → Tantivy (+ mirrors) → indexed
        → tracker scrape → seeders/leechers refresh
        → HTTP API
```

Enrichment is not run immediately for BEP 51-only hashes; the scheduler enqueues once public peers exist (cached, DHT, or trackers). The scheduler uses `domain::pipeline::may_schedule_enrichment` for `discovered` and `failed` statuses only.

## Layout

| Module | Role |
|--------|------|
| `app` | Wires workers, HTTP server, graceful shutdown (`CancellationToken`) |
| `dht` | Single mainline node, BEP 51 outbound client (`krpc/`), routing persistence |
| `ports` | `DhtPeerLookup` trait; enrichment uses `Arc<dyn DhtPeerLookup>` |
| `workers` | Discovery batching, enrichment, indexer, tracker scrape loops |
| `enrich` | Stable re-exports of discovery/enrichment worker entrypoints |
| `indexer` / `scrape` (crate root) | Re-exports `workers::indexer` and `workers::scrape` |
| `store/sql/` | Schema, discovery, peers, torrent lifecycle, index, stats, swarm SQL |
| `search` | Tantivy full-text index |
| `domain/pipeline` | Enrichment eligibility helpers |
| `api` | Axum handlers |

## DHT

- One UDP socket and routing table via `mainline` server mode on `config.dht.bind`.
- Outbound BEP 51 uses ephemeral UDP sockets (indexer role per [BEP 51](https://www.bittorrent.org/beps/bep_0051.html)).
- Inbound `sample_infohashes` is **not** handled on the bound socket; mainline does not implement BEP 51 server mode. Indexers normally query others; answering samples is optional network participation. `SampleStore` and `krpc` encode helpers exist for tests and a future stack that supports inbound BEP 51.
- Inbound get_peers / announce_peer are parsed from mainline `RequestSpecific` via `Debug` text (mainline does not export request enum variants). Parsing expects mainline `Id` debug as 40-character hex inside `Id(...)`.
- Bootstrap nodes merge config bootstrap with persisted `dht.routing_table_path`.
- If `discovery_tx` fills, events are dropped and `discovery_dropped_total` increments.

## Configuration

| File | Use |
|------|-----|
| `config/default.toml` | Reference production defaults (`6881` / `8080`) |
| `config/test.toml` | Fast intervals, non-default DHT/API ports for tests |
| `config/classification.toml` | Blocklist, content types, tags |

All bind addresses and paths come from the chosen config file. Do not assume `6881` or `8080` unless that config sets them.

## Native service (optional)

Example user unit (`~/.config/systemd/user/bt-scraper.service`):

```ini
WorkingDirectory=/path/to/bt_scraper
ExecStart=/path/to/bt_scraper/target/release/bt-scraper run --config config/default.toml
```

After `cargo build --release`: `systemctl --user daemon-reload && systemctl --user restart bt-scraper`.

Logs (if configured in the unit): `logs/scraper.log` under the repo (gitignored).

## Docker

Image expects config at `/data/config.toml`. Copy and edit paths under `/data`:

```bash
cp config/default.toml data/config.toml
```

Reference defaults are shipped at `/etc/bt-scraper/config.toml`. Published ports follow that file (`6881/udp`, `8080/tcp` with stock default).

```bash
docker build -t bt-scraper .
docker compose up -d bt-scraper
```

## Tests

```bash
./scripts/test-all.sh          # unit + integration (59 tests; ignores live DHT)
./scripts/test-live.sh         # release binary + real DHT/API smoke (optional)
cargo test -- --ignored        # live BEP51 sample only (needs UDP)
```

See [CODE_MAP.md](CODE_MAP.md) for a full file index.

## Operational notes

- Enrichment WARNs (`metadata timeout`, `read handshake: early eof`) are normal for DHT sampling; many peers are stale or non-BitTorrent.
- Tune `enrichment.peer_timeout_secs` and `peers_per_hash` in config if enrich success rate is too low.
