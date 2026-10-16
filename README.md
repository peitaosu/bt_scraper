# bt-scraper

BitTorrent DHT indexer in Rust: discover infohashes, fetch metadata, index for search, and track swarm counts.

## Build

```bash
cargo build --release
```

## Run

```bash
mkdir -p data logs
./target/release/bt-scraper run --config config/default.toml
```

UDP `dht.bind` and TCP `api.bind` come from the config file (defaults in `config/default.toml`: `6881` and `8080`). Use `config/test.toml` or a local copy with different binds when those ports are already in use.

## CLI

```bash
./target/release/bt-scraper stats --config config/default.toml
./target/release/bt-scraper backfill --config config/default.toml
./target/release/bt-scraper reindex --config config/default.toml
./target/release/bt-scraper export --config config/default.toml --output export.jsonl
```

## API

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness |
| `GET /stats` | Pipeline counters |
| `GET /search?q=&sort=seeders` | Full-text search |
| `GET /torrent/{info_hash}` | Torrent detail with files |
| `GET /metrics` | Prometheus metrics |

Base URL is `http://<api.bind>/` from your config (e.g. `http://127.0.0.1:18080` when using `config/test.toml`).

## Docker

```bash
docker build -t bt-scraper .
docker compose up -d bt-scraper
```

On first run, copy `config/default.toml` to `data/config.toml` and set `store.path`, `index.path`, and `dht.routing_table_path` under `/data`.

## Tests

```bash
./scripts/test-all.sh
./scripts/test-live.sh
cargo test -- --ignored
```

See [Architecture.md](Architecture.md) for pipeline, modules, and deployment notes. See [CODE_MAP.md](CODE_MAP.md) for the file index.
