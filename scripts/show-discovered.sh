#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DISCOVER_MIN="${DISCOVER_MIN:-20}"
WAIT_SECS="${WAIT_SECS:-180}"
ENRICH_WAIT="${ENRICH_WAIT:-60}"
API_URL="${API_URL:-http://127.0.0.1:18080}"

cargo build --release

RUN_DIR="$(mktemp -d)"
DB_PATH="$RUN_DIR/torrents.db"
INDEX_PATH="$RUN_DIR/search"
ROUTING_PATH="$RUN_DIR/routing.json"
CONFIG_PATH="$RUN_DIR/config.toml"

sed -e "s|data/test-torrents.db|$DB_PATH|" \
    -e "s|data/test-search|$INDEX_PATH|" \
    -e "s|data/test-routing.json|$ROUTING_PATH|" \
    "$ROOT/config/test.toml" > "$CONFIG_PATH"

pkill -f "bt-scraper run --config.*config.toml" 2>/dev/null || true
sleep 1

echo "==> starting scraper"
RUST_LOG="${RUST_LOG:-bt_scraper=info}" \
  "$ROOT/target/release/bt-scraper" run --config "$CONFIG_PATH" > "$RUN_DIR/scraper.log" 2>&1 &
PID=$!

cleanup() {
  kill "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  rm -rf "$RUN_DIR"
}
trap cleanup EXIT

echo "==> waiting up to ${WAIT_SECS}s for >= ${DISCOVER_MIN} discoveries"
TOTAL=0
for ((elapsed = 0; elapsed < WAIT_SECS; elapsed += 5)); do
  if curl -sf "$API_URL/health" >/dev/null 2>&1; then
    STATS="$(curl -sf "$API_URL/stats")"
    TOTAL="$(printf '%s' "$STATS" | python3 -c 'import json,sys; print(json.load(sys.stdin)["total"])')"
    ENRICHED="$(printf '%s' "$STATS" | python3 -c 'import json,sys; print(json.load(sys.stdin)["enriched"])')"
    INDEXED="$(printf '%s' "$STATS" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("indexed", 0))')"
    echo "  t=${elapsed}s total=${TOTAL} enriched=${ENRICHED} indexed=${INDEXED}"
    if [ "${TOTAL:-0}" -ge "$DISCOVER_MIN" ]; then
      break
    fi
  fi
  sleep 5
done

if [ "${TOTAL:-0}" -lt "$DISCOVER_MIN" ]; then
  echo "==> only found ${TOTAL:-0} torrents (wanted $DISCOVER_MIN)"
fi

if [ "$ENRICH_WAIT" -gt 0 ]; then
  echo "==> waiting ${ENRICH_WAIT}s for enrichment"
  sleep "$ENRICH_WAIT"
fi

echo "==> pipeline stats"
curl -sf "$API_URL/stats" | python3 -m json.tool

export DB_PATH API_URL
python3 << 'PYEOF'
import json
import sqlite3
import urllib.request
import datetime
import os

db = os.environ["DB_PATH"]
api = os.environ.get("API_URL", "http://127.0.0.1:18080")
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row

rows = conn.execute(
    """
    SELECT hex(info_hash) AS hash, status, name, total_length, seeders, leechers,
           attempt_count, discovered_at, enriched_at, next_retry_at, files_json,
           content_type, tags_json
    FROM torrents
    ORDER BY discovered_at ASC
    LIMIT 20
    """
).fetchall()

print(f"\n{'=' * 72}")
print(f"DISCOVERED TORRENTS ({len(rows)} shown)")
print(f"{'=' * 72}")

for i, row in enumerate(rows, 1):
    h = row["hash"].lower()
    disc = (
        datetime.datetime.utcfromtimestamp(row["discovered_at"]).isoformat() + "Z"
        if row["discovered_at"]
        else None
    )
    enrich = (
        datetime.datetime.utcfromtimestamp(row["enriched_at"]).isoformat() + "Z"
        if row["enriched_at"]
        else None
    )
    retry = (
        datetime.datetime.utcfromtimestamp(row["next_retry_at"]).isoformat() + "Z"
        if row["next_retry_at"]
        else None
    )
    peer_count = conn.execute(
        "SELECT COUNT(*) FROM peers WHERE info_hash = unhex(?)", (h,)
    ).fetchone()[0]
    peers = conn.execute(
        "SELECT source, hex(ip) AS ip_hex, port FROM peers WHERE info_hash = unhex(?) LIMIT 5",
        (h,),
    ).fetchall()

    print(f"\n#{i}  {h}")
    print(f"    status:        {row['status']}")
    print(f"    discovered_at: {disc}")
    print(f"    enriched_at:   {enrich or '-'}")
    print(f"    next_retry_at: {retry or '-'}")
    print(f"    attempts:      {row['attempt_count']}")
    print(f"    seeders:       {row['seeders']}  leechers: {row['leechers']}")
    if row["name"]:
        print(f"    name:          {row['name']}")
    if row["content_type"]:
        print(f"    content_type:  {row['content_type']}")
    if row["tags_json"]:
        try:
            tags = json.loads(row["tags_json"])
            if tags:
                print(f"    tags:          {', '.join(tags)}")
        except json.JSONDecodeError:
            pass
    if row["total_length"]:
        print(f"    total_length:  {row['total_length']:,} bytes")
    if row["files_json"]:
        try:
            files = json.loads(row["files_json"])
            print(f"    files:         {len(files)} file(s)")
            for f in files[:3]:
                path = "/".join(f.get("path", []))
                print(f"                   - {path} ({f.get('length', 0):,} bytes)")
            if len(files) > 3:
                print(f"                   ... and {len(files) - 3} more")
        except json.JSONDecodeError:
            pass
    print(f"    peers:         {peer_count} known")
    for p in peers:
        ip_hex = p["ip_hex"]
        if len(ip_hex) == 8:
            ip = ".".join(str(int(ip_hex[j : j + 2], 16)) for j in range(0, 8, 2))
        else:
            ip = ip_hex
        print(f"                   - {ip}:{p['port']} ({p['source']})")
    if peer_count > 5:
        print(f"                   ... and {peer_count - 5} more peers")

    try:
        with urllib.request.urlopen(f"{api}/torrent/{h}", timeout=3) as resp:
            api_data = json.loads(resp.read())
            if api_data.get("name"):
                print(f"    api_name:      {api_data['name']}")
    except Exception:
        print(f"    api:           not indexed yet")

conn.close()
PYEOF

echo ""
echo "==> API checks"
curl -sf "$API_URL/search?limit=1" >/dev/null && echo "  /search OK"
curl -sf "$API_URL/metrics" | grep -q hashes_discovered_total && echo "  /metrics OK"
