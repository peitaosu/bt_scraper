#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DISCOVER_MIN="${DISCOVER_MIN:-1}"
ENRICHED_MIN="${ENRICHED_MIN:-0}"
INDEXED_MIN="${INDEXED_MIN:-0}"
WAIT_SECS="${WAIT_SECS:-180}"

pick_free_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

DHT_PORT="${DHT_PORT:-$(pick_free_port)}"
API_PORT="${API_PORT:-$(pick_free_port)}"
API_URL="${API_URL:-http://127.0.0.1:${API_PORT}}"

echo "==> unit tests"
"$ROOT/scripts/test-all.sh"

echo "==> release build"
cargo build --release

RUN_DIR="$(mktemp -d)"
trap 'rm -rf "$RUN_DIR"' EXIT

DB_PATH="$RUN_DIR/torrents.db"
INDEX_PATH="$RUN_DIR/search"
ROUTING_PATH="$RUN_DIR/routing.json"
CONFIG_PATH="$RUN_DIR/config.toml"
sed -e "s|data/test-torrents.db|$DB_PATH|" \
    -e "s|data/test-search|$INDEX_PATH|" \
    -e "s|data/test-routing.json|$ROUTING_PATH|" \
    -e "s|0.0.0.0:16881|0.0.0.0:${DHT_PORT}|" \
    -e "s|127.0.0.1:18080|127.0.0.1:${API_PORT}|" \
    "$ROOT/config/test.toml" > "$CONFIG_PATH"
if [ -f "$ROOT/data/routing.json" ]; then
  cp "$ROOT/data/routing.json" "$ROUTING_PATH"
fi

echo "==> starting scraper (config: $CONFIG_PATH, dht_port=${DHT_PORT}, api_port=${API_PORT})"
RUST_LOG="${RUST_LOG:-bt_scraper=info}" \
  "$ROOT/target/release/bt-scraper" run --config "$CONFIG_PATH" &
PID=$!

cleanup() {
  if kill -0 "$PID" 2>/dev/null; then
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "==> waiting up to ${WAIT_SECS}s for DHT discovery"
TOTAL=0
ENRICHED=0
INDEXED=0
for ((elapsed = 0; elapsed < WAIT_SECS; elapsed += 5)); do
  if curl -sf "$API_URL/health" >/dev/null 2>&1; then
    STATS="$(curl -sf "$API_URL/stats")"
  else
    sleep 5
    continue
  fi

  if command -v python3 >/dev/null 2>&1; then
    TOTAL="$(printf '%s' "$STATS" | python3 -c 'import json,sys; print(json.load(sys.stdin)["total"])')"
    ENRICHED="$(printf '%s' "$STATS" | python3 -c 'import json,sys; print(json.load(sys.stdin)["enriched"])')"
    INDEXED="$(printf '%s' "$STATS" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("indexed", 0))')"
  else
    TOTAL="$(printf '%s' "$STATS" | sed -n 's/.*"total":\([0-9]*\).*/\1/p')"
    ENRICHED="$(printf '%s' "$STATS" | sed -n 's/.*"enriched":\([0-9]*\).*/\1/p')"
    INDEXED="$(printf '%s' "$STATS" | sed -n 's/.*"indexed":\([0-9]*\).*/\1/p')"
  fi

  echo "  t=${elapsed}s total=${TOTAL} enriched=${ENRICHED} indexed=${INDEXED}"

  if [ "${TOTAL:-0}" -ge "$DISCOVER_MIN" ] && [ "${ENRICHED:-0}" -ge "$ENRICHED_MIN" ] && [ "${INDEXED:-0}" -ge "$INDEXED_MIN" ]; then
    echo "==> checking API endpoints"
    curl -sf "$API_URL/search?limit=1" >/dev/null
    curl -sf "$API_URL/metrics" | grep -q "hashes_discovered_total"
    echo "==> PASS live test"
    echo "     discovered >= $DISCOVER_MIN, enriched >= $ENRICHED_MIN, indexed >= $INDEXED_MIN"
    exit 0
  fi
  sleep 5
done

echo "==> FAIL live test"
echo "     expected total >= $DISCOVER_MIN enriched >= $ENRICHED_MIN indexed >= $INDEXED_MIN within ${WAIT_SECS}s"
echo "     last stats: total=${TOTAL:-0} enriched=${ENRICHED:-0} indexed=${INDEXED:-0}"
exit 1
