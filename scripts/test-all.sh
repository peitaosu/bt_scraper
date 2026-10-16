#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> cargo test (unit + integration; network tests ignored)"
cargo test 2>&1

echo "==> all tests passed"
