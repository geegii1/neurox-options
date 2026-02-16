#!/bin/zsh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PY="$ROOT/.venv/bin/python"
mkdir -p "$ROOT/state"

SLEEP_SEC="${TICK_SLEEP_SEC:-2}"

while true; do
  if ! "$PY" -m services.tick >/dev/null 2>>"$ROOT/state/tick_loop.err.log"; then
    echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") tick_failed rc=$?" >> "$ROOT/state/tick_loop.err.log"
  fi
  sleep "$SLEEP_SEC"
done
