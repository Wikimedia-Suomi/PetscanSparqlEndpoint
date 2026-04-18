#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Expected Python interpreter at $PYTHON_BIN" >&2
  exit 1
fi

export DJANGO_SECRET_KEY="${DJANGO_SECRET_KEY:-dev-only-change-me}"
export OXIGRAPH_BASE_DIR="${OXIGRAPH_BASE_DIR:-$ROOT_DIR/data/oxigraph}"

cd "$ROOT_DIR"
exec "$PYTHON_BIN" manage.py benchmark_example_datasets "$@"
