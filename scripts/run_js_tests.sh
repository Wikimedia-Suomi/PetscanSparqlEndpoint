#!/usr/bin/env bash
set -euo pipefail

if [[ -x ".venv/bin/python" ]]; then
  PYTHON=".venv/bin/python"
else
  PYTHON="python3"
fi

: "${DJANGO_SECRET_KEY:?DJANGO_SECRET_KEY is required}"
: "${OXIGRAPH_BASE_DIR:?OXIGRAPH_BASE_DIR is required}"

mkdir -p "${OXIGRAPH_BASE_DIR}"

echo "Running browser-executed JavaScript helper tests..."
"${PYTHON}" -m pytest tests/test_js_helpers_browser.py "$@"
