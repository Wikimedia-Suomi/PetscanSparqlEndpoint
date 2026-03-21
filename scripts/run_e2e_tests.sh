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

E2E_OXIGRAPH_BASE_DIR="$(mktemp -d "${OXIGRAPH_BASE_DIR%/}/playwright-e2e.XXXXXX")"
trap 'rm -rf "${E2E_OXIGRAPH_BASE_DIR}"' EXIT

export OXIGRAPH_BASE_DIR="${E2E_OXIGRAPH_BASE_DIR}"
: "${PLAYWRIGHT_DEFAULT_TIMEOUT_MS:=60000}"
: "${PETSCAN_E2E_EXPECT_TIMEOUT_MS:=${PLAYWRIGHT_DEFAULT_TIMEOUT_MS}}"
: "${PETSCAN_E2E_PSID:=43641756}"
: "${PETSCAN_E2E_OUTPUT_LIMIT:=5}"

echo "Running Playwright E2E tests against live PetScan..."
echo "Using psid=${PETSCAN_E2E_PSID} output_limit=${PETSCAN_E2E_OUTPUT_LIMIT}"
"${PYTHON}" -m pytest tests/test_playwright_e2e.py "$@"
