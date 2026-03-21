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

echo "Running lint (ruff)..."
"${PYTHON}" -m ruff check .

echo "Running type checks (mypy)..."
"${PYTHON}" -m mypy

echo "Running security scan (bandit)..."
"${PYTHON}" -m bandit -c pyproject.toml -r petscan petscan_endpoint manage.py

echo "Running dependency audit (pip-audit)..."
"${PYTHON}" -m pip_audit -r requirements.txt --cache-dir /tmp/pip-audit-cache

echo "Resetting coverage data..."
"${PYTHON}" -m coverage erase

echo "Running Django tests with coverage..."
"${PYTHON}" -m coverage run --parallel-mode manage.py test

echo "Running API snapshot tests with coverage..."
"${PYTHON}" -m coverage run --parallel-mode -m pytest tests/test_api_snapshots.py

echo "Combining coverage data..."
"${PYTHON}" -m coverage combine

echo "Coverage report..."
"${PYTHON}" -m coverage report -m
