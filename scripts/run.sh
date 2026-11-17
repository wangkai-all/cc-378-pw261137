#!/usr/bin/env bash
# Start the API + web console.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p data
if [ ! -f data/app.sqlite3 ]; then
  .venv/bin/python scripts/seed.py
fi
exec .venv/bin/uvicorn app.api:app --host 0.0.0.0 --port "${PORT:-8000}"
