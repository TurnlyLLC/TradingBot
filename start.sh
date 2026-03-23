#!/usr/bin/env bash
set -euo pipefail
mkdir -p /app/data /app/data/backups /app/data/logs
exec python railway_app.py
