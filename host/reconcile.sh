#!/bin/sh
set -eu
BASE=/mnt/ssd/moneyflow
SCOPE="${1:-all}"; FINAL="${2:-false}"; EXTRA=""
[ "$FINAL" = true ] && EXTRA="--final"
/usr/bin/python3 "$BASE/scheduler.py" check >/dev/null 2>&1 || exit 0
docker run --rm --name "market-reconcile-$SCOPE" --env-file "$BASE/config/worker.env" -v "$BASE/data:/data" -v "$BASE/config:/config" ghcr.io/aklimzzh/market-data-worker:latest reconcile --scope "$SCOPE" $EXTRA >> "$BASE/logs/reconcile_$(date +%F).log" 2>&1
