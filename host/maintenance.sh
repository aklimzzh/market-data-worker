#!/bin/sh
set -eu

BASE_DIR="/mnt/ssd/moneyflow"
CONFIG_DIR="$BASE_DIR/config"
DATA_DIR="$BASE_DIR/data"
LOG_DIR="$BASE_DIR/logs"
SCHEDULER="$BASE_DIR/scheduler.py"
IMAGE="ghcr.io/aklimzzh/market-data-worker:latest"
CONTAINER="market-maintenance"

mkdir -p "$CONFIG_DIR" "$DATA_DIR/status" "$DATA_DIR/archive" "$LOG_DIR"

TRADE_DATE="$(/usr/bin/python3 "$SCHEDULER" previous)"
LOG_FILE="$LOG_DIR/maintenance_${TRADE_DATE}.log"

echo "==================================================" >> "$LOG_FILE"
echo "$(date '+%Y-%m-%d %H:%M:%S') maintenance started date=$TRADE_DATE" >> "$LOG_FILE"

docker rm -f "$CONTAINER" >/dev/null 2>&1 || true

docker run --rm \
  --name "$CONTAINER" \
  --env-file "$CONFIG_DIR/worker.env" \
  -v "$DATA_DIR:/data" \
  -v "$CONFIG_DIR:/config" \
  "$IMAGE" \
  maintenance --date "$TRADE_DATE" --sleep 0.5 >> "$LOG_FILE" 2>&1
RC=$?

echo "$(date '+%Y-%m-%d %H:%M:%S') maintenance finished rc=$RC" >> "$LOG_FILE"
exit "$RC"
