#!/bin/sh
BASE_DIR="/mnt/ssd/moneyflow"
CONFIG_DIR="$BASE_DIR/config"
DATA_DIR="$BASE_DIR/data"
LOG_DIR="$BASE_DIR/logs"
STATUS_DIR="$BASE_DIR/status"
IMAGE="ghcr.io/aklimzzh/market-data-worker:latest"
CONTAINER="market-collector"
SCHEDULER="$BASE_DIR/scheduler.py"

mkdir -p "$CONFIG_DIR" "$DATA_DIR/raw" "$DATA_DIR/archive" "$DATA_DIR/pending" "$LOG_DIR" "$STATUS_DIR"

echo "$(date '+%Y-%m-%d %H:%M:%S') collector start check" >> "$LOG_DIR/collector-host.log"
/usr/bin/python3 "$SCHEDULER" check >> "$LOG_DIR/collector-host.log" 2>&1
RC=$?
if [ "$RC" -eq 10 ]; then
  echo "$(date '+%Y-%m-%d %H:%M:%S') NON_TRADING_DAY; skip" >> "$LOG_DIR/collector-host.log"
  exit 0
fi
if [ "$RC" -ne 0 ]; then
  echo "$(date '+%Y-%m-%d %H:%M:%S') calendar check failed rc=$RC" >> "$LOG_DIR/collector-host.log"
  exit "$RC"
fi

if docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "$(date '+%Y-%m-%d %H:%M:%S') collector already running" >> "$LOG_DIR/collector-host.log"
  exit 0
fi

docker rm -f "$CONTAINER" >/dev/null 2>&1 || true

docker run -d \
  --name "$CONTAINER" \
  --restart no \
  --env-file "$CONFIG_DIR/worker.env" \
  -v "$DATA_DIR:/data" \
  -v "$CONFIG_DIR:/config" \
  --log-opt max-size=10m \
  --log-opt max-file=3 \
  "$IMAGE" collector >> "$LOG_DIR/collector-host.log" 2>&1
RC=$?

echo "$(date '+%Y-%m-%d %H:%M:%S') docker start rc=$RC" >> "$LOG_DIR/collector-host.log"
exit "$RC"
