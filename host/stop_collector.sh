#!/bin/sh
BASE_DIR="/mnt/ssd/moneyflow"
LOG_DIR="$BASE_DIR/logs"
CONTAINER="market-collector"
mkdir -p "$LOG_DIR"
if docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "$(date '+%Y-%m-%d %H:%M:%S') safety stopping collector" >> "$LOG_DIR/collector-host.log"
  docker stop -t 30 "$CONTAINER" >> "$LOG_DIR/collector-host.log" 2>&1
else
  echo "$(date '+%Y-%m-%d %H:%M:%S') collector not running; nothing to stop" >> "$LOG_DIR/collector-host.log"
fi
