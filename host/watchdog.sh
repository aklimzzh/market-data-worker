#!/bin/sh
set -eu

BASE_DIR="/mnt/ssd/moneyflow"
LOG_FILE="$BASE_DIR/logs/watchdog.log"
START_SCRIPT="$BASE_DIR/start_collector.sh"

mkdir -p "$BASE_DIR/logs"

# Recovery windows (Asia/Shanghai host time):
# - 09:10-11:29 morning session
# - 13:00-15:40 afternoon + post-close final-check window
# Lunch is intentionally excluded.
HHMM="$(date '+%H%M')"
case "$HHMM" in
  09[1-5][0-9]|10[0-5][0-9]|11[0-2][0-9]|13[0-5][0-9]|14[0-5][0-9]|15[0-3][0-9]|1540) ;;
  *) exit 0 ;;
esac

set +e
/usr/bin/python3 "$BASE_DIR/scheduler.py" check >/dev/null 2>&1
RC=$?
set -e
[ "$RC" -eq 0 ] || exit 0

if docker ps --format '{{.Names}}' | grep -qx 'market-collector'; then
  exit 0
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') collector missing; restarting" >> "$LOG_FILE"
"$START_SCRIPT" >> "$LOG_FILE" 2>&1
