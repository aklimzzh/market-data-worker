import argparse

from .calendar_update import run as calendar_update
from .collector import run as collector
from .fundflow_backfill import run as fundflow_backfill
from .fundflow_daily_backfill import run as fundflow_daily_backfill
from .index_backfill import run as index_backfill
from .index_minute_backfill import run as index_minute_backfill
from .maintenance import run as maintenance
from .reconciliation import run as reconciliation


def main():
    parser = argparse.ArgumentParser(description="Market Data Worker")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("collector")
    sub.add_parser("calendar-update")

    backfill = sub.add_parser("fundflow-backfill")
    backfill.add_argument("--date", required=True)
    backfill.add_argument("--sleep", type=float, default=0.5)

    daily = sub.add_parser("fundflow-daily-backfill")
    daily.add_argument("--days", type=int, default=100)
    daily.add_argument("--sleep", type=float, default=0.3)

    indexes = sub.add_parser("index-backfill")
    group = indexes.add_mutually_exclusive_group(required=True)
    group.add_argument("--date")
    group.add_argument("--start")
    indexes.add_argument("--end")
    indexes.add_argument("--sleep", type=float, default=1.0)

    # Index 1-minute historical backfill
    minute_indexes = sub.add_parser("index-minute-backfill")
    minute_indexes.add_argument("--start", required=True)
    minute_indexes.add_argument("--end", required=True)
    minute_indexes.add_argument("--sleep", type=float, default=0.10)
    minute_indexes.add_argument("--dry-run", action="store_true")

    reconcile = sub.add_parser("reconcile")
    reconcile.add_argument(
        "--scope",
        choices
