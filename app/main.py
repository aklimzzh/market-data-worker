import argparse

from .calendar_update import run as calendar_update
from .collector import run as collector
from .fundflow_backfill import run as fundflow_backfill


def main():
    parser = argparse.ArgumentParser(
        description="Market Data Worker"
    )

    sub = parser.add_subparsers(
        dest="cmd",
        required=True,
    )

    sub.add_parser(
        "collector",
        help="Run the real-time market collector",
    )

    sub.add_parser(
        "calendar-update",
        help="Update the local trading calendar",
    )

    backfill_parser = sub.add_parser(
        "fundflow-backfill",
        help="Backfill one trading day and archive it to Parquet and CrateDB",
    )

    backfill_parser.add_argument(
        "--date",
        required=True,
        help="Trading date in YYYY-MM-DD format",
    )

    backfill_parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Delay between Sina sector-history requests in seconds",
    )

    args = parser.parse_args()

    if args.cmd == "collector":
        collector()
    elif args.cmd == "calendar-update":
        calendar_update()
    elif args.cmd == "fundflow-backfill":
        fundflow_backfill(
            args.date,
            args.sleep,
        )


if __name__ == "__main__":
    main()
