import argparse

from .calendar_update import run as calendar_update
from .collector import run as collector
from .fundflow_backfill import run as fundflow_backfill
from .maintenance import run as maintenance


def main():
    parser = argparse.ArgumentParser(description="Market Data Worker")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("collector")
    sub.add_parser("calendar-update")

    backfill = sub.add_parser("fundflow-backfill")
    backfill.add_argument("--date", required=True)
    backfill.add_argument("--sleep", type=float, default=0.5)

    maintain = sub.add_parser("maintenance")
    maintain.add_argument("--date", required=True)
    maintain.add_argument("--sleep", type=float, default=0.5)
    maintain.add_argument("--refresh-calendar", action="store_true")

    args = parser.parse_args()
    if args.cmd == "collector":
        collector()
    elif args.cmd == "calendar-update":
        calendar_update()
    elif args.cmd == "fundflow-backfill":
        fundflow_backfill(args.date, args.sleep)
    elif args.cmd == "maintenance":
        maintenance(args.date, args.sleep, args.refresh_calendar)


if __name__ == "__main__":
    main()
