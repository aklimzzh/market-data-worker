import argparse
from .calendar_update import run as calendar_update
from .collector import run as collector
from .fundflow_backfill import run as fundflow_backfill
from .fundflow_daily_backfill import run as fundflow_daily_backfill
from .index_backfill import run as index_backfill
from .maintenance import run as maintenance
from .reconciliation import run as reconciliation
def main():
    parser=argparse.ArgumentParser(description="Market Data Worker");sub=parser.add_subparsers(dest="cmd",required=True)
    sub.add_parser("collector");sub.add_parser("calendar-update")
    backfill=sub.add_parser("fundflow-backfill");backfill.add_argument("--date",required=True);backfill.add_argument("--sleep",type=float,default=.5)
    daily=sub.add_parser("fundflow-daily-backfill");daily.add_argument("--days",type=int,default=100);daily.add_argument("--sleep",type=float,default=.3)
    indexes=sub.add_parser("index-backfill");group=indexes.add_mutually_exclusive_group(required=True);group.add_argument("--date");group.add_argument("--start");indexes.add_argument("--end");indexes.add_argument("--sleep",type=float,default=1.0)
    reconcile=sub.add_parser("reconcile");reconcile.add_argument("--scope",choices=("sector","index","all"),default="all");reconcile.add_argument("--final",action="store_true")
    maintain=sub.add_parser("maintenance");maintain.add_argument("--date",required=True);maintain.add_argument("--sleep",type=float,default=.5);maintain.add_argument("--refresh-calendar",action="store_true")
    args=parser.parse_args()
    if args.cmd=="collector":collector()
    elif args.cmd=="calendar-update":calendar_update()
    elif args.cmd=="fundflow-backfill":fundflow_backfill(args.date,args.sleep)
    elif args.cmd=="fundflow-daily-backfill":fundflow_daily_backfill(args.days,args.sleep)
    elif args.cmd=="index-backfill":
        start=args.date or args.start;end=args.date or args.end
        if not end:parser.error("index-backfill --start requires --end")
        index_backfill(start,end,args.sleep)
    elif args.cmd=="reconcile":reconciliation(args.scope,args.final)
    elif args.cmd=="maintenance":maintenance(args.date,args.sleep,args.refresh_calendar)
if __name__=="__main__":main()
