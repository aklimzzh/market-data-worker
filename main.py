import argparse
from .collector import run as collector
from .calendar_update import run as calendar_update

def main():
    parser=argparse.ArgumentParser()
    sub=parser.add_subparsers(dest="cmd",required=True)
    sub.add_parser("collector")
    sub.add_parser("calendar-update")
    args=parser.parse_args()
    if args.cmd=="collector": collector()
    else: calendar_update()
if __name__=="__main__": main()
