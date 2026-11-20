import argparse
from .collector import run as collector
from .calendar_update import run as calendar_update
from .maintenance import run as maintenance

def main():
    p=argparse.ArgumentParser(); sub=p.add_subparsers(dest="cmd",required=True)
    sub.add_parser("collector"); sub.add_parser("calendar-update")
    m=sub.add_parser("maintenance"); m.add_argument("--date",required=True)
    a=p.parse_args()
    if a.cmd=="collector": collector()
    elif a.cmd=="calendar-update": calendar_update()
    else: maintenance(a.date)
if __name__=="__main__": main()
