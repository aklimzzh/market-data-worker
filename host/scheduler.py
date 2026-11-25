#!/usr/bin/env python3
import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

BASE = Path('/mnt/ssd/moneyflow')
CALENDAR = BASE / 'config' / 'trading_calendar.json'
STATUS = BASE / 'status' / 'scheduler.json'

RC_TRADING = 0
RC_NON_TRADING = 10
RC_ERROR = 20


def write_status(payload):
    STATUS.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATUS.with_suffix('.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    tmp.replace(STATUS)


def load_calendar():
    if not CALENDAR.exists():
        raise RuntimeError(f'calendar missing: {CALENDAR}')
    data = json.loads(CALENDAR.read_text(encoding='utf-8'))
    dates = data.get('trade_dates')
    if not isinstance(dates, list) or not dates:
        raise RuntimeError('trade_dates is empty or invalid')
    return data, set(dates)


def check_trade_day(target):
    data, dates = load_calendar()
    return target in dates, data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('command', choices=['check', 'previous', 'status'])
    ap.add_argument('--date', help='YYYY-MM-DD; defaults to today')
    a = ap.parse_args()
    target = a.date or date.today().isoformat()
    try:
        data, dates = load_calendar()
        if a.command == 'check':
            is_trade = target in dates
            payload = {'time': datetime.now().astimezone().isoformat(timespec='seconds'), 'target_date': target,
                       'is_trade_day': is_trade, 'calendar_updated_at': data.get('updated_at'),
                       'calendar_range_end': data.get('range_end')}
            write_status(payload)
            print(json.dumps(payload, ensure_ascii=False))
            return RC_TRADING if is_trade else RC_NON_TRADING
        if a.command == 'previous':
            past = sorted(d for d in dates if d < target)
            if not past:
                raise RuntimeError(f'no previous trade day before {target}')
            print(past[-1]); return 0
        print(STATUS.read_text(encoding='utf-8') if STATUS.exists() else '{}'); return 0
    except Exception as exc:
        payload = {'time': datetime.now().astimezone().isoformat(timespec='seconds'), 'target_date': target,
                   'error': f'{type(exc).__name__}: {exc}'}
        write_status(payload); print(json.dumps(payload, ensure_ascii=False), file=sys.stderr); return RC_ERROR

if __name__ == '__main__':
    sys.exit(main())
