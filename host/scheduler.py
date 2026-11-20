#!/usr/bin/env python3
import argparse,json,subprocess,sys
from datetime import date
from pathlib import Path
CAL=Path('/mnt/ssd/moneyflow/config/trading_calendar.json')
IMAGE='ghcr.io/YOUR_GITHUB_USER/market-data-worker:0.1.0'

def load(): return json.loads(CAL.read_text(encoding='utf-8'))['trade_dates']
def run(cmd): print('+',' '.join(cmd)); subprocess.run(cmd,check=True)
def main():
 p=argparse.ArgumentParser(); p.add_argument('mode',choices=['market','maintenance','safety-stop']); a=p.parse_args(); ds=load(); today=date.today().isoformat()
 if a.mode=='market':
  if today not in ds: print('NON_TRADING_DAY',today); return
  subprocess.run(['docker','rm','-f','market-collector'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); run(['docker','run','-d','--name','market-collector','--env-file','/mnt/ssd/moneyflow/config/worker.env','-v','/mnt/hdd/fundflow:/data','-v','/mnt/ssd/moneyflow/config:/config',IMAGE,'collector'])
 elif a.mode=='maintenance':
  past=[d for d in ds if d<today]
  if not past: raise SystemExit('no previous trade date')
  td=past[-1]; run(['docker','run','--rm','--name','market-maintenance','--env-file','/mnt/ssd/moneyflow/config/worker.env','-v','/mnt/hdd/fundflow:/data','-v','/mnt/ssd/moneyflow/config:/config',IMAGE,'maintenance','--date',td])
 else: subprocess.run(['docker','stop','-t','30','market-collector'])
if __name__=='__main__': main()
