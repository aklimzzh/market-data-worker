from __future__ import annotations
import csv
from datetime import date,datetime,timedelta
from zoneinfo import ZoneInfo
import pandas as pd,requests
from ..config import DATA_DIR,SERV00_API_TOKEN,SERV00_INGEST_URL
from ..sources.index_realtime import fetch
TZ=ZoneInfo("Asia/Shanghai")
FIELDS=["index_code","index_symbol","index_name","trade_date","minute_ts","close_price","change_pct","cumulative_volume","cumulative_amount","minute_volume","minute_amount","period_price_change","source"]
def endpoint():
 base=(SERV00_INGEST_URL or "").split("/api/v1/fundflow/import")[0].split("/api/fundflow/ingest")[0].rstrip("/")
 if not base:raise RuntimeError("SERV00_INGEST_URL is empty")
 return base+"/api/v1/index/minute/import"
def path(day):return DATA_DIR/"index_raw"/day.strftime("%Y/%m")/f"{day}.csv"
def last(day):
 d={}; p=path(day)
 if p.exists():
  with p.open(encoding="utf-8",newline="") as h:
   for r in csv.DictReader(h):d[r["index_code"]]=r
 return d
def num(r,k):
 try:return float(r[k]) if r and r.get(k) not in (None,"") else None
 except:return None
def enrich(rows,day):
 old=last(day)
 for r in rows:
  x=old.get(r["index_code"]); p=num(x,"close_price"); v=num(x,"cumulative_volume"); a=num(x,"cumulative_amount")
  r["period_price_change"]=None if p is None else r["close_price"]-p
  r["minute_volume"]=None if v is None else max(0,int(round(r["cumulative_volume"]-v)))
  r["minute_amount"]=None if a is None else max(0,r["cumulative_amount"]-a)
 return rows
def append(rows):
 p=path(date.fromisoformat(rows[0]["trade_date"]));p.parent.mkdir(parents=True,exist_ok=True);new=not p.exists()
 with p.open("a",encoding="utf-8",newline="") as h:
  w=csv.DictWriter(h,fieldnames=FIELDS)
  if new:w.writeheader()
  w.writerows(rows)
def upload(rows):
 r=requests.post(endpoint(),headers={"Authorization":f"Bearer {SERV00_API_TOKEN}"},json={"minute_ts":rows[0]["minute_ts"],"records":rows},timeout=(5,30));r.raise_for_status();return r.json()
def collect(session=None):
 rows=enrich(fetch(session),datetime.now(TZ).date());append(rows);return rows,upload(rows)
def finalize(day):
 p=path(day)
 if not p.exists():raise FileNotFoundError(p)
 f=pd.read_csv(p).drop_duplicates(["index_code","minute_ts"],keep="last").sort_values(["minute_ts","index_code"]); out=DATA_DIR/"index_archive"/day.strftime("%Y/%m")/f"{day}.parquet";out.parent.mkdir(parents=True,exist_ok=True);f.to_parquet(out,index=False,compression="zstd")
 counts=f.groupby("minute_ts")["index_code"].nunique();bad={str(k):int(v) for k,v in counts.items() if int(v)!=4}
 return {"trade_date":str(day),"rows":len(f),"index_count":int(f.index_code.nunique()),"minute_count":int(f.minute_ts.nunique()),"expected_minutes":241,"incomplete_minutes":bad,"complete":not bad and int(f.minute_ts.nunique())==241,"parquet":str(out)}
def retention():
 r=requests.post(endpoint().replace("/import","/retention"),headers={"Authorization":f"Bearer {SERV00_API_TOKEN}"},timeout=(5,30));r.raise_for_status();return r.json()
