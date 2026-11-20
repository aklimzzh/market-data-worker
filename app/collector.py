import time
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo
import requests
from .config import DATA_DIR,SERV00_INGEST_URL,SERV00_API_TOKEN,SINA_INTERVAL
from .sources.sina import fetch_snapshot
from .services.local_store import append_csv,write_status
from .services.serv00 import post_batch
TZ=ZoneInfo("Asia/Shanghai")

def phase(t):
    if dt_time(9,15)<=t<dt_time(9,30): return "opening_auction_observe"
    if dt_time(9,30)<=t<=dt_time(11,30): return "continuous_am"
    if dt_time(13,0)<=t<=dt_time(15,0): return "continuous_pm"
    if dt_time(15,0)<t<=dt_time(15,12): return "closing_revision"
    return "idle"

def normalize(rows,now):
    out=[]
    for x in rows:
        out.append({"trade_date":now.date().isoformat(),"snapshot_time":now.isoformat(timespec="seconds"),"source":"sina","sector_type":x["sector_type"],"sector_code":x["category"],"sector_name":x["name"],"avg_price":x.get("avg_price"),"pct_change":x.get("avg_changeratio"),"turnover":x.get("turnover"),"in_amount":x.get("inamount"),"out_amount":x.get("outamount"),"net_amount":x.get("netamount"),"net_ratio":x.get("ratioamount"),"leader_code":x.get("ts_symbol"),"leader_name":x.get("ts_name"),"leader_price":x.get("ts_trade"),"leader_pct_change":x.get("ts_changeratio"),"leader_net_ratio":x.get("ts_ratioamount")})
    return out

def run():
    session=requests.Session(); last_minute=None
    while True:
        now=datetime.now(TZ); p=phase(now.time())
        if now.time()>dt_time(15,12): break
        if p=="idle": time.sleep(10); continue
        minute=now.replace(second=0,microsecond=0)
        if minute==last_minute: time.sleep(1); continue
        rows=normalize(fetch_snapshot(session),now)
        csv_path=DATA_DIR/"raw"/now.strftime("%Y/%m")/f"{now.date().isoformat()}.csv"
        append_csv(csv_path,rows)
        payload={"trade_date":now.date().isoformat(),"snapshot_time":now.isoformat(timespec="seconds"),"phase":p,"records":rows}
        try:
            result=post_batch(SERV00_INGEST_URL,SERV00_API_TOKEN,payload); state="OK"; error=None
        except Exception as exc:
            result=None; state="PENDING"; error=str(exc)
            pend=DATA_DIR/"pending"/f"{now.strftime('%Y%m%d_%H%M%S')}.json"
            pend.parent.mkdir(parents=True,exist_ok=True); pend.write_text(__import__('json').dumps(payload,ensure_ascii=False),encoding='utf-8')
        write_status(DATA_DIR/"status/collector.json",{"state":state,"phase":p,"last_snapshot":now.isoformat(timespec="seconds"),"rows":len(rows),"error":error,"serv00":result})
        last_minute=minute
        time.sleep(max(1,SINA_INTERVAL-(datetime.now(TZ)-now).total_seconds()))
