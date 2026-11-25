from __future__ import annotations
import re
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
TZ=ZoneInfo("Asia/Shanghai")
CONFIG={"SSE":("sh000001","000001","上证指数"),"SZSE":("sz399001","399001","深证成指"),"STAR50":("sh000688","000688","科创50"),"CHINEXT":("sz399006","399006","创业板指")}
URL="https://hq.sinajs.cn/list="+",".join(v[0] for v in CONFIG.values()); HEADERS={"Referer":"https://finance.sina.com.cn/","User-Agent":"Mozilla/5.0"}
def _f(v):
 try:return float(v) if v not in (None,"") else None
 except:return None
def _shares(code,raw):
 if raw is None:return None
 return int(round(raw*100)) if code in {"SSE","STAR50"} else int(round(raw))
def fetch(session=None):
 session=session or requests.Session(); r=session.get(URL,headers=HEADERS,timeout=(5,15)); r.raise_for_status(); text=r.content.decode("gbk",errors="replace")
 lookup={v[0]:k for k,v in CONFIG.items()}; out=[]
 for line in text.splitlines():
  m=re.search(r'hq_str_(\w+)="(.*)"',line)
  if not m or m.group(1) not in lookup:continue
  code=lookup[m.group(1)]; f=m.group(2).split(',')
  if len(f)<32:continue
  cur,prev=_f(f[3]),_f(f[2]); ts=datetime.fromisoformat(f"{f[30]}T{f[31]}").replace(tzinfo=TZ).replace(second=0,microsecond=0)
  out.append({"index_code":code,"index_symbol":CONFIG[code][1],"index_name":CONFIG[code][2],"trade_date":ts.date().isoformat(),"minute_ts":ts.isoformat(timespec="seconds"),"close_price":cur,"change_pct":None if cur is None or not prev else (cur/prev-1)*100,"cumulative_volume":_shares(code,_f(f[8])),"cumulative_amount":_f(f[9]),"source":"sina"})
 if {x["index_code"] for x in out} != set(CONFIG):raise RuntimeError(f"expected 4 indexes, got {[x['index_code'] for x in out]}")
 return sorted(out,key=lambda x:list(CONFIG).index(x["index_code"]))
