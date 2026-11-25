from __future__ import annotations
import json
from datetime import date, datetime
from zoneinfo import ZoneInfo
import akshare as ak
import pandas as pd
from .config import DATA_DIR
from .services.index_minute import upload
from .sources.index_realtime import CONFIG
TZ=ZoneInfo("Asia/Shanghai")
ALIASES={"day":"minute_ts","时间":"minute_ts","datetime":"minute_ts","date":"minute_ts","close":"price","收盘":"price","volume":"volume","成交量":"volume","amount":"amount","成交额":"amount"}

def _fetch(symbol:str)->pd.DataFrame:
    errors=[]
    for call in (
        lambda: ak.stock_zh_a_hist_min_sina(symbol=symbol, period="1"),
        lambda: ak.stock_zh_a_minute(symbol=symbol, period="1", adjust=""),
    ):
        try:
            frame=call()
            if isinstance(frame,pd.DataFrame) and not frame.empty: return frame
        except Exception as exc: errors.append(f"{type(exc).__name__}: {exc}")
    raise RuntimeError(f"no compatible Sina 1m function for {symbol}: {errors}")

def _normalise(frame:pd.DataFrame,code:str)->pd.DataFrame:
    renamed={c:ALIASES.get(str(c).strip(),str(c).strip().lower()) for c in frame.columns}
    f=frame.rename(columns=renamed).copy()
    required={"minute_ts","price","volume"}
    if not required.issubset(f.columns): raise RuntimeError(f"{code}: columns unavailable: {list(frame.columns)}")
    f["minute_ts"]=pd.to_datetime(f["minute_ts"],errors="coerce")
    f=f.dropna(subset=["minute_ts"]).sort_values("minute_ts")
    if "amount" not in f.columns: f["amount"]=None
    symbol,name=CONFIG[code][1],CONFIG[code][2]
    f["index_code"]=code; f["index_symbol"]=symbol; f["index_name"]=name; f["source"]="sina"
    return f

def run(start:str,end:str)->dict:
    start_day=date.fromisoformat(start); end_day=date.fromisoformat(end)
    if start_day>end_day: raise ValueError("start cannot be later than end")
    frames=[]
    for code,(sina_symbol,_,_) in CONFIG.items():
        f=_normalise(_fetch(sina_symbol),code)
        local_dates=f["minute_ts"].dt.date
        f=f[(local_dates>=start_day)&(local_dates<=end_day)]
        if f.empty: raise RuntimeError(f"{code}: requested range unavailable from Sina")
        frames.append(f)
    all_rows=pd.concat(frames,ignore_index=True)
    all_rows["minute_ts"]=all_rows["minute_ts"].dt.tz_localize(TZ,ambiguous="NaT",nonexistent="NaT")
    all_rows=all_rows.dropna(subset=["minute_ts"]).sort_values(["minute_ts","index_code"])
    uploaded=0; incomplete=[]
    for minute,group in all_rows.groupby("minute_ts"):
        records=[]
        for _,r in group.iterrows():
            records.append({"index_code":r.index_code,"index_symbol":r.index_symbol,"index_name":r.index_name,
              "trade_date":minute.date().isoformat(),"minute_ts":minute.isoformat(timespec="seconds"),
              "price":None if pd.isna(r.price) else float(r.price),"volume":None if pd.isna(r.volume) else float(r.volume),
              "amount":None if pd.isna(r.amount) else float(r.amount),"period_price_change":None,
              "period_volume":None,"period_amount":None,"market_phase":"backfill","source":"sina"})
        if len(records)!=4: incomplete.append({"minute_ts":minute.isoformat(),"indexes":len(records)}); continue
        upload(records); uploaded+=4
    archive=DATA_DIR/"index_archive"/"backfill"/f"{start}_{end}.parquet"; archive.parent.mkdir(parents=True,exist_ok=True)
    all_rows.to_parquet(archive,index=False,compression="zstd")
    result={"state":"SUCCESS" if not incomplete else "PARTIAL","start":start,"end":end,"rows":len(all_rows),"uploaded":uploaded,"incomplete":incomplete,"parquet":str(archive),"finished_at":datetime.now(TZ).isoformat(timespec="seconds")}
    path=DATA_DIR/"status"/f"index_minute_backfill_{start}_{end}.json"; path.parent.mkdir(parents=True,exist_ok=True); path.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(result,ensure_ascii=False,indent=2)); return result
