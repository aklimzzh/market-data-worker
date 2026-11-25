from __future__ import annotations
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from .services.intraday_repair import repair_now
from .services.daily_finalize import finalize_parquet
from .services.index_minute import collect,finalize
TZ=ZoneInfo("Asia/Shanghai")
def run(scope="all",final=False):
 now=datetime.now(TZ);result={"state":"RUNNING","scope":scope,"final":final,"started_at":now.isoformat(timespec="seconds")}
 try:
  if scope in {"sector","all"}:result["sector"]={"repair":repair_now(now,sleep_seconds=.25),"parquet":finalize_parquet(now.date()) if final else None}
  if scope in {"index","all"}:
   rows,up=collect();result["index"]={"snapshot_rows":len(rows),"upload":up,"parquet":finalize(now.date()) if final else None}
  result["state"]="SUCCESS"
 except Exception as e:result["state"]="FAILED";result["error"]=f"{type(e).__name__}: {e}"
 result["finished_at"]=datetime.now(TZ).isoformat(timespec="seconds");print(json.dumps(result,ensure_ascii=False,indent=2))
 if result["state"]=="FAILED":raise RuntimeError(result["error"])
 return result
