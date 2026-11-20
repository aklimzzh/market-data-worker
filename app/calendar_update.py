import json, os, shutil
from datetime import date, datetime
from zoneinfo import ZoneInfo
import akshare as ak
from .config import CONFIG_DIR

def run():
    df = ak.calendar_trade_date_hist_sina()
    dates = sorted({x.date() if hasattr(x,"date") else x for x in df["trade_date"]})
    today = date.today()
    past = [d for d in dates if d < today][-10:]
    future = [d for d in dates if d >= today]
    if not future:
        raise RuntimeError("No current or future trading dates returned")
    payload = {
        "schema_version":1,"source":"akshare","akshare_version":ak.__version__,
        "updated_at":datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds"),
        "range_start":past[0].isoformat() if past else future[0].isoformat(),
        "range_end":future[-1].isoformat(),"trade_dates":[d.isoformat() for d in past+future],
    }
    CONFIG_DIR.mkdir(parents=True,exist_ok=True)
    target=CONFIG_DIR/"trading_calendar.json"; tmp=CONFIG_DIR/"trading_calendar.json.tmp"; bak=CONFIG_DIR/"trading_calendar.json.bak"
    tmp.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf-8")
    json.loads(tmp.read_text(encoding="utf-8"))
    if target.exists(): shutil.copy2(target,bak)
    os.replace(tmp,target)
    print(json.dumps(payload,ensure_ascii=False,indent=2))
