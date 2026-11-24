from __future__ import annotations

import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from .config import DATA_DIR
from .sources.sina_daily import fetch_sector_daily
from .sources.sina_history import sector_catalog
from .services.cratedb import ensure_schema, upload_daily
from .services.serv00_online import upload_daily_records

TZ = ZoneInfo("Asia/Shanghai")


def _float(value):
    if value in (None, "", "-"):
        return None
    return float(value)


def _int(value):
    if value in (None, "", "-"):
        return None
    return int(float(value))


def run(days: int = 100, sleep_seconds: float = 0.3) -> dict:
    if days < 1 or days > 100:
        raise ValueError("days must be between 1 and 100")
    session = requests.Session()
    sectors = sector_catalog(session)
    if len(sectors) != 229:
        raise RuntimeError(f"expected 229 sectors, got {len(sectors)}")

    records = []
    failures = []
    for index, sector in enumerate(sectors, start=1):
        try:
            points = fetch_sector_daily(session, sector, days)
            for point in points:
                trade_date = str(point["opendate"])
                records.append({
                    "source": "sina",
                    "sector_code": sector["sector_code"],
                    "sector_type": sector["sector_type"],
                    "sector_name": sector["sector_name"],
                    "trade_date": trade_date,
                    "close_ts": f"{trade_date}T15:00:00+08:00",
                    "avg_price": _float(point.get("avg_price")),
                    "pct_change": _float(point.get("avg_changeratio")),
                    "in_amount": None,
                    "out_amount": None,
                    "net_amount": _float(point.get("netamount")),
                    "net_ratio": _float(point.get("ratioamount")),
                    "turnover": _float(point.get("turnover")),
                    "main_net_amount": _float(point.get("r0_net")),
                    "main_net_ratio": _float(point.get("r0_ratio")),
                    "main_compass": _float(point.get("r0x_ratio")),
                    "main_compass_count": _int(point.get("cnt_r0x_ratio")),
                })
        except Exception as exc:
            failures.append({"sector": sector["sector_code"], "error": str(exc)})
        if index % 25 == 0 or index == 229:
            print(f"[DAILY-BACKFILL] fetched={index}/229 rows={len(records)} failures={len(failures)}", flush=True)
        time.sleep(sleep_seconds)

    if failures:
        path = DATA_DIR / "status" / "fundflow_daily_backfill_failures.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8")
        raise RuntimeError(f"daily backfill incomplete: {len(failures)} sectors failed; see {path}")

    unique = {(r["source"], r["sector_code"], r["trade_date"]): r for r in records}
    records = sorted(unique.values(), key=lambda item: (item["trade_date"], item["sector_code"]))
    serv00_count = upload_daily_records(records)
    ensure_schema()
    crate_count = upload_daily(records)

    result = {
        "state": "SUCCESS",
        "days_requested": days,
        "records": len(records),
        "sectors": len({r["sector_code"] for r in records}),
        "first_date": min(r["trade_date"] for r in records),
        "last_date": max(r["trade_date"] for r in records),
        "serv00_upserted": serv00_count,
        "cratedb_upserted": crate_count,
        "completed_at": datetime.now(TZ).isoformat(timespec="seconds"),
    }
    path = DATA_DIR / "status" / "fundflow_daily_backfill.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    return result
