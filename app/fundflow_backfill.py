from __future__ import annotations

import json
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from .config import DATA_DIR
from .sources.sina_history import sector_catalog, fetch_sector_day
from .services.cratedb import ensure_schema, upload_1m, replace_aggregates, counts

TZ = ZoneInfo("Asia/Shanghai")


def as_float(value):
    if value in (None, "", "-"):
        return None
    return float(value)


def run(target_date: str, sleep_seconds: float = 0.5) -> None:
    trade_date = date.fromisoformat(target_date)
    session = requests.Session()
    sectors = sector_catalog(session)
    if len(sectors) != 229:
        raise RuntimeError(f"expected 229 sectors, got {len(sectors)}")
    records = []
    failures = []
    for index, sector in enumerate(sectors, start=1):
        try:
            points = fetch_sector_day(session, sector, trade_date)
            for point in points:
                minute_ts = datetime.fromisoformat(f"{point['opendate']}T{point['ticktime']}").replace(tzinfo=TZ)
                records.append({
                    "source":"sina",
                    "sector_code":sector["sector_code"],
                    "sector_type":sector["sector_type"],
                    "sector_name":sector["sector_name"],
                    "trade_date":target_date,
                    "minute_ts":minute_ts.isoformat(timespec="seconds"),
                    "avg_price":as_float(point.get("avg_price")),
                    "pct_change":as_float(point.get("avg_changeratio")),
                    "in_amount":as_float(point.get("inamount")),
                    "out_amount":as_float(point.get("outamount")),
                    "net_amount":as_float(point.get("netamount")),
                    "net_ratio":as_float(point.get("ratioamount")),
                    "r0_ratio":as_float(point.get("r0_ratio")),
                    "r3_ratio":as_float(point.get("r3_ratio")),
                })
        except Exception as exc:
            failures.append({"sector":sector,"error":str(exc)})
        print(f"[{index:03d}/229] {sector['sector_code']} records={len(records)} failures={len(failures)}", flush=True)
        time.sleep(sleep_seconds)
    if failures:
        error_path = DATA_DIR / "status" / f"backfill_{target_date}_failures.json"
        error_path.parent.mkdir(parents=True, exist_ok=True)
        error_path.write_text(json.dumps(failures,ensure_ascii=False,indent=2),encoding="utf-8")
        raise RuntimeError(f"backfill incomplete: {len(failures)} sectors failed; see {error_path}")
    frame = pd.DataFrame.from_records(records)
    duplicate_count = int(frame.duplicated(["source","sector_code","minute_ts"]).sum())
    if duplicate_count:
        raise RuntimeError(f"duplicate rows found: {duplicate_count}")
    sector_count = int(frame["sector_code"].nunique())
    minute_count = int(frame["minute_ts"].nunique())
    if sector_count != 229 or minute_count < 200:
        raise RuntimeError(f"unexpected completeness: sectors={sector_count}, minutes={minute_count}")
    archive = DATA_DIR / "archive" / target_date[:4] / target_date[5:7] / f"{target_date}.parquet"
    archive.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(archive,index=False,compression="zstd")
    ensure_schema()
    rows = [
        [
            row.source,row.sector_code,row.sector_type,row.sector_name,
            row.trade_date,row.minute_ts,row.avg_price,row.pct_change,
            row.in_amount,row.out_amount,row.net_amount,row.net_ratio,
            row.r0_ratio,row.r3_ratio,
        ]
        for row in frame.itertuples(index=False)
    ]
    upload_results = upload_1m(rows)
    replace_aggregates(target_date)
    crate_counts = counts(target_date)
    expected_daily = 229
    if crate_counts["sector_flow_1m"] != len(frame) or crate_counts["sector_flow_daily"] != expected_daily:
        raise RuntimeError(f"CrateDB validation failed: {crate_counts}; parquet rows={len(frame)}")
    status = {
        "trade_date":target_date,"completed_at":datetime.now(TZ).isoformat(timespec="seconds"),
        "parquet":str(archive),"parquet_rows":len(frame),"sector_count":sector_count,
        "minute_count":minute_count,"upload_results":upload_results,"cratedb":crate_counts,
    }
    status_path = DATA_DIR / "status" / f"backfill_{target_date}.json"
    status_path.parent.mkdir(parents=True,exist_ok=True)
    status_path.write_text(json.dumps(status,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(status,ensure_ascii=False,indent=2))
