from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from ..config import DATA_DIR

TZ = ZoneInfo("Asia/Shanghai")
EXPECTED_SECTORS = 229


def _official_minute(value: str) -> bool:
    stamp = datetime.fromisoformat(value).astimezone(TZ).time()
    return time(9, 30) <= stamp <= time(11, 30) or time(13, 1) <= stamp <= time(15, 0)


def finalize_parquet(trade_date: date) -> dict:
    target = trade_date.isoformat()
    csv_path = DATA_DIR / "raw" / trade_date.strftime("%Y/%m") / f"{target}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    frame = pd.read_csv(csv_path, dtype={"sector_code": str})
    if frame.empty:
        raise RuntimeError(f"empty CSV: {csv_path}")
    frame = frame[frame["minute_ts"].astype(str).map(_official_minute)].copy()
    frame = frame.drop_duplicates(["source", "sector_code", "minute_ts"], keep="last")
    frame = frame.sort_values(["minute_ts", "sector_code"])

    counts = frame.groupby("minute_ts")["sector_code"].nunique()
    incomplete = {
        str(minute): int(count)
        for minute, count in counts.items()
        if int(count) != EXPECTED_SECTORS
    }
    expected_minutes = 241
    actual_minutes = int(frame["minute_ts"].nunique())

    archive = DATA_DIR / "archive" / target[:4] / target[5:7] / f"{target}.parquet"
    archive.parent.mkdir(parents=True, exist_ok=True)
    temp = archive.with_suffix(".parquet.tmp")
    frame.to_parquet(temp, index=False, compression="zstd")
    temp.replace(archive)

    result = {
        "trade_date": target,
        "csv_path": str(csv_path),
        "parquet_path": str(archive),
        "parquet_rows": int(len(frame)),
        "sector_count": int(frame["sector_code"].nunique()),
        "minute_count": actual_minutes,
        "expected_minutes": expected_minutes,
        "incomplete_minutes": incomplete,
        "complete": actual_minutes == expected_minutes and not incomplete,
        "bytes": archive.stat().st_size,
        "completed_at": datetime.now(TZ).isoformat(timespec="seconds"),
    }
    status_path = DATA_DIR / "status" / f"daily_finalize_{target}.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    temp_status = status_path.with_suffix(".json.tmp")
    temp_status.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_status.replace(status_path)
    print(f"[PARQUET] finalized rows={len(frame)} minutes={actual_minutes} complete={result['complete']} path={archive}", flush=True)
    return result
