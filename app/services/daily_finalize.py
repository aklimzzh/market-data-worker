from __future__ import annotations

import json
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pandas as pd

from ..config import DATA_DIR

TZ = ZoneInfo("Asia/Shanghai")
EXPECTED_SECTORS = 229


def _normalize_minutes(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    raw_rows = len(frame)
    timestamps = pd.to_datetime(frame["minute_ts"], errors="coerce", utc=True)
    invalid_timestamp_rows = int(timestamps.isna().sum())
    local = timestamps.dt.tz_convert(TZ).dt.floor("min")
    clock = local.dt.time

    # Monitoring window agreed for the production system:
    # pre-open/AM 09:15-11:30, pre-open/PM and PM/post-close 12:59-15:30.
    allowed = (
        ((clock >= time(9, 15)) & (clock <= time(11, 30)))
        | ((clock >= time(12, 59)) & (clock <= time(15, 30)))
    )
    allowed = allowed & timestamps.notna()
    outside = frame.loc[~allowed, ["sector_code", "minute_ts"]].copy()

    result = frame.loc[allowed].copy()
    result["minute_ts"] = local.loc[allowed].map(
        lambda value: value.isoformat(timespec="seconds")
    )
    before_dedup = len(result)
    result = result.drop_duplicates(
        ["source", "sector_code", "minute_ts"], keep="last"
    )
    duplicate_rows = before_dedup - len(result)
    result = result.sort_values(["minute_ts", "sector_code"])

    quality = {
        "raw_rows": raw_rows,
        "invalid_timestamp_rows": invalid_timestamp_rows,
        "outside_window_rows": int(len(outside)),
        "outside_window_sample": outside.head(50).to_dict(orient="records"),
        "duplicate_rows_removed": int(duplicate_rows),
        "final_rows": int(len(result)),
    }
    return result, quality


def finalize_parquet(trade_date: date) -> dict:
    target = trade_date.isoformat()
    csv_path = DATA_DIR / "raw" / trade_date.strftime("%Y/%m") / f"{target}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    raw = pd.read_csv(csv_path, dtype={"sector_code": str})
    if raw.empty:
        raise RuntimeError(f"empty CSV: {csv_path}")

    frame, quality = _normalize_minutes(raw)
    if frame.empty:
        raise RuntimeError(f"no rows inside final monitoring window: {csv_path}")

    counts = frame.groupby("minute_ts")["sector_code"].nunique()
    incomplete = {
        str(minute): int(count)
        for minute, count in counts.items()
        if int(count) != EXPECTED_SECTORS
    }
    sector_count = int(frame["sector_code"].nunique())
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
        "sector_count": sector_count,
        "observed_minutes": actual_minutes,
        "first_minute": str(frame["minute_ts"].min()),
        "last_minute": str(frame["minute_ts"].max()),
        "incomplete_minutes": incomplete,
        "quality": quality,
        # Sparse Sina history is valid. It is quality information, not a failure.
        "complete": sector_count == EXPECTED_SECTORS and len(frame) > 0,
        "bytes": archive.stat().st_size,
        "completed_at": datetime.now(TZ).isoformat(timespec="seconds"),
    }

    status_path = DATA_DIR / "status" / f"daily_finalize_{target}.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    temp_status = status_path.with_suffix(".json.tmp")
    temp_status.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    temp_status.replace(status_path)
    print(
        f"[PARQUET] sector finalized rows={len(frame)} "
        f"minutes={actual_minutes} outside={quality['outside_window_rows']} "
        f"duplicates={quality['duplicate_rows_removed']} path={archive}",
        flush=True,
    )
    return result
