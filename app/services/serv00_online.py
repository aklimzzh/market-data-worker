from __future__ import annotations

import math
import os
from datetime import date, datetime
from typing import Any

import pandas as pd
import requests

BASE_URL = os.getenv("SERV00_BASE_URL", "").rstrip("/")
API_TOKEN = os.getenv("SERV00_API_TOKEN", "")
BATCH_SIZE = int(os.getenv("SERV00_ARCHIVE_BATCH_SIZE", "1000"))


def _value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _post(path: str, payload: dict[str, Any], timeout: int = 120) -> dict[str, Any]:
    if not BASE_URL:
        raise RuntimeError("SERV00_BASE_URL is empty")
    if not API_TOKEN:
        raise RuntimeError("SERV00_API_TOKEN is empty")
    response = requests.post(
        f"{BASE_URL}{path}",
        headers={"Authorization": f"Bearer {API_TOKEN}"},
        json=payload,
        timeout=(10, timeout),
    )
    response.raise_for_status()
    result = response.json()
    if not result.get("success"):
        raise RuntimeError(f"Serv00 returned failure: {result}")
    return result


def build_5m(frame: pd.DataFrame) -> list[dict[str, Any]]:
    ordered = frame.copy()
    ordered["minute_ts"] = pd.to_datetime(ordered["minute_ts"], utc=True)
    ordered = ordered.sort_values(["source", "sector_code", "minute_ts"])
    ordered["bucket_ts"] = ordered["minute_ts"].dt.floor("5min")
    result = (
        ordered.groupby(["source", "sector_code", "bucket_ts"], sort=True, as_index=False)
        .tail(1)
        .sort_values(["bucket_ts", "sector_code"])
    )
    columns = [
        "source", "sector_code", "sector_type", "sector_name", "trade_date",
        "bucket_ts", "avg_price", "pct_change", "in_amount", "out_amount",
        "net_amount", "net_ratio",
    ]
    return [{key: _value(row[key]) for key in columns} for _, row in result.iterrows()]


def build_daily(frame: pd.DataFrame) -> list[dict[str, Any]]:
    ordered = frame.copy()
    ordered["minute_ts"] = pd.to_datetime(ordered["minute_ts"], utc=True)
    result = (
        ordered.sort_values(["source", "sector_code", "minute_ts"])
        .groupby(["source", "sector_code"], sort=True, as_index=False)
        .tail(1)
        .sort_values("sector_code")
        .rename(columns={"minute_ts": "close_ts"})
    )
    columns = [
        "source", "sector_code", "sector_type", "sector_name", "trade_date",
        "close_ts", "avg_price", "pct_change", "in_amount", "out_amount",
        "net_amount", "net_ratio",
    ]
    return [{key: _value(row[key]) for key in columns} for _, row in result.iterrows()]


def _upload(granularity: str, records: list[dict[str, Any]]) -> int:
    total = 0
    batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
    for start in range(0, len(records), BATCH_SIZE):
        batch = records[start:start + BATCH_SIZE]
        number = start // BATCH_SIZE + 1
        result = _post(
            "/api/v1/fundflow/archive/import",
            {"granularity": granularity, "records": batch},
        )
        total += int(result.get("upserted", 0))
        print(f"[SERV00:{granularity}] batch {number}/{batches} rows={len(batch)} total={total}", flush=True)
    return total


def upload_online_history(frame: pd.DataFrame) -> dict[str, Any]:
    five_minute = build_5m(frame)
    daily = build_daily(frame)
    print(f"[SERV00] prepared 5m={len(five_minute)} daily={len(daily)}", flush=True)
    uploaded_5m = _upload("5m", five_minute)
    uploaded_daily = _upload("daily", daily)
    retention = _post("/api/v1/fundflow/archive/retention", {})
    return {
        "sector_flow_5m": uploaded_5m,
        "sector_flow_daily": uploaded_daily,
        "retention": retention,
    }


def upload_daily_records(records: list[dict[str, Any]]) -> int:
    """Upload already-aggregated Sina daily history without rebuilding from 1m."""
    return _upload("daily", records)
