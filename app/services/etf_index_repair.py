from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any


def configure_environment() -> tuple[str, str]:
    base_url = (
        os.getenv("ETF_API_URL", "")
        or os.getenv("SERV00_BASE_URL", "")
    ).strip().rstrip("/")
    token = (
        os.getenv("ETF_API_TOKEN", "")
        or os.getenv("SERV00_API_TOKEN", "")
    ).strip()
    if not base_url or not token:
        raise RuntimeError(
            "ETF_API_URL/SERV00_BASE_URL and ETF_API_TOKEN/SERV00_API_TOKEN are required"
        )
    os.environ["ETF_API_URL"] = base_url
    os.environ["ETF_API_TOKEN"] = token
    return base_url, token


def repair_etf_exchange(trade_date: str, exchange: str) -> dict[str, Any]:
    if exchange not in {"SSE", "SZSE"}:
        raise ValueError(f"unsupported ETF exchange: {exchange}")
    configure_environment()
    # Reuse the existing production collector and upload protocol unchanged.
    from src.main import run as run_etf

    print(f"[ETF:{exchange}] repair started date={trade_date}", flush=True)
    rc = run_etf(
        target=trade_date,
        exchange=exchange,
        no_upload=False,
        allow_not_ready=False,
    )
    if rc not in (None, 0):
        raise RuntimeError(f"ETF repair returned rc={rc}")
    print(f"[ETF:{exchange}] repair completed", flush=True)
    return {"ok": True, "exchange": exchange, "trade_date": trade_date}


def repair_indexes(trade_date: str, missing_indexes: list[str]) -> dict[str, Any]:
    if not missing_indexes:
        return {"ok": True, "repaired": []}
    api_url, api_token = configure_environment()

    # Disable scheduled-run jitter inside the on-demand repair container.
    os.environ["INDEX_STARTUP_JITTER_MAX"] = "0"
    os.environ["INDEX_BETWEEN_INDEX_MIN"] = "0.5"
    os.environ["INDEX_BETWEEN_INDEX_MAX"] = "1.5"
    os.environ["INDEX_SOURCE_WAIT_SECONDS"] = "2"

    from src import sync_index_daily as index_sync

    target_day = date.fromisoformat(trade_date)
    target_text = target_day.strftime("%Y%m%d")
    repaired = []
    sources = {}

    for index_code in missing_indexes:
        if index_code not in index_sync.INDEX_CONFIG:
            raise RuntimeError(f"unknown index code: {index_code}")
        print(f"[INDEX:{index_code}] repair started date={trade_date}", flush=True)
        frame, source = index_sync.fetch_with_fallback(
            index_code,
            target_text,
            target_text,
            target_day,
            target_day,
        )
        records = index_sync.records_from_frame(frame, index_code, source)
        records = [
            row for row in records
            if str(row.get("trade_date")) == trade_date
        ]
        if len(records) != 1:
            raise RuntimeError(
                f"{index_code}: expected exactly one row for {trade_date}, got {len(records)}"
            )
        index_sync.upload_records(api_url, api_token, index_code, records)
        repaired.append(index_code)
        sources[index_code] = source
        print(f"[INDEX:{index_code}] repair completed source={source}", flush=True)

    return {"ok": True, "repaired": repaired, "sources": sources}
