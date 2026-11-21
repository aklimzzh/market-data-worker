from __future__ import annotations

import os
import time
from typing import Any

import requests

EXPECTED_INDEXES = {"SSE", "SZSE", "STAR50", "CHINEXT"}


def _base_url() -> str:
    value = (
        os.getenv("SERV00_BASE_URL", "")
        or os.getenv("ETF_API_URL", "")
    ).strip().rstrip("/")
    if not value:
        raise RuntimeError("SERV00_BASE_URL or ETF_API_URL is required")
    return value


def status_overview(days: int = 30) -> dict[str, Any]:
    # Cache buster is intentional because this endpoint is used for repair decisions.
    response = requests.get(
        f"{_base_url()}/api/v1/status-overview",
        params={"days": str(days), "_": str(int(time.time()))},
        headers={"Cache-Control": "no-cache", "Accept": "application/json"},
        timeout=(10, 45),
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("success") is not True:
        raise RuntimeError(f"status-overview failed: {payload}")
    return payload


def audit_trade_date(trade_date: str) -> dict[str, Any]:
    payload = status_overview(30)
    day = next(
        (item for item in payload.get("days", []) if item.get("trade_date") == trade_date),
        None,
    )
    if day is None:
        return {
            "ok": False,
            "trade_date": trade_date,
            "overall_state": "MISSING",
            "missing_exchanges": ["SSE", "SZSE"],
            "missing_indexes": sorted(EXPECTED_INDEXES),
            "markets": {},
            "indexes": {"actual_count": 0, "codes": []},
        }

    markets = day.get("markets") or {}
    missing_exchanges = []
    for exchange in ("SSE", "SZSE"):
        market = markets.get(exchange) or {}
        actual = int(market.get("actual_count") or 0)
        state = market.get("state")
        if actual <= 0 or state != "OK":
            missing_exchanges.append(exchange)

    index_info = day.get("indexes") or {}
    codes = {str(item) for item in index_info.get("codes", [])}
    missing_indexes = sorted(EXPECTED_INDEXES - codes)

    return {
        "ok": not missing_exchanges and not missing_indexes,
        "trade_date": trade_date,
        "overall_state": day.get("overall_state"),
        "missing_exchanges": missing_exchanges,
        "missing_indexes": missing_indexes,
        "markets": markets,
        "indexes": index_info,
    }
