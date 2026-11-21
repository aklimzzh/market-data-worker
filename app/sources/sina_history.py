from __future__ import annotations

import time
from datetime import date
from typing import Any

import requests

from .sina import fetch_snapshot

HISTORY_URL = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssx_bkzj_fszs"
HEADERS = {
    "Referer": "https://vip.stock.finance.sina.com.cn/",
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
}


def sector_catalog(session: requests.Session | None = None) -> list[dict[str, str]]:
    rows, _ = fetch_snapshot(session)
    return [
        {
            "sector_code": row["category"],
            "sector_name": row["name"],
            "sector_type": row["sector_type"],
        }
        for row in rows
    ]


def fetch_sector_day(
    session: requests.Session,
    sector: dict[str, str],
    target_date: date,
    retries: int = 2,
) -> list[dict[str, Any]]:
    category_prefix = "0" if sector["sector_type"] == "industry" else "1"
    params = {
        "page": "1",
        "num": "300",
        "bankuai": f"{category_prefix}/{sector['sector_code']}",
        "sort": "time",
    }
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = session.get(HISTORY_URL, params=params, headers=HEADERS, timeout=(5, 20))
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list) or len(payload) != 2 or not isinstance(payload[1], list):
                raise ValueError("unexpected Sina history payload")
            points = payload[1]
            selected = [point for point in points if point.get("opendate") == target_date.isoformat()]
            if not selected:
                available = sorted({point.get("opendate") for point in points if point.get("opendate")})
                raise ValueError(f"target date unavailable; returned dates={available}")
            return selected
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1 if attempt == 0 else 3)
    raise RuntimeError(f"history fetch failed for {sector['sector_code']}: {last_error}")
