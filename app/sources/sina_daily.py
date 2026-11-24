from __future__ import annotations

import time
from typing import Any

import requests

URL = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_bkzj_zjlrqs"
HEADERS = {
    "Referer": "https://vip.stock.finance.sina.com.cn/",
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
}


def fetch_sector_daily(session: requests.Session, sector: dict[str, str], days: int, retries: int = 2) -> list[dict[str, Any]]:
    prefix = "0" if sector["sector_type"] == "industry" else "1"
    params = {
        "page": "1",
        "num": str(days),
        "sort": "opendate",
        "asc": "0",
        "bankuai": f"{prefix}/{sector['sector_code']}",
    }
    last_error = None
    for attempt in range(retries + 1):
        try:
            response = session.get(URL, params=params, headers=HEADERS, timeout=(5, 20))
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list) or not payload:
                raise ValueError("empty or unexpected Sina daily payload")
            return payload
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1 if attempt == 0 else 3)
    raise RuntimeError(f"daily history failed for {sector['sector_code']}: {last_error}")
