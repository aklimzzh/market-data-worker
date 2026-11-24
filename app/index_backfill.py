from __future__ import annotations

import json
import os
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

from src import sync_index_daily as sync

TZ = ZoneInfo("Asia/Shanghai")


def _is_eastmoney_rate_limit(exc: Exception) -> bool:
    text = f"{type(exc).__name__}: {exc}".lower()
    return (
        "push2ratelimiterror" in text
        or "rate limit" in text
        or "ratelimit" in text
        or "remotedisconnected" in text
    )


def run(start: str, end: str, sleep_seconds: float = 1.0) -> dict:
    start_day = date.fromisoformat(start)
    end_day = date.fromisoformat(end)
    if start_day > end_day:
        raise ValueError("start cannot be later than end")

    api_url = (os.getenv("ETF_API_URL") or os.getenv("SERV00_BASE_URL") or "").strip().rstrip("/")
    token = (os.getenv("ETF_API_TOKEN") or os.getenv("SERV00_API_TOKEN") or "").strip()
    if not api_url or not token:
        raise RuntimeError(
            "ETF_API_URL/SERV00_BASE_URL and ETF_API_TOKEN/SERV00_API_TOKEN are required"
        )

    start_text = start_day.strftime("%Y%m%d")
    end_text = end_day.strftime("%Y%m%d")
    eastmoney_disabled = False

    result = {
        "start": start,
        "end": end,
        "started_at": datetime.now(TZ).isoformat(timespec="seconds"),
        "source_order": ["tencent", "sina", "eastmoney"],
        "indexes": {},
    }

    for position, code in enumerate(sync.INDEX_ORDER):
        attempts = [
            ("tencent", lambda code=code: sync.fetch_tencent(code, start_day, end_day)),
            ("sina", lambda code=code: sync.fetch_sina(code, start_day, end_day)),
        ]
        if not eastmoney_disabled:
            attempts.append(
                (
                    "eastmoney",
                    lambda code=code: sync.fetch_eastmoney(
                        code, start_text, end_text, start_day, end_day
                    ),
                )
            )

        errors = []
        for source, fetcher in attempts:
            try:
                print(
                    f"[INDEX-BACKFILL:{code}] source={source} fetching {start}..{end}",
                    flush=True,
                )
                frame = fetcher()
                if source == "eastmoney" and "volume" in frame.columns:
                    frame = frame.copy()
                    frame["volume"] = frame["volume"] * 100

                records = sync.records_from_frame(frame, code, source)
                uploaded = 0
                for offset in range(0, len(records), 500):
                    batch = records[offset : offset + 500]
                    sync.upload_records(api_url, token, code, batch)
                    uploaded += len(batch)

                result["indexes"][code] = {
                    "ok": True,
                    "source": source,
                    "rows": uploaded,
                    "errors_before_success": errors,
                }
                break
            except Exception as exc:
                message = f"{source}: {type(exc).__name__}: {exc}"
                errors.append(message)
                print(f"[INDEX-BACKFILL:{code}] {message}", flush=True)
                if source == "eastmoney" and _is_eastmoney_rate_limit(exc):
                    eastmoney_disabled = True
                    print(
                        "[INDEX-BACKFILL] Eastmoney disabled for the remainder of this run",
                        flush=True,
                    )
                time.sleep(2)
        else:
            result["indexes"][code] = {"ok": False, "errors": errors}

        if position < len(sync.INDEX_ORDER) - 1:
            time.sleep(sleep_seconds)

    failed = [code for code, item in result["indexes"].items() if not item.get("ok")]
    result["state"] = "SUCCESS" if not failed else "PARTIAL"
    result["failed"] = failed
    result["eastmoney_disabled"] = eastmoney_disabled
    result["finished_at"] = datetime.now(TZ).isoformat(timespec="seconds")
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    if failed:
        raise RuntimeError(f"index backfill failed: {failed}")
    return result
