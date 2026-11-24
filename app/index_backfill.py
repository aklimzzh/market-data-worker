from __future__ import annotations

import json
import os
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

from src import sync_index_daily as sync

TZ = ZoneInfo("Asia/Shanghai")


def run(start: str, end: str, sleep_seconds: float = 1.0) -> dict:
    start_day = date.fromisoformat(start)
    end_day = date.fromisoformat(end)
    if start_day > end_day:
        raise ValueError("start cannot be later than end")

    api_url = (os.getenv("ETF_API_URL") or os.getenv("SERV00_BASE_URL") or "").strip().rstrip("/")
    token = (os.getenv("ETF_API_TOKEN") or os.getenv("SERV00_API_TOKEN") or "").strip()
    if not api_url or not token:
        raise RuntimeError("ETF_API_URL/SERV00_BASE_URL and ETF_API_TOKEN/SERV00_API_TOKEN are required")

    start_text = start_day.strftime("%Y%m%d")
    end_text = end_day.strftime("%Y%m%d")
    result = {
        "start": start,
        "end": end,
        "started_at": datetime.now(TZ).isoformat(timespec="seconds"),
        "indexes": {},
    }
    for position, code in enumerate(sync.INDEX_ORDER):
        # For wide historical ranges, Eastmoney is attempted first because it accepts
        # explicit start/end parameters. Sina and Tencent remain fallbacks.
        attempts = (
            ("eastmoney", lambda: sync.fetch_eastmoney(code, start_text, end_text, start_day, end_day)),
            ("tencent", lambda: sync.fetch_tencent(code, start_day, end_day)),
            ("sina", lambda: sync.fetch_sina(code, start_day, end_day)),
        )
        errors = []
        for source, fetcher in attempts:
            try:
                print(f"[INDEX-BACKFILL:{code}] source={source} fetching {start}..{end}", flush=True)
                frame = fetcher()
                # Eastmoney historical volume is in lots (hands); the database
                # canonical unit is shares. Tencent/Sina adapters already yield shares.
                if source == "eastmoney" and "volume" in frame.columns:
                    frame = frame.copy()
                    frame["volume"] = frame["volume"] * 100
                records = sync.records_from_frame(frame, code, source)
                uploaded = 0
                for offset in range(0, len(records), 500):
                    batch = records[offset:offset + 500]
                    sync.upload_records(api_url, token, code, batch)
                    uploaded += len(batch)
                result["indexes"][code] = {"ok": True, "source": source, "rows": uploaded}
                break
            except Exception as exc:
                errors.append(f"{source}: {type(exc).__name__}: {exc}")
                print(f"[INDEX-BACKFILL:{code}] {errors[-1]}", flush=True)
                time.sleep(2)
        else:
            result["indexes"][code] = {"ok": False, "errors": errors}
        if position < len(sync.INDEX_ORDER) - 1:
            time.sleep(sleep_seconds)

    failed = [code for code, item in result["indexes"].items() if not item.get("ok")]
    result["state"] = "SUCCESS" if not failed else "PARTIAL"
    result["failed"] = failed
    result["finished_at"] = datetime.now(TZ).isoformat(timespec="seconds")
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    if failed:
        raise RuntimeError(f"index backfill failed: {failed}")
    return result
