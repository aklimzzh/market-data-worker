from __future__ import annotations

import json
import time
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo

import requests

from .config import (
    COLLECT_AUCTION,
    DATA_DIR,
    SERV00_API_TOKEN,
    SERV00_INGEST_URL,
    SINA_INTERVAL,
)
from .services.intraday_repair import maybe_start, repair_now
from .services.daily_finalize import finalize_parquet
from .services.local_store import append_csv, save_pending, write_json_atomic
from .services.serv00 import post_batch
from .sources.sina import fetch_snapshot
from .services.index_minute import collect as collect_indexes, finalize as finalize_indexes

TZ = ZoneInfo("Asia/Shanghai")
FINAL_CHECK_TIMES = (dt_time(15, 35), dt_time(15, 40))
FINAL_RETRY_DEADLINE = dt_time(15, 44)


def phase(value: dt_time) -> str:
    if COLLECT_AUCTION and dt_time(9, 15) <= value < dt_time(9, 30):
        return "opening_auction_observe"
    if dt_time(9, 30) <= value <= dt_time(11, 30):
        return "continuous_am"
    if dt_time(13, 0) <= value <= dt_time(15, 0):
        return "continuous_pm"
    if dt_time(15, 0) < value <= dt_time(15, 12):
        return "closing_revision"
    if dt_time(15, 35) <= value < dt_time(15, 36):
        return "closing_revision"
    if dt_time(15, 40) <= value < dt_time(15, 41):
        return "closing_revision"
    return "idle"


def normalize(rows, minute_ts, market_phase):
    output = []
    for item in rows:
        output.append({
            "trade_date": minute_ts.date().isoformat(),
            "minute_ts": minute_ts.isoformat(timespec="seconds"),
            "source": "sina",
            "market_phase": market_phase,
            "sector_type": item["sector_type"],
            "sector_code": item["category"],
            "sector_name": item["name"],
            "avg_price": item.get("avg_price"),
            "pct_change": item.get("avg_changeratio"),
            "turnover": item.get("turnover"),
            "in_amount": item.get("inamount"),
            "out_amount": item.get("outamount"),
            "net_amount": item.get("netamount"),
            "net_ratio": item.get("ratioamount"),
            "leader_code": item.get("ts_symbol"),
            "leader_name": item.get("ts_name"),
            "leader_price": item.get("ts_trade"),
            "leader_pct_change": item.get("ts_changeratio"),
            "leader_net_ratio": item.get("ts_ratioamount"),
        })
    return output


def pending_count() -> int:
    return len(list((DATA_DIR / "pending").glob("*.json")))


def retry_pending():
    if not SERV00_INGEST_URL:
        return 0
    pending_dir = DATA_DIR / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    sent = 0
    for path in sorted(pending_dir.glob("*.json"))[:10]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            post_batch(SERV00_INGEST_URL, SERV00_API_TOKEN, payload)
            path.unlink()
            sent += 1
        except Exception:
            break
    return sent


def _is_final_check(bucket: datetime) -> bool:
    return bucket.time() in FINAL_CHECK_TIMES


def _status_base(final_checks: list[str]) -> dict:
    return {
        "final_checks": final_checks,
        "pending_files": pending_count(),
    }


def run():
    session = requests.Session()
    last_bucket = None
    repair_triggers: set[str] = set()
    final_checks: list[str] = []
    pending_flushed = 0
    finalization_result = None
    status_path = DATA_DIR / "status" / "collector.json"

    while True:
        now = datetime.now(TZ)
        current_time = now.time()

        # After the 15:40 final snapshot, keep retrying pending uploads until
        # everything is delivered. 15:44 is the hard deadline; host safety-stop
        # remains at 15:45.
        if dt_time(15, 40) < current_time:
            final_1540_done = any(value.endswith("T15:40:00+08:00") for value in final_checks)
            pending_flushed += retry_pending()
            remaining = pending_count()
            if current_time >= FINAL_RETRY_DEADLINE:
                write_json_atomic(status_path, {
                    "state": "STOPPED",
                    "stopped_at": now.isoformat(timespec="seconds"),
                    "reason": "post-close hard deadline reached",
                    "final_checks": final_checks,
                    "pending_flushed": pending_flushed,
                    "pending_files": remaining,
                    "finalization": finalization_result,
                    "warning": None if final_1540_done else "15:40 final snapshot was not completed",
                })
                return
            write_json_atomic(status_path, {
                "state": "FINALIZING",
                "phase": "post_close_upload_retry",
                "time": now.isoformat(timespec="seconds"),
                "final_checks": final_checks,
                "pending_flushed": pending_flushed,
                "pending_files": remaining,
            })
            time.sleep(10)
            continue

        market_phase = phase(current_time)
        bucket_key = now.replace(second=0, microsecond=0).isoformat(timespec="minutes")
        rolling_due = (
            current_time.second < 10
            and current_time.minute % 10 == 0
            and (
                dt_time(9, 40) <= current_time <= dt_time(11, 30)
                or dt_time(13, 10) <= current_time <= dt_time(15, 0)
            )
        )
        midday_due = (
            current_time.second < 10
            and current_time.hour == 11
            and current_time.minute == 45
        )
        if rolling_due and bucket_key not in repair_triggers:
            if maybe_start(now, num=30, mode="rolling"):
                repair_triggers.add(bucket_key)
        if midday_due and bucket_key not in repair_triggers:
            if maybe_start(now, num=300, mode="midday"):
                repair_triggers.add(bucket_key)

        if market_phase == "idle":
            idle_phase = "post_close_wait" if dt_time(15, 12) < current_time < dt_time(15, 40) else "idle"
            write_json_atomic(status_path, {
                "state": "IDLE",
                "phase": idle_phase,
                "time": now.isoformat(timespec="seconds"),
                **_status_base(final_checks),
            })
            time.sleep(10)
            continue

        bucket = now.replace(second=0, microsecond=0)
        if bucket == last_bucket:
            time.sleep(1)
            continue

        started = time.monotonic()
        try:
            raw, request_metrics = fetch_snapshot(session)
            index_rows, index_upload = collect_indexes(session)
            records = normalize(raw, bucket, market_phase)
            csv_path = DATA_DIR / "raw" / bucket.strftime("%Y/%m") / f"{bucket.date().isoformat()}.csv"
            append_csv(csv_path, records)
            payload = {
                "schema_version": 1,
                "trade_date": bucket.date().isoformat(),
                "minute_ts": bucket.isoformat(timespec="seconds"),
                "market_phase": market_phase,
                "records": records,
            }
            try:
                result = post_batch(SERV00_INGEST_URL, SERV00_API_TOKEN, payload)
                upload_state, error = "OK", None
                pending_flushed += retry_pending()
            except Exception as exc:
                upload_state, error = "PENDING", str(exc)
                save_pending(
                    DATA_DIR / "pending" / f"{bucket.strftime('%Y%m%d_%H%M%S')}.json",
                    payload,
                )
                result = None

            if _is_final_check(bucket) and bucket.isoformat(timespec="seconds") not in final_checks:
                final_checks.append(bucket.isoformat(timespec="seconds"))

            write_json_atomic(status_path, {
                "state": "RUNNING",
                "phase": market_phase,
                "last_snapshot": bucket.isoformat(timespec="seconds"),
                "is_final_check": _is_final_check(bucket),
                "rows": len(records),
                "unique_sectors": len({record["sector_code"] for record in records}),
                "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
                "requests": request_metrics,
                  "index_rows": len(index_rows),
                  "index_serv00_result": index_upload,
                "serv00_state": upload_state,
                "serv00_result": result,
                "error": error,
                "pending_flushed": pending_flushed,
                **_status_base(final_checks),
            })
            last_bucket = bucket
        except Exception as exc:
            write_json_atomic(status_path, {
                "state": "ERROR",
                "phase": market_phase,
                "time": now.isoformat(timespec="seconds"),
                "error": f"{type(exc).__name__}: {exc}",
                **_status_base(final_checks),
            })
            time.sleep(10)

        time.sleep(max(1, SINA_INTERVAL - (datetime.now(TZ) - now).total_seconds()))
