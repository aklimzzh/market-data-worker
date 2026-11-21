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
from .services.intraday_repair import maybe_start
from .services.local_store import append_csv, save_pending, write_json_atomic
from .services.serv00 import post_batch
from .sources.sina import fetch_snapshot

TZ = ZoneInfo("Asia/Shanghai")


def phase(value):
    if COLLECT_AUCTION and dt_time(9, 15) <= value < dt_time(9, 30):
        return "opening_auction_observe"
    if dt_time(9, 30) <= value <= dt_time(11, 30):
        return "continuous_am"
    if dt_time(13, 0) <= value <= dt_time(15, 0):
        return "continuous_pm"
    if dt_time(15, 0) < value <= dt_time(15, 12):
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


def run():
    session = requests.Session()
    last_bucket = None
    last_repair_check = 0.0
    status_path = DATA_DIR / "status" / "collector.json"

    while True:
        now = datetime.now(TZ)
        if now.time() > dt_time(15, 12):
            retry_pending()
            write_json_atomic(status_path, {
                "state": "STOPPED",
                "stopped_at": now.isoformat(timespec="seconds"),
                "reason": "market session completed",
            })
            return

        market_phase = phase(now.time())
        monotonic_now = time.monotonic()
        if (
            market_phase in {"continuous_am", "continuous_pm"}
            and monotonic_now - last_repair_check >= 600
        ):
            maybe_start(now, minimum_gap=3, sleep_seconds=0.5)
            last_repair_check = monotonic_now

        if market_phase == "idle":
            write_json_atomic(status_path, {
                "state": "IDLE",
                "phase": "idle",
                "time": now.isoformat(timespec="seconds"),
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
                retry_pending()
            except Exception as exc:
                upload_state, error = "PENDING", str(exc)
                save_pending(
                    DATA_DIR / "pending" / f"{bucket.strftime('%Y%m%d_%H%M%S')}.json",
                    payload,
                )
                result = None

            write_json_atomic(status_path, {
                "state": "RUNNING",
                "phase": market_phase,
                "last_snapshot": bucket.isoformat(timespec="seconds"),
                "rows": len(records),
                "unique_sectors": len({record["sector_code"] for record in records}),
                "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
                "requests": request_metrics,
                "serv00_state": upload_state,
                "serv00_result": result,
                "error": error,
                "pending_files": len(list((DATA_DIR / "pending").glob("*.json"))),
            })
            last_bucket = bucket
        except Exception as exc:
            write_json_atomic(status_path, {
                "state": "ERROR",
                "phase": market_phase,
                "time": now.isoformat(timespec="seconds"),
                "error": f"{type(exc).__name__}: {exc}",
            })
            time.sleep(10)

        time.sleep(max(1, SINA_INTERVAL - (datetime.now(TZ) - now).total_seconds()))
