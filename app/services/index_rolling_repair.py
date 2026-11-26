from __future__ import annotations

import csv
import threading
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from ..config import DATA_DIR
from ..index_minute_backfill import fetch_sina, normalise, record
from ..sources.index_realtime import CONFIG
from .index_minute import FIELDS, upload_batch
from .local_store import write_json_atomic

TZ = ZoneInfo("Asia/Shanghai")
_LOCK = threading.Lock()
_RUNNING = False


def _status_path(day):
    return DATA_DIR / "status" / f"index_rolling_repair_{day.isoformat()}.json"


def _append(records):
    if not records:
        return
    day = datetime.fromisoformat(records[0]["minute_ts"]).astimezone(TZ).date()
    target = DATA_DIR / "index_raw" / day.strftime("%Y/%m") / f"{day}.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    new = not target.exists()
    with target.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        if new:
            writer.writeheader()
        writer.writerows(records)


def repair_now(now: datetime, minutes: int | None = 30, mode: str = "rolling", morning_only: bool = False):
    now = now.astimezone(TZ)
    day = now.date()
    started = datetime.now(TZ)
    frames, failures = [], []
    for code, (sina_symbol, _, _) in CONFIG.items():
        try:
            frame = normalise(fetch_sina(sina_symbol), code)
            frame = frame[frame["trade_date"] == day]
            if morning_only:
                frame = frame[frame["minute_ts"].dt.time <= time(11, 30)]
            elif minutes is not None:
                cutoff = (now - timedelta(minutes=minutes)).replace(tzinfo=None)
                ceiling = now.replace(tzinfo=None)
                frame = frame[(frame["minute_ts"] >= cutoff) & (frame["minute_ts"] <= ceiling)]
            if not frame.empty:
                frames.append(frame)
        except Exception as exc:
            failures.append({"index_code": code, "error": f"{type(exc).__name__}: {exc}"})
    data = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if data.empty:
        records = []
    else:
        data = data.sort_values(["minute_ts", "index_code"]).drop_duplicates(["index_code", "minute_ts"], keep="last")
        records = [record(row) for _, row in data.iterrows()]
    uploaded = 0
    for offset in range(0, len(records), 400):
        batch = records[offset:offset + 400]
        result = upload_batch(batch)
        uploaded += int(result.get("upserted", len(batch)))
    _append(records)
    output = {
        "state": "SUCCESS" if not failures else "PARTIAL", "mode": mode,
        "trade_date": day.isoformat(), "minutes": minutes, "morning_only": morning_only,
        "indexes_success": len(CONFIG) - len(failures), "indexes_failed": len(failures),
        "rows": len(records), "rows_upserted": uploaded, "failures": failures,
        "duration_seconds": round((datetime.now(TZ) - started).total_seconds(), 1),
        "finished_at": datetime.now(TZ).isoformat(timespec="seconds"),
    }
    write_json_atomic(_status_path(day), output)
    print(f"[INDEX-REPAIR] finished mode={mode} rows={len(records)} uploaded={uploaded} failures={len(failures)}", flush=True)
    return output


def _background(now, minutes, mode, morning_only):
    global _RUNNING
    try:
        repair_now(now, minutes=minutes, mode=mode, morning_only=morning_only)
    except Exception as exc:
        payload = {"state": "FAILED", "mode": mode, "error": f"{type(exc).__name__}: {exc}", "finished_at": datetime.now(TZ).isoformat(timespec="seconds")}
        write_json_atomic(_status_path(now.astimezone(TZ).date()), payload)
        print(f"[INDEX-REPAIR] failed: {payload['error']}", flush=True)
    finally:
        with _LOCK:
            _RUNNING = False


def maybe_start(now: datetime, minutes: int | None = 30, mode: str = "rolling", morning_only: bool = False):
    global _RUNNING
    with _LOCK:
        if _RUNNING:
            print(f"[INDEX-REPAIR] skip mode={mode}; previous run active", flush=True)
            return False
        _RUNNING = True
    threading.Thread(target=_background, args=(now, minutes, mode, morning_only), name="index-rolling-repair", daemon=True).start()
    print(f"[INDEX-REPAIR] started mode={mode} minutes={minutes} morning_only={morning_only}", flush=True)
    return True
