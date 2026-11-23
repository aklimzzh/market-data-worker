from __future__ import annotations

import csv
import json
import threading
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from ..config import DATA_DIR, SERV00_API_TOKEN, SERV00_INGEST_URL
from ..sources.sina_history import fetch_sector_day, sector_catalog
from .local_store import append_csv, write_json_atomic
from .serv00 import post_batch

TZ = ZoneInfo("Asia/Shanghai")
_LOCK = threading.Lock()
_RUNNING = False


def _csv_path(trade_date: date) -> Path:
    return DATA_DIR / "raw" / trade_date.strftime("%Y/%m") / f"{trade_date.isoformat()}.csv"


def _existing_minutes(trade_date: date) -> set[str]:
    path = _csv_path(trade_date)
    if not path.exists():
        return set()
    minutes: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            value = row.get("minute_ts")
            if value:
                minutes.add(value)
    return minutes


def _phase_for_tick(ticktime: str) -> str | None:
    if "09:30:00" <= ticktime <= "11:30:00":
        return "continuous_am"
    if "13:00:00" <= ticktime <= "15:00:00":
        return "continuous_pm"
    return None


def _trading_minutes_before(trade_date: date, cutoff: datetime) -> set[str]:
    """Return expected trading-minute timestamps older than the safe cutoff."""
    cutoff = cutoff.astimezone(TZ).replace(second=0, microsecond=0)
    expected: set[str] = set()
    sessions = ((9, 30, 11, 30), (13, 0, 15, 0))
    for sh, sm, eh, em in sessions:
        cursor = datetime(trade_date.year, trade_date.month, trade_date.day, sh, sm, tzinfo=TZ)
        end = datetime(trade_date.year, trade_date.month, trade_date.day, eh, em, tzinfo=TZ)
        while cursor <= end and cursor < cutoff:
            expected.add(cursor.isoformat(timespec="seconds"))
            cursor += timedelta(minutes=1)
    return expected


def _missing_minutes(trade_date: date, now: datetime, safety_delay: int) -> tuple[set[str], set[str], datetime]:
    safe_cutoff = now.astimezone(TZ).replace(second=0, microsecond=0) - timedelta(minutes=safety_delay)
    expected = _trading_minutes_before(trade_date, safe_cutoff)
    existing = _existing_minutes(trade_date)
    return expected - existing, expected, safe_cutoff


def _record(sector: dict, point: dict) -> dict:
    minute = datetime.fromisoformat(f"{point['opendate']}T{point['ticktime']}").replace(tzinfo=TZ)
    return {
        "trade_date": point["opendate"],
        "minute_ts": minute.isoformat(timespec="seconds"),
        "source": "sina",
        "market_phase": _phase_for_tick(point["ticktime"]) or "backfill",
        "sector_type": sector["sector_type"],
        "sector_code": sector["sector_code"],
        "sector_name": sector["sector_name"],
        "avg_price": point.get("avg_price"),
        "pct_change": point.get("avg_changeratio"),
        "turnover": None,
        "in_amount": point.get("inamount"),
        "out_amount": point.get("outamount"),
        "net_amount": point.get("netamount"),
        "net_ratio": point.get("ratioamount"),
        "leader_code": None,
        "leader_name": None,
        "leader_price": None,
        "leader_pct_change": None,
        "leader_net_ratio": None,
    }


def _write_status(trade_date: date, payload: dict) -> None:
    path = DATA_DIR / "status" / f"intraday_repair_{trade_date.isoformat()}.json"
    write_json_atomic(path, payload)


def _worker(
    trade_date: date,
    now: datetime,
    candidate_minutes: set[str],
    safe_cutoff: datetime,
    sleep_seconds: float,
) -> None:
    global _RUNNING
    started = datetime.now(TZ)
    status = {
        "state": "RUNNING",
        "trade_date": trade_date.isoformat(),
        "started_at": started.isoformat(timespec="seconds"),
        "started_from": now.replace(second=0, microsecond=0).isoformat(timespec="seconds"),
        "safe_cutoff": safe_cutoff.isoformat(timespec="seconds"),
        "candidate_minutes": sorted(candidate_minutes),
    }
    _write_status(trade_date, status)
    try:
        session = requests.Session()
        sectors = sector_catalog(session)
        if len(sectors) != 229:
            raise RuntimeError(f"expected 229 sectors, got {len(sectors)}")

        grouped: dict[str, list[dict]] = defaultdict(list)
        failures = []
        for index, sector in enumerate(sectors, start=1):
            try:
                points = fetch_sector_day(session, sector, trade_date)
                for point in points:
                    phase = _phase_for_tick(point["ticktime"])
                    if phase is None:
                        continue
                    record = _record(sector, point)
                    if record["minute_ts"] in candidate_minutes:
                        grouped[record["minute_ts"]].append(record)
            except Exception as exc:
                failures.append({"sector": sector["sector_code"], "error": str(exc)})
            if index % 25 == 0 or index == 229:
                print(f"[INTRADAY-REPAIR] fetched={index}/229 failures={len(failures)}", flush=True)
            time.sleep(sleep_seconds)

        if failures:
            raise RuntimeError(f"history fetch failed for {len(failures)} sectors")

        repaired = []
        skipped = []
        csv_path = _csv_path(trade_date)
        for minute in sorted(candidate_minutes):
            records = grouped.get(minute, [])
            unique = {item["sector_code"]: item for item in records}
            if len(unique) != 229:
                skipped.append({"minute_ts": minute, "records": len(unique)})
                continue
            records = list(unique.values())
            payload = {
                "schema_version": 1,
                "trade_date": trade_date.isoformat(),
                "minute_ts": minute,
                "market_phase": records[0]["market_phase"],
                "records": records,
            }
            post_batch(SERV00_INGEST_URL, SERV00_API_TOKEN, payload)
            append_csv(csv_path, records)
            repaired.append(minute)
            print(f"[INTRADAY-REPAIR] repaired {minute}", flush=True)

        status.update({
            "state": "SUCCESS" if not skipped else "PARTIAL",
            "missing_minutes": len(candidate_minutes),
            "repaired_minutes": repaired,
            "skipped_minutes": skipped,
            "finished_at": datetime.now(TZ).isoformat(timespec="seconds"),
        })
        _write_status(trade_date, status)
    except Exception as exc:
        status.update({
            "state": "FAILED",
            "error": f"{type(exc).__name__}: {exc}",
            "finished_at": datetime.now(TZ).isoformat(timespec="seconds"),
        })
        _write_status(trade_date, status)
        print(f"[INTRADAY-REPAIR] failed: {status['error']}", flush=True)
    finally:
        with _LOCK:
            _RUNNING = False


def maybe_start(
    now: datetime,
    minimum_gap: int = 1,
    sleep_seconds: float = 0.5,
    safety_delay: int = 5,
) -> bool:
    """Start a background repair when any internally missing safe minute exists."""
    global _RUNNING
    if not SERV00_INGEST_URL:
        return False
    trade_date = now.astimezone(TZ).date()
    missing, expected, safe_cutoff = _missing_minutes(trade_date, now, safety_delay)
    if len(missing) < minimum_gap:
        return False
    with _LOCK:
        if _RUNNING:
            return False
        _RUNNING = True
    thread = threading.Thread(
        target=_worker,
        args=(trade_date, now, missing, safe_cutoff, sleep_seconds),
        name="intraday-repair",
        daemon=True,
    )
    thread.start()
    print(
        f"[INTRADAY-REPAIR] started missing={len(missing)} "
        f"expected={len(expected)} safety_delay={safety_delay}min",
        flush=True,
    )
    return True
