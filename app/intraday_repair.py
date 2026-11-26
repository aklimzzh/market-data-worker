from __future__ import annotations

import csv
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
EXPECTED_SECTORS = 229


def _csv_path(trade_date: date) -> Path:
    return DATA_DIR / "raw" / trade_date.strftime("%Y/%m") / f"{trade_date.isoformat()}.csv"


def _existing_complete_minutes(trade_date: date) -> set[str]:
    """A minute exists only when all 229 unique sectors were written."""
    path = _csv_path(trade_date)
    if not path.exists():
        return set()
    sectors_by_minute: dict[str, set[str]] = defaultdict(set)
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            minute = row.get("minute_ts")
            sector = row.get("sector_code")
            if minute and sector:
                sectors_by_minute[minute].add(sector)
    return {
        minute
        for minute, sectors in sectors_by_minute.items()
        if len(sectors) == EXPECTED_SECTORS
    }


def _phase_for_tick(ticktime: str) -> str | None:
    if "09:30:00" <= ticktime <= "11:30:00":
        return "continuous_am"
    if "13:00:00" <= ticktime <= "14:56:59":
        return "continuous_pm"
    if "14:57:00" <= ticktime <= "14:59:59":
        return "closing_auction"
    if ticktime == "15:00:00":
        return "closing_auction_final"
    return None


def expected_trading_minutes(trade_date: date, cutoff: datetime | None = None) -> set[str]:
    cutoff_value = None
    if cutoff is not None:
        cutoff_value = cutoff.astimezone(TZ).replace(second=0, microsecond=0)
    expected: set[str] = set()
    for sh, sm, eh, em in ((9, 30, 11, 30), (13, 1, 15, 0)):
        cursor = datetime(trade_date.year, trade_date.month, trade_date.day, sh, sm, tzinfo=TZ)
        end = datetime(trade_date.year, trade_date.month, trade_date.day, eh, em, tzinfo=TZ)
        while cursor <= end:
            if cutoff_value is None or cursor < cutoff_value:
                expected.add(cursor.isoformat(timespec="seconds"))
            cursor += timedelta(minutes=1)
    return expected


def missing_minutes(trade_date: date, now: datetime, safety_delay: int) -> tuple[set[str], set[str], datetime]:
    safe_cutoff = now.astimezone(TZ).replace(second=0, microsecond=0) - timedelta(minutes=safety_delay)
    expected = expected_trading_minutes(trade_date, safe_cutoff)
    existing = _existing_complete_minutes(trade_date)
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
    write_json_atomic(DATA_DIR / "status" / f"intraday_repair_{trade_date.isoformat()}.json", payload)


def _repair(
    trade_date: date,
    requested_at: datetime,
    candidate_minutes: set[str],
    safe_cutoff: datetime,
    sleep_seconds: float,
    mode: str,
) -> dict:
    started = datetime.now(TZ)
    status = {
        "state": "RUNNING",
        "mode": mode,
        "trade_date": trade_date.isoformat(),
        "started_at": started.isoformat(timespec="seconds"),
        "started_from": requested_at.replace(second=0, microsecond=0).isoformat(timespec="seconds"),
        "safe_cutoff": safe_cutoff.isoformat(timespec="seconds"),
        "candidate_minutes": sorted(candidate_minutes),
    }
    _write_status(trade_date, status)
    if not candidate_minutes:
        status.update({
            "state": "SUCCESS",
            "missing_minutes": 0,
            "repaired_minutes": [],
            "skipped_minutes": [],
            "finished_at": datetime.now(TZ).isoformat(timespec="seconds"),
        })
        _write_status(trade_date, status)
        return status

    session = requests.Session()
    sectors = sector_catalog(session)
    if len(sectors) != EXPECTED_SECTORS:
        raise RuntimeError(f"expected {EXPECTED_SECTORS} sectors, got {len(sectors)}")

    grouped: dict[str, list[dict]] = defaultdict(list)
    failures: list[dict] = []
    for index, sector in enumerate(sectors, start=1):
        try:
            for point in fetch_sector_day(session, sector, trade_date):
                if _phase_for_tick(point["ticktime"]) is None:
                    continue
                record = _record(sector, point)
                if record["minute_ts"] in candidate_minutes:
                    grouped[record["minute_ts"]].append(record)
        except Exception as exc:
            failures.append({"sector": sector["sector_code"], "error": str(exc)})
        if index % 25 == 0 or index == EXPECTED_SECTORS:
            print(f"[INTRADAY-REPAIR] fetched={index}/{EXPECTED_SECTORS} failures={len(failures)}", flush=True)
        time.sleep(sleep_seconds)
    if failures:
        raise RuntimeError(f"history fetch failed for {len(failures)} sectors")

    repaired: list[str] = []
    skipped: list[dict] = []
    csv_path = _csv_path(trade_date)
    for minute in sorted(candidate_minutes):
        unique = {item["sector_code"]: item for item in grouped.get(minute, [])}
        if len(unique) != EXPECTED_SECTORS:
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

    remaining = sorted(expected_trading_minutes(trade_date) - _existing_complete_minutes(trade_date))
    status.update({
        "state": "SUCCESS" if not skipped and not remaining else "PARTIAL",
        "missing_minutes": len(candidate_minutes),
        "repaired_minutes": repaired,
        "skipped_minutes": skipped,
        "remaining_missing_minutes": remaining,
        "finished_at": datetime.now(TZ).isoformat(timespec="seconds"),
    })
    _write_status(trade_date, status)
    return status


def _background_worker(*args) -> None:
    global _RUNNING
    trade_date = args[0]
    try:
        _repair(*args)
    except Exception as exc:
        payload = {
            "state": "FAILED",
            "mode": "intraday",
            "trade_date": trade_date.isoformat(),
            "error": f"{type(exc).__name__}: {exc}",
            "finished_at": datetime.now(TZ).isoformat(timespec="seconds"),
        }
        _write_status(trade_date, payload)
        print(f"[INTRADAY-REPAIR] failed: {payload['error']}", flush=True)
    finally:
        with _LOCK:
            _RUNNING = False


def maybe_start(now: datetime, minimum_gap: int = 1, sleep_seconds: float = 0.5, safety_delay: int = 5) -> bool:
    global _RUNNING
    if not SERV00_INGEST_URL:
        return False
    trade_date = now.astimezone(TZ).date()
    missing, expected, safe_cutoff = missing_minutes(trade_date, now, safety_delay)
    if len(missing) < minimum_gap:
        return False
    with _LOCK:
        if _RUNNING:
            return False
        _RUNNING = True
    thread = threading.Thread(
        target=_background_worker,
        args=(trade_date, now, missing, safe_cutoff, sleep_seconds, "intraday"),
        name="intraday-repair",
        daemon=True,
    )
    thread.start()
    print(f"[INTRADAY-REPAIR] started missing={len(missing)} expected={len(expected)} safety_delay={safety_delay}min", flush=True)
    return True


def repair_now(now: datetime, sleep_seconds: float = 0.25) -> dict:
    """Blocking final repair for every incomplete official trading minute."""
    if not SERV00_INGEST_URL:
        raise RuntimeError("SERV00_INGEST_URL is empty")
    trade_date = now.astimezone(TZ).date()
    expected = expected_trading_minutes(trade_date)
    missing = expected - _existing_complete_minutes(trade_date)
    print(f"[FINAL-REPAIR] missing_before={len(missing)} expected={len(expected)}", flush=True)
    result = _repair(trade_date, now, missing, now.astimezone(TZ), sleep_seconds, "final")
    result["expected_minutes"] = len(expected)
    result["complete_minutes"] = len(_existing_complete_minutes(trade_date))
    return result
