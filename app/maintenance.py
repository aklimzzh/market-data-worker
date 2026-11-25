from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .calendar_update import run as calendar_update
from .config import DATA_DIR
from .fundflow_backfill import run as fundflow_backfill
from .services.etf_index_repair import repair_etf_exchange, repair_indexes
from .services.serv00_audit import audit_trade_date
from .services.index_minute import retention as index_retention
from .services.cratedb import ensure_schema, index_1m_count, upload_index_1m

TZ = ZoneInfo("Asia/Shanghai")


def atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)


def prune_parquet(keep: int = 5) -> list[str]:
    files = sorted((DATA_DIR / "archive").glob("**/*.parquet"), key=lambda item: item.name)
    removed = []
    if len(files) > keep:
        for path in files[:-keep]:
            path.unlink()
            removed.append(str(path))
    return removed



def archive_index_1m(trade_date: str) -> dict:
    import pandas as pd

    day = date.fromisoformat(trade_date)
    parquet = DATA_DIR / "index_archive" / day.strftime("%Y/%m") / f"{trade_date}.parquet"
    if not parquet.exists():
        raise FileNotFoundError(f"index 1m parquet missing: {parquet}")
    frame = pd.read_parquet(parquet)
    if frame.empty:
        raise RuntimeError(f"index 1m parquet is empty: {parquet}")
    frame = frame.drop_duplicates(["index_code", "minute_ts"], keep="last")
    records = frame.where(frame.notna(), None).to_dict(orient="records")
    ensure_schema()
    upserted = upload_index_1m(records)
    crate_rows = index_1m_count(trade_date)
    expected_rows = len(records)
    if crate_rows < expected_rows:
        raise RuntimeError(
            f"CrateDB index_flow_1m incomplete: expected={expected_rows} actual={crate_rows}"
        )
    return {
        "ok": True,
        "parquet": str(parquet),
        "source_rows": expected_rows,
        "crate_upserted": upserted,
        "crate_rows": crate_rows,
    }

def run(trade_date: str, sleep_seconds: float = 0.5, refresh_calendar: bool = False) -> None:
    status_path = DATA_DIR / "status" / f"maintenance_{trade_date}.json"
    status = {
        "trade_date": trade_date,
        "started_at": datetime.now(TZ).isoformat(timespec="seconds"),
        "state": "RUNNING",
        "steps": {},
    }
    atomic_json(status_path, status)
    try:
        print(f"[MAINTENANCE] trade_date={trade_date}", flush=True)
        before = audit_trade_date(trade_date)
        status["steps"]["audit_before"] = before
        print(
            f"[AUDIT:BEFORE] missing_exchanges={before['missing_exchanges']} "
            f"missing_indexes={before['missing_indexes']}",
            flush=True,
        )

        etf_repairs = [
            repair_etf_exchange(trade_date, exchange)
            for exchange in before["missing_exchanges"]
        ]
        status["steps"]["etf_repair"] = {"ok": True, "repaired": etf_repairs}
        status["steps"]["index_repair"] = repair_indexes(
            trade_date,
            before["missing_indexes"],
        )

        after = audit_trade_date(trade_date)
        status["steps"]["audit_after"] = after
        if not after["ok"]:
            raise RuntimeError(
                f"repair validation failed: exchanges={after['missing_exchanges']} "
                f"indexes={after['missing_indexes']}"
            )

        fundflow_backfill(trade_date, sleep_seconds)
        backfill_path = DATA_DIR / "status" / f"backfill_{trade_date}.json"
        status["steps"]["fundflow"] = {
            "ok": True,
            **json.loads(backfill_path.read_text(encoding="utf-8")),
        }

        removed = prune_parquet(keep=5)
        status["steps"]["local_retention"] = {
            "ok": True,
            "keep_trade_days": 5,
            "removed": removed,
        }

        # Archive must succeed before Serv00 retention is allowed.
        status["steps"]["index_1m_archive"] = archive_index_1m(trade_date)
        status["steps"]["index_1m_retention"] = index_retention()

        if refresh_calendar:
            print("[CALENDAR] Saturday refresh started", flush=True)
            calendar_update()
            status["steps"]["calendar_update"] = {"ok": True}

        status["state"] = "SUCCESS"
        status["finished_at"] = datetime.now(TZ).isoformat(timespec="seconds")
        atomic_json(status_path, status)
        print(json.dumps(status, ensure_ascii=False, indent=2), flush=True)
    except Exception as exc:
        status["state"] = "FAILED"
        status["error"] = f"{type(exc).__name__}: {exc}"
        status["finished_at"] = datetime.now(TZ).isoformat(timespec="seconds")
        atomic_json(status_path, status)
        raise
