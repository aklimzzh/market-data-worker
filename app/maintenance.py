from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .calendar_update import run as calendar_update
from .config import DATA_DIR
from .fundflow_backfill import run as fundflow_backfill
from .services.archive_day import archive_day
from .services.etf_index_repair import repair_etf_exchange, repair_indexes
from .services.index_minute import retention as index_retention
from .services.serv00_audit import audit_trade_date

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


def _sector_parquet_exists(trade_date: str) -> bool:
    day = datetime.fromisoformat(trade_date)
    return (DATA_DIR / "archive" / day.strftime("%Y/%m") / f"{trade_date}.parquet").exists()


def run(
    trade_date: str,
    sleep_seconds: float = 0.5,
    refresh_calendar: bool = False,
    archive_only: bool = False,
) -> None:
    status_path = DATA_DIR / "status" / f"maintenance_{trade_date}.json"
    status = {
        "trade_date": trade_date,
        "archive_only": archive_only,
        "started_at": datetime.now(TZ).isoformat(timespec="seconds"),
        "state": "RUNNING",
        "steps": {},
    }
    atomic_json(status_path, status)
    try:
        print(
            f"[MAINTENANCE] trade_date={trade_date} archive_only={archive_only}",
            flush=True,
        )
        if not archive_only:
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
                trade_date, before["missing_indexes"]
            )
            after = audit_trade_date(trade_date)
            status["steps"]["audit_after"] = after
            if not after["ok"]:
                raise RuntimeError(
                    f"repair validation failed: exchanges={after['missing_exchanges']} "
                    f"indexes={after['missing_indexes']}"
                )

            # Rolling/final repair already created the authoritative sector parquet.
            # Only fall back to Sina history when the parquet is genuinely missing.
            if _sector_parquet_exists(trade_date):
                status["steps"]["fundflow"] = {
                    "ok": True,
                    "skipped": True,
                    "reason": "sector parquet already exists",
                }
                print("[FUNDFLOW] skipped; existing sector parquet found", flush=True)
            else:
                print("[FUNDFLOW] parquet missing; fallback backfill started", flush=True)
                fundflow_backfill(trade_date, sleep_seconds)
                backfill_path = DATA_DIR / "status" / f"backfill_{trade_date}.json"
                status["steps"]["fundflow"] = {
                    "ok": True,
                    **json.loads(backfill_path.read_text(encoding="utf-8")),
                }

        status["steps"]["crate_archive"] = archive_day(trade_date)

        # Retention is allowed only after both Sector and Index archives validate.
        status["steps"]["index_1m_retention"] = index_retention()

        if not archive_only:
            removed = prune_parquet(keep=5)
            status["steps"]["local_retention"] = {
                "ok": True,
                "keep_trade_days": 5,
                "removed": removed,
            }
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
