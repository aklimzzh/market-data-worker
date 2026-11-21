from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .config import DATA_DIR
from .fundflow_backfill import run as fundflow_backfill
from .services.etf_index_repair import repair_etf_exchange, repair_indexes
from .services.serv00_audit import audit_trade_date

TZ = ZoneInfo("Asia/Shanghai")


def atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)


def prune_parquet(keep: int = 5) -> list[str]:
    files = sorted(
        (DATA_DIR / "archive").glob("**/*.parquet"),
        key=lambda item: item.name,
    )
    removed = []
    if len(files) > keep:
        for path in files[:-keep]:
            path.unlink()
            removed.append(str(path))
    return removed


def run(trade_date: str, sleep_seconds: float = 0.5) -> None:
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

        print("[AUDIT:BEFORE] checking ETF and index data", flush=True)
        before = audit_trade_date(trade_date)
        status["steps"]["audit_before"] = before
        print(
            f"[AUDIT:BEFORE] state={before['overall_state']} "
            f"missing_exchanges={before['missing_exchanges']} "
            f"missing_indexes={before['missing_indexes']}",
            flush=True,
        )

        etf_repairs = []
        for exchange in before["missing_exchanges"]:
            etf_repairs.append(repair_etf_exchange(trade_date, exchange))
        status["steps"]["etf_repair"] = {
            "ok": True,
            "repaired": etf_repairs,
        }

        index_repair = repair_indexes(trade_date, before["missing_indexes"])
        status["steps"]["index_repair"] = index_repair

        print("[AUDIT:AFTER] rechecking ETF and index data", flush=True)
        after = audit_trade_date(trade_date)
        status["steps"]["audit_after"] = after
        if not after["ok"]:
            raise RuntimeError(
                "ETF/index repair validation failed: "
                f"missing_exchanges={after['missing_exchanges']} "
                f"missing_indexes={after['missing_indexes']}"
            )
        print("[AUDIT:AFTER] ETF and index data complete", flush=True)

        print("[FUNDFLOW] backfill, Parquet and CrateDB started", flush=True)
        fundflow_backfill(trade_date, sleep_seconds)
        backfill_path = DATA_DIR / "status" / f"backfill_{trade_date}.json"
        backfill_status = json.loads(backfill_path.read_text(encoding="utf-8"))
        status["steps"]["fundflow"] = {"ok": True, **backfill_status}
        print("[FUNDFLOW] completed", flush=True)

        removed = prune_parquet(keep=5)
        status["steps"]["local_retention"] = {
            "ok": True,
            "keep_trade_days": 5,
            "removed": removed,
        }
        print(f"[RETENTION] removed={len(removed)}", flush=True)

        status["state"] = "SUCCESS"
        status["finished_at"] = datetime.now(TZ).isoformat(timespec="seconds")
        atomic_json(status_path, status)
        print(json.dumps(status, ensure_ascii=False, indent=2), flush=True)
    except Exception as exc:
        status["state"] = "FAILED"
        status["error"] = f"{type(exc).__name__}: {exc}"
        status["finished_at"] = datetime.now(TZ).isoformat(timespec="seconds")
        atomic_json(status_path, status)
        print(f"[MAINTENANCE] FAILED: {status['error']}", flush=True)
        raise
