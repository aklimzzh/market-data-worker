from __future__ import annotations

import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from .config import DATA_DIR
from .sources.sina_daily import fetch_sector_daily
from .sources.sina_history import sector_catalog
from .services.cratedb import ensure_schema, upload_daily
from .services.serv00_online import upload_daily_records

TZ = ZoneInfo("Asia/Shanghai")


def _float(value):
    if value in (None, "", "-"):
        return None
    return float(value)


def _int(value):
    if value in (None, "", "-"):
        return None
    return int(float(value))


def _write_status(payload: dict) -> None:
    path = DATA_DIR / "status" / "fundflow_daily_backfill.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)


def run(days: int = 250, sleep_seconds: float = 0.3) -> dict:
    if days < 1 or days > 250:
        raise ValueError("days must be between 1 and 100")

    result = {
        "state": "RUNNING",
        "days_requested": days,
        "started_at": datetime.now(TZ).isoformat(timespec="seconds"),
        "steps": {
            "sina": {"ok": False, "rows": 0, "failures": 0},
            "serv00": {"ok": False, "upserted": 0},
            "cratedb": {"ok": False, "upserted": 0},
        },
    }
    _write_status(result)

    session = requests.Session()
    sectors = sector_catalog(session)
    if len(sectors) != 229:
        raise RuntimeError(f"expected 229 sectors, got {len(sectors)}")

    records = []
    failures = []
    for index, sector in enumerate(sectors, start=1):
        try:
            points = fetch_sector_daily(session, sector, days)
            for point in points:
                trade_date = str(point["opendate"])
                records.append(
                    {
                        "source": "sina",
                        "sector_code": sector["sector_code"],
                        "sector_type": sector["sector_type"],
                        "sector_name": sector["sector_name"],
                        "trade_date": trade_date,
                        "close_ts": f"{trade_date}T15:00:00+08:00",
                        "avg_price": _float(point.get("avg_price")),
                        "pct_change": _float(point.get("avg_changeratio")),
                        "in_amount": None,
                        "out_amount": None,
                        "net_amount": _float(point.get("netamount")),
                        "net_ratio": _float(point.get("ratioamount")),
                        "turnover": _float(point.get("turnover")),
                        "main_net_amount": _float(point.get("r0_net")),
                        "main_net_ratio": _float(point.get("r0_ratio")),
                        "main_compass": _float(point.get("r0x_ratio")),
                        "main_compass_count": _int(point.get("cnt_r0x_ratio")),
                    }
                )
        except Exception as exc:
            failures.append({"sector": sector["sector_code"], "error": str(exc)})
        if index % 25 == 0 or index == 229:
            print(
                f"[DAILY-BACKFILL] fetched={index}/229 rows={len(records)} failures={len(failures)}",
                flush=True,
            )
        time.sleep(sleep_seconds)

    result["steps"]["sina"] = {
        "ok": not failures,
        "rows": len(records),
        "failures": len(failures),
    }
    if failures:
        failure_path = DATA_DIR / "status" / "fundflow_daily_backfill_failures.json"
        failure_path.parent.mkdir(parents=True, exist_ok=True)
        failure_path.write_text(
            json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        result["state"] = "FAILED"
        result["error"] = f"daily backfill incomplete: {len(failures)} sectors failed"
        result["finished_at"] = datetime.now(TZ).isoformat(timespec="seconds")
        _write_status(result)
        raise RuntimeError(f"{result['error']}; see {failure_path}")

    unique = {
        (row["source"], row["sector_code"], row["trade_date"]): row
        for row in records
    }
    records = sorted(
        unique.values(), key=lambda item: (item["trade_date"], item["sector_code"])
    )
    result.update(
        {
            "records": len(records),
            "sectors": len({row["sector_code"] for row in records}),
            "first_date": min(row["trade_date"] for row in records),
            "last_date": max(row["trade_date"] for row in records),
        }
    )

    serv00_error = None
    try:
        serv00_count = upload_daily_records(records)
        result["steps"]["serv00"] = {"ok": True, "upserted": serv00_count}
    except Exception as exc:
        serv00_error = f"{type(exc).__name__}: {exc}"
        result["steps"]["serv00"] = {
            "ok": False,
            "upserted": 0,
            "error": serv00_error,
        }

    crate_error = None
    try:
        ensure_schema()
        crate_count = upload_daily(records)
        result["steps"]["cratedb"] = {"ok": True, "upserted": crate_count}
    except Exception as exc:
        crate_error = f"{type(exc).__name__}: {exc}"
        result["steps"]["cratedb"] = {
            "ok": False,
            "upserted": 0,
            "error": crate_error,
        }

    if result["steps"]["serv00"]["ok"] and result["steps"]["cratedb"]["ok"]:
        result["state"] = "SUCCESS"
    elif result["steps"]["serv00"]["ok"] or result["steps"]["cratedb"]["ok"]:
        result["state"] = "PARTIAL"
    else:
        result["state"] = "FAILED"

    result["completed_at"] = datetime.now(TZ).isoformat(timespec="seconds")
    _write_status(result)
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)

    if result["state"] == "FAILED":
        raise RuntimeError(
            f"Serv00 failed: {serv00_error}; CrateDB failed: {crate_error}"
        )
    return result
