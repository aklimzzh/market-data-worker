from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from zoneinfo import ZoneInfo

import requests

from ..config import DATA_DIR, SERV00_API_TOKEN, SERV00_INGEST_URL
from ..sources.sina_history import sector_catalog
from .local_store import append_csv, write_json_atomic

TZ = ZoneInfo("Asia/Shanghai")
HISTORY_URL = (
    "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
    "MoneyFlow.ssx_bkzj_fszs"
)
HEADERS = {
    "Referer": "https://vip.stock.finance.sina.com.cn/",
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
}
_LOCK = threading.Lock()
_RUNNING = False


def _status_path(day: date, sector_type: str):
    return DATA_DIR / "status" / f"intraday_repair_{sector_type}_{day.isoformat()}.json"


def _csv_path(day: date):
    return DATA_DIR / "raw" / day.strftime("%Y/%m") / f"{day.isoformat()}.csv"


def _phase(ticktime: str) -> str | None:
    if "09:30:00" <= ticktime <= "11:30:00":
        return "continuous_am"
    if "13:00:00" <= ticktime <= "15:00:00":
        return "continuous_pm"
    return None


def _bankuai(sector: dict) -> str:
    prefix = "0" if sector["sector_type"] == "industry" else "1"
    return f"{prefix}/{sector['sector_code']}"


def _fetch_one(sector: dict, day: date, num: int) -> tuple[list[dict], str | None]:
    error = None
    for attempt in range(2):
        try:
            response = requests.get(
                HISTORY_URL,
                params={
                    "page": "1",
                    "num": str(num),
                    "bankuai": _bankuai(sector),
                    "sort": "time",
                },
                headers=HEADERS,
                timeout=(5, 20),
            )
            response.raise_for_status()
            payload = response.json()
            points = payload[1] if isinstance(payload, list) and len(payload) >= 2 else []
            records = []
            for point in points:
                if point.get("opendate") != day.isoformat():
                    continue
                phase = _phase(str(point.get("ticktime", "")))
                if phase is None:
                    continue
                minute = datetime.fromisoformat(
                    f"{point['opendate']}T{point['ticktime']}"
                ).replace(tzinfo=TZ)
                records.append({
                    "trade_date": day.isoformat(),
                    "minute_ts": minute.isoformat(timespec="seconds"),
                    "source": "sina",
                    "market_phase": phase,
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
                })
            return records, None
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            if attempt == 0:
                time.sleep(1)
    return [], error


def _repair_url() -> str:
    if not SERV00_INGEST_URL:
        raise RuntimeError("SERV00_INGEST_URL is empty")
    base = SERV00_INGEST_URL.split("/api/v1/fundflow/import")[0].split("/api/fundflow/ingest")[0].rstrip("/")
    return base + "/api/v1/fundflow/repair"


def _upload(records: list[dict], batch_size: int = 1000) -> int:
    total = 0
    url = _repair_url()
    for offset in range(0, len(records), batch_size):
        batch = records[offset:offset + batch_size]
        response = requests.post(
            url,
            headers={"Authorization": f"Bearer {SERV00_API_TOKEN}"},
            json={"records": batch},
            timeout=(10, 120),
        )
        response.raise_for_status()
        result = response.json()
        if not result.get("success"):
            raise RuntimeError(f"Serv00 repair failed: {result}")
        total += int(result.get("upserted", len(batch)))
        print(
            f"[SECTOR-REPAIR] upload {offset + 1}-{offset + len(batch)} "
            f"of {len(records)} total={total}",
            flush=True,
        )
    return total


def repair_type_now(
    now: datetime,
    sector_type: str,
    num: int = 30,
    mode: str = "rolling",
) -> dict:
    day = now.astimezone(TZ).date()
    started = datetime.now(TZ)
    status = {
        "state": "RUNNING",
        "mode": mode,
        "num": num,
        "trade_date": day.isoformat(),
        "started_at": started.isoformat(timespec="seconds"),
    }
    write_json_atomic(_status_path(day, sector_type), status)
    catalog = sector_catalog(requests.Session())
    sectors = [item for item in catalog if item["sector_type"] == sector_type]
    expected = len(sectors)
    if expected <= 0:
        raise RuntimeError(f"empty catalog for {sector_type}")
    status["sector_type"] = sector_type
    status["sectors_requested"] = expected

    records: list[dict] = []
    failures: list[dict] = []
    latest_by_sector: list[str] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_one, sector, day, num): sector for sector in sectors}
        completed = 0
        for future in as_completed(futures):
            sector = futures[future]
            completed += 1
            try:
                rows, error = future.result()
                if error:
                    failures.append({"sector": sector["sector_code"], "error": error})
                else:
                    records.extend(rows)
                    if rows:
                        latest_by_sector.append(max(row["minute_ts"] for row in rows))
            except Exception as exc:
                failures.append({"sector": sector["sector_code"], "error": str(exc)})
            if completed % 25 == 0 or completed == expected:
                print(
                    f"[SECTOR-REPAIR] fetched={completed}/{expected} "
                    f"rows={len(records)} failures={len(failures)}",
                    flush=True,
                )

    # Sparse history is expected. Upsert every authoritative row returned by Sina.
    unique = {
        (row["source"], row["sector_code"], row["minute_ts"]): row
        for row in records
    }
    records = sorted(unique.values(), key=lambda row: (row["minute_ts"], row["sector_code"]))
    upserted = _upload(records) if records else 0
    if records:
        append_csv(_csv_path(day), records)

    status.update({
        "state": "SUCCESS" if not failures else "PARTIAL",
        "sectors_requested": expected,
        "sectors_success": expected - len(failures),
        "sectors_failed": len(failures),
        "historical_rows": len(records),
        "rows_upserted": upserted,
        "earliest_tick": min((row["minute_ts"] for row in records), default=None),
        "latest_tick_min": min(latest_by_sector, default=None),
        "latest_tick_max": max(latest_by_sector, default=None),
        "failures": failures[:50],
        "duration_seconds": round((datetime.now(TZ) - started).total_seconds(), 1),
        "finished_at": datetime.now(TZ).isoformat(timespec="seconds"),
    })
    write_json_atomic(_status_path(day, sector_type), status)
    print(
        f"[SECTOR-REPAIR] finished mode={mode} rows={len(records)} "
        f"upserted={upserted} failures={len(failures)}",
        flush=True,
    )
    return status


def _background(now: datetime, sector_type: str, num: int, mode: str) -> None:
    global _RUNNING
    try:
        repair_type_now(now, sector_type, num=num, mode=mode)
    except Exception as exc:
        payload = {
            "state": "FAILED",
            "mode": mode,
            "trade_date": now.astimezone(TZ).date().isoformat(),
            "error": f"{type(exc).__name__}: {exc}",
            "finished_at": datetime.now(TZ).isoformat(timespec="seconds"),
        }
        write_json_atomic(_status_path(now.astimezone(TZ).date(), sector_type), payload)
        print(f"[SECTOR-REPAIR] failed: {payload['error']}", flush=True)
    finally:
        with _LOCK:
            _RUNNING = False


def maybe_start(now: datetime, sector_type: str = "industry", num: int = 30, mode: str = "rolling", **_ignored) -> bool:
    global _RUNNING
    if not SERV00_INGEST_URL:
        return False
    with _LOCK:
        if _RUNNING:
            print(f"[SECTOR-REPAIR] skip mode={mode}; previous run active", flush=True)
            return False
        _RUNNING = True
    threading.Thread(
        target=_background,
        args=(now, sector_type, num, mode),
        name=f"sector-repair-{sector_type}-{mode}",
        daemon=True,
    ).start()
    print(f"[SECTOR-REPAIR:{sector_type}] started mode={mode} num={num}", flush=True)
    return True


def repair_now(now: datetime, sleep_seconds: float = 0.0, num: int = 300, mode: str = "final") -> dict:
    return {
        "industry": repair_type_now(now, "industry", num=num, mode=mode),
        "concept": repair_type_now(now, "concept", num=num, mode=mode),
    }
