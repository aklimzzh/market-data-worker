from __future__ import annotations

import csv
import json
import time as sleep_time
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from ..config import DATA_DIR, SERV00_API_TOKEN, SERV00_INGEST_URL
from ..sources.index_realtime import fetch

TZ = ZoneInfo("Asia/Shanghai")
FIELDS = [
    "index_code", "index_symbol", "index_name", "trade_date", "minute_ts",
    "close_price", "change_pct", "cumulative_volume", "cumulative_amount",
    "minute_volume", "minute_amount", "period_price_change", "source",
]
EXPECTED_INDEXES = {"SSE", "SZSE", "STAR50", "CHINEXT"}


def endpoint():
    base = (SERV00_INGEST_URL or "").split("/api/v1/fundflow/import")[0].split("/api/fundflow/ingest")[0].rstrip("/")
    if not base:
        raise RuntimeError("SERV00_INGEST_URL is empty")
    return base + "/api/v1/index/minute/import"


def path(day):
    return DATA_DIR / "index_raw" / day.strftime("%Y/%m") / f"{day}.csv"


def last(day):
    values = {}
    source = path(day)
    if source.exists():
        with source.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                values[row["index_code"]] = row
    return values


def num(row, key):
    try:
        return float(row[key]) if row and row.get(key) not in (None, "") else None
    except (TypeError, ValueError):
        return None


def enrich(rows, day):
    old = last(day)
    for row in rows:
        previous = old.get(row["index_code"])
        price = num(previous, "close_price")
        volume = num(previous, "cumulative_volume")
        amount = num(previous, "cumulative_amount")
        row["period_price_change"] = None if price is None else row["close_price"] - price
        row["minute_volume"] = None if volume is None else max(0, int(round(row["cumulative_volume"] - volume)))
        row["minute_amount"] = None if amount is None else max(0, row["cumulative_amount"] - amount)
    return rows


def append(rows):
    target = path(date.fromisoformat(rows[0]["trade_date"]))
    target.parent.mkdir(parents=True, exist_ok=True)
    new = not target.exists()
    with target.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        if new:
            writer.writeheader()
        writer.writerows(rows)


def upload(rows):
    response = requests.post(
        endpoint(),
        headers={"Authorization": f"Bearer {SERV00_API_TOKEN}"},
        json={"minute_ts": rows[0]["minute_ts"], "records": rows},
        timeout=(5, 30),
    )
    response.raise_for_status()
    return response.json()


def collect(session=None):
    rows = enrich(fetch(session), datetime.now(TZ).date())
    append(rows)
    return rows, upload(rows)


def _normalize_final(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict, dict]:
    raw_rows = len(frame)
    timestamps = pd.to_datetime(frame["minute_ts"], errors="coerce", utc=True)
    invalid_timestamp_rows = int(timestamps.isna().sum())
    local = timestamps.dt.tz_convert(TZ).dt.floor("min")
    clock = local.dt.time
    allowed = (
        ((clock >= time(9, 15)) & (clock <= time(11, 30)))
        | ((clock >= time(12, 59)) & (clock <= time(15, 30)))
    )
    allowed = allowed & timestamps.notna()

    outside = frame.loc[~allowed, ["index_code", "minute_ts"]].copy()
    result = frame.loc[allowed].copy()
    result["minute_ts"] = local.loc[allowed].map(
        lambda value: value.isoformat(timespec="seconds")
    )
    before_dedup = len(result)
    result = result.drop_duplicates(["index_code", "minute_ts"], keep="last")
    duplicate_rows = before_dedup - len(result)
    result = result.sort_values(["minute_ts", "index_code"])

    per_index = {}
    for code in sorted(EXPECTED_INDEXES | set(result["index_code"].dropna().astype(str))):
        raw_code = frame[frame["index_code"].astype(str) == code]
        final_code = result[result["index_code"].astype(str) == code]
        raw_code_ts = pd.to_datetime(raw_code["minute_ts"], errors="coerce", utc=True)
        raw_code_local = raw_code_ts.dt.tz_convert(TZ).dt.floor("min")
        raw_code_clock = raw_code_local.dt.time
        raw_allowed = (
            ((raw_code_clock >= time(9, 15)) & (raw_code_clock <= time(11, 30)))
            | ((raw_code_clock >= time(12, 59)) & (raw_code_clock <= time(15, 30)))
        ) & raw_code_ts.notna()
        per_index[code] = {
            "raw_rows": int(len(raw_code)),
            "outside_window_rows": int((~raw_allowed).sum()),
            "final_rows": int(len(final_code)),
            "first_minute": str(final_code["minute_ts"].min()) if not final_code.empty else None,
            "last_minute": str(final_code["minute_ts"].max()) if not final_code.empty else None,
            "observed_minutes": int(final_code["minute_ts"].nunique()),
            "volume_rows": int(final_code["minute_volume"].notna().sum()) if "minute_volume" in final_code else 0,
            "amount_rows": int(final_code["minute_amount"].notna().sum()) if "minute_amount" in final_code else 0,
        }

    quality = {
        "raw_rows": raw_rows,
        "invalid_timestamp_rows": invalid_timestamp_rows,
        "outside_window_rows": int(len(outside)),
        "outside_window_sample": outside.head(50).to_dict(orient="records"),
        "duplicate_rows_removed": int(duplicate_rows),
        "final_rows": int(len(result)),
    }
    return result, quality, per_index


def finalize(day):
    source = path(day)
    if not source.exists():
        raise FileNotFoundError(source)
    raw = pd.read_csv(source, dtype={"index_code": str})
    if raw.empty:
        raise RuntimeError(f"empty CSV: {source}")

    frame, quality, per_index = _normalize_final(raw)
    if frame.empty:
        raise RuntimeError(f"no index rows inside final monitoring window: {source}")

    out = DATA_DIR / "index_archive" / day.strftime("%Y/%m") / f"{day}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    temp = out.with_suffix(".parquet.tmp")
    frame.to_parquet(temp, index=False, compression="zstd")
    temp.replace(out)

    index_count = int(frame["index_code"].nunique())
    result = {
        "trade_date": str(day),
        "rows": int(len(frame)),
        "index_count": index_count,
        "observed_minutes": int(frame["minute_ts"].nunique()),
        "first_minute": str(frame["minute_ts"].min()),
        "last_minute": str(frame["minute_ts"].max()),
        "quality": quality,
        "per_index": per_index,
        # Sparse observations are allowed; all four indexes must exist.
        "complete": EXPECTED_INDEXES.issubset(set(frame["index_code"].astype(str))),
        "parquet": str(out),
        "bytes": out.stat().st_size,
        "completed_at": datetime.now(TZ).isoformat(timespec="seconds"),
    }
    status_path = DATA_DIR / "status" / f"index_finalize_{day}.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    temp_status = status_path.with_suffix(".json.tmp")
    temp_status.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    temp_status.replace(status_path)
    print(
        f"[PARQUET] index finalized rows={len(frame)} "
        f"indexes={index_count} outside={quality['outside_window_rows']} "
        f"duplicates={quality['duplicate_rows_removed']} path={out}",
        flush=True,
    )
    return result


def retention():
    response = requests.post(
        endpoint().replace("/import", "/retention"),
        headers={"Authorization": f"Bearer {SERV00_API_TOKEN}"},
        timeout=(5, 30),
    )
    response.raise_for_status()
    return response.json()


def upload_batch(records, max_attempts=3):
    if not records or len(records) > 400:
        raise ValueError("batch must contain 1..400 records")
    url = endpoint().replace("/minute/import", "/minute/batch")
    delays = (0, 2, 5)
    last_error = None
    for attempt in range(max_attempts):
        if delays[attempt]:
            sleep_time.sleep(delays[attempt])
        try:
            response = requests.post(
                url,
                headers={"Authorization": f"Bearer {SERV00_API_TOKEN}"},
                json={"records": records},
                timeout=(10, 90),
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
    raise RuntimeError(
        f"batch upload failed after {max_attempts} attempts: {last_error}"
    )


def remote_status(days=60):
    url = endpoint().replace("/minute/import", "/minute/status")
    response = requests.get(url, params={"days": days}, timeout=(10, 60))
    response.raise_for_status()
    return response.json()
