from __future__ import annotations

import json
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import akshare as ak
import pandas as pd

from .config import DATA_DIR
from .services.index_minute import remote_status, upload_batch
from .sources.index_realtime import CONFIG

TZ = ZoneInfo("Asia/Shanghai")

COLUMN_ALIASES = {
    "day": "minute_ts",
    "date": "minute_ts",
    "datetime": "minute_ts",
    "时间": "minute_ts",
    "close": "close_price",
    "收盘": "close_price",
    "volume": "minute_volume",
    "成交量": "minute_volume",
    "amount": "minute_amount",
    "成交额": "minute_amount",
}


def _fetch_sina(symbol: str) -> pd.DataFrame:
    """Fetch the recent 1-minute window supported by the installed AKShare."""
    errors: list[str] = []
    calls = (
        lambda: ak.stock_zh_a_hist_min_sina(symbol=symbol, period="1"),
        lambda: ak.stock_zh_a_minute(symbol=symbol, period="1", adjust=""),
    )
    for call in calls:
        try:
            frame = call()
            if isinstance(frame, pd.DataFrame) and not frame.empty:
                return frame
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
    raise RuntimeError(f"no compatible Sina 1m function for {symbol}: {errors}")


def _normalise(frame: pd.DataFrame, code: str) -> pd.DataFrame:
    renamed = {
        column: COLUMN_ALIASES.get(str(column).strip(), str(column).strip().lower())
        for column in frame.columns
    }
    data = frame.rename(columns=renamed).copy()
    required = {"minute_ts", "close_price", "minute_volume"}
    missing = required - set(data.columns)
    if missing:
        raise RuntimeError(
            f"{code}: missing columns {sorted(missing)}; actual={list(frame.columns)}"
        )

    data["minute_ts"] = pd.to_datetime(data["minute_ts"], errors="coerce")
    data["close_price"] = pd.to_numeric(data["close_price"], errors="coerce")
    data["minute_volume"] = pd.to_numeric(data["minute_volume"], errors="coerce")
    if "minute_amount" not in data.columns:
        data["minute_amount"] = pd.NA
    data["minute_amount"] = pd.to_numeric(data["minute_amount"], errors="coerce")
    data = data.dropna(subset=["minute_ts", "close_price", "minute_volume"])
    data = data.sort_values("minute_ts").drop_duplicates("minute_ts", keep="last")

    symbol, name = CONFIG[code][1], CONFIG[code][2]
    data["index_code"] = code
    data["index_symbol"] = symbol
    data["index_name"] = name
    data["trade_date"] = data["minute_ts"].dt.date
    data["source"] = "sina"

    # The historical AKShare endpoint returns per-minute volume/amount, unlike
    # the real-time quote endpoint which returns cumulative values.
    data["minute_volume"] = data["minute_volume"].round().astype("int64")
    data["cumulative_volume"] = data.groupby("trade_date")["minute_volume"].cumsum()
    data["cumulative_amount"] = data.groupby("trade_date")["minute_amount"].cumsum()
    data["period_price_change"] = data.groupby("trade_date")["close_price"].diff()
    data["change_pct"] = pd.NA
    return data


def _json_number(value, integer: bool = False):
    if pd.isna(value):
        return None
    return int(value) if integer else float(value)


def _record(row: pd.Series) -> dict:
    minute = row["minute_ts"]
    if minute.tzinfo is None:
        minute = minute.tz_localize(TZ)
    else:
        minute = minute.tz_convert(TZ)
    return {
        "index_code": row["index_code"],
        "index_symbol": row["index_symbol"],
        "index_name": row["index_name"],
        "trade_date": row["trade_date"].isoformat(),
        "minute_ts": minute.isoformat(timespec="seconds"),
        "close_price": _json_number(row["close_price"]),
        "change_pct": _json_number(row["change_pct"]),
        "cumulative_volume": _json_number(row["cumulative_volume"], integer=True),
        "cumulative_amount": _json_number(row["cumulative_amount"]),
        "minute_volume": _json_number(row["minute_volume"], integer=True),
        "minute_amount": _json_number(row["minute_amount"]),
        "period_price_change": _json_number(row["period_price_change"]),
        "source": "sina",
    }


def _write_day_parquet(day: date, day_frame: pd.DataFrame) -> str:
    target = (
        DATA_DIR
        / "index_archive"
        / day.strftime("%Y/%m")
        / f"{day.isoformat()}.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    day_frame.sort_values(["minute_ts", "index_code"]).to_parquet(
        target,
        index=False,
        compression="zstd",
    )
    return str(target)


def run(start: str, end: str, sleep_seconds: float = 0.10, dry_run: bool = False) -> dict:
    start_day = date.fromisoformat(start)
    end_day = date.fromisoformat(end)
    if start_day > end_day:
        raise ValueError("start cannot be later than end")

    frames: list[pd.DataFrame] = []
    availability: dict[str, dict] = {}
    for code, (sina_symbol, _, _) in CONFIG.items():
        frame = _normalise(_fetch_sina(sina_symbol), code)
        available_start = frame["trade_date"].min()
        available_end = frame["trade_date"].max()
        availability[code] = {
            "available_start": available_start.isoformat(),
            "available_end": available_end.isoformat(),
        }
        frame = frame[
            (frame["trade_date"] >= start_day) & (frame["trade_date"] <= end_day)
        ]
        if frame.empty:
            raise RuntimeError(
                f"{code}: requested range {start}..{end} is outside Sina window "
                f"{available_start}..{available_end}"
            )
        frames.append(frame)

    data = pd.concat(frames, ignore_index=True)
    all_codes = set(CONFIG)
    uploaded = 0
    skipped_days: list[str] = []
    repaired_days: list[str] = []
    upload_failures: list[dict] = []
    parquet_files: list[str] = []

    remote = {} if dry_run else remote_status(60)
    remote_counts = {
        (str(item.get("trade_date")), item.get("index_code")): int(item.get("row_count", 0))
        for item in remote.get("data", [])
    }

    for trade_day, day_frame in data.groupby("trade_date", sort=True):
        day_frame = day_frame.sort_values(["minute_ts", "index_code"])
        expected = {
            code: int((day_frame["index_code"] == code).sum()) for code in all_codes
        }
        is_complete = all(
            remote_counts.get((trade_day.isoformat(), code), 0) >= count
            for code, count in expected.items()
        )
        parquet_files.append(_write_day_parquet(trade_day, day_frame))
        if is_complete and not dry_run:
            skipped_days.append(trade_day.isoformat())
            continue
        repaired_days.append(trade_day.isoformat())
        records = [_record(row) for _, row in day_frame.iterrows()]
        if dry_run:
            continue
        for offset in range(0, len(records), 400):
            batch = records[offset:offset + 400]
            try:
                result = upload_batch(batch)
                uploaded += int(result.get("upserted", len(batch)))
            except Exception as exc:
                upload_failures.append({
                    "trade_date": trade_day.isoformat(),
                    "offset": offset,
                    "rows": len(batch),
                    "error": f"{type(exc).__name__}: {exc}",
                })
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    state = "SUCCESS"
    if incomplete or upload_failures:
        state = "PARTIAL"
    result = {
        "state": state,
        "dry_run": dry_run,
        "requested_start": start,
        "requested_end": end,
        "source_availability": availability,
        "source_rows": int(len(data)),
        "uploaded_rows": uploaded,
        "skipped_complete_days": skipped_days,
        "processed_days": repaired_days,
        "upload_failure_count": len(upload_failures),
        "upload_failures": upload_failures[:100],
        "parquet_files": parquet_files,
        "finished_at": datetime.now(TZ).isoformat(timespec="seconds"),
    }
    status_path = DATA_DIR / "status" / f"index_minute_backfill_{start}_{end}.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    if upload_failures and not dry_run:
        raise RuntimeError(f"{len(upload_failures)} minute batches failed to upload")
    return result
