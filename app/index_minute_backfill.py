from __future__ import annotations

import json
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

import akshare as ak
import pandas as pd

from .config import DATA_DIR
from .services.index_minute import remote_status, upload_batch
from .sources.index_realtime import CONFIG

TZ = ZoneInfo("Asia/Shanghai")
ALIASES = {
    "day": "minute_ts", "date": "minute_ts", "datetime": "minute_ts", "时间": "minute_ts",
    "close": "close_price", "收盘": "close_price",
    "volume": "minute_volume", "成交量": "minute_volume",
    "amount": "minute_amount", "成交额": "minute_amount",
}


def progress(message: str) -> None:
    print(f"[{datetime.now(TZ).strftime('%H:%M:%S')}] {message}", flush=True)


def fetch_sina(symbol: str) -> pd.DataFrame:
    errors = []
    for name, call in (
        ("stock_zh_a_hist_min_sina", lambda: ak.stock_zh_a_hist_min_sina(symbol=symbol, period="1")),
        ("stock_zh_a_minute", lambda: ak.stock_zh_a_minute(symbol=symbol, period="1", adjust="")),
    ):
        try:
            progress(f"  trying {name}")
            frame = call()
            if isinstance(frame, pd.DataFrame) and not frame.empty:
                return frame
        except Exception as exc:
            errors.append(f"{name}: {type(exc).__name__}: {exc}")
            progress(f"  {name} failed: {type(exc).__name__}: {exc}")
    raise RuntimeError(f"no compatible Sina 1m function for {symbol}: {errors}")


def normalise(frame: pd.DataFrame, code: str) -> pd.DataFrame:
    frame = frame.rename(columns={c: ALIASES.get(str(c).strip(), str(c).strip().lower()) for c in frame.columns}).copy()
    required = {"minute_ts", "close_price", "minute_volume"}
    if not required.issubset(frame.columns):
        raise RuntimeError(f"{code}: missing {sorted(required-set(frame.columns))}; actual={list(frame.columns)}")
    frame["minute_ts"] = pd.to_datetime(frame["minute_ts"], errors="coerce")
    frame["close_price"] = pd.to_numeric(frame["close_price"], errors="coerce")
    frame["minute_volume"] = pd.to_numeric(frame["minute_volume"], errors="coerce")
    if "minute_amount" not in frame.columns:
        frame["minute_amount"] = pd.NA
    frame["minute_amount"] = pd.to_numeric(frame["minute_amount"], errors="coerce")
    frame = frame.dropna(subset=["minute_ts", "close_price", "minute_volume"]).sort_values("minute_ts").drop_duplicates("minute_ts", keep="last")
    symbol, name = CONFIG[code][1], CONFIG[code][2]
    frame["index_code"] = code; frame["index_symbol"] = symbol; frame["index_name"] = name
    frame["trade_date"] = frame["minute_ts"].dt.date; frame["source"] = "sina"
    frame["minute_volume"] = frame["minute_volume"].round().astype("int64")
    frame["cumulative_volume"] = frame.groupby("trade_date")["minute_volume"].cumsum()
    frame["cumulative_amount"] = frame.groupby("trade_date")["minute_amount"].cumsum()
    frame["period_price_change"] = frame.groupby("trade_date")["close_price"].diff()
    frame["change_pct"] = pd.NA
    return frame


def number(value, integer=False):
    if pd.isna(value): return None
    return int(value) if integer else float(value)


def record(row: pd.Series) -> dict:
    minute = row["minute_ts"]
    minute = minute.tz_localize(TZ) if minute.tzinfo is None else minute.tz_convert(TZ)
    return {
        "index_code": row["index_code"], "index_symbol": row["index_symbol"], "index_name": row["index_name"],
        "trade_date": row["trade_date"].isoformat(), "minute_ts": minute.isoformat(timespec="seconds"),
        "close_price": number(row["close_price"]), "change_pct": number(row["change_pct"]),
        "cumulative_volume": number(row["cumulative_volume"], True), "cumulative_amount": number(row["cumulative_amount"]),
        "minute_volume": number(row["minute_volume"], True), "minute_amount": number(row["minute_amount"]),
        "period_price_change": number(row["period_price_change"]), "source": "sina",
    }


def write_parquet(day: date, frame: pd.DataFrame) -> str:
    target = DATA_DIR / "index_archive" / day.strftime("%Y/%m") / f"{day.isoformat()}.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.sort_values(["minute_ts", "index_code"]).to_parquet(target, index=False, compression="zstd")
    return str(target)


def run(start: str, end: str, sleep_seconds: float = 0.10, dry_run: bool = False) -> dict:
    start_day, end_day = date.fromisoformat(start), date.fromisoformat(end)
    if start_day > end_day: raise ValueError("start cannot be later than end")
    progress(f"START range={start}..{end} dry_run={dry_run}")
    frames, availability = [], {}
    for no, (code, (sina_symbol, _, _)) in enumerate(CONFIG.items(), 1):
        progress(f"FETCH {no}/4 {code} {sina_symbol} started")
        t0 = time.monotonic(); frame = normalise(fetch_sina(sina_symbol), code)
        a, b = frame["trade_date"].min(), frame["trade_date"].max()
        availability[code] = {"available_start": a.isoformat(), "available_end": b.isoformat()}
        frame = frame[(frame["trade_date"] >= start_day) & (frame["trade_date"] <= end_day)]
        progress(f"FETCH {code} done filtered_rows={len(frame)} available={a}..{b} elapsed={time.monotonic()-t0:.1f}s")
        if not frame.empty: frames.append(frame)
    if not frames: raise RuntimeError("requested range has no Sina minute data")
    data = pd.concat(frames, ignore_index=True)
    progress(f"SOURCE READY rows={len(data)} trading_days={data['trade_date'].nunique()}")

    remote_counts = {}
    if not dry_run:
        progress("SERV00 status check started")
        status = remote_status(60)
        remote_counts = {(str(x.get("trade_date")), x.get("index_code")): int(x.get("row_count", 0)) for x in status.get("data", [])}
        progress(f"SERV00 status check done groups={len(remote_counts)}")

    uploaded = 0; skipped = []; processed = []; failures = []; parquet_files = []
    days = list(data.groupby("trade_date", sort=True))
    for day_no, (trade_day, day_frame) in enumerate(days, 1):
        day_frame = day_frame.sort_values(["minute_ts", "index_code"])
        progress(f"DAY {day_no}/{len(days)} {trade_day} rows={len(day_frame)}")
        parquet = write_parquet(trade_day, day_frame); parquet_files.append(parquet)
        expected = {code: int((day_frame["index_code"] == code).sum()) for code in CONFIG}
        complete = (not dry_run) and all(remote_counts.get((trade_day.isoformat(), code), 0) >= count for code, count in expected.items())
        if complete:
            skipped.append(trade_day.isoformat()); progress(f"  SKIP already complete expected={expected}"); continue
        processed.append(trade_day.isoformat())
        records = [record(row) for _, row in day_frame.iterrows()]
        batches = (len(records) + 399) // 400
        if dry_run:
            progress(f"  DRY-RUN would upload rows={len(records)} batches={batches}"); continue
        for batch_no, offset in enumerate(range(0, len(records), 400), 1):
            batch = records[offset:offset+400]
            progress(f"  UPLOAD batch {batch_no}/{batches} rows={len(batch)} started")
            t0 = time.monotonic()
            try:
                result = upload_batch(batch); count = int(result.get("upserted", len(batch))); uploaded += count
                progress(f"  UPLOAD batch {batch_no}/{batches} OK upserted={count} elapsed={time.monotonic()-t0:.1f}s total={uploaded}")
            except Exception as exc:
                failures.append({"trade_date": trade_day.isoformat(), "batch": batch_no, "error": f"{type(exc).__name__}: {exc}"})
                progress(f"  UPLOAD batch {batch_no}/{batches} FAILED {type(exc).__name__}: {exc}")
            if sleep_seconds > 0: time.sleep(sleep_seconds)
        progress(f"DAY {trade_day} finished parquet={parquet}")

    state = "PARTIAL" if failures else "SUCCESS"
    result = {"state": state, "dry_run": dry_run, "requested_start": start, "requested_end": end,
              "source_availability": availability, "source_rows": len(data), "uploaded_rows": uploaded,
              "skipped_complete_days": skipped, "processed_days": processed,
              "upload_failure_count": len(failures), "upload_failures": failures, "parquet_files": parquet_files,
              "finished_at": datetime.now(TZ).isoformat(timespec="seconds")}
    status_path = DATA_DIR / "status" / f"index_minute_backfill_{start}_{end}.json"
    status_path.parent.mkdir(parents=True, exist_ok=True); status_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    progress(f"FINISHED state={state} uploaded={uploaded} skipped_days={len(skipped)} failures={len(failures)}")
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    if failures and not dry_run: raise RuntimeError(f"{len(failures)} batch uploads failed")
    return result
