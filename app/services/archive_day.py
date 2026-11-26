from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ..config import DATA_DIR
from .cratedb import (
    counts,
    ensure_schema,
    index_1m_count,
    replace_aggregates,
    upload_1m,
    upload_index_1m,
)


def _none(value: Any) -> Any:
    return None if pd.isna(value) else value


def _sector_path(day: date) -> Path:
    return DATA_DIR / "archive" / day.strftime("%Y/%m") / f"{day.isoformat()}.parquet"


def _index_path(day: date) -> Path:
    return DATA_DIR / "index_archive" / day.strftime("%Y/%m") / f"{day.isoformat()}.parquet"


def _sector_rows(frame: pd.DataFrame):
    for _, row in frame.iterrows():
        yield [
            _none(row.get("source", "sina")),
            _none(row["sector_code"]),
            _none(row["sector_type"]),
            _none(row["sector_name"]),
            _none(row["trade_date"]),
            _none(row["minute_ts"]),
            _none(row.get("avg_price")),
            _none(row.get("pct_change")),
            _none(row.get("in_amount")),
            _none(row.get("out_amount")),
            _none(row.get("net_amount")),
            _none(row.get("net_ratio")),
            _none(row.get("r0_ratio")),
            _none(row.get("r3_ratio")),
        ]


def _index_records(frame: pd.DataFrame):
    for _, row in frame.iterrows():
        yield {
            "index_code": _none(row["index_code"]),
            "index_symbol": _none(row["index_symbol"]),
            "index_name": _none(row["index_name"]),
            "trade_date": _none(row["trade_date"]),
            "minute_ts": _none(row["minute_ts"]),
            "close_price": _none(row.get("close_price")),
            "change_pct": _none(row.get("change_pct")),
            "cumulative_volume": _none(row.get("cumulative_volume")),
            "cumulative_amount": _none(row.get("cumulative_amount")),
            "minute_volume": _none(row.get("minute_volume")),
            "minute_amount": _none(row.get("minute_amount")),
            "period_price_change": _none(row.get("period_price_change")),
            "source": _none(row.get("source", "sina")),
            "received_at": _none(row.get("received_at", row["minute_ts"])),
        }


def archive_day(trade_date: str) -> dict:
    day = date.fromisoformat(trade_date)
    sector_path = _sector_path(day)
    index_path = _index_path(day)
    if not sector_path.exists():
        raise FileNotFoundError(f"sector parquet missing: {sector_path}")
    if not index_path.exists():
        raise FileNotFoundError(f"index parquet missing: {index_path}")

    print(f"[ARCHIVE] reading sector parquet {sector_path}", flush=True)
    sector = pd.read_parquet(sector_path).drop_duplicates(
        ["source", "sector_code", "minute_ts"], keep="last"
    )
    print(f"[ARCHIVE] reading index parquet {index_path}", flush=True)
    index = pd.read_parquet(index_path).drop_duplicates(
        ["index_code", "minute_ts"], keep="last"
    )
    if sector.empty or index.empty:
        raise RuntimeError(
            f"empty parquet: sector_rows={len(sector)} index_rows={len(index)}"
        )

    ensure_schema()
    print(f"[ARCHIVE] sector 1m upload rows={len(sector)}", flush=True)
    sector_upserted = upload_1m(_sector_rows(sector), batch_size=1000)
    print("[ARCHIVE] rebuilding sector 5m/daily", flush=True)
    replace_aggregates(trade_date)
    sector_counts = counts(trade_date)
    if sector_counts["sector_flow_1m"] < len(sector):
        raise RuntimeError(
            f"CrateDB sector incomplete expected={len(sector)} "
            f"actual={sector_counts['sector_flow_1m']}"
        )

    print(f"[ARCHIVE] index 1m upload rows={len(index)}", flush=True)
    index_upserted = upload_index_1m(_index_records(index), batch_size=400)
    index_count = index_1m_count(trade_date)
    if index_count < len(index):
        raise RuntimeError(
            f"CrateDB index incomplete expected={len(index)} actual={index_count}"
        )

    result = {
        "ok": True,
        "trade_date": trade_date,
        "sector": {
            "parquet": str(sector_path),
            "source_rows": len(sector),
            "crate_upserted": sector_upserted,
            "crate_counts": sector_counts,
        },
        "index": {
            "parquet": str(index_path),
            "source_rows": len(index),
            "crate_upserted": index_upserted,
            "crate_rows": index_count,
        },
    }
    print(f"[ARCHIVE] completed {result}", flush=True)
    return result
