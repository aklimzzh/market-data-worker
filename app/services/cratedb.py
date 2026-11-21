from __future__ import annotations

import os
from collections.abc import Iterable
from typing import Any

import requests

CRATEDB_URL = os.getenv("CRATEDB_URL", "").rstrip("/")
CRATEDB_USER = os.getenv("CRATEDB_USER", "")
CRATEDB_PASSWORD = os.getenv("CRATEDB_PASSWORD", "")

SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS doc.sector_flow_1m (
        source TEXT NOT NULL,
        sector_code TEXT NOT NULL,
        sector_type TEXT NOT NULL,
        sector_name TEXT NOT NULL,
        trade_date TIMESTAMP WITH TIME ZONE NOT NULL,
        minute_ts TIMESTAMP WITH TIME ZONE NOT NULL,
        avg_price DOUBLE PRECISION,
        pct_change DOUBLE PRECISION,
        in_amount DOUBLE PRECISION,
        out_amount DOUBLE PRECISION,
        net_amount DOUBLE PRECISION,
        net_ratio DOUBLE PRECISION,
        r0_ratio DOUBLE PRECISION,
        r3_ratio DOUBLE PRECISION,
        PRIMARY KEY (source, sector_code, minute_ts)
    ) CLUSTERED INTO 1 SHARDS
    """,
    """
    CREATE TABLE IF NOT EXISTS doc.sector_flow_5m (
        source TEXT NOT NULL,
        sector_code TEXT NOT NULL,
        sector_type TEXT NOT NULL,
        sector_name TEXT NOT NULL,
        trade_date TIMESTAMP WITH TIME ZONE NOT NULL,
        bucket_ts TIMESTAMP WITH TIME ZONE NOT NULL,
        avg_price DOUBLE PRECISION,
        pct_change DOUBLE PRECISION,
        in_amount DOUBLE PRECISION,
        out_amount DOUBLE PRECISION,
        net_amount DOUBLE PRECISION,
        net_ratio DOUBLE PRECISION,
        PRIMARY KEY (source, sector_code, bucket_ts)
    ) CLUSTERED INTO 1 SHARDS
    """,
    """
    CREATE TABLE IF NOT EXISTS doc.sector_flow_daily (
        source TEXT NOT NULL,
        sector_code TEXT NOT NULL,
        sector_type TEXT NOT NULL,
        sector_name TEXT NOT NULL,
        trade_date TIMESTAMP WITH TIME ZONE NOT NULL,
        close_ts TIMESTAMP WITH TIME ZONE NOT NULL,
        avg_price DOUBLE PRECISION,
        pct_change DOUBLE PRECISION,
        in_amount DOUBLE PRECISION,
        out_amount DOUBLE PRECISION,
        net_amount DOUBLE PRECISION,
        net_ratio DOUBLE PRECISION,
        PRIMARY KEY (source, sector_code, trade_date)
    ) CLUSTERED INTO 1 SHARDS
    """,
]

INSERT_1M = """
INSERT INTO doc.sector_flow_1m (
 source,sector_code,sector_type,sector_name,trade_date,minute_ts,
 avg_price,pct_change,in_amount,out_amount,net_amount,net_ratio,r0_ratio,r3_ratio
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT (source,sector_code,minute_ts) DO UPDATE SET
 sector_type=excluded.sector_type, sector_name=excluded.sector_name,
 trade_date=excluded.trade_date, avg_price=excluded.avg_price,
 pct_change=excluded.pct_change, in_amount=excluded.in_amount,
 out_amount=excluded.out_amount, net_amount=excluded.net_amount,
 net_ratio=excluded.net_ratio, r0_ratio=excluded.r0_ratio, r3_ratio=excluded.r3_ratio
"""


def _endpoint() -> str:
    if not CRATEDB_URL:
        raise RuntimeError("CRATEDB_URL is empty")
    return f"{CRATEDB_URL}/_sql"


def sql(stmt: str, args: list[Any] | None = None, bulk_args: list[list[Any]] | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {"stmt": stmt}
    if args is not None:
        body["args"] = args
    if bulk_args is not None:
        body["bulk_args"] = bulk_args
    response = requests.post(
        _endpoint(),
        auth=(CRATEDB_USER, CRATEDB_PASSWORD),
        json=body,
        timeout=(10, 120),
    )
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(payload["error"])
    return payload


def ensure_schema() -> None:
    for statement in SCHEMA_SQL:
        sql(statement)


def upload_1m(rows: Iterable[list[Any]], batch_size: int = 1000) -> int:
    batch: list[list[Any]] = []
    total = 0
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            result = sql(INSERT_1M, bulk_args=batch)
            total += len(result.get("results", []))
            batch.clear()
    if batch:
        result = sql(INSERT_1M, bulk_args=batch)
        total += len(result.get("results", []))
    return total


def replace_aggregates(trade_date: str) -> None:
    sql("DELETE FROM doc.sector_flow_5m WHERE trade_date = ?", [trade_date])
    sql("DELETE FROM doc.sector_flow_daily WHERE trade_date = ?", [trade_date])
    sql(
        """
        INSERT INTO doc.sector_flow_5m (
          source,sector_code,sector_type,sector_name,trade_date,bucket_ts,
          avg_price,pct_change,in_amount,out_amount,net_amount,net_ratio
        )
        SELECT source,sector_code,max(sector_type),max(sector_name),trade_date,
               date_bin('5 minutes'::INTERVAL, minute_ts, date_trunc('day', minute_ts)),
               max_by(avg_price,minute_ts),max_by(pct_change,minute_ts),
               max_by(in_amount,minute_ts),max_by(out_amount,minute_ts),
               max_by(net_amount,minute_ts),max_by(net_ratio,minute_ts)
        FROM doc.sector_flow_1m
        WHERE trade_date = ?
        GROUP BY source,sector_code,trade_date,
                 date_bin('5 minutes'::INTERVAL, minute_ts, date_trunc('day', minute_ts))
        """,
        [trade_date],
    )
    sql(
        """
        INSERT INTO doc.sector_flow_daily (
          source,sector_code,sector_type,sector_name,trade_date,close_ts,
          avg_price,pct_change,in_amount,out_amount,net_amount,net_ratio
        )
        SELECT source,sector_code,max(sector_type),max(sector_name),trade_date,
               max(minute_ts),max_by(avg_price,minute_ts),max_by(pct_change,minute_ts),
               max_by(in_amount,minute_ts),max_by(out_amount,minute_ts),
               max_by(net_amount,minute_ts),max_by(net_ratio,minute_ts)
        FROM doc.sector_flow_1m
        WHERE trade_date = ?
        GROUP BY source,sector_code,trade_date
        """,
        [trade_date],
    )


def counts(trade_date: str) -> dict[str, int]:
    output = {}
    for table in ("sector_flow_1m", "sector_flow_5m", "sector_flow_daily"):
        result = sql(f"SELECT count(*) FROM doc.{table} WHERE trade_date = ?", [trade_date])
        output[table] = int(result["rows"][0][0])
    return output
