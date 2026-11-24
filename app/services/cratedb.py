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
      source TEXT NOT NULL, sector_code TEXT NOT NULL, sector_type TEXT NOT NULL,
      sector_name TEXT NOT NULL, trade_date TIMESTAMP WITH TIME ZONE NOT NULL,
      minute_ts TIMESTAMP WITH TIME ZONE NOT NULL, avg_price DOUBLE PRECISION,
      pct_change DOUBLE PRECISION, in_amount DOUBLE PRECISION, out_amount DOUBLE PRECISION,
      net_amount DOUBLE PRECISION, net_ratio DOUBLE PRECISION, r0_ratio DOUBLE PRECISION,
      r3_ratio DOUBLE PRECISION, PRIMARY KEY (source,sector_code,minute_ts)
    ) CLUSTERED INTO 1 SHARDS
    """,
    """
    CREATE TABLE IF NOT EXISTS doc.sector_flow_5m (
      source TEXT NOT NULL, sector_code TEXT NOT NULL, sector_type TEXT NOT NULL,
      sector_name TEXT NOT NULL, trade_date TIMESTAMP WITH TIME ZONE NOT NULL,
      bucket_ts TIMESTAMP WITH TIME ZONE NOT NULL, avg_price DOUBLE PRECISION,
      pct_change DOUBLE PRECISION, in_amount DOUBLE PRECISION, out_amount DOUBLE PRECISION,
      net_amount DOUBLE PRECISION, net_ratio DOUBLE PRECISION,
      PRIMARY KEY (source,sector_code,bucket_ts)
    ) CLUSTERED INTO 1 SHARDS
    """,
    """
    CREATE TABLE IF NOT EXISTS doc.sector_flow_daily (
      source TEXT NOT NULL, sector_code TEXT NOT NULL, sector_type TEXT NOT NULL,
      sector_name TEXT NOT NULL, trade_date TIMESTAMP WITH TIME ZONE NOT NULL,
      close_ts TIMESTAMP WITH TIME ZONE NOT NULL, avg_price DOUBLE PRECISION,
      pct_change DOUBLE PRECISION, in_amount DOUBLE PRECISION, out_amount DOUBLE PRECISION,
      net_amount DOUBLE PRECISION, net_ratio DOUBLE PRECISION, turnover DOUBLE PRECISION,
      main_net_amount DOUBLE PRECISION, main_net_ratio DOUBLE PRECISION,
      main_compass DOUBLE PRECISION, main_compass_count INTEGER,
      PRIMARY KEY (source,sector_code,trade_date)
    ) CLUSTERED INTO 1 SHARDS
    """,
]

DAILY_EXTRA_COLUMNS = (
    ("turnover", "DOUBLE PRECISION"),
    ("main_net_amount", "DOUBLE PRECISION"),
    ("main_net_ratio", "DOUBLE PRECISION"),
    ("main_compass", "DOUBLE PRECISION"),
    ("main_compass_count", "INTEGER"),
)

INSERT_1M = """
INSERT INTO doc.sector_flow_1m (
 source,sector_code,sector_type,sector_name,trade_date,minute_ts,
 avg_price,pct_change,in_amount,out_amount,net_amount,net_ratio,r0_ratio,r3_ratio
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT (source,sector_code,minute_ts) DO UPDATE SET
 sector_type=excluded.sector_type,sector_name=excluded.sector_name,
 trade_date=excluded.trade_date,avg_price=excluded.avg_price,pct_change=excluded.pct_change,
 in_amount=excluded.in_amount,out_amount=excluded.out_amount,net_amount=excluded.net_amount,
 net_ratio=excluded.net_ratio,r0_ratio=excluded.r0_ratio,r3_ratio=excluded.r3_ratio
"""

INSERT_DAILY = """
INSERT INTO doc.sector_flow_daily (
 source,sector_code,sector_type,sector_name,trade_date,close_ts,
 avg_price,pct_change,in_amount,out_amount,net_amount,net_ratio,
 turnover,main_net_amount,main_net_ratio,main_compass,main_compass_count
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT (source,sector_code,trade_date) DO UPDATE SET
 sector_type=excluded.sector_type,sector_name=excluded.sector_name,close_ts=excluded.close_ts,
 avg_price=excluded.avg_price,pct_change=excluded.pct_change,
 in_amount=coalesce(excluded.in_amount,sector_flow_daily.in_amount),
 out_amount=coalesce(excluded.out_amount,sector_flow_daily.out_amount),
 net_amount=excluded.net_amount,net_ratio=excluded.net_ratio,turnover=excluded.turnover,
 main_net_amount=excluded.main_net_amount,main_net_ratio=excluded.main_net_ratio,
 main_compass=excluded.main_compass,main_compass_count=excluded.main_compass_count
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
        _endpoint(), auth=(CRATEDB_USER, CRATEDB_PASSWORD), json=body, timeout=(10, 120)
    )
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if not response.ok:
        detail = payload if payload is not None else response.text
        raise RuntimeError(f"CrateDB HTTP {response.status_code}: {detail}")
    if isinstance(payload, dict) and "error" in payload:
        raise RuntimeError(f"CrateDB SQL error: {payload['error']}")
    return payload or {}


def _daily_columns() -> set[str]:
    result = sql(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'doc' AND table_name = 'sector_flow_daily'
        """
    )
    return {str(row[0]) for row in result.get("rows", [])}


def ensure_schema() -> None:
    for statement in SCHEMA_SQL:
        sql(statement)
    existing = _daily_columns()
    for column, data_type in DAILY_EXTRA_COLUMNS:
        if column not in existing:
            sql(f'ALTER TABLE doc.sector_flow_daily ADD COLUMN "{column}" {data_type}')
            existing.add(column)


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


def upload_daily(records: Iterable[dict[str, Any]], batch_size: int = 1000) -> int:
    batch: list[list[Any]] = []
    total = 0
    for item in records:
        batch.append([
            item.get("source", "sina"), item["sector_code"], item["sector_type"], item["sector_name"],
            item["trade_date"], item["close_ts"], item.get("avg_price"), item.get("pct_change"),
            item.get("in_amount"), item.get("out_amount"), item.get("net_amount"), item.get("net_ratio"),
            item.get("turnover"), item.get("main_net_amount"), item.get("main_net_ratio"),
            item.get("main_compass"), item.get("main_compass_count"),
        ])
        if len(batch) >= batch_size:
            result = sql(INSERT_DAILY, bulk_args=batch)
            total += len(result.get("results", []))
            batch.clear()
    if batch:
        result = sql(INSERT_DAILY, bulk_args=batch)
        total += len(result.get("results", []))
    return total


def replace_aggregates(trade_date: str) -> None:
    sql("DELETE FROM doc.sector_flow_5m WHERE trade_date = ?", [trade_date])
    sql(
        """
        INSERT INTO doc.sector_flow_5m (
          source,sector_code,sector_type,sector_name,trade_date,bucket_ts,
          avg_price,pct_change,in_amount,out_amount,net_amount,net_ratio
        )
        SELECT source,sector_code,max(sector_type),max(sector_name),trade_date,
               date_bin('5 minutes'::INTERVAL,minute_ts,date_trunc('day',minute_ts)),
               max_by(avg_price,minute_ts),max_by(pct_change,minute_ts),
               max_by(in_amount,minute_ts),max_by(out_amount,minute_ts),
               max_by(net_amount,minute_ts),max_by(net_ratio,minute_ts)
        FROM doc.sector_flow_1m WHERE trade_date = ?
        GROUP BY source,sector_code,trade_date,
                 date_bin('5 minutes'::INTERVAL,minute_ts,date_trunc('day',minute_ts))
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
        FROM doc.sector_flow_1m WHERE trade_date = ?
        GROUP BY source,sector_code,trade_date
        ON CONFLICT (source,sector_code,trade_date) DO UPDATE SET
          sector_type=excluded.sector_type,sector_name=excluded.sector_name,
          close_ts=excluded.close_ts,avg_price=excluded.avg_price,pct_change=excluded.pct_change,
          in_amount=excluded.in_amount,out_amount=excluded.out_amount,
          net_amount=excluded.net_amount,net_ratio=excluded.net_ratio
        """,
        [trade_date],
    )


def counts(trade_date: str) -> dict[str, int]:
    output = {}
    for table in ("sector_flow_1m", "sector_flow_5m", "sector_flow_daily"):
        result = sql(f"SELECT count(*) FROM doc.{table} WHERE trade_date = ?", [trade_date])
        output[table] = int(result["rows"][0][0])
    return output
