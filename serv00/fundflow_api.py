import os
from flask import Blueprint, jsonify, request
from psycopg.rows import dict_row
import psycopg

fundflow_bp = Blueprint("fundflow", __name__, url_prefix="/api/fundflow")

def get_connection():
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)

def as_float(value):
    if value in (None, "", "-"):
        return None
    return float(value)

def authorized():
    expected = os.environ.get("FUNDFLOW_API_TOKEN", "")
    supplied = request.headers.get("Authorization", "")
    return bool(expected) and supplied == f"Bearer {expected}"

@fundflow_bp.post("/ingest")
def ingest():
    if not authorized():
        return jsonify({"ok":False,"error":"unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    records = payload.get("records")
    if not isinstance(records, list) or len(records) != 229:
        return jsonify({"ok":False,"error":"records must contain exactly 229 items"}), 400
    minute_ts = payload.get("minute_ts")
    phase = payload.get("market_phase")
    codes = [r.get("sector_code") for r in records]
    if len(set(codes)) != 229:
        return jsonify({"ok":False,"error":"sector_code must be unique"}), 400
    master_rows=[]; flow_rows=[]; latest_rows=[]
    for r in records:
        common=(r["source"],r["sector_code"],r["sector_type"],r["sector_name"])
        master_rows.append(common)
        values=(as_float(r.get("avg_price")),as_float(r.get("pct_change")),as_float(r.get("turnover")),as_float(r.get("in_amount")),as_float(r.get("out_amount")),as_float(r.get("net_amount")),as_float(r.get("net_ratio")),r.get("leader_code"),r.get("leader_name"),as_float(r.get("leader_price")),as_float(r.get("leader_pct_change")),as_float(r.get("leader_net_ratio")))
        flow_rows.append((minute_ts,r["trade_date"],r["source"],phase,r["sector_code"],r["sector_type"],r["sector_name"],*values))
        latest_rows.append((r["source"],r["sector_code"],r["sector_type"],r["sector_name"],minute_ts,phase,*values))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany("""INSERT INTO sector_master(source,sector_code,sector_type,sector_name) VALUES(%s,%s,%s,%s) ON CONFLICT(source,sector_code) DO UPDATE SET sector_type=excluded.sector_type,sector_name=excluded.sector_name,updated_at=now()""",master_rows)
            cur.executemany("""INSERT INTO sector_flow_1m(minute_ts,trade_date,source,market_phase,sector_code,sector_type,sector_name,avg_price,pct_change,turnover,in_amount,out_amount,net_amount,net_ratio,leader_code,leader_name,leader_price,leader_pct_change,leader_net_ratio) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(source,sector_code,minute_ts) DO UPDATE SET market_phase=excluded.market_phase,avg_price=excluded.avg_price,pct_change=excluded.pct_change,turnover=excluded.turnover,in_amount=excluded.in_amount,out_amount=excluded.out_amount,net_amount=excluded.net_amount,net_ratio=excluded.net_ratio,received_at=now()""",flow_rows)
            cur.executemany("""INSERT INTO sector_latest(source,sector_code,sector_type,sector_name,minute_ts,market_phase,avg_price,pct_change,turnover,in_amount,out_amount,net_amount,net_ratio,leader_code,leader_name,leader_price,leader_pct_change,leader_net_ratio) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(source,sector_code) DO UPDATE SET sector_type=excluded.sector_type,sector_name=excluded.sector_name,minute_ts=excluded.minute_ts,market_phase=excluded.market_phase,avg_price=excluded.avg_price,pct_change=excluded.pct_change,turnover=excluded.turnover,in_amount=excluded.in_amount,out_amount=excluded.out_amount,net_amount=excluded.net_amount,net_ratio=excluded.net_ratio,leader_code=excluded.leader_code,leader_name=excluded.leader_name,leader_price=excluded.leader_price,leader_pct_change=excluded.leader_pct_change,leader_net_ratio=excluded.leader_net_ratio,updated_at=now()""",latest_rows)
    return jsonify({"ok":True,"minute_ts":minute_ts,"received":len(records)})

@fundflow_bp.get("/latest")
def latest():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM sector_latest ORDER BY net_amount DESC NULLS LAST")
        rows=cur.fetchall()
    return jsonify({"ok":True,"count":len(rows),"data":rows})
