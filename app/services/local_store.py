import csv
import json
import os
from pathlib import Path

FIELDS = [
    "trade_date","minute_ts","source","market_phase","sector_type","sector_code","sector_name",
    "avg_price","pct_change","turnover","in_amount","out_amount","net_amount","net_ratio",
    "leader_code","leader_name","leader_price","leader_pct_change","leader_net_ratio"
]

def append_csv(path: Path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerows(records)
        fh.flush()
        os.fsync(fh.fileno())

def write_json_atomic(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)

def save_pending(path: Path, payload):
    write_json_atomic(path, payload)
