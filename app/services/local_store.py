import csv, json, os
from pathlib import Path
FIELDS=["trade_date","snapshot_time","source","sector_type","sector_code","sector_name","avg_price","pct_change","turnover","in_amount","out_amount","net_amount","net_ratio","leader_code","leader_name","leader_price","leader_pct_change","leader_net_ratio"]

def append_csv(path:Path, records):
    path.parent.mkdir(parents=True,exist_ok=True); exists=path.exists()
    with path.open("a",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=FIELDS)
        if not exists: w.writeheader()
        w.writerows(records); f.flush(); os.fsync(f.fileno())

def write_status(path:Path,payload):
    path.parent.mkdir(parents=True,exist_ok=True); tmp=path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf-8"); tmp.replace(path)
