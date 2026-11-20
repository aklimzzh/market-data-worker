from pathlib import Path
import pandas as pd
from .config import DATA_DIR

def run(trade_date):
    src=list((DATA_DIR/"raw").glob(f"**/{trade_date}.csv"))
    if not src: raise FileNotFoundError(f"raw CSV not found for {trade_date}")
    df=pd.read_csv(src[0]); expected=df["sector_code"].nunique()
    if expected<229: raise RuntimeError(f"only {expected} sectors found")
    out=DATA_DIR/"archive"/trade_date[:4]/trade_date[5:7]/f"{trade_date}.parquet"; out.parent.mkdir(parents=True,exist_ok=True)
    df.to_parquet(out,index=False,compression="zstd")
    print({"trade_date":trade_date,"rows":len(df),"sectors":expected,"parquet":str(out)})
    print("NOTE: ETF/index repair, Serv00 reconciliation and CrateDB upload are placeholders until their existing API/schema details are supplied.")
