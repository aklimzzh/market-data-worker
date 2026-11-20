import time
import requests
URL = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_bkzj_bk"
HEADERS = {"Referer":"https://vip.stock.finance.sina.com.cn/","User-Agent":"Mozilla/5.0","Accept":"*/*"}
PARTS = [
 ("industry", {"page":"1","num":"100","sort":"category","asc":"1","fenlei":"0"},48),
 ("concept", {"page":"1","num":"150","sort":"category","asc":"1","fenlei":"1"},150),
 ("concept", {"page":"2","num":"150","sort":"category","asc":"1","fenlei":"1"},31),
]

def fetch_snapshot(session=None, retries=2):
    s=session or requests.Session(); rows=[]
    for sector_type, params, expected in PARTS:
        last=None
        for attempt in range(retries+1):
            try:
                r=s.get(URL,params=params,headers=HEADERS,timeout=(5,15)); r.raise_for_status(); part=r.json()
                if not isinstance(part,list) or len(part)!=expected: raise ValueError(f"expected {expected}, got {len(part) if isinstance(part,list) else 'non-list'}")
                for x in part: x["sector_type"]=sector_type
                rows.extend(part); break
            except Exception as exc:
                last=exc
                if attempt<retries: time.sleep(1 if attempt==0 else 3)
        else: raise RuntimeError(f"Sina request failed: {last}")
        time.sleep(.4)
    unique={x["category"]:x for x in rows}
    if len(unique)!=229: raise ValueError(f"expected 229 unique sectors, got {len(unique)}")
    return list(unique.values())
