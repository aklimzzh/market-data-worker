import requests

def post_batch(url, token, payload):
    if not url: return {"skipped":True}
    r=requests.post(url,json=payload,headers={"Authorization":f"Bearer {token}"},timeout=(5,30))
    r.raise_for_status(); return r.json() if r.content else {"ok":True}
