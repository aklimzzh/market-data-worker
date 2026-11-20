import requests

def post_batch(url, token, payload):
    if not url:
        return {"skipped": True, "reason": "SERV00_INGEST_URL empty"}
    response = requests.post(
        url,
        json=payload,
        headers={"Authorization": f"Bearer {token}", "Content-Type":"application/json"},
        timeout=(5, 30),
    )
    response.raise_for_status()
    return response.json() if response.content else {"ok": True}
