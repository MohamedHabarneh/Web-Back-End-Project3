import httpx

def report_score(url, json):
    resp = httpx.post(url, json=json)
    resp.raise_for_status()
    return resp