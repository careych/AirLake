# check_latest.py
import os, requests
H = {"X-API-Key": os.environ["OPENAQ_KEY"]}
BASE = "https://api.openaq.org/v3"
for loc in (4946812, 4946813, 6123215):
    try:
        r = requests.get(f"{BASE}/locations/{loc}/latest", headers=H, timeout=30)
        print(loc, "status:", r.status_code)
        print(r.json())
    except Exception as e:
        print(loc, "lỗi:", e)