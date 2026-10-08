# check_license.py
import os, json, requests
H = {"X-API-Key": os.environ["OPENAQ_KEY"]}
BASE = "https://api.openaq.org/v3"
for loc in (4946812, 4946813, 6123215):
    l = requests.get(f"{BASE}/locations/{loc}", headers=H, timeout=30).json()["results"][0]
    print(f"\n== {loc} {l['name']}")
    print("licenses:", json.dumps(l.get("licenses"), ensure_ascii=False))
    print("owner:", (l.get("owner") or {}).get("name"))
    print("provider:", (l.get("provider") or {}).get("name"))