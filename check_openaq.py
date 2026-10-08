# check_openaq.py   |   pip install requests   |   python check_openaq.py
import os, json, requests
from datetime import datetime, timezone

H = {"X-API-Key": os.environ["OPENAQ_KEY"]}
BASE = "https://api.openaq.org/v3"
r = requests.get(f"{BASE}/locations", headers=H, timeout=30,
                 params={"coordinates": "21.0285,105.8542", "radius": 25000, "limit": 100})
r.raise_for_status()
print("Header giới hạn tần suất:", {k: v for k, v in r.headers.items() if "ratelimit" in k.lower()})
locs = r.json()["results"]
print(len(locs), "trạm quanh Hà Nội\n")

now, fresh = datetime.now(timezone.utc), 0
for l in locs:
    params = sorted({(s.get("parameter") or {}).get("name") for s in l.get("sensors", [])})
    last = (l.get("datetimeLast") or {}).get("utc")
    age_h = None
    if last:
        age_h = (now - datetime.fromisoformat(last.replace("Z", "+00:00"))).total_seconds() / 3600
    ok = "pm25" in params and age_h is not None and age_h <= 3
    fresh += ok
    print(l["id"], l["name"], "|", (l.get("provider") or {}).get("name"), "| pm25:", "pm25" in params,
          "| tuổi dữ liệu (giờ):", None if age_h is None else round(age_h, 1),
          "| từ:", (l.get("datetimeFirst") or {}).get("utc"))
print("\nTrạm PM2.5 cập nhật trong 3 giờ:", fresh)
if locs:
    print(json.dumps(locs[0], indent=2)[:8000])   # xem tên trường và giấy phép thực tế