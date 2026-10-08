# freshness_report.py
import os
from datetime import UTC, datetime

import requests

H = {"X-API-Key": os.environ["OPENAQ_KEY"]}
BASE = "https://api.openaq.org/v3"
now = datetime.now(UTC)

for loc in (4946812, 4946813, 6123215):
    l = requests.get(f"{BASE}/locations/{loc}", headers=H, timeout=30).json()[
        "results"
    ][0]
    sensor_param = {
        s["id"]: (s.get("parameter") or {}).get("name") for s in l.get("sensors", [])
    }
    latest = requests.get(
        f"{BASE}/locations/{loc}/latest", headers=H, timeout=30
    ).json()["results"]
    print(f"\n== {loc} {l['name']}")
    for r in sorted(latest, key=lambda x: sensor_param.get(x["sensorsId"]) or ""):
        sid = r["sensorsId"]
        param = sensor_param.get(sid, "???")
        ts = r["datetime"]["utc"]
        age_h = (now - datetime.fromisoformat(ts)).total_seconds() / 3600
        flag = "TƯƠI" if age_h <= 1 else ("CŨ" if age_h <= 24 else "CHẾT")
        print(
            f"  {param:<8} sensorId={sid:<10} giá trị={r['value']:<10} tuổi(giờ)={age_h:7.1f}  [{flag}]"
        )
