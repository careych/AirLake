# probe_locations.py   |   python probe_locations.py
import os

import requests

H = {"X-API-Key": os.environ["OPENAQ_KEY"]}
BASE = "https://api.openaq.org/v3"
for loc in (4946812, 4946813, 6123215):
    l = requests.get(f"{BASE}/locations/{loc}", headers=H, timeout=30).json()[
        "results"
    ][0]
    print(
        "\n==",
        loc,
        l["name"],
        "| từ:",
        (l.get("datetimeFirst") or {}).get("utc"),
        "| gần nhất:",
        (l.get("datetimeLast") or {}).get("utc"),
    )
    for s in l.get("sensors", []):
        if (s.get("parameter") or {}).get("name") == "pm25":
            m = (
                requests.get(
                    f"{BASE}/sensors/{s['id']}/measurements",
                    headers=H,
                    timeout=30,
                    params={"limit": 8},
                )
                .json()
                .get("results", [])
            )
            print("  PM2.5, các mốc thời gian gần nhất:")
            for x in m:
                print(
                    "   ",
                    (x.get("period") or {}).get("datetimeFrom", {}).get("utc")
                    or x.get("datetime"),
                    x.get("value"),
                )
