"""CAMS regional PM2.5 estimate for every current Hanoi phường/xã.

One Open-Meteo request asks for all 126 area centres. Open-Meteo reports a
0.1° grid point per location, but CAMS Global is a 0.4° (~45 km) model, so
many points carry exactly the same series. Areas are grouped by identical
series (the same native model cell): every area in a group gets the same
value, and the page says so. Nothing is interpolated between groups.
"""

import json
import pkgutil
from collections import Counter

from nowcast import openmeteo

AREAS = json.loads(pkgutil.get_data("nowcast", "hanoi_areas.json"))["areas"]
FORECAST_HOURS = 96
STEP = 0.4  # CAMS Global grid; cell centres at multiples of 0.4°


def cell(lat, lon):
    """Native CAMS cell containing a point: centre and bounds (south, west, north, east)."""
    c = (round(round(lat / STEP) * STEP, 1), round(round(lon / STEP) * STEP, 1))
    h = STEP / 2
    return c, [
        round(c[0] - h, 1),
        round(c[1] - h, 1),
        round(c[0] + h, 1),
        round(c[1] + h, 1),
    ]


def params():
    return {
        "latitude": ",".join(str(a["lat"]) for a in AREAS),
        "longitude": ",".join(str(a["lon"]) for a in AREAS),
        "hourly": "pm2_5",
        "domains": "cams_global",
        "past_days": 1,
        "forecast_days": 5,
        "timezone": "GMT",
    }


def build(meta, bodies, now):
    if not isinstance(bodies, list) or len(bodies) != len(AREAS):
        raise ValueError("Expected one model series per Hanoi area")
    groups = {}
    for area, body in zip(AREAS, bodies, strict=True):
        key = tuple(body["hourly"]["pm2_5"])
        g = groups.setdefault(
            key, {"body": body, "areas": [], "points": set(), "at": area}
        )
        g["areas"].append(area["osm"])
        g["points"].add((body["latitude"], body["longitude"]))
    out = []
    ordered = sorted(groups.values(), key=lambda g: (-len(g["areas"]), g["areas"][0]))
    for i, g in enumerate(ordered):
        m = openmeteo.normalize(
            meta, g["body"], now, point=(g["at"]["lat"], g["at"]["lon"])
        )
        by_osm = {a["osm"]: a for a in AREAS}
        cells = Counter(cell(by_osm[o]["lat"], by_osm[o]["lon"])[0] for o in g["areas"])
        centre = cells.most_common(1)[0][0]
        out.append(
            {
                "group": i,
                "areas": g["areas"],
                # verified: every area of this series lies in one native cell
                "cell": {
                    "centre": list(centre),
                    "bounds": cell(*centre)[1],
                    "verified": len(cells) == 1,
                },
                "grid_points": sorted([list(x) for x in g["points"]]),
                "current": m["current"],
                "forecast": m["forecast"][:FORECAST_HOURS],
                "daily": m["daily"],
            }
        )
    return {
        "kind": "model",
        "source": openmeteo.SOURCE,
        "model_run_at": m["model_run_at"],
        "model_available_at": m["model_available_at"],
        "area_count": len(AREAS),
        "group_count": len(out),
        "grouping": "areas whose CAMS series are identical share one native 0.4° model cell and therefore one value; no interpolation. Cell edges run through central Hanoi (about 21.0°N and 105.8°E), so neighbouring wards can differ only because of the grid",
        "groups": out,
    }
