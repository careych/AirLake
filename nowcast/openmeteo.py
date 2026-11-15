"""CAMS Global PM2.5 via Open-Meteo for one Hanoi-area grid cell.

The model is a coarse (0.4°, ~45 km) regional estimate, natively 3-hourly and
interpolated to hours by Open-Meteo. It is never presented as a measurement.
"""

import math
from datetime import UTC, datetime, timedelta

from historical.data import VIETNAM
from ingestion.openaq import stamp, utc

API = "https://air-quality-api.open-meteo.com/v1/air-quality"
META = "https://air-quality-api.open-meteo.com/data/cams_global/static/meta.json"
# Central Hanoi (Hoàn Kiếm). Other Hanoi points share or neighbour this coarse
# cell; district-level model comparisons would only reflect grid boundaries.
POINT = (21.0285, 105.8542)
SOURCE = {
    "name": "CAMS Global Atmospheric Composition Forecasts",
    "provider": "Copernicus Atmosphere Monitoring Service (ECMWF) via Open-Meteo",
    "domain": "cams_global",
    "resolution": "0.4° (~45 km)",
    "native_time_step_hours": 3,
    "licence": "CC BY 4.0 (Open-Meteo); Copernicus licence attribution",
    "attribution": "Generated using Copernicus Atmosphere Monitoring Service information; data via Open-Meteo.com (CC BY 4.0)",
    "url": "https://open-meteo.com/en/docs/air-quality-api",
}
PARAMS = {
    "latitude": POINT[0],
    "longitude": POINT[1],
    "hourly": "pm2_5",
    "domains": "cams_global",
    "past_days": 1,
    "forecast_days": 5,
    "timezone": "GMT",
}


def epoch(value):
    if type(value) is not int or value <= 0:
        raise ValueError("Invalid model metadata time")
    return datetime.fromtimestamp(value, UTC)


def window(series, start, hours):
    rows = [
        r
        for r in series
        if start <= utc(r["valid_at"]) < start + timedelta(hours=hours)
    ]
    return (
        math.fsum(r["pm25"] for r in rows) / len(rows) if len(rows) == hours else None
    )


def normalize(meta, body, now):
    """Validate one Open-Meteo response; split into current estimate and forecast."""
    now = utc(now)
    run_at, available_at = (
        epoch(meta["last_run_initialisation_time"]),
        epoch(meta["last_run_availability_time"]),
    )
    if meta.get("update_interval_seconds") != 43200 or available_at < run_at:
        raise ValueError("Unexpected CAMS Global update metadata")
    lat, lon = body["latitude"], body["longitude"]
    if abs(lat - POINT[0]) > 0.5 or abs(lon - POINT[1]) > 0.5:
        raise ValueError("Model grid cell is not the Hanoi-area cell")
    if body.get("hourly_units", {}).get("pm2_5") != "μg/m³":
        raise ValueError("Unexpected model unit")
    times, values = body["hourly"]["time"], body["hourly"]["pm2_5"]
    if len(times) != len(values) or not times:
        raise ValueError("Model series length mismatch")
    series = []
    previous = None
    for t, v in zip(times, values, strict=True):
        at = datetime.fromisoformat(t).replace(tzinfo=UTC)
        if previous is not None and at - previous != timedelta(hours=1):
            raise ValueError("Model times are not contiguous hours")
        previous = at
        if v is None:
            continue
        if type(v) not in (int, float) or not math.isfinite(v) or not 0 <= v <= 1000:
            raise ValueError("Invalid model PM2.5")
        series.append({"valid_at": stamp(at), "pm25": float(v)})
    hour = now.replace(minute=0, second=0, microsecond=0)
    past = [r for r in series if utc(r["valid_at"]) <= hour]
    future = [r for r in series if utc(r["valid_at"]) > hour]
    if not past or len(future) < 24:
        raise ValueError("Model response lacks a current hour or a 24-hour forecast")
    for r in future:
        r["hours_ahead"] = int((utc(r["valid_at"]) - hour).total_seconds() // 3600)
        r["lead_hours_from_run"] = int(
            (utc(r["valid_at"]) - run_at).total_seconds() // 3600
        )
    days = {}
    for r in future:
        days.setdefault(
            utc(r["valid_at"]).astimezone(VIETNAM).date().isoformat(), []
        ).append(r["pm25"])
    daily = [
        {
            "date_local": d,
            "hours": len(v),
            "mean": math.fsum(v) / len(v),
            "min": min(v),
            "max": max(v),
        }
        for d, v in sorted(days.items())
        if len(v) >= 20
    ]
    # Relative 3-hour windows inside the next 24 hours, local daytime only.
    windows = []
    for k in range(22):
        start = hour + timedelta(hours=k + 1)
        local = start.astimezone(VIETNAM)
        if 6 <= local.hour <= 18:
            mean = window(future, start, 3)
            if mean is not None:
                windows.append(
                    {
                        "start_utc": stamp(start),
                        "end_utc": stamp(start + timedelta(hours=3)),
                        "mean": mean,
                    }
                )
    ranked = sorted(windows, key=lambda w: (w["mean"], w["start_utc"]))
    distinct = bool(ranked) and ranked[-1]["mean"] >= 1.2 * ranked[0]["mean"]
    return {
        "kind": "model",
        "source": SOURCE,
        "grid_cell": {"latitude": lat, "longitude": lon, "requested": list(POINT)},
        "model_run_at": stamp(run_at),
        "model_available_at": stamp(available_at),
        "update_interval_hours": 12,
        "current": {**past[-1], "kind": "model_estimate"},
        "forecast": future,
        "forecast_horizon_hours": future[-1]["hours_ahead"],
        "daily": daily,
        "relative_windows_24h": {
            "lower": ranked[0] if distinct else None,
            "higher": ranked[-1] if distinct else None,
            "distinct": distinct,
            "rule": "lowest/highest 3-hour model mean, 06:00-21:00 Vietnam time, next 24 h; shown only if the higher window is >=20% above the lower; relative, not a safety guarantee",
        },
    }


def fetch(get, now):
    return normalize(get(META), get(API, PARAMS), now)
