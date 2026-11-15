"""Serving projection for "now": observations, model estimate and forecast kept apart.

Times: observed_at (sensor), fetched_at (our fetch), published_at (projection
written), model_run_at / model_available_at (CAMS), valid_at (forecast hour).
"""

from datetime import timedelta

from ingestion.openaq import stamp, utc
from nowcast import sensor_community

# Freshness by source cadence and measured delivery delay (2026-11-15, 24
# stored projections): Dragon's hourly value (stamped at the END of the hour)
# reached AirLake 16-20 min after the hour, so with a 15-minute refresh a
# normal age is ~16-80 min; Sensor.Community readings (~150 s apart) arrived
# 2-4 min after measurement, so a normal age is under ~22 min.
POLICY = {
    6123215: {"fresh_seconds": 5400, "stale_seconds": 10800},  # 1.5 h / 3 h
    99385: {"fresh_seconds": 1500, "stale_seconds": 3600},  # 25 min / 1 h
}

DRAGON = {
    "station_id": 6123215,
    "name": "Dragon (AirGradient)",
    "place": "Gia Lâm · Ocean Park",
    "area": "Xã Gia Lâm, Hà Nội",
    "provider": "AirGradient via OpenAQ",
    "licence": "CC BY 4.0",
    "licence_url": "https://creativecommons.org/licenses/by/4.0/",
    "attribution": "Dragon sensor data by AirGradient via OpenAQ, CC BY 4.0",
    "share_alike": False,
    "exact_location": True,
    "latitude": 20.9933,
    "longitude": 105.9441,
    "station_type": "low_cost",
    "cadence_seconds": 3600,
    "observed_at_meaning": "end of the averaging hour",
}
COMMUNITY = {
    "station_id": 99385,
    **sensor_community.SENSORS[99385],
    "provider": "Sensor.Community",
    "place": "Tây Hồ · cảm biến cộng đồng",
    "area": "Phường Tây Hồ, Hà Nội (vị trí gần đúng)",
    **sensor_community.LICENCE,
    "cadence_seconds": 150,
    "observed_at_meaning": "time of a single ~150-second reading",
}


def openaq_latest(body, sensor_id=14581375):
    """Dragon PM2.5 from OpenAQ /locations/{id}/latest."""
    for r in body["results"]:
        if r["sensorsId"] == sensor_id:
            v = r["value"]
            if type(v) not in (int, float) or not 0 <= v <= 1000:
                raise ValueError("Invalid OpenAQ PM2.5")
            return {"observed_at": stamp(utc(r["datetime"]["utc"])), "pm25": float(v)}
    return None


def freshness(station_id, observed_at, now):
    """fresh / delayed / stale by observation age; never by fetch or publish time."""
    policy = POLICY[station_id]
    base = {
        **policy,
        "threshold_seconds": policy["fresh_seconds"],  # alert eligibility (bot)
        "criterion": "observation age: fresh <= fresh_seconds < delayed <= stale_seconds < stale",
    }
    if observed_at is None:
        return {**base, "status": "no_data", "age_seconds": None}
    age = (utc(now) - utc(observed_at)).total_seconds()
    status = (
        "invalid_future"
        if age < -300
        else "fresh"
        if age <= policy["fresh_seconds"]
        else "delayed"
        if age <= policy["stale_seconds"]
        else "stale"
    )
    return {**base, "status": status, "age_seconds": age}


def observation(meta, source, now):
    """source: {status, reading, fetched_at, error}; failed fetches keep the last reading."""
    reading = source.get("reading") or {}
    item = {
        k: v
        for k, v in meta.items()
        if k not in ("cadence_seconds", "observed_at_meaning")
    }
    item.update(
        kind="observed",
        observed_at=reading.get("observed_at"),
        pm25=reading.get("pm25"),
        fetched_at=source.get("fetched_at"),
        fetch_status=source["status"],
        fetch_error=source.get("error"),
    )
    item["freshness"] = freshness(meta["station_id"], item["observed_at"], now)
    item["observed_at_meaning"] = meta["observed_at_meaning"]
    return item


def recent(previous, observations, now, hours=24):
    """Rolling 24-hour series of distinct real observations per station.

    Built from what AirLake actually published since deployment; gaps stay gaps.
    """
    series = {
        str(k): list(v) for k, v in ((previous or {}).get("recent") or {}).items()
    }
    cutoff = utc(now) - timedelta(hours=hours)
    for o in observations:
        rows = series.setdefault(str(o["station_id"]), [])
        usable = (
            o["fetch_status"] == "ok" and o["observed_at"] and o["pm25"] is not None
        )
        if usable and (
            not rows or utc(rows[-1]["observed_at"]) < utc(o["observed_at"])
        ):
            rows.append({"observed_at": o["observed_at"], "pm25": o["pm25"]})
        series[str(o["station_id"])] = [
            r for r in rows if utc(r["observed_at"]) >= cutoff
        ]
    return series


def build(now, dragon, community, model, validation, previous=None):
    now = utc(now)
    observations = [
        observation(DRAGON, dragon, now),
        observation(COMMUNITY, community, now),
    ]
    return {
        "recent": recent(previous, observations, now),
        "schema_version": 2,
        "published_at": stamp(now),
        "geography": "Hà Nội: 2 điểm quan trắc ngoài trời có giấy phép chia sẻ; mô hình CAMS ô lưới ~45 km cho khu vực trung tâm",
        "observations": observations,
        "model": model,
        "validation": validation,
        "licence_note": "Each source keeps its own licence; the Sensor.Community part is a derived database under ODbL 1.0.",
    }
