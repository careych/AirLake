"""Serving projection for "now": observations, model estimate and forecast kept apart.

Times: observed_at (sensor), fetched_at (our fetch), published_at (projection
written), model_run_at / model_available_at (CAMS), valid_at (forecast hour).
"""

from datetime import timedelta

from ingestion.openaq import stamp, utc
from live_data.contract import freshness
from nowcast import sensor_community

DRAGON = {
    "station_id": 6123215,
    "name": "Dragon (AirGradient)",
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
}
COMMUNITY = {
    "station_id": 99385,
    **sensor_community.SENSORS[99385],
    "provider": "Sensor.Community",
    **sensor_community.LICENCE,
    "cadence_seconds": 150,
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


def observation(meta, source, now):
    """source: {status, reading, fetched_at, error}; failed fetches keep the last reading."""
    reading = source.get("reading") or {}
    item = {k: v for k, v in meta.items() if k != "cadence_seconds"}
    item.update(
        kind="observed",
        observed_at=reading.get("observed_at"),
        pm25=reading.get("pm25"),
        fetched_at=source.get("fetched_at"),
        fetch_status=source["status"],
        fetch_error=source.get("error"),
    )
    item["freshness"] = freshness(item["observed_at"], now, meta["cadence_seconds"])
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
