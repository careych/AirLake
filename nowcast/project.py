"""Serving projection for "now": observations, model estimate and forecast kept apart."""

from ingestion.openaq import stamp, utc
from live_data.contract import freshness
from nowcast import sensor_community

DRAGON = {
    "station_id": 6123215,
    "name": "Dragon (AirGradient)",
    "provider": "AirGradient via OpenAQ",
    "licence": "CC BY 4.0",
    "latitude": 20.9933,
    "longitude": 105.9441,
    "station_type": "low_cost",
    "cadence_seconds": 3600,
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


def observation(meta, reading, now, ingested_at):
    item = {k: v for k, v in meta.items() if k != "cadence_seconds"}
    item.update(
        kind="observed",
        observed_at=None,
        pm25=None,
        ingested_at=stamp(utc(ingested_at)),
    )
    if reading:
        item.update(reading)
    item["freshness"] = freshness(item["observed_at"], now, meta["cadence_seconds"])
    return item


def build(now, dragon, community, model, validation, ingested_at=None):
    now = utc(now)
    ingested = ingested_at or now
    sc = {
        "station_id": 99385,
        **sensor_community.SENSORS[99385],
        "provider": "Sensor.Community",
        "licence": sensor_community.LICENCE,
        "cadence_seconds": 150,
    }
    observations = [
        observation(DRAGON, dragon, now, ingested),
        observation(sc, community, now, ingested),
    ]
    return {
        "schema_version": 1,
        "generated_at": stamp(now),
        "geography": "Hà Nội: 2 điểm quan trắc ngoài trời có giấy phép chia sẻ; mô hình CAMS ô lưới ~45 km cho khu vực trung tâm",
        "observations": observations,
        "model": model,
        "validation": validation,
    }
