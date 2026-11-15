"""Sensor.Community outdoor PM2.5 (SDS011) near Hanoi; low-cost observations."""

import csv
import io
import math
from collections import defaultdict
from datetime import UTC, datetime

from ingestion.openaq import stamp

# Sensor.Community publishes readings under DbCL 1.0, which requires ODbL
# compliance; derived databases we publish are offered under ODbL 1.0.
LICENCE = {
    "licence": "ODbL 1.0 (database) / DbCL 1.0 (contents)",
    "licence_url": "https://opendatacommons.org/licenses/odbl/1-0/",
    "attribution": "Contains Sensor.Community data (sensor.community), available under the Open Database License (ODbL 1.0)",
    "share_alike": True,
}
# Verified 2026-11-15: the only outdoor PM sensor reporting within 40 km of
# central Hanoi. Coordinates are rounded by the network.
SENSORS = {
    99385: {
        "name": "Cảm biến cộng đồng #99385 (khu vực Tây Hồ)",
        "latitude": 21.070,
        "longitude": 105.824,
        "sensor_type": "SDS011",
        "station_type": "low_cost",
        "exact_location": False,  # network reports an approximate location
    }
}
LATEST = "https://data.sensor.community/airrohr/v1/sensor/{}/"
ARCHIVE = "https://archive.sensor.community/{day}/{day}_sds011_sensor_{id}.csv"
MIN_READINGS_PER_HOUR = 12  # ~24 expected at the observed ~150 s spacing


def pm25(value):
    v = float(value)
    if not math.isfinite(v) or not 0 <= v <= 1000:
        raise ValueError("Invalid SDS011 PM2.5")
    return v


def latest(records, sensor_id):
    """Most recent outdoor P2 reading from the 5-minute API response."""
    best = None
    for r in records:
        if r["sensor"]["id"] != sensor_id or str(r["location"].get("indoor")) not in (
            "0",
            "False",
            "false",
        ):
            continue
        values = {v["value_type"]: v["value"] for v in r["sensordatavalues"]}
        if "P2" not in values:
            continue
        at = datetime.fromisoformat(r["timestamp"]).replace(tzinfo=UTC)
        if best is None or at > best[0]:
            best = (at, pm25(values["P2"]))
    if best is None:
        return None
    return {"observed_at": stamp(best[0]), "pm25": best[1]}


def hourly(text, sensor_id):
    """Hour-bucket means from one archive CSV; sparse hours are dropped, not filled."""
    buckets = defaultdict(list)
    for r in csv.DictReader(io.StringIO(text), delimiter=";"):
        if int(r["sensor_id"]) != sensor_id or not r["P2"]:
            continue
        at = datetime.fromisoformat(r["timestamp"]).replace(tzinfo=UTC)
        buckets[at.replace(minute=0, second=0, microsecond=0)].append(pm25(r["P2"]))
    return [
        {
            "station_id": sensor_id,
            "observed_hour_utc": stamp(h),
            "pm25": math.fsum(v) / len(v),
            "readings": len(v),
        }
        for h, v in sorted(buckets.items())
        if len(v) >= MIN_READINGS_PER_HOUR
    ]


def fetch_latest(get, sensor_id=99385):
    return latest(get(LATEST.format(sensor_id)), sensor_id)
