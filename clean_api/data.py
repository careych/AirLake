"""Verified historical data and bounded filtering; no SDK or import-time I/O."""

import hashlib
import json
import math
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

ACCOUNT = "701364614078"
REGION = "ap-southeast-2"
DATABASE = "airlake_it2_20261108_37179997ad2b42959f4387637e003ab8_gold"
FACT_SNAPSHOT = "2020624562194805942"
DIM_SNAPSHOT = "5513456602871200404"
START = "2026-11-07T02:00:00Z"
END = "2026-11-08T03:00:00Z"
STATIONS = (4946812, 4946813, 6123215)
EVIDENCE_SHA256 = "21d76b6bd19d906bce9601d6ae4b3e6724a9cab62c23cb5d2f94b10b5815a50a"
FIELDS = ("location_id", "station_name", "hour_start", "pm25_avg", "aqi")
DIM_FIELDS = ("location_id", "name", "lat", "lon", "station_type")


def utc(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)", value
    ):
        raise ValueError("Use an ISO-8601 UTC timestamp with Z or +00:00")
    return datetime.fromisoformat(value)


def stamp(value):
    return utc(value).isoformat().replace("+00:00", "Z")


def integer(value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError("Integer outside allowed range")
    return value


def finite(value, low, high):
    if (
        type(value) not in (int, float)
        or not math.isfinite(value)
        or not low <= value <= high
    ):
        raise ValueError("Invalid finite numeric value")
    return value


class Dataset:
    def __init__(self, facts, dimensions, *, evidence_sha256):
        if not isinstance(facts, list) or not isinstance(dimensions, list):
            raise TypeError("Expected Gold row lists")
        if not 1 <= len(facts) <= 1000 or not 1 <= len(dimensions) <= 100:
            raise ValueError("Historical dataset exceeds bounds or is empty")
        stations = {}
        for raw in dimensions:
            if not isinstance(raw, dict) or set(raw) != set(DIM_FIELDS):
                raise ValueError("Dimension schema mismatch")
            row = dict(raw)
            location = integer(row["location_id"], 1, 2**31 - 1)
            if location not in STATIONS or location in stations:
                raise ValueError("Duplicate or unapproved dimension key")
            if not isinstance(row["name"], str) or not row["name"].strip():
                raise ValueError("Missing station name")
            finite(row["lat"], -90, 90)
            finite(row["lon"], -180, 180)
            if row["station_type"] is not None and not isinstance(
                row["station_type"], str
            ):
                raise ValueError("Invalid station_type")
            stations[location] = row
        rows, keys = [], set()
        for raw in facts:
            if not isinstance(raw, dict) or set(raw) != {
                "location_id",
                "hour_start",
                "pm25_avg",
                "aqi",
            }:
                raise ValueError("Hourly schema mismatch")
            location = integer(raw["location_id"], 1, 2**31 - 1)
            if location not in stations:
                raise ValueError("Orphan hourly fact")
            hour = utc(raw["hour_start"])
            if (
                hour.minute
                or hour.second
                or hour.microsecond
                or not utc(START) <= hour < utc(END)
            ):
                raise ValueError("Hourly key outside approved UTC scope")
            key = (location, hour)
            if key in keys:
                raise ValueError("Duplicate hourly key")
            keys.add(key)
            rows.append(
                {
                    "location_id": location,
                    "station_name": stations[location]["name"],
                    "hour_start": stamp(raw["hour_start"]),
                    "pm25_avg": finite(raw["pm25_avg"], 0, float("inf")),
                    "aqi": integer(raw["aqi"], 0, 500),
                }
            )
        self.rows = tuple(
            sorted(rows, key=lambda r: (r["location_id"], r["hour_start"]))
        )
        self.stations = tuple(stations[k] for k in sorted(stations))
        self.evidence_sha256 = evidence_sha256

    def metadata(self):
        return {
            "dataset": "AirLake historical bounded OpenAQ batch; NOT a live feed",
            "backend": "retained_cloud_acceptance_copy",
            "window_start_utc": START,
            "window_end_utc_exclusive": END,
            "timezone": "UTC",
            "pm25_units": "µg/m³",
            "aqi_method": "simplified_hourly_mean_mvp; NOT official VN_AQI",
            "source": "OpenAQ API v3; station names retained from Gold dim_location",
            "source_url": "https://openaq.org/",
            "licensing": {
                "status": "Provider-specific redistribution terms not retained; public redistribution not cleared",
                "terms_url": "https://docs.openaq.org/about/terms",
                "scope": "localhost demonstration only",
            },
            "provenance": {
                "account": ACCOUNT,
                "region": REGION,
                "catalog": "AwsDataCatalog",
                "database": DATABASE,
                "hourly_table": "pm25_aqi_hourly",
                "dimension_table": "dim_location",
                "hourly_snapshot": FACT_SNAPSHOT,
                "dimension_snapshot": DIM_SNAPSHOT,
                "acceptance_completed_at_utc": "2026-11-08T06:51:16.229414Z",
                "evidence_sha256": self.evidence_sha256,
                "athena_called_per_request": False,
            },
        }

    def page(self, params):
        allowed = {
            "location_id",
            "station_id",
            "start",
            "end",
            "limit",
            "offset",
            "format",
        }
        if set(params) - allowed or any(len(v) != 1 for v in params.values()):
            raise ValueError("Unknown or repeated query parameter")
        p = {k: v[0] for k, v in params.items()}
        if "location_id" in p and "station_id" in p:
            raise ValueError("Use location_id or station_id, not both")
        location = p.get("location_id", p.get("station_id"))
        if location is not None and (
            not re.fullmatch(r"[1-9][0-9]{0,9}", location)
            or int(location) not in STATIONS
        ):
            raise ValueError("Unknown or invalid station ID")
        start, end = utc(p.get("start", START)), utc(p.get("end", END))
        if not utc(START) <= start < end <= utc(END):
            raise ValueError("Time filter must stay inside the historical batch")
        for key in ("limit", "offset"):
            if key in p and not re.fullmatch(r"[0-9]{1,4}", p[key]):
                raise ValueError("Invalid pagination parameter")
        limit = integer(int(p.get("limit", "50")), 1, 100)
        offset = integer(int(p.get("offset", "0")), 0, 1000)
        fmt = p.get("format", "json")
        if fmt not in ("json", "csv"):
            raise ValueError("format must be json or csv")
        selected = [
            dict(r)
            for r in self.rows
            if (location is None or r["location_id"] == int(location))
            and start <= utc(r["hour_start"]) < end
        ]
        rows = selected[offset : offset + limit]
        next_page = None
        if offset + len(rows) < len(selected):
            next_page = "/api/v1/gold/hourly?" + urlencode(
                {**p, "offset": offset + len(rows), "limit": limit}
            )
        return {
            "data": rows,
            "count": len(rows),
            "total": len(selected),
            "next": next_page,
            "metadata": self.metadata(),
        }, fmt


def load_evidence(path):
    content = Path(path).read_bytes()
    if hashlib.sha256(content).hexdigest() != EVIDENCE_SHA256:
        raise ValueError(
            "Acceptance evidence SHA-256 does not match the approved artifact"
        )
    value = json.loads(content)
    if (
        value.get("status")
        != "PASS_REAL_559_RECORD_CLOUD_PIPELINE_AND_AUTHENTICATED_ATHENA_DASHBOARD"
    ):
        raise ValueError("Acceptance status mismatch")
    for table, expected in (
        ("HOURLY_TABLE", FACT_SNAPSHOT),
        ("DIMENSION_TABLE", DIM_SNAPSHOT),
    ):
        if (
            str(value["unchanged_rerun"]["tables"][table]["current_snapshot_id"])
            != expected
        ):
            raise ValueError("Acceptance snapshot mismatch")
    data = Dataset(
        value["hourly_rows"], value["dimension_rows"], evidence_sha256=EVIDENCE_SHA256
    )
    if len(data.rows) != 72 or len(data.stations) != 3:
        raise ValueError("Acceptance row count mismatch")
    return data
