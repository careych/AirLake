"""IAM-only, bounded historical JSON/CSV access to an accepted immutable export."""

import csv
import io
import json
import os
import re
from datetime import timedelta
from urllib.parse import parse_qs, urlencode
from zoneinfo import ZoneInfo

from final_demo.lambda_api import reply
from historical.analytics import summarize
from ingestion.openaq import PILOTS, finite, utc
from live_data.contract import ACCOUNT, BUCKET, sha


def parameters(event):
    query = event.get("rawQueryString", "")
    if len(query) > 1024:
        raise ValueError("Query too long")
    raw = parse_qs(query, keep_blank_values=True, max_num_fields=8)
    allowed = {
        "station_id",
        "location_id",
        "sensor_id",
        "start",
        "end",
        "granularity",
        "limit",
        "offset",
        "format",
    }
    if (
        set(raw) - allowed
        or any(len(v) != 1 for v in raw.values())
        or "station_id" in raw
        and "location_id" in raw
    ):
        raise ValueError("Invalid filters")
    p = {k: v[0] for k, v in raw.items()}
    for k in ("station_id", "location_id", "sensor_id", "limit", "offset"):
        if k in p and not re.fullmatch("[0-9]{1,10}", p[k]):
            raise ValueError("Invalid integer")
    loc = int(p.get("station_id", p.get("location_id", "0"))) or None
    sensor = int(p.get("sensor_id", "0")) or None
    if (
        (("station_id" in p or "location_id" in p) and loc not in PILOTS)
        or ("sensor_id" in p and sensor not in PILOTS.values())
        or loc
        and sensor
        and PILOTS[loc] != sensor
    ):
        raise ValueError("Unknown station/sensor mapping")
    if (
        not 1 <= int(p.get("limit", "100")) <= 1000
        or not 0 <= int(p.get("offset", "0")) <= 3000
    ):
        raise ValueError("Pagination bound")
    if p.get("granularity", "hourly") not in ("hourly", "daily") or p.get(
        "format", "json"
    ) not in ("json", "csv"):
        raise ValueError("Unsupported output")
    for k in ("start", "end"):
        if k in p:
            utc(p[k])
    if "start" in p and "end" in p and utc(p["start"]) >= utc(p["end"]):
        raise ValueError("Invalid time range")
    return p


def load(store, environment):
    key = environment.get("AIRLAKE_HISTORY_EXPORT_KEY", "")
    expected = environment.get("AIRLAKE_HISTORY_EXPORT_SHA256", "")
    proof_sha = environment.get("AIRLAKE_HISTORY_ACCEPTANCE_SHA256", "")
    if (
        not re.fullmatch(
            r"historical-demo/20261114_[a-f0-9]{8}/outputs/export.json", key
        )
        or not re.fullmatch("[a-f0-9]{64}", expected)
        or not re.fullmatch("[a-f0-9]{64}", proof_sha)
    ):
        raise RuntimeError("Historical acceptance is not configured")
    raw = store.s3.get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)[
        "Body"
    ].read(4000001)
    evidence = store.s3.get_object(
        Bucket=BUCKET,
        Key=key.rsplit("/", 1)[0] + "/acceptance.json",
        ExpectedBucketOwner=ACCOUNT,
    )["Body"].read(100001)
    if (
        len(raw) > 4000000
        or len(evidence) > 100000
        or sha(raw) != expected
        or sha(evidence) != proof_sha
    ):
        raise RuntimeError("Immutable historical hash mismatch")
    value = json.loads(raw)
    proof = json.loads(evidence)
    p = value["provenance"]
    if (
        p["execution"] != "AWS_GLUE"
        or proof.get("export_sha256") != expected
        or proof.get("hourly_all_rows_match") != "PASS"
        or proof.get("daily_all_rows_match") != "PASS"
        or proof.get("snapshots") != p["snapshots"]
    ):
        raise RuntimeError("Independent acceptance missing")
    if not 1 <= len(value["facts"]) <= 2304 or not 1 <= len(value["daily"]) <= 96:
        raise RuntimeError("Export row bounds")
    dims = {r["location_id"]: r for r in value["dimensions"]}
    if set(dims) != set(PILOTS) or len(dims) != len(value["dimensions"]):
        raise RuntimeError("Dimension keys")
    for r in value["facts"]:
        if (
            set(r) != {"location_id", "hour_start", "pm25_avg", "aqi"}
            or r["location_id"] not in dims
            or type(r["aqi"]) is not int
            or not 0 <= r["aqi"] <= 500
        ):
            raise RuntimeError("Invalid historical Gold schema/AQI")
    summarize(value["facts"], p["window_start_utc"], p["window_end_utc"], dims)
    keys = set()
    for r in value["daily"]:
        key = (r["location_id"], r["date_local"])
        if (
            key in keys
            or r["location_id"] not in dims
            or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", r["date_local"])
            or type(r["hours_present"]) is not int
            or not 1 <= r["hours_present"] <= 24
        ):
            raise RuntimeError("Daily key or quality mismatch")
        keys.add(key)
        finite(r["pm25_avg"], 0, float("inf"))
    return value, expected


def handle(event, store, environment=None):
    p = parameters(event)
    value, digest = load(store, os.environ if environment is None else environment)
    prov = value["provenance"]
    begin, end = utc(prov["window_start_utc"]), utc(prov["window_end_utc"])
    start, stop = (
        utc(p["start"]) if "start" in p else begin,
        utc(p["end"]) if "end" in p else end,
    )
    if not begin <= start < stop <= end:
        raise ValueError("Time filters outside accepted historical window")
    loc = int(p.get("station_id", p.get("location_id", "0"))) or None
    sensor = int(p.get("sensor_id", "0")) or None
    if sensor:
        loc = next(i for i, s in PILOTS.items() if s == sensor)
    daily = p.get("granularity", "hourly") == "daily"
    dims = {r["location_id"]: r for r in value["dimensions"]}
    if daily and (
        start.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).hour
        or stop.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).hour
        or start.minute
        or stop.minute
        or start.second
        or stop.second
        or start.microsecond
        or stop.microsecond
    ):
        raise ValueError("Daily filters require Vietnam midnight boundaries")
    rows = []
    for r in value["daily"] if daily else value["facts"]:
        if loc and r["location_id"] != loc:
            continue
        t = utc(r["date_local"] + "T00:00:00+00:00") if daily else utc(r["hour_start"])
        if daily:
            t = t - timedelta(hours=7)
        if start <= t < stop:
            rows.append(
                {
                    **r,
                    "sensor_id": PILOTS[r["location_id"]],
                    "station_name": dims[r["location_id"]]["name"],
                }
            )
    rows.sort(
        key=lambda r: (r["location_id"], r["date_local"] if daily else r["hour_start"])
    )
    limit, offset = int(p.get("limit", "100")), int(p.get("offset", "0"))
    page = rows[offset : offset + limit]
    metadata = {
        "source": "OpenAQ archived genuine measurements",
        "units": "µg/m³",
        "timezone": "Asia/Ho_Chi_Minh for date_local; UTC for hour_start",
        "classification": "historical bounded batch; not a live feed",
        "aqi_method": "simplified hourly MVP; NOT official VN_AQI",
        "export_sha256": digest,
        "provenance": prov,
        "redistribution": "private authorized research only; provider-specific public rights must be verified separately",
    }
    if p.get("format", "json") == "csv":
        fields = ["location_id", "sensor_id", "station_name"] + (
            [
                "date_local",
                "pm25_avg",
                "hours_present",
                "hours_expected",
                "coverage_percent",
                "complete_24_hours",
            ]
            if daily
            else ["hour_start", "pm25_avg", "aqi"]
        )
        stream = io.StringIO()
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in page:
            r = {k: row[k] for k in fields}
            if r["station_name"].lstrip().startswith(("=", "+", "-", "@")):
                r["station_name"] = "'" + r["station_name"]
            writer.writerow(r)
        return reply(
            200,
            stream.getvalue(),
            "text/csv; charset=utf-8",
            {"x-export-sha256": digest, "x-data-classification": "historical-not-live"},
        )
    return reply(
        200,
        {
            "data": page,
            "count": len(page),
            "total": len(rows),
            "next": "/api/v1/history?"
            + urlencode({**p, "offset": offset + limit, "limit": limit})
            if offset + len(page) < len(rows)
            else None,
            "metadata": metadata,
        },
    )
