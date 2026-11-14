"""Bounded versioned current-hourly export; promotion follows Athena acceptance."""

import csv
import io
import json
from urllib.parse import parse_qs

from final_demo.lambda_api import reply
from ingestion.openaq import PILOTS, finite, utc
from live_data.contract import ACCOUNT, BUCKET, DATABASE, PREFIX, sha


def load(store):
    pointer = store.read("EXPORT", "CURRENT")
    key = pointer.get("key", "")
    if not key.startswith(PREFIX + "exports/") or not key.endswith("/gold.json"):
        raise RuntimeError("No independently verified current export")
    raw = store.s3.get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)[
        "Body"
    ].read(500001)
    if len(raw) > 500000 or sha(raw) != pointer.get("sha256"):
        raise RuntimeError("Export hash mismatch")
    value = json.loads(raw)
    from live_data.refresh import allowed_database, validate_export

    database = value["provenance"]["database"]
    if not allowed_database(database) or not pointer.get("athena_query_id"):
        raise RuntimeError("Export is not independently accepted")
    if database != DATABASE:
        digest = value["provenance"].get("input_manifest_sha256", "")
        validate_export(raw, digest)
        if (
            key != PREFIX + "exports/" + digest + "/gold.json"
            or pointer.get("database") != database
            or pointer.get("snapshots") != value["provenance"]["snapshots"]
        ):
            raise RuntimeError("Pointer version/snapshots mismatch")
    dimensions = {r["location_id"]: r for r in value["dimensions"]}
    if len(dimensions) != len(value["dimensions"]) or set(dimensions) - set(PILOTS):
        raise RuntimeError("Invalid dimension keys")
    rows = []
    keys = set()
    if not 1 <= len(value["facts"]) <= 1000:
        raise RuntimeError("Export row bound")
    for r in value["facts"]:
        if (
            set(r) != {"location_id", "hour_start", "pm25_avg", "aqi"}
            or r["location_id"] not in dimensions
        ):
            raise RuntimeError("Invalid Gold schema/join")
        hour = utc(r["hour_start"])
        key = (r["location_id"], hour)
        if hour.minute or hour.second or hour.microsecond or key in keys:
            raise RuntimeError("Invalid UTC grain")
        keys.add(key)
        finite(r["pm25_avg"], 0, float("inf"))
        if type(r["aqi"]) is not int or not 0 <= r["aqi"] <= 500:
            raise RuntimeError("Invalid AQI")
        rows.append({**r, "station_name": dimensions[r["location_id"]]["name"]})
    return (
        sorted(rows, key=lambda r: (r["location_id"], r["hour_start"])),
        value["provenance"],
        pointer,
    )


def handle(event, store):
    raw_query = event.get("rawQueryString", "")
    if len(raw_query) > 4096:
        raise ValueError("Query too long")
    params = parse_qs(raw_query, keep_blank_values=True, max_num_fields=6)
    if set(params) - {
        "location_id",
        "start",
        "end",
        "limit",
        "offset",
        "format",
    } or any(len(v) != 1 for v in params.values()):
        raise ValueError("Invalid parameters")
    p = {k: v[0] for k, v in params.items()}
    for k in ("location_id", "limit", "offset"):
        if k in p and (not p[k].isdecimal() or len(p[k]) > 10):
            raise ValueError("Invalid integer")
    loc = int(p["location_id"]) if "location_id" in p else None
    if loc is not None and loc not in PILOTS:
        raise ValueError("Unknown station")
    limit = int(p.get("limit", "100"))
    offset = int(p.get("offset", "0"))
    if not 1 <= limit <= 100 or not 0 <= offset <= 1000:
        raise ValueError("Pagination bounds")
    start = utc(p["start"]) if "start" in p else None
    end = utc(p["end"]) if "end" in p else None
    if start and end and start >= end:
        raise ValueError("Time bounds")
    fmt = p.get("format", "json")
    if fmt not in ("json", "csv"):
        raise ValueError("Format")
    rows, provenance, pointer = load(store)
    selected = [
        r
        for r in rows
        if (loc is None or r["location_id"] == loc)
        and (start is None or utc(r["hour_start"]) >= start)
        and (end is None or utc(r["hour_start"]) < end)
    ]
    page = selected[offset : offset + limit]
    if fmt == "csv":
        stream = io.StringIO()
        w = csv.DictWriter(
            stream,
            fieldnames=["location_id", "station_name", "hour_start", "pm25_avg", "aqi"],
        )
        w.writeheader()
        for row in page:
            row = dict(row)
            if row["station_name"].lstrip().startswith(("=", "+", "-", "@")):
                row["station_name"] = "'" + row["station_name"]
            w.writerow(row)
        return reply(
            200,
            stream.getvalue(),
            "text/csv; charset=utf-8",
            {
                "x-data-classification": "bounded-observations-not-continuous",
                "x-aqi-method": "simplified-hourly-MVP-not-VN_AQI",
            },
        )
    return reply(
        200,
        {
            "data": page,
            "count": len(page),
            "total": len(selected),
            "next_offset": offset + limit if offset + limit < len(selected) else None,
            "metadata": {
                "provenance": provenance,
                "athena_query_id": pointer["athena_query_id"],
                "export_sha256": pointer["sha256"],
                "previous_verified_version_available": bool(pointer.get("previous")),
                "pm25_units": "µg/m³",
                "timezone": "UTC",
                "refresh": "versioned export promoted only after independent Athena verification; not continuous",
                "last_observed_hour": max(r["hour_start"] for r in rows),
            },
        },
    )
