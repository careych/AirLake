"""A field allowlist separates public licensed data from restricted demo data."""

import csv
import io
import json
import math
from datetime import UTC, datetime, timedelta

PILOTS = {4946812, 4946813, 6123215}
LICENSED = {6123215}


def utc(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("An explicit UTC instant is required")
    return result


def project(export, inventory, coverage, mode, source_counts):
    if mode not in ("public", "instructor"):
        raise ValueError("Unknown exposure mode")
    license_info = coverage["license_41"]
    if (
        not license_info
        or license_info.get("id") != 41
        or license_info.get("redistributionAllowed") is not True
        or license_info.get("modificationAllowed") is not True
        or license_info.get("sourceUrl")
        != "https://creativecommons.org/licenses/by/4.0/"
    ):
        raise ValueError("Public license proof is incomplete")
    selected = LICENSED if mode == "public" else PILOTS
    metadata = {r["id"]: r for r in coverage["locations"]}
    dims = {r["location_id"]: r for r in export["dimensions"]}
    if len(dims) != len(export["dimensions"]) or set(dims) != PILOTS:
        raise ValueError("Unexpected station grain")
    stations = []
    for loc in sorted(selected):
        meta = metadata[loc]
        if meta.get("isMobile") is not False:
            raise ValueError("Unverified station")
        licenses = meta.get("licenses") or []
        public = loc in LICENSED and any(r.get("id") == 41 for r in licenses)
        if public:
            grant = next(r for r in licenses if r.get("id") == 41)
            if not grant.get("dateFrom") or not (grant.get("attribution") or {}).get(
                "name"
            ):
                raise ValueError("License period and attribution required")
            for fact in export["facts"]:
                if fact["location_id"] == loc and (
                    fact["hour_start"][:10] < grant["dateFrom"]
                    or (
                        grant.get("dateTo")
                        and fact["hour_start"][:10] > grant["dateTo"]
                    )
                ):
                    raise ValueError("Observation outside verified license period")
        if mode == "public" and not public:
            raise ValueError("Station redistribution not approved")
        coords = meta["coordinates"]
        if not all(
            type(coords.get(k)) in (int, float)
            and math.isfinite(coords[k])
            and low <= coords[k] <= high
            for k, low, high in (("latitude", -90, 90), ("longitude", -180, 180))
        ):
            raise ValueError("Unverified coordinates")
        latest = inventory["latest"].get(str(loc), {})
        observation = None
        if latest.get("observed_at"):
            utc(latest["observed_at"])
            utc(latest["ingested_at"])
            if public and (
                latest["observed_at"][:10] < grant["dateFrom"]
                or (
                    grant.get("dateTo") and latest["observed_at"][:10] > grant["dateTo"]
                )
            ):
                raise ValueError("Latest observation outside verified license period")
            value = latest["pm25"]
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or value < 0
                or latest["units"] != "µg/m³"
            ):
                raise ValueError("Invalid latest reading")
            observation = {
                k: latest[k] for k in ("observed_at", "ingested_at", "pm25", "units")
            }
            observation["cadence_seconds"] = latest.get("cadence_seconds")
        attribution = (
            next((r for r in licenses if r.get("id") == 41), {}).get("attribution")
            or {}
        ).get("name")
        stations.append(
            {
                "id": loc,
                "name": dims[loc]["name"].strip(),
                "coordinates": coords,
                "provider": (meta.get("provider") or {}).get("name", "OpenAQ"),
                "attribution": attribution,
                "license": "CC BY 4.0" if public else None,
                "license_url": license_info["sourceUrl"] if public else None,
                "downloads_allowed": public,
                "latest": observation,
                "accepted_measurements": source_counts[str(loc)],
            }
        )
    hourly = []
    keys = set()
    for row in export["facts"]:
        loc = row["location_id"]
        if loc not in selected:
            continue
        hour = utc(row["hour_start"])
        key = (loc, hour)
        if key in keys or hour.minute or hour.second or hour.microsecond:
            raise ValueError("Invalid station/hour key")
        keys.add(key)
        if (
            type(row["pm25_avg"]) not in (int, float)
            or not math.isfinite(row["pm25_avg"])
            or row["pm25_avg"] < 0
            or type(row["aqi"]) is not int
            or not 0 <= row["aqi"] <= 500
        ):
            raise ValueError("Invalid hourly reading")
        hourly.append(
            {
                "station_id": loc,
                "observed_hour_utc": row["hour_start"],
                "pm25": row["pm25_avg"],
                "aqi_mvp": row["aqi"],
            }
        )
    if not hourly:
        raise ValueError("No verified observations")
    end = max(utc(r["observed_hour_utc"]) for r in hourly) + timedelta(hours=1)
    operations = inventory.get("operations_status")
    if operations is not None:
        if set(operations) != {"verified_at", "collection_enabled", "refresh_enabled"}:
            raise ValueError("Unexpected operational metadata")
        utc(operations["verified_at"])
        if any(
            type(operations[k]) is not bool
            for k in ("collection_enabled", "refresh_enabled")
        ):
            raise ValueError("Operational status must be explicit")
    return {
        "schema_version": 1,
        "operations_status": operations,
        "mode": mode,
        "published_at": datetime.now(UTC).isoformat(),
        "computed_at": export["provenance"]["computed_at"],
        "window_end_utc": end.isoformat(),
        "window_start_utc": (end - timedelta(hours=24)).isoformat(),
        "stations": stations,
        "hourly": sorted(
            hourly, key=lambda r: (r["station_id"], r["observed_hour_utc"])
        ),
        "source": "OpenAQ",
        "units": "µg/m³",
        "timezone": "UTC",
        "aqi_method": "simplified_mvp_not_official_vn_aqi",
        "automatic_refresh": False,
        "seven_day_completeness_verified": False,
        "telegram": {
            "public_subscriptions_verified": False,
            "delivery_verified_in_this_release": False,
        },
    }


def download(data, station_id, start=None, end=None, fmt="json"):
    station = next((r for r in data["stations"] if r["id"] == station_id), None)
    if not station or station.get("downloads_allowed") is not True:
        raise ValueError("Redistribution is not cleared")
    start = utc(start) if start else utc(data["window_start_utc"])
    end = utc(end) if end else utc(data["window_end_utc"])
    if start >= end:
        raise ValueError("Invalid time window")
    rows = [
        r
        for r in data["hourly"]
        if r["station_id"] == station_id and start <= utc(r["observed_hour_utc"]) < end
    ]
    if fmt == "json":
        return json.dumps(
            {
                "data": rows,
                "units": data["units"],
                "timezone": "UTC",
                "source": f"OpenAQ / {station['provider']} / {station['attribution'] or ''}",
                "license": station["license_url"],
                "aqi_note": "Chỉ số thử nghiệm, không phải VN_AQI chính thức.",
            },
            ensure_ascii=False,
            allow_nan=False,
        )
    if fmt != "csv":
        raise ValueError("Unsupported format")
    stream = io.StringIO()
    writer = csv.DictWriter(
        stream,
        fieldnames=[
            "station_id",
            "observed_hour_utc",
            "pm25",
            "aqi_mvp",
            "units",
            "source",
            "license",
        ],
    )
    writer.writeheader()
    writer.writerows(
        {
            **r,
            "units": data["units"],
            "source": f"OpenAQ / {station['provider']} / {station['attribution'] or ''}",
            "license": station["license_url"],
        }
        for r in rows
    )
    return stream.getvalue()
