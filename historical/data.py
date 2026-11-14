"""Read existing archive files without mutation; never fabricates missing observations."""

import csv
import gzip
import hashlib
import json
import math
import re
import statistics
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from zoneinfo import ZoneInfo

from ingestion.openaq import FIELDS, PILOTS, finite, stamp, utc

VIETNAM = ZoneInfo("Asia/Ho_Chi_Minh")


def scan(root, start, end, locations=tuple(PILOTS), station_metadata=None):
    root = Path(root).resolve()
    start, end = utc(start), utc(end)
    if not 0 < (end - start).total_seconds() <= 32 * 86400 or not set(locations) <= set(
        PILOTS
    ):
        raise ValueError("Bounded supported stations and at most 32 days required")
    lower = (start - timedelta(days=1)).strftime("%Y%m%d")
    upper = (end + timedelta(days=1)).strftime("%Y%m%d")
    paths = []
    for p in root.rglob("*.csv.gz"):
        m = re.fullmatch(r"location-(\d+)-(\d{8})\.csv\.gz", p.name)
        if m and int(m[1]) in locations and lower <= m[2] <= upper:
            paths.append(p)
    if not paths or len(paths) > 110:
        raise ValueError("Missing or excessive archive file set")
    accepted = {}
    rejected = Counter()
    duplicates = Counter()
    total = Counter()
    metadata = defaultdict(set)
    files = []
    row_count = 0
    coordinate_normalizations = Counter()
    verified = {r["id"]: r for r in station_metadata or []}
    for path in sorted(paths):
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            raise ValueError("Source escaped archive root")
        raw = path.read_bytes()
        files.append(
            {
                "path": str(path.relative_to(root)),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "compressed_bytes": len(raw),
            }
        )
        with gzip.open(path, "rt", newline="") as stream:
            reader = csv.DictReader(stream)
            if len(reader.fieldnames or []) != len(FIELDS) or set(
                reader.fieldnames
            ) != set(FIELDS):
                raise ValueError("Unexpected archive CSV schema")
            for line, r in enumerate(reader, 2):
                row_count += 1
                if row_count > 1000000:
                    raise ValueError("Archive scan row budget exceeded")
                if r["parameter"] != "pm25":
                    continue
                try:
                    loc, sensor = int(r["location_id"]), int(r["sensors_id"])
                    if loc not in locations or PILOTS.get(loc) != sensor:
                        raise ValueError("mapping")
                    parsed = datetime.fromisoformat(r["datetime"])
                    if parsed.tzinfo is None:
                        raise ValueError("timezone")
                    measured = utc(parsed.astimezone(UTC))
                    if not start <= measured < end:
                        continue
                    total[loc] += 1
                    if r["units"] != "µg/m³":
                        raise ValueError("units")
                    value = float(r["value"])
                    lat = float(r["lat"])
                    lon = float(r["lon"])
                    finite(value, 0, float("inf"))
                    finite(lat, -90, 90)
                    finite(lon, -180, 180)
                    if not r["location"].strip():
                        raise ValueError("metadata")
                except (ValueError, TypeError, KeyError, OverflowError) as error:
                    rejected[
                        type(error).__name__ + ":" + str(error)
                        if str(error) in ("mapping", "units", "metadata")
                        else "invalid_numeric_or_time"
                    ] += 1
                    continue
                original_coordinates = None
                if verified:
                    expected = verified[loc]["coordinates"]
                    if (
                        abs(lat - expected["latitude"]) > 1e-10
                        or abs(lon - expected["longitude"]) > 1e-10
                    ):
                        raise ValueError(
                            "Historical coordinate differs from verified station; review required"
                        )
                    if lat != expected["latitude"] or lon != expected["longitude"]:
                        original_coordinates = {"lat": r["lat"], "lon": r["lon"]}
                        r = {
                            **r,
                            "lat": str(expected["latitude"]),
                            "lon": str(expected["longitude"]),
                        }
                        lat, lon = expected["latitude"], expected["longitude"]
                        coordinate_normalizations[loc] += 1
                key = (loc, sensor, stamp(measured))
                canonical = (value, r["units"], r["location"].strip(), lat, lon)
                if key in accepted:
                    if accepted[key]["canonical"] != canonical:
                        raise ValueError(
                            "Conflicting duplicate source identity; do not select an arbitrary value"
                        )
                    duplicates[loc] += 1
                    continue
                metadata[loc].add((r["location"].strip(), lat, lon))
                accepted[key] = {
                    "record": {k: r[k] for k in FIELDS},
                    "canonical": canonical,
                    "source": {
                        "archive_file": str(path.relative_to(root)),
                        "line": line,
                        "source_datetime": r["datetime"],
                        "coordinate_serialization_original": original_coordinates,
                    },
                }
    expected_hours = int((end - start).total_seconds() / 3600)
    stations = {}
    hourly_groups = defaultdict(list)
    for (loc, _, t), v in accepted.items():
        hourly_groups[(loc, utc(t).replace(minute=0, second=0, microsecond=0))].append(
            v["canonical"][0]
        )
    hourly = [
        {
            "location_id": loc,
            "hour_start": stamp(hour),
            "pm25_avg": math.fsum(values) / len(values),
            "source_count": len(values),
        }
        for (loc, hour), values in sorted(hourly_groups.items())
    ]
    for loc in locations:
        if len(metadata[loc]) != 1:
            raise ValueError("Historical Bronze metadata is missing or conflicting")
        rows = [(utc(k[2]), v) for k, v in accepted.items() if k[0] == loc]
        rows.sort(key=lambda x: x[0])
        times = [r[0] for r in rows]
        hours = {t.replace(minute=0, second=0, microsecond=0) for t in times}
        missing = [
            stamp(start + timedelta(hours=i))
            for i in range(expected_hours)
            if start + timedelta(hours=i) not in hours
        ]
        gaps = [int((b - a).total_seconds()) for a, b in pairwise(times) if b > a]
        stations[str(loc)] = {
            "sensor_id": PILOTS[loc],
            "actual_records": len(rows),
            "raw_candidates": total[loc],
            "duplicate_identities": duplicates[loc],
            "earliest_observed_at": stamp(times[0]) if times else None,
            "latest_observed_at": stamp(times[-1]) if times else None,
            "actual_days": len(
                {t.astimezone(VIETNAM).date().isoformat() for t in times}
            ),
            "hourly_keys": len(hours),
            "expected_hours": expected_hours,
            "hourly_coverage_percent": 100 * len(hours) / expected_hours,
            "missing_hour_count": len(missing),
            "missing_hours_utc": missing,
            "estimated_cadence_seconds": statistics.median(gaps) if gaps else None,
            "finite_values_outside_mvp_breakpoints": sum(
                r[1]["canonical"][0] >= 500 for r in rows
            ),
            "metadata": [
                {"name": n, "latitude": lat, "longitude": lon}
                for n, lat, lon in sorted(metadata[loc])
            ],
            "coordinate_serialization_normalized": coordinate_normalizations[loc],
            "quality_flags": "archive does not contain original API flagInfo; no zero-flag claim",
        }
    if not accepted:
        raise ValueError("No real compatible PM2.5 data")
    return {
        "source": "OpenAQ archived CSV.gz",
        "window_start_utc": stamp(start),
        "window_end_utc": stamp(end),
        "timezone": "Asia/Ho_Chi_Minh",
        "scanned_files": files,
        "scanned_csv_rows_all_parameters": row_count,
        "accepted_unique_records": len(accepted),
        "rejected": dict(rejected),
        "stations": stations,
        "hourly_reference": hourly,
    }, [
        {"record": v["record"], "source": v["source"]}
        for _, v in sorted(accepted.items())
    ]


def save_scan(output, result, records):
    output = Path(output)
    if output.exists() or any(
        (p / ".git").exists() for p in (output.resolve(), *output.resolve().parents)
    ):
        raise ValueError("New private output outside Git required")
    output.mkdir(mode=0o700, parents=True)
    body = b"".join(
        json.dumps(r, ensure_ascii=False, sort_keys=True, allow_nan=False).encode()
        + b"\n"
        for r in records
    )
    (output / "records.jsonl").write_bytes(body)
    result = {**result, "records_sha256": hashlib.sha256(body).hexdigest()}
    (output / "audit.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
    for p in output.iterdir():
        p.chmod(0o600)
    return result
