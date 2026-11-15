"""Rolling hourly PM2.5 history per public station, from real observations only.

Each station has one series object of hourly means keyed by the UTC START of
the averaging hour: a row covers [hour_start, hour_start + 1 h). OpenAQ labels
hourly values by the END of the hour (verified 2026-11-15: archive value at
01:00+07 == /hours period 17:00Z-18:00Z); Sensor.Community readings are
instantaneous and are bucketed by the hour they fall in.

Row basis (a better basis replaces a worse one for the same hour):
- openaq_archive Dragon value from the OpenAQ daily archive (one decimal)
- openaq_hourly  Dragon value from OpenAQ /sensors/{id}/hours (values >= 100
                 come back rounded to whole numbers; used until the archive,
                 which lags ~4 days, has the hour)
- sc_archive     mean of >= 12 Sensor.Community archive readings (~24 expected)
- sc_sampled     provisional mean of >= 2 readings AirLake sampled every 15 min;
                 replaced by sc_archive once the daily archive file appears
Missing hours stay missing; nothing is interpolated or taken from the model.
Vietnam has no daylight saving time, so local dates use a fixed UTC+7.
"""

import csv
import io
import json
import math
import statistics
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from ingestion.openaq import stamp, utc
from nowcast import sensor_community
from nowcast.http import UpstreamError

LOCAL = timedelta(hours=7)
HOUR = timedelta(hours=1)
START = "2026-08-31T17:00:00Z"  # 2026-09-01 00:00 Vietnam time
DRAGON, COMMUNITY = 6123215, 99385
DRAGON_SENSOR = 14581375
OPENAQ_HOURS = "https://api.openaq.org/v3/sensors/{}/hours"
RANK = {"sc_sampled": 0, "sc_archive": 1, "openaq_hourly": 1, "openaq_archive": 2}
VALID_DAY_HOURS = 18  # 75 % of a day, as for US EPA daily PM2.5 validity
MIN_SAMPLED = 2
CONVENTION = (
    "hour_start is the UTC start of the averaging hour [hour_start, hour_start+1h)"
)


def key(station):
    return f"nowcast/v1/history/{station}.json"


def local_date(hour_start):
    return (utc(hour_start) + LOCAL).date().isoformat()


def local_midnight_utc(date):
    return utc(f"{date}T00:00:00+00:00") - LOCAL


def value(v):
    if type(v) not in (int, float) or not math.isfinite(v) or not 0 <= v <= 1000:
        raise ValueError("Invalid PM2.5")
    return float(v)


def openaq_hours(body):
    rows = []
    for r in body["results"]:
        period = r["period"]
        if period["interval"] != "01:00:00" or r["parameter"]["name"] != "pm25":
            raise ValueError("Unexpected OpenAQ period")
        start = utc(period["datetimeFrom"]["utc"])
        if utc(period["datetimeTo"]["utc"]) - start != HOUR or start.minute:
            raise ValueError("Unexpected OpenAQ hour")
        n = r["coverage"]["observedCount"]
        if type(n) is int and n >= 1:
            rows.append(
                {
                    "hour_start": stamp(start),
                    "pm25": value(r["value"]),
                    "n": n,
                    "basis": "openaq_hourly",
                }
            )
    return rows


def openaq_archive_hours(text, sensor_id=DRAGON_SENSOR):
    """Rows of an OpenAQ archive CSV; its datetime marks the END of the hour."""
    rows = []
    for r in csv.DictReader(io.StringIO(text)):
        if r["parameter"] != "pm25" or int(r["sensors_id"]) != sensor_id:
            continue
        if r["units"] != "µg/m³":
            raise ValueError("Unexpected unit")
        end = datetime.fromisoformat(r["datetime"]).astimezone(UTC)
        if end.minute or end.second:
            raise ValueError("Unexpected archive timestamp")
        rows.append(
            {
                "hour_start": stamp(end - HOUR),
                "pm25": value(float(r["value"])),
                "n": 1,
                "basis": "openaq_archive",
            }
        )
    return rows


def sc_archive_hours(text, sensor_id=COMMUNITY):
    return [
        {
            "hour_start": r["observed_hour_utc"],
            "pm25": r["pm25"],
            "n": r["readings"],
            "basis": "sc_archive",
        }
        for r in sensor_community.hourly(text, sensor_id)
    ]


def sc_sampled_hours(readings, now):
    """Completed hours with >= 2 distinct readings AirLake actually fetched."""
    buckets = defaultdict(list)
    for r in readings:
        at = utc(r["observed_at"])
        buckets[at.replace(minute=0, second=0)].append(value(r["pm25"]))
    return [
        {
            "hour_start": stamp(h),
            "pm25": math.fsum(v) / len(v),
            "n": len(v),
            "basis": "sc_sampled",
        }
        for h, v in sorted(buckets.items())
        if len(v) >= MIN_SAMPLED and h + HOUR <= utc(now)
    ]


def merge(rows, new):
    out = {r["hour_start"]: r for r in rows}
    for r in new:
        if utc(r["hour_start"]) < utc(START):
            continue
        old = out.get(r["hour_start"])
        if old is None or RANK[r["basis"]] >= RANK[old["basis"]]:
            out[r["hour_start"]] = r
    return [out[k] for k in sorted(out)]


def series(station, rows, now):
    return {
        "schema_version": 1,
        "station_id": station,
        "updated_at": stamp(now),
        "hour_convention": CONVENTION,
        "rows": rows,
    }


def annotate(rows):
    """Flags for uncertain records; values are never altered or removed."""
    out = []
    for i, r in enumerate(rows):
        t = utc(r["hour_start"])
        near = [
            x["pm25"]
            for x in rows[max(0, i - 3) : i + 4]
            if x is not r and abs(utc(x["hour_start"]) - t) <= 3 * HOUR
        ]
        flags = []
        if r["pm25"] < 2:
            flags.append("very_low")
        if len(near) >= 3:
            m = statistics.median(near)
            if r["pm25"] >= 3 * max(m, 1) and r["pm25"] - m >= 50:
                flags.append("isolated_peak")
        if r["basis"] == "sc_sampled":
            flags.append("provisional")
        out.append({**r, "flags": flags})
    return out


def window(rows, start, end):
    return [r for r in rows if start <= utc(r["hour_start"]) < end]


def daily(rows):
    days = defaultdict(list)
    for r in rows:
        days[local_date(r["hour_start"])].append(r["pm25"])
    return [
        {
            "date_local": d,
            "pm25": math.fsum(v) / len(v),
            "hours": len(v),
            "valid_day": len(v) >= VALID_DAY_HOURS,
            "complete_day": len(v) == 24,
            "min_hourly": min(v),
            "max_hourly": max(v),
        }
        for d, v in sorted(days.items())
    ]


def coverage(rows, start, end):
    """What the range actually contains: hours, local days, gaps."""
    expected = int((end - start) / HOUR)
    present = {r["hour_start"] for r in window(rows, start, end)}
    gaps, run = [], None
    for i in range(expected):
        h = stamp(start + i * HOUR)
        if h not in present:
            run = run or [h, 0]
            run[1] += 1
        elif run:
            gaps.append(run)
            run = None
    if run:
        gaps.append(run)
    per_day = daily(window(rows, start, end))
    full_days = []  # local calendar days lying entirely inside the range
    d = (start + LOCAL).date()
    if local_midnight_utc(d.isoformat()) < start:
        d += timedelta(days=1)
    while local_midnight_utc((d + timedelta(days=1)).isoformat()) <= end:
        full_days.append(d.isoformat())
        d += timedelta(days=1)
    by_date = {x["date_local"]: x for x in per_day}
    return {
        "hours_expected": expected,
        "hours_present": len(present),
        "percent": round(100 * len(present) / expected, 1) if expected else 0.0,
        "days_in_range": len(full_days),
        "valid_days": sum(1 for x in full_days if by_date.get(x, {}).get("valid_day")),
        "complete_days": sum(
            1 for x in full_days if by_date.get(x, {}).get("complete_day")
        ),
        "days_without_data": sum(1 for x in full_days if x not in by_date),
        "gaps": [
            {
                "start": g[0],
                "end": stamp(utc(g[0]) + g[1] * HOUR),
                "hours": g[1],
            }
            for g in sorted(gaps, key=lambda g: -g[1])[:10]
        ],
        "gap_count": len(gaps),
        "first_hour": min(present) if present else None,
        "last_hour": max(present) if present else None,
    }


# ---- scheduled update (refresh Lambda) ----------------------------------
def update(store, http, openaq_key, projection, now):
    """Extend each existing series; a failing source never blocks the projection."""
    now = utc(now)
    out = {}
    for station in (DRAGON, COMMUNITY):
        raw, etag = store.get(key(station))
        if raw is None:
            out[station] = "no_series"  # created only by the reviewed backfill
            continue
        current = json.loads(raw)
        rows, new, errors = current["rows"], [], []
        if station == DRAGON:
            latest = utc(rows[-1]["hour_start"]) if rows else utc(START)
            stale = now - utc(current["updated_at"]) >= timedelta(hours=6)
            if latest + 2 * HOUR <= now or stale:
                try:
                    new += openaq_hours(
                        http(
                            OPENAQ_HOURS.format(DRAGON_SENSOR),
                            params={
                                "datetime_from": stamp(
                                    now.replace(minute=0, second=0)
                                    - timedelta(hours=72)
                                ),
                                "limit": 100,
                            },
                            headers={"X-API-Key": openaq_key},
                        )
                    )
                except (UpstreamError, ValueError, KeyError, TypeError) as e:
                    errors.append(getattr(e, "reason", "invalid_response"))
        else:
            new += sc_sampled_hours(
                (projection.get("recent") or {}).get(str(COMMUNITY), []), now
            )
            for back in (1, 2):
                day = (now - timedelta(days=back)).date().isoformat()
                archived = any(
                    r["basis"] == "sc_archive" and r["hour_start"].startswith(day)
                    for r in rows
                )
                if archived or now.minute >= 15:  # at most one try per hour
                    continue
                try:
                    new += sc_archive_hours(
                        http(
                            sensor_community.ARCHIVE.format(day=day, id=COMMUNITY),
                            text=True,
                            timeout=20,
                        )
                    )
                except (UpstreamError, ValueError, KeyError) as e:
                    errors.append(getattr(e, "reason", "invalid_response"))
        merged = merge(rows, new)
        if merged == rows:
            out[station] = "unchanged" if not errors else "source_error"
            continue
        body = json.dumps(
            series(station, merged, now), sort_keys=True, allow_nan=False
        ).encode()
        if now.minute < 15:  # one audit copy per hour; S3 versioning keeps the rest
            store.put_new(
                f"nowcast/v1/history/versions/{station}/{stamp(now).replace(':', '')}.json",
                body,
            )
        store.put(key(station), body, {"If-Match": etag})
        out[station] = "updated" if not errors else "updated_partial"
    return out
