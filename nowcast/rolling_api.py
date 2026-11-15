"""Public rolling history: hourly/daily PM2.5 per licensed station, with coverage.

Ranges are explicit and bounded: local dates (YYYY-MM-DD, Vietnam midnight,
end exclusive) or whole UTC hours (YYYY-MM-DDTHH:00:00Z); at most 100 days,
never before 2026-09-01 Vietnam time and never in the future. Every response
states what the range actually contains (hours present/expected, valid and
complete days, gaps) so a short or patchy range cannot pass for a full one.
"""

import csv
import io
import json
import re
from datetime import timedelta
from urllib.parse import parse_qs, urlencode

from ingestion.openaq import stamp, utc
from nowcast import history as h
from nowcast import project

MAX_DAYS = 100
KEYS = {"station_id", "start", "end", "granularity", "format", "limit", "offset"}
PLACES = {
    m["station_id"]: {"name": m["place"], "area": m["area"]}
    for m in (project.DRAGON, project.COMMUNITY)
}
BASIS = {
    "openaq_archive": "hourly value from the OpenAQ daily archive for the AirGradient sensor",
    "openaq_hourly": "hourly value from the OpenAQ API (values >= 100 rounded to whole numbers by OpenAQ); replaced by the archive value when it appears",
    "sc_archive": "mean of >= 12 Sensor.Community archive readings in the hour (~24 expected)",
    "sc_sampled": "provisional mean of >= 2 readings sampled by AirLake every 15 min; replaced by the archive mean next day",
}
METHOD = {
    "hour_convention": h.CONVENTION,
    "timezone": "UTC for *_utc; Asia/Ho_Chi_Minh (UTC+7, no DST) for *_local and date_local",
    "daily": f"mean of the hourly values on that Vietnam calendar day; valid_day needs >= {h.VALID_DAY_HOURS}/24 hours, complete_day 24/24",
    "flags": "very_low: < 2 µg/m³; isolated_peak: >= 3x and >= 50 µg/m³ above the median of up to 6 neighbouring hours; provisional: sc_sampled. Flagged values are kept unchanged.",
    "gaps": "missing hours are left missing; no interpolation, no model values",
    "basis": BASIS,
    "sensors": "low-cost optical sensors (AirGradient, SDS011); indicative, not reference-grade, no humidity correction",
}


def station(sid):
    meta = project.DRAGON if sid == h.DRAGON else project.COMMUNITY
    return {
        "station_id": sid,
        **PLACES[sid],
        "provider": meta["provider"],
        "source_name": meta["name"],
        "sensor_type": meta.get(
            "sensor_type", "AirGradient low-cost optical sensor (model not published)"
        ),
        "latitude": meta["latitude"],
        "longitude": meta["longitude"],
        "exact_location": meta["exact_location"],
        "licence": meta["licence"],
        "licence_url": meta["licence_url"],
        "attribution": meta["attribution"],
        "share_alike": meta["share_alike"],
    }


def moment(text, now):
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return h.local_midnight_utc(text)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:00:00Z", text):
        return utc(text.replace("Z", "+00:00"))
    raise ValueError("Use YYYY-MM-DD (Vietnam date) or YYYY-MM-DDTHH:00:00Z")


def params(raw, now):
    if len(raw) > 512:
        raise ValueError("Query too long")
    q = parse_qs(raw, keep_blank_values=True, max_num_fields=8)
    if set(q) - KEYS or any(len(v) != 1 for v in q.values()):
        raise ValueError("Invalid filters")
    p = {k: v[0] for k, v in q.items()}
    if not re.fullmatch(r"\d{1,10}", p.get("station_id", "")):
        raise ValueError("station_id is required")
    sid = int(p["station_id"])
    if sid not in PLACES:
        raise LookupError("Station not available for public download")
    today = (utc(now) + h.LOCAL).date().isoformat()
    end = moment(p["end"], now) if "end" in p else h.local_midnight_utc(today)
    start = moment(p["start"], now) if "start" in p else end - timedelta(days=7)
    limit, offset = p.get("limit", "1000"), p.get("offset", "0")
    if not (limit.isdigit() and offset.isdigit()):
        raise ValueError("Invalid pagination")
    ceiling = utc(now).replace(minute=0, second=0) + h.HOUR
    if (
        not start < end
        or end - start > timedelta(days=MAX_DAYS)
        or start < utc(h.START)
        or end > ceiling + timedelta(days=1)
        or p.get("granularity", "hourly") not in ("hourly", "daily")
        or p.get("format", "json") not in ("json", "csv")
        or not 1 <= int(limit) <= 1000
        or not 0 <= int(offset) <= 3000
    ):
        raise ValueError("Invalid range or option")
    return {
        "station_id": sid,
        "start": start,
        "end": min(end, ceiling),
        "requested_end": end,
        "granularity": p.get("granularity", "hourly"),
        "format": p.get("format", "json"),
        "limit": int(limit),
        "offset": int(offset),
    }


def local(t):
    return (t + h.LOCAL).strftime("%Y-%m-%dT%H:%M:%S+07:00")


def rounded(row):
    return {k: round(v, 2) if isinstance(v, float) else v for k, v in row.items()}


def build(series, p, now):
    rows = h.window(series["rows"], p["start"], p["end"])
    if p["granularity"] == "hourly":
        out = [
            rounded(
                {
                    "hour_start_utc": r["hour_start"],
                    "hour_start_local": local(utc(r["hour_start"])),
                    "pm25": r["pm25"],
                    "n_readings": r["n"],
                    "basis": r["basis"],
                    "flags": r["flags"],
                }
            )
            for r in h.annotate(rows)
        ]
    else:
        out = [rounded(d) for d in h.daily(rows)]
    page = out[p["offset"] : p["offset"] + p["limit"]]
    following = p["offset"] + p["limit"]
    query = {
        "station_id": p["station_id"],
        "start": stamp(p["start"]),
        "end": stamp(p["requested_end"]),
        "granularity": p["granularity"],
        "limit": p["limit"],
        "offset": following,
    }
    return {
        "station": station(p["station_id"]),
        "granularity": p["granularity"],
        "units": "µg/m³",
        "pollutant": "PM2.5",
        "kind": "observed",
        "range": {
            "start_utc": stamp(p["start"]),
            "end_utc": stamp(p["end"]),
            "start_local": local(p["start"]),
            "end_local": local(p["end"]),
            "end_clipped_to_now": p["end"] < p["requested_end"],
        },
        "coverage": h.coverage(series["rows"], p["start"], p["end"]),
        "series_updated_at": series["updated_at"],
        "methodology": METHOD,
        "total_rows": len(out),
        "limit": p["limit"],
        "offset": p["offset"],
        "next": "/v1/history?" + urlencode(query) if following < len(out) else None,
        "rows": page,
    }


def to_csv(value):
    s = value["station"]
    hourly = value["granularity"] == "hourly"
    fields = (
        ["hour_start_utc", "hour_start_local", "pm25", "n_readings", "basis", "flags"]
        if hourly
        else [
            "date_local",
            "pm25",
            "hours",
            "valid_day",
            "complete_day",
            "min_hourly",
            "max_hourly",
        ]
    )
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow(["station_id", *fields, "attribution", "licence"])
    for r in value["rows"]:
        w.writerow(
            [
                s["station_id"],
                *("|".join(r[f]) if f == "flags" else r[f] for f in fields),
                s["attribution"],
                s["licence"],
            ]
        )
    return out.getvalue()


def stations(series_by_station, now):
    out = []
    for sid in PLACES:
        rows = series_by_station[sid]["rows"]
        out.append(
            {
                **station(sid),
                "history": {
                    "first_hour_utc": rows[0]["hour_start"] if rows else None,
                    "last_hour_utc": rows[-1]["hour_start"] if rows else None,
                    "hours": len(rows),
                    "updated_at": series_by_station[sid]["updated_at"],
                },
                "links": {
                    "last_7_days": f"/v1/history?station_id={sid}",
                    "daily_csv": f"/v1/history?station_id={sid}&granularity=daily&format=csv",
                },
            }
        )
    return {
        "stations": out,
        "coverage_note": "Two outdoor low-cost sensors with redistribution licences; they do not describe air quality across Hanoi.",
        "restricted": "Other Hanoi stations seen on OpenAQ list no redistribution licence and are not served here.",
        "generated_at": stamp(now),
    }


def serialize(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False)
