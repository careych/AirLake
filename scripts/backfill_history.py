"""Build the initial rolling history series from licensed upstream sources.

Read-only upstream calls, written to a private packet outside Git:
- Dragon: OpenAQ daily archive files (public S3 bucket openaq-data-archive,
  synced beforehand with `aws s3 sync --no-sign-request` into raw/openaq/),
  plus OpenAQ /v3/sensors/14581375/hours for the days the archive lacks
- Sensor.Community #99385: one daily archive CSV per UTC day (cached; a
  re-run only fetches days not yet cached; missing days stay missing)
The release uploads series-<id>.json only after review of manifest.json.
The OpenAQ key is read from OPENAQ_KEY and never printed.
"""

import argparse
import gzip
import hashlib
import json
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ingestion.openaq import stamp, utc
from nowcast import history, sensor_community
from nowcast.http import UpstreamError, get


def dragon(key, end):
    rows, page, calls = [], 1, 0
    while True:
        body = get(
            history.OPENAQ_HOURS.format(history.DRAGON_SENSOR),
            params={
                "datetime_from": history.START,
                "datetime_to": stamp(end),
                "limit": 1000,
                "page": page,
            },
            headers={"X-API-Key": key},
            timeout=30,
        )
        calls += 1
        rows += history.openaq_hours(body)
        if len(body["results"]) < 1000 or page >= 5:
            return rows, calls
        page += 1
        time.sleep(1)


def archive(raw):
    rows = []
    for path in sorted(raw.rglob("location-6123215-*.csv.gz")):
        rows += history.openaq_archive_hours(
            gzip.decompress(path.read_bytes()).decode()
        )
    return rows


def community(raw, end):
    raw.mkdir(parents=True, exist_ok=True)
    rows, calls, missing = [], 0, []
    day = utc(history.START).date()
    while day < end.date():
        path = raw / f"{day}.csv"
        if not path.exists():
            try:
                text = get(
                    sensor_community.ARCHIVE.format(day=day, id=history.COMMUNITY),
                    text=True,
                    timeout=40,
                )
                path.write_text(text)
            except UpstreamError as e:
                missing.append({"day": day.isoformat(), "reason": e.reason})
            calls += 1
            time.sleep(1)
        if path.exists():
            rows += history.sc_archive_hours(path.read_text())
        day += timedelta(days=1)
    return rows, calls, missing


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--packet", type=Path, required=True)
    a.add_argument("--end", help="UTC whole hour; default: current hour")
    x = a.parse_args()
    end = (
        utc(x.end.replace("Z", "+00:00"))
        if x.end
        else datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    )
    packet = x.packet
    if any(
        (p / ".git").exists() for p in (packet.resolve(), *packet.resolve().parents)
    ):
        raise SystemExit("Packet must be outside Git")
    packet.mkdir(mode=0o700, parents=True, exist_ok=True)
    key = os.environ.get("OPENAQ_KEY", "").strip()
    if not key:
        raise SystemExit("OPENAQ_KEY is not set")
    d_rows, d_calls = dragon(key, end)
    d_rows += archive(packet / "raw" / "openaq")  # merge prefers archive values
    c_rows, c_calls, missing = community(packet / "raw" / "sc", end)
    manifest = {
        "built_at": stamp(datetime.now(UTC).replace(microsecond=0)),
        "range_start": history.START,
        "range_end": stamp(end),
        "requests": {"openaq_hours": d_calls, "sensor_community_archive": c_calls},
        "sc_missing_days": missing,
        "series": {},
    }
    for sid, rows in ((history.DRAGON, d_rows), (history.COMMUNITY, c_rows)):
        rows = [r for r in history.merge([], rows) if utc(r["hour_start"]) < end]
        body = json.dumps(
            history.series(sid, rows, end), sort_keys=True, allow_nan=False
        ).encode()
        (packet / f"series-{sid}.json").write_bytes(body)
        manifest["series"][str(sid)] = {
            "hours": len(rows),
            "first_hour": rows[0]["hour_start"] if rows else None,
            "last_hour": rows[-1]["hour_start"] if rows else None,
            "sha256": hashlib.sha256(body).hexdigest(),
            "bytes": len(body),
            "coverage": history.coverage(rows, utc(history.START), end),
        }
    (packet / "manifest.json").write_text(json.dumps(manifest, indent=1))
    print(
        json.dumps(
            {
                k: {kk: v[kk] for kk in ("hours", "first_hour", "last_hour", "sha256")}
                for k, v in manifest["series"].items()
            }
            | {"requests": manifest["requests"], "missing": len(missing)}
        )
    )


if __name__ == "__main__":
    main()
