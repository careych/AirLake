"""Bounded read-only Hanoi source survey; no claims of full city coverage."""

import argparse
import json
from datetime import timedelta
from pathlib import Path

from ingestion.openaq import API, PILOTS, key_from_environment, stamp, utc
from live_data.contract import now_utc


def survey(api, now):
    candidates = api.get(
        "/locations",
        {
            "coordinates": "21.0285,105.8542",
            "radius": 20000,
            "parameters_id": 2,
            "limit": 30,
            "page": 1,
        },
    )["results"]
    byid = {r["id"]: r for r in candidates}
    for loc in PILOTS:
        rows = api.get("/locations/" + str(loc))["results"]
        if len(rows) == 1:
            byid[loc] = rows[0]
    pilots = [byid[k] for k in PILOTS if k in byid]
    others = sorted(
        [r for r in byid.values() if r["id"] not in PILOTS],
        key=lambda r: (r.get("datetimeLast") or {}).get("utc") or "",
        reverse=True,
    )[:5]
    report = {
        "status": "PASS_READ_ONLY_DISCOVERY",
        "checked_at_utc": stamp(now),
        "scope": "First 30 matches within 20km of 21.0285,105.8542; all three pilots plus five most recent candidate locations; not an administrative or city-wide census",
        "stations": [],
    }
    for row in [*pilots, *others]:
        info = {
            "location_id": row["id"],
            "name": row.get("name"),
            "coordinates": row.get("coordinates"),
            "is_mobile": row.get("isMobile"),
            "original_pilot": row["id"] in PILOTS,
            "provider": row.get("provider"),
            "licenses": row.get("licenses"),
            "sensors": [],
        }
        sensors = [
            s
            for s in row.get("sensors", [])
            if s.get("parameter", {}).get("name") == "pm25"
        ][:1]
        for ss in sensors:
            detail = api.get("/sensors/" + str(ss["id"]))["results"][0]
            values = api.get(
                "/sensors/" + str(ss["id"]) + "/measurements",
                {
                    "datetime_from": stamp(now - timedelta(days=7)),
                    "datetime_to": stamp(now),
                    "limit": 2,
                    "page": 1,
                },
            )
            last = (detail.get("datetimeLast") or {}).get("utc")
            periods = [r.get("period", {}) for r in values["results"]]
            cadence = []
            for period in periods:
                try:
                    seconds = (
                        utc(period["datetimeTo"]["utc"])
                        - utc(period["datetimeFrom"]["utc"])
                    ).total_seconds()
                    if seconds > 0:
                        cadence.append(seconds)
                except (KeyError, ValueError, TypeError):
                    pass
            info["sensors"].append(
                {
                    "sensor_id": ss["id"],
                    "units": ss["parameter"].get("units"),
                    "last_observed_at": last,
                    "age_seconds": (now - utc(last)).total_seconds() if last else None,
                    "seven_day_found_as_reported": values.get("meta", {}).get("found"),
                    "seven_day_samples": len(values["results"]),
                    "sample_period_seconds": cadence,
                    "observed_spacing_seconds": sorted(
                        {
                            (
                                utc(b["period"]["datetimeTo"]["utc"])
                                - utc(a["period"]["datetimeTo"]["utc"])
                            ).total_seconds()
                            for a, b in zip(values["results"], values["results"][1:])
                            if utc(b["period"]["datetimeTo"]["utc"])
                            > utc(a["period"]["datetimeTo"]["utc"])
                        }
                    ),
                    "coverage_note": "Two samples prove availability, not seven-day completeness; provider aggregate coverage metadata is not treated as validated uptime",
                }
            )
        report["stations"].append(info)
    return report


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--env-file")
    a.add_argument("--output", type=Path, required=True)
    args = a.parse_args()
    if args.output.exists():
        a.exit(1, "STOP: preserve existing discovery evidence\n")
    try:
        report = survey(API(key_from_environment(args.env_file)), now_utc())
        args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
        print("PASS: bounded read-only source discovery; no AWS calls or writes")
    except Exception as e:  # noqa: BLE001 -- credentials and upstream responses stay private
        print(
            json.dumps(
                {
                    "status": "SOURCE_DISCOVERY_BLOCKED",
                    "exception_type": type(e).__name__,
                }
            )
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
