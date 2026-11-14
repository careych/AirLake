"""Render a manually fetched latest API response; no polling or network access."""

import argparse
import html
import json
from pathlib import Path


def render(value):
    escape = lambda v: html.escape(str(v))
    rows = []
    for r in value["data"]:
        f = r["freshness"]
        status = f["status"]
        # Missing data is visibly unknown, never a green health classification.
        label = {
            "no_data": "NO DATA",
            "stale": "STALE",
            "recent": "RECENT OBSERVATION (not a health assessment)",
            "invalid_future": "INVALID TIME",
        }.get(status, "UNKNOWN")
        rows.append(
            "<tr>"
            + "".join(
                "<td>" + escape(v) + "</td>"
                for v in [
                    r.get("station", r["location_id"]),
                    r.get("pm25", "—"),
                    r.get("units", "µg/m³"),
                    r.get("observed_at", "—"),
                    label,
                    r["poll"]["status"],
                ]
            )
            + "</tr>"
        )
    return (
        '<!doctype html><meta charset="utf-8"><title>AirLake latest readings</title><style>body{font:16px system-ui;margin:2rem}table{border-collapse:collapse}td,th{border:1px solid #aaa;padding:.6rem}</style><h1>Latest accepted PM2.5 readings</h1><p>Manual snapshot generated '
        + escape(value["generated_at"])
        + " UTC. No automatic refresh. Polling every five minutes does not guarantee five-minute source freshness. No instantaneous AQI.</p><table><tr><th>Station</th><th>PM2.5</th><th>Unit</th><th>Observed at UTC</th><th>Freshness</th><th>Last poll</th></tr>"
        + "".join(rows)
        + "</table><p>Historical hourly trends remain a separate Superset dashboard. Missing observations are unknown, not healthy. Source: OpenAQ v3; private demonstration only.</p>"
    )


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--input", type=Path, required=True)
    a.add_argument("--output", type=Path, required=True)
    args = a.parse_args()
    if args.output.exists():
        a.exit(1, "STOP: output exists\n")
    value = json.loads(args.input.read_text())
    args.output.write_text(render(value))
    print(
        "PASS: rendered supplied API snapshot; no network calls; provenance remains in input JSON"
    )


if __name__ == "__main__":
    main()
