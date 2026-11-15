"""Fetch the three live layers once and write the "now" projection (local preview).

Bounded: one OpenAQ, one Sensor.Community and two Open-Meteo requests.
"""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import requests

from ingestion.openaq import key_from_environment
from nowcast import openmeteo, project, sensor_community

UA = {"User-Agent": "AirLake Hanoi air-quality pilot (university project)"}


def getter(headers=None):
    http = requests.Session()
    http.trust_env = False

    def get(url, params=None):
        r = http.get(url, params=params, headers={**UA, **(headers or {})}, timeout=30)
        r.raise_for_status()
        return r.json()

    return get


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--out", type=Path, required=True)
    a.add_argument("--validation", type=Path, required=True)
    a.add_argument("--env-file", type=Path)
    x = a.parse_args()
    now = datetime.now(UTC).replace(microsecond=0)
    plain = getter()
    openaq = getter({"X-API-Key": key_from_environment(x.env_file)})
    value = project.build(
        now,
        project.openaq_latest(
            openaq("https://api.openaq.org/v3/locations/6123215/latest")
        ),
        sensor_community.fetch_latest(plain),
        openmeteo.fetch(plain, now),
        json.loads(x.validation.read_text()),
    )
    x.out.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)
    )
    print(
        json.dumps(
            {
                "observations": [
                    (o["station_id"], o["observed_at"]) for o in value["observations"]
                ],
                "model_run_at": value["model"]["model_run_at"],
            }
        )
    )


if __name__ == "__main__":
    main()
