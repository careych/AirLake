"""Scheduled refresh: fetch -> validate -> versioned S3 -> current projection.

Observations and model output are stored as separate datasets. The CAMS
forecast is downloaded only when Open-Meteo reports a new model run.
"""

import json

from ingestion.openaq import stamp, utc
from live_data.contract import ACCOUNT, BUCKET
from nowcast import openmeteo, project, sensor_community
from nowcast.public_api import CURRENT_KEY

PREFIX = "nowcast/v1/"


def put(s3, key, body, immutable=True):
    extra = {"IfNoneMatch": "*"} if immutable else {}
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body,
        ExpectedBucketOwner=ACCOUNT,
        ServerSideEncryption="AES256",
        ContentType="application/json",
        **extra,
    )


def get(s3, key):
    try:
        return s3.get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)[
            "Body"
        ].read(4_000_001)
    except s3.exceptions.NoSuchKey:
        return None


def run(s3, http, openaq_key, validation, now):
    now = utc(now)
    meta = http(openmeteo.META)
    run_key = (
        PREFIX
        + "model/cams_global/"
        + str(meta["last_run_initialisation_time"])
        + ".json"
    )
    raw = get(s3, run_key)
    if raw is None:
        raw = json.dumps(http(openmeteo.API, openmeteo.PARAMS), sort_keys=True).encode()
        put(s3, run_key, raw)
    model = openmeteo.normalize(meta, json.loads(raw), now)
    dragon = project.openaq_latest(
        http(
            "https://api.openaq.org/v3/locations/6123215/latest",
            headers={"X-API-Key": openaq_key},
        )
    )
    community = sensor_community.fetch_latest(http)
    stamp_key = stamp(now).replace(":", "")
    for name, reading in (
        ("openaq-6123215", dragon),
        ("sensor-community-99385", community),
    ):
        if not reading:
            continue
        at = reading["observed_at"]
        key = f"{PREFIX}observations/{name}/{at[:10]}/{at.replace(':', '')}.json"
        if get(s3, key) is None:  # one immutable object per source observation
            put(s3, key, json.dumps(reading).encode())
    value = project.build(now, dragon, community, model, validation)
    body = json.dumps(
        value, ensure_ascii=False, sort_keys=True, allow_nan=False
    ).encode()
    put(s3, f"{PREFIX}projections/{stamp_key}.json", body)
    put(s3, CURRENT_KEY, body, immutable=False)
    return {
        "status": "PUBLISHED",
        "generated_at": value["generated_at"],
        "model_run_at": model["model_run_at"],
        "observed_at": {
            o["station_id"]: o["observed_at"] for o in value["observations"]
        },
    }
