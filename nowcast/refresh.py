"""Scheduled refresh: fetch -> validate -> versioned S3 -> current projection.

Observations and model output are stored as separate datasets. A failing
source keeps its last good reading (which then ages into "stale"); the CAMS
forecast is downloaded only when Open-Meteo reports a new run. Nothing is
published if no model run is available or S3 rejects a write.
"""

import json
import os

from ingestion.openaq import stamp, utc
from live_data.contract import ACCOUNT, BUCKET
from nowcast import openmeteo, project, sensor_community
from nowcast.http import UpstreamError
from nowcast.public_api import CURRENT_KEY

PREFIX = "nowcast/v1/"


def conditional_writes(s3):
    """Older Lambda-bundled botocore lacks IfNoneMatch; keys are unique per time anyway."""
    try:
        return "IfNoneMatch" in s3.meta.service_model.operation_model("PutObject").input_shape.members
    except AttributeError:
        return True


def put(s3, key, body, immutable=True):
    extra = {"IfNoneMatch": "*"} if immutable and conditional_writes(s3) else {}
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


def attempt(fn):
    try:
        return fn(), None
    except UpstreamError as e:
        return None, e.reason
    except (ValueError, KeyError, TypeError, IndexError):
        return None, "invalid_response"


def source(previous, station_id, fn, now):
    reading, error = attempt(fn)
    if error is None:
        return {"status": "ok", "reading": reading, "collected_at": stamp(now)}
    old = next(
        (
            o
            for o in (previous or {}).get("observations", [])
            if o["station_id"] == station_id
        ),
        None,
    )
    return {
        "status": "fetch_failed",
        "error": error,
        "reading": {"observed_at": old["observed_at"], "pm25": old["pm25"]}
        if old and old["observed_at"]
        else None,
        "collected_at": old.get("collected_at") if old else None,
    }


def run_model(s3, http, previous, now):
    meta, error = attempt(lambda: http(openmeteo.META))
    run = meta.get("last_run_initialisation_time") if meta else None
    if error is None and run is not None:
        key = f"{PREFIX}model/cams_global/{run}.json"
        raw = get(s3, key)
        if raw is None:
            body, error = attempt(lambda: http(openmeteo.API, openmeteo.PARAMS))
            if error is None:
                raw = json.dumps(body, sort_keys=True).encode()
        if raw is not None:
            model, error = attempt(
                lambda: openmeteo.normalize(meta, json.loads(raw), now)
            )
            if error is None:
                if get(s3, key) is None:
                    put(s3, key, raw)
                model["fetch_status"] = "ok"
                return model, meta
    old = (previous or {}).get("model")
    if not old:
        raise RuntimeError("No validated model run available; nothing published")
    run = int(utc(old["model_run_at"]).timestamp())
    raw = get(s3, f"{PREFIX}model/cams_global/{run}.json")
    old_meta = {
        "last_run_initialisation_time": run,
        "last_run_availability_time": int(utc(old["model_available_at"]).timestamp()),
        "update_interval_seconds": 43200,
    }
    model = openmeteo.normalize(old_meta, json.loads(raw), now)
    model.update(fetch_status="reused_previous_run", fetch_error=error)
    return model, old_meta


def run(s3, http, openaq_key, validation, now):
    now = utc(now)
    raw_previous = get(s3, CURRENT_KEY)
    previous = json.loads(raw_previous) if raw_previous else None
    model, _ = run_model(s3, http, previous, now)
    dragon = source(
        previous,
        6123215,
        lambda: project.openaq_latest(
            http(
                "https://api.openaq.org/v3/locations/6123215/latest",
                headers={"X-API-Key": openaq_key},
            )
        ),
        now,
    )
    community = source(
        previous, 99385, lambda: sensor_community.fetch_latest(http), now
    )
    for name, src in (
        ("openaq-6123215", dragon),
        ("sensor-community-99385", community),
    ):
        reading = src["reading"]
        if src["status"] != "ok" or not reading:
            continue
        at = reading["observed_at"]
        key = f"{PREFIX}observations/{name}/{at[:10]}/{at.replace(':', '')}.json"
        if get(s3, key) is None:  # one immutable object per source observation
            put(
                s3,
                key,
                json.dumps({**reading, "collected_at": src["collected_at"]}).encode(),
            )
    value = project.build(now, dragon, community, model, validation)
    body = json.dumps(
        value, ensure_ascii=False, sort_keys=True, allow_nan=False
    ).encode()
    put(s3, f"{PREFIX}projections/{stamp(now).replace(':', '')}.json", body)
    put(s3, CURRENT_KEY, body, immutable=False)  # only after the immutable copy exists
    return {
        "status": "PUBLISHED",
        "published_at": value["published_at"],
        "model_run_at": model["model_run_at"],
        "model_fetch": model["fetch_status"],
        "sources": {o["station_id"]: o["fetch_status"] for o in value["observations"]},
    }


def handler(event, context):
    """Lambda entry: OpenAQ key from the existing Secrets Manager secret."""
    import boto3

    from nowcast import http

    secret = boto3.client("secretsmanager").get_secret_value(
        SecretId=os.environ["AIRLAKE_OPENAQ_SECRET_ARN"]
    )["SecretString"]
    if (
        not isinstance(secret, str)
        or not secret.strip()
        or "\n" in secret
        or secret.startswith("{")
    ):
        raise ValueError("Expected plaintext OpenAQ key")
    key = secret.strip()
    validation = json.loads(
        (
            __import__("pathlib").Path(__file__).with_name("validation_public.json")
        ).read_text()
    )
    from datetime import UTC, datetime

    result = run(
        boto3.client("s3"),
        http.get,
        key,
        validation,
        datetime.now(UTC).replace(microsecond=0),
    )
    print(json.dumps({"metric": "AirLakeNowcastRefresh", **result}))
    return result
