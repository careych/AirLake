"""Scheduled refresh: fetch -> validate -> versioned S3 -> current projection.

Observations and model output are stored as separate datasets. A failing
source keeps its last good reading (which then ages into "stale"); the CAMS
forecast is downloaded only when Open-Meteo reports a new run. Nothing is
published if no model run is available or S3 rejects a write.
"""

import hashlib
import json
import os
from datetime import timedelta

from ingestion.openaq import stamp, utc
from live_data.contract import ACCOUNT, BUCKET
from nowcast import openmeteo, project, sensor_community
from nowcast.http import UpstreamError
from nowcast.public_api import CURRENT_KEY

PREFIX = "nowcast/v1/"


LICENSED = {6123215, 99385}


class Store:
    """S3 with conditional writes sent as raw If-None-Match / If-Match headers.

    The header is injected in a before-call hook, so it works with any botocore
    version bundled in the Lambda runtime; S3 enforces it server-side (412).
    """

    def __init__(self, s3):
        self.s3 = s3
        self.pending = None
        events = getattr(getattr(s3, "meta", None), "events", None)
        self.hooked = events is not None
        if self.hooked:
            events.register("before-call.s3.PutObject", self.inject)

    def inject(self, params, **kwargs):
        if self.pending:
            params["headers"].update(self.pending)

    def keys(self, prefix):
        out, token = [], None
        while True:
            kw = {"Bucket": BUCKET, "Prefix": prefix, "ExpectedBucketOwner": ACCOUNT}
            if token:
                kw["ContinuationToken"] = token
            page = self.s3.list_objects_v2(**kw)
            out += [o["Key"] for o in page.get("Contents", [])]
            if not page.get("IsTruncated"):
                return out
            token = page["NextContinuationToken"]

    def get(self, key):
        try:
            r = self.s3.get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)
        except self.s3.exceptions.NoSuchKey:
            return None, None
        return r["Body"].read(4_000_001), r.get("ETag")

    def put(self, key, body, condition):
        self.pending = condition
        try:
            extra = {} if self.hooked else {"Condition": condition}  # test doubles
            self.s3.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=body,
                ExpectedBucketOwner=ACCOUNT,
                ServerSideEncryption="AES256",
                ContentType="application/json",
                **extra,
            )
        finally:
            self.pending = None

    def put_new(self, key, body):
        """Create once; an identical existing object is accepted, a different one stops."""
        try:
            self.put(key, body, {"If-None-Match": "*"})
        except Exception as e:
            if not precondition_failed(e) or self.get(key)[0] != body:
                raise

    def replace_current(self, body, etag):
        """Swap the pointer only if nobody changed it since we read it."""
        self.put(
            CURRENT_KEY, body, {"If-Match": etag} if etag else {"If-None-Match": "*"}
        )


def precondition_failed(e):
    return getattr(e, "response", {}).get("Error", {}).get("Code") in (
        "PreconditionFailed",
        "412",
    )


def content_key(prefix, label, body):
    return (
        f"{prefix}{label.replace(':', '')}-{hashlib.sha256(body).hexdigest()[:16]}.json"
    )


def validate(value, previous):
    """Last checks before a projection may become current."""
    if (
        value["schema_version"] != 2
        or {o["station_id"] for o in value["observations"]} - LICENSED
        or value["model"]["kind"] != "model"
        or len(value["model"]["forecast"]) < 24
    ):
        raise ValueError("Projection failed validation")
    if previous and utc(value["published_at"]) <= utc(previous["published_at"]):
        raise ValueError("Projection is not newer than the current one")


def attempt(fn):
    try:
        return fn(), None
    except UpstreamError as e:
        return None, e.reason
    except (ValueError, KeyError, TypeError, IndexError):
        return None, "invalid_response"


def source(previous, station_id, fn, now):
    reading, error = attempt(fn)
    if error is None and reading is not None:
        return {"status": "ok", "reading": reading, "fetched_at": stamp(now)}
    old = next(
        (
            o
            for o in (previous or {}).get("observations", [])
            if o["station_id"] == station_id
        ),
        None,
    )
    return {  # no new reading: keep the last one so it ages into "stale"
        "status": "fetch_failed" if error else "no_new_reading",
        "error": error,
        "reading": {"observed_at": old["observed_at"], "pm25": old["pm25"]}
        if old and old["observed_at"]
        else None,
        "fetched_at": old.get("fetched_at") if old else None,
    }


def run_model(store, http, previous, now):
    meta, error = attempt(lambda: http(openmeteo.META))
    run = meta.get("last_run_initialisation_time") if meta else None
    if error is None and run is not None:
        key = f"{PREFIX}model/cams_global/{run}.json"
        raw, _ = store.get(key)
        if raw is None:
            body, error = attempt(lambda: http(openmeteo.API, openmeteo.PARAMS))
            if error is None:
                raw = json.dumps(body, sort_keys=True).encode()
                model, error = attempt(lambda: openmeteo.normalize(meta, body, now))
                if error is None:
                    store.put_new(key, raw)  # only validated runs are stored
        if raw is not None and error is None:
            model, error = attempt(
                lambda: openmeteo.normalize(meta, json.loads(raw), now)
            )
            if error is None:
                model["fetch_status"] = "ok"
                return model
    old = (previous or {}).get("model")
    if not old:
        raise RuntimeError("No validated model run available; nothing published")
    run = int(utc(old["model_run_at"]).timestamp())
    raw, _ = store.get(f"{PREFIX}model/cams_global/{run}.json")
    old_meta = {
        "last_run_initialisation_time": run,
        "last_run_availability_time": int(utc(old["model_available_at"]).timestamp()),
        "update_interval_seconds": 43200,
    }
    model = openmeteo.normalize(old_meta, json.loads(raw), now)
    model.update(fetch_status="reused_previous_run", fetch_error=error)
    return model


SOURCES = {"openaq-6123215": "6123215", "sensor-community-99385": "99385"}


def stored_recent(store, now):
    """One-time seed of the 24-hour series from immutable stored observations."""
    series = {}
    days = sorted({(now - timedelta(hours=h)).date().isoformat() for h in (0, 24)})
    for name, station in SOURCES.items():
        rows = []
        for day in days:
            for key in store.keys(f"{PREFIX}observations/{name}/{day}/"):
                reading = json.loads(store.get(key)[0])
                if now - timedelta(hours=24) <= utc(reading["observed_at"]) <= now:
                    rows.append(
                        {"observed_at": reading["observed_at"], "pm25": reading["pm25"]}
                    )
        series[station] = sorted(
            {r["observed_at"]: r for r in rows}.values(), key=lambda r: r["observed_at"]
        )
    return series


def run(s3, http, openaq_key, validation, now):
    now = utc(now)
    store = s3 if isinstance(s3, Store) else Store(s3)
    raw_previous, etag = store.get(CURRENT_KEY)
    previous = json.loads(raw_previous) if raw_previous else None
    if previous is not None and "recent" not in previous:
        previous = {**previous, "recent": stored_recent(store, now)}
    model = run_model(store, http, previous, now)
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
        if src["status"] == "ok" and src["reading"]:
            body = json.dumps(
                src["reading"], sort_keys=True
            ).encode()  # one object per observation
            at = src["reading"]["observed_at"]
            store.put_new(
                content_key(f"{PREFIX}observations/{name}/{at[:10]}/", at, body), body
            )
    value = project.build(now, dragon, community, model, validation, previous)
    validate(value, previous)
    body = json.dumps(
        value, ensure_ascii=False, sort_keys=True, allow_nan=False
    ).encode()
    key = content_key(
        f"{PREFIX}projections/{value['published_at'][:10]}/",
        value["published_at"],
        body,
    )
    store.put_new(key, body)
    store.replace_current(body, etag)  # only after the immutable copy exists
    return {
        "status": "PUBLISHED",
        "published_at": value["published_at"],
        "projection_key": key,
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
