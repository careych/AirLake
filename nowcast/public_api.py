"""Public read-only API: current projection, rolling licensed history, September archive.

The Function URL is publicly callable whatever CORS says, so every request is
bounded here: GET/HEAD only, five exact paths, strict query allowlists,
licensed stations only, ranges of at most 100 days, at most 1,000 rows and
1 MB per response, generic errors, and only fixed S3 objects: the current
projection, one history series per licensed station, and the immutable
September export pinned by SHA-256. The IAM-protected
private API is separate and unchanged.
"""

import hashlib
import json
import os
import time
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlencode

from final_demo.lambda_api import reply
from live_data.contract import ACCOUNT, BUCKET
from nowcast import history, rolling_api

LICENSED_HISTORY = {
    "station_id": "6123215",
    "location_id": "6123215",
    "sensor_id": "14581375",
}
HISTORY_KEYS = {
    "station_id",
    "location_id",
    "sensor_id",
    "start",
    "end",
    "granularity",
    "limit",
    "offset",
    "format",
}
ORIGINS = {
    "https://public.dhyvgpdsnj4zi.amplifyapp.com",
    "https://demo.dhyvgpdsnj4zi.amplifyapp.com",
}
CURRENT_KEY = "nowcast/v1/current.json"
ARCHIVE = "/v1/archive/september-2026"
MAX_BODY = 1_000_000
NOW_TTL_SECONDS = 60
HISTORY_TTL_SECONDS = 300
ROUTES = ("/v1", "/v1/now", "/v1/stations", "/v1/history", ARCHIVE)
SECURITY = {
    "content-security-policy": "default-src 'none'; frame-ancestors 'none'",
    "referrer-policy": "no-referrer",
}
DOCS = {
    "service": "AirLake public data API (Hanoi PM2.5 pilot)",
    "routes": {
        "GET /v1/now": "Current projection: observations, CAMS model estimate and forecast, kept separate; no query parameters",
        "GET /v1/stations": "Public stations with licence, location precision and history availability",
        "GET /v1/history": "Rolling cleaned hourly/daily PM2.5 per public station since 2026-09-01 (Vietnam time), with coverage and gaps",
        "GET /v1/archive/september-2026": "Frozen AWS Glue/Athena export for Dragon, September 2026 (previous /v1/history)",
    },
    "history_parameters": {
        "station_id": "required: 6123215 (Gia Lâm, CC BY 4.0) or 99385 (Tây Hồ, ODbL 1.0)",
        "start, end": "YYYY-MM-DD (Vietnam date, end exclusive) or YYYY-MM-DDTHH:00:00Z; default the last 7 complete Vietnam days; max 100 days",
        "granularity": "hourly | daily (default hourly)",
        "format": "json | csv",
        "limit": "1-1000 (default 1000)",
        "offset": "0-3000",
    },
    "fields": {
        "hour_start_utc": "UTC start of the averaging hour [start, start+1h)",
        "pm25": "PM2.5 mean in µg/m³",
        "n_readings": "readings behind the hourly value",
        "basis": "how the hour was produced (see methodology in each response)",
        "flags": "very_low | isolated_peak | provisional; values are never altered",
        "date_local / valid_day / complete_day": "Vietnam calendar day; valid with >= 18/24 hours",
    },
    "licences": {
        "6123215": "CC BY 4.0, attribution 'AirGradient via OpenAQ'",
        "99385": "Sensor.Community data under ODbL 1.0 (derived database offered under ODbL 1.0)",
        "now.model": "CAMS via Open-Meteo, CC BY 4.0; credit Copernicus Atmosphere Monitoring Service",
    },
    "not_served": "Other Hanoi stations have no redistribution licence; no SQL access",
    "errors": {
        "400": "invalid parameters",
        "404": "unknown route or unlicensed station",
        "405": "not GET",
        "503": "temporarily unavailable",
    },
    "limits": "responses are cached; heavy use may be throttled",
}
_cache = {}


class Store:
    def __init__(self, s3):
        self.s3 = s3


class CachedS3:
    """Immutable objects are cached per container; the current projection for 60 s."""

    def __init__(self, s3):
        self.s3 = s3

    def get_object(self, Bucket, Key, ExpectedBucketOwner):
        import io

        ttl = (
            NOW_TTL_SECONDS
            if Key == CURRENT_KEY
            else HISTORY_TTL_SECONDS
            if Key.startswith("nowcast/v1/history/")
            else None
        )
        hit = _cache.get(Key)
        if hit and (ttl is None or time.monotonic() - hit[0] < ttl):
            return {"Body": io.BytesIO(hit[1])}
        body = self.s3.get_object(
            Bucket=Bucket, Key=Key, ExpectedBucketOwner=ExpectedBucketOwner
        )["Body"].read(4_000_001)
        _cache[Key] = (time.monotonic(), body)
        return {"Body": io.BytesIO(body)}


def cors(event):
    origin = (event.get("headers") or {}).get("origin")
    return (
        {"access-control-allow-origin": origin, "vary": "origin"}
        if origin in ORIGINS
        else {"vary": "origin"}
    )


def history_event(event):
    """Pin every history request to the licensed station; reject unknown keys."""
    raw = event.get("rawQueryString", "")
    if len(raw) > 512:
        raise ValueError("Query too long")
    params = parse_qs(raw, keep_blank_values=True, max_num_fields=8)
    if set(params) - HISTORY_KEYS or any(len(v) != 1 for v in params.values()):
        raise ValueError("Invalid filters")
    for key in ("station_id", "location_id", "sensor_id"):
        if key in params and params.pop(key)[0] != LICENSED_HISTORY[key]:
            raise LookupError("Station is not licensed for public download")
    params["station_id"] = [LICENSED_HISTORY["station_id"]]
    return {**event, "rawQueryString": urlencode(params, doseq=True)}


def series(store, station):
    body = store.s3.get_object(
        Bucket=BUCKET, Key=history.key(station), ExpectedBucketOwner=ACCOUNT
    )["Body"].read(4_000_001)
    value = json.loads(body)
    if value.get("schema_version") != 1 or value.get("station_id") != station:
        raise RuntimeError("Unexpected history series")
    return value


def rolling(event, store, headers, path):
    now = datetime.now(UTC).replace(microsecond=0)
    if path == "/v1/stations":
        value = rolling_api.stations(
            {s: series(store, s) for s in rolling_api.PLACES}, now
        )
        body, kind = rolling_api.serialize(value), "application/json; charset=utf-8"
    else:
        p = rolling_api.params(event.get("rawQueryString", ""), now)
        value = rolling_api.build(series(store, p["station_id"]), p, now)
        if p["format"] == "csv":
            body, kind = rolling_api.to_csv(value), "text/csv; charset=utf-8"
        else:
            body, kind = rolling_api.serialize(value), "application/json; charset=utf-8"
    if len(body.encode()) > MAX_BODY:
        raise RuntimeError("Response too large")
    etag = '"' + hashlib.sha256(body.encode()).hexdigest()[:32] + '"'
    out = {
        **headers,
        "content-type": kind,
        "cache-control": "public, max-age=300",
        "etag": etag,
    }
    if kind.startswith("text/csv"):
        out["content-disposition"] = (
            f'attachment; filename="airlake-{p["station_id"]}-{p["granularity"]}.csv"'
        )
    if (event.get("headers") or {}).get("if-none-match") == etag:
        return reply(304, "", headers=out)
    return {"statusCode": 200, "headers": out, "body": body}


def handler(event, context, store=None, environment=None):
    headers = {**cors(event), **SECURITY}
    method = event.get("requestContext", {}).get("http", {}).get("method")
    path = event.get("rawPath")
    if method == "OPTIONS":
        return reply(
            204,
            "",
            headers={
                **headers,
                "access-control-allow-methods": "GET, HEAD",
                "access-control-max-age": "86400",
            },
        )
    if method not in ("GET", "HEAD"):
        return reply(405, {"error": "read_only"}, headers=headers)
    if path not in ROUTES:
        return reply(404, {"error": "unknown_route", "docs": "/v1"}, headers=headers)
    if path not in ("/v1/history", ARCHIVE) and event.get("rawQueryString"):
        return reply(
            400, {"error": "no_parameters_accepted", "docs": "/v1"}, headers=headers
        )
    try:
        if path == "/v1":
            result = reply(
                200, DOCS, headers={**headers, "cache-control": "public, max-age=3600"}
            )
        else:
            store = store or Store(CachedS3(__import__("boto3").client("s3")))
            if path == "/v1/now":
                body = store.s3.get_object(
                    Bucket=BUCKET, Key=CURRENT_KEY, ExpectedBucketOwner=ACCOUNT
                )["Body"].read(MAX_BODY + 1)
                if len(body) > MAX_BODY or json.loads(body).get("schema_version") != 2:
                    raise RuntimeError("Unexpected projection")
                etag = '"' + hashlib.sha256(body).hexdigest()[:32] + '"'
                cache = {**headers, "cache-control": "public, max-age=60", "etag": etag}
                if (event.get("headers") or {}).get("if-none-match") == etag:
                    return reply(304, "", headers=cache)  # repeat polls: no body
                result = reply(200, body.decode(), headers=cache)
            elif path in ("/v1/stations", "/v1/history"):
                result = rolling(event, store, headers, path)
                if result["statusCode"] == 304:
                    return result
            else:
                from historical.api import handle

                result = handle(
                    history_event(event),
                    store,
                    os.environ if environment is None else environment,
                )
                if result["statusCode"] == 200 and result["headers"][
                    "content-type"
                ].startswith("application/json"):
                    value = json.loads(result["body"])
                    if value.get("next"):
                        value["next"] = value["next"].replace(
                            "/api/v1/history?", ARCHIVE + "?", 1
                        )
                    result["body"] = json.dumps(
                        value, ensure_ascii=False, allow_nan=False
                    )
                elif result["statusCode"] == 200:
                    result["headers"]["content-disposition"] = (
                        'attachment; filename="airlake-dragon-september-2026.csv"'
                    )
                result["headers"].update(
                    headers, **{"cache-control": "public, max-age=3600"}
                )
                if len(result["body"].encode()) > MAX_BODY:
                    raise RuntimeError("Response too large")
    except LookupError:
        return reply(
            404, {"error": "station_not_licensed_for_public_download"}, headers=headers
        )
    except ValueError:
        return reply(400, {"error": "invalid_request", "docs": "/v1"}, headers=headers)
    except Exception:  # noqa: BLE001 -- never expose SDK payloads or object keys
        return reply(
            503,
            {"error": "temporarily_unavailable"},
            headers={**headers, "retry-after": "60"},
        )
    if method == "HEAD":
        result["body"] = ""
    return result
