"""Public read-only API: licensed Dragon history and the current projection.

No AWS credentials in the browser and no query execution: fixed routes read
verified objects. The IAM-protected private API stays unchanged.
"""

import json
import os
from urllib.parse import parse_qs, urlencode

from final_demo.lambda_api import reply
from live_data.contract import ACCOUNT, BUCKET

LICENSED_HISTORY = {"station_id": "6123215", "sensor_id": "14581375"}
ORIGINS = {
    "https://public.dhyvgpdsnj4zi.amplifyapp.com",
    "https://demo.dhyvgpdsnj4zi.amplifyapp.com",
}
CURRENT_KEY = "nowcast/v1/current.json"
DOCS = {
    "service": "AirLake public data API (Hanoi PM2.5 pilot)",
    "routes": {
        "/v1/now": "Current projection: observations, CAMS model estimate and forecast, kept separate",
        "/v1/history": "Cleaned hourly/daily PM2.5 for Dragon (AirGradient, CC BY 4.0), September 2026",
    },
    "history_parameters": {
        "granularity": "hourly | daily (default hourly)",
        "start, end": "UTC ISO-8601, whole hours, inside the accepted window",
        "format": "json | csv",
        "limit": "1-1000 (default 100)",
        "offset": "0-3000",
    },
    "units": "PM2.5 µg/m³; hour_start in UTC; date_local in Asia/Ho_Chi_Minh",
    "licence": "Dragon data CC BY 4.0 (AirGradient via OpenAQ); model data CC BY 4.0 via Open-Meteo, Copernicus CAMS attribution",
    "errors": "400 invalid filters, 404 unknown route or unlicensed station, 405 non-GET",
}


def cors(event):
    origin = (event.get("headers") or {}).get("origin")
    return (
        {"access-control-allow-origin": origin, "vary": "origin"}
        if origin in ORIGINS
        else {"vary": "origin"}
    )


def history_event(event):
    """Pin every history request to the licensed station; reject any other."""
    raw = event.get("rawQueryString", "")
    if len(raw) > 1024:
        raise ValueError("Query too long")
    params = parse_qs(raw, keep_blank_values=True, max_num_fields=8)
    for key in ("station_id", "location_id", "sensor_id"):
        values = params.pop(key, [])
        if any(
            v != LICENSED_HISTORY.get(key, LICENSED_HISTORY["station_id"])
            for v in values
        ):
            raise LookupError("Station is not licensed for public download")
    params["station_id"] = [LICENSED_HISTORY["station_id"]]
    return {**event, "rawQueryString": urlencode(params, doseq=True)}


class Store:
    def __init__(self, s3):
        self.s3 = s3


def handler(event, context, store=None, environment=None):
    headers = cors(event)
    method = event.get("requestContext", {}).get("http", {}).get("method")
    path = event.get("rawPath")
    if method == "OPTIONS":
        return reply(
            204, "", headers={**headers, "access-control-allow-methods": "GET"}
        )
    if method not in ("GET", "HEAD"):
        return reply(405, {"error": "read_only"}, headers=headers)
    try:
        if path in ("/v1", "/v1/"):
            result = reply(200, DOCS, headers=headers)
        elif path == "/v1/now":
            store = store or Store(__import__("boto3").client("s3"))
            body = store.s3.get_object(
                Bucket=BUCKET, Key=CURRENT_KEY, ExpectedBucketOwner=ACCOUNT
            )["Body"].read(2_000_001)
            if json.loads(body).get("schema_version") != 1:
                raise RuntimeError("Unexpected projection")
            result = reply(
                200,
                body.decode(),
                headers={**headers, "cache-control": "public, max-age=300"},
            )
        elif path == "/v1/history":
            from historical.api import handle

            store = store or Store(__import__("boto3").client("s3"))
            result = handle(
                history_event(event),
                store,
                os.environ if environment is None else environment,
            )
            result["headers"].update(
                headers, **{"cache-control": "public, max-age=3600"}
            )
        else:
            return reply(
                404, {"error": "unknown_route", "docs": "/v1"}, headers=headers
            )
    except LookupError:
        return reply(
            404, {"error": "station_not_licensed_for_public_download"}, headers=headers
        )
    except ValueError:
        return reply(400, {"error": "invalid_request", "docs": "/v1"}, headers=headers)
    except Exception:  # noqa: BLE001 -- never expose SDK payloads or object keys
        return reply(503, {"error": "temporarily_unavailable"}, headers=headers)
    if method == "HEAD":
        result["body"] = ""
    return result
