"""Private latest readings alongside the unchanged historical API routes."""

from decimal import Decimal
from urllib.parse import parse_qs

from final_demo.lambda_api import handler as historical_handler
from final_demo.lambda_api import reply
from ingestion.openaq import PILOTS
from live_data.contract import ACCOUNT, freshness, now_utc
from live_data.store import AWSStore, clients


def plain(value):
    if isinstance(value, Decimal):
        return int(value) if value == int(value) else float(value)
    if isinstance(value, dict):
        return {k: plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [plain(v) for v in value]
    return value


def latest(store, now, location=None):
    rows = []
    for loc, sensor in PILOTS.items():
        if location is not None and loc != location:
            continue
        item = plain(store.read("LATEST", str(loc)))
        status = plain(store.read("POLL", str(loc)))
        row = {
            k: item[k]
            for k in (
                "location_id",
                "sensor_id",
                "station",
                "observed_at",
                "ingested_at",
                "pm25",
                "units",
                "source",
                "record_sha256",
            )
            if k in item
        }
        row.update(
            location_id=loc,
            sensor_id=sensor,
            freshness=freshness(
                item.get("observed_at"), now, item.get("cadence_seconds")
            ),
            poll={
                "last_poll_at": status.get("last_poll_at"),
                "status": status.get("status", "not_polled"),
            },
            aqi=None,
            aqi_note="No AQI is calculated from instantaneous readings; hourly MVP AQI is a separate Gold product.",
        )
        rows.append(row)
    return {
        "data": rows,
        "count": len(rows),
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "metadata": {
            "source": "accepted OpenAQ v3 raw observations via immutable S3",
            "units": "µg/m³",
            "classification": "latest accepted observations; may be stale or absent",
            "poll_interval_seconds": 300,
            "poll_interval_is_not_source_freshness": True,
            "licensing": "private demo; per-source terms retained; redistribution not cleared",
        },
    }


def handler(event, context, store=None, now=None):
    if event.get("rawPath") not in ("/api/v1/latest", "/api/v1/current/hourly"):
        return historical_handler(event, context)
    request = event.get("requestContext", {})
    if request.get("authorizer", {}).get("iam", {}).get("accountId") != ACCOUNT:
        return reply(403, {"error": "iam_authenticated_same_account_required"})
    if request.get("http", {}).get("method") not in ("GET", "HEAD"):
        return reply(405, {"error": "read_only"})
    try:
        if event.get("rawPath") == "/api/v1/current/hourly":
            from live_data.export import handle

            result = handle(event, store or AWSStore(clients()))
            if request["http"]["method"] == "HEAD":
                result["body"] = ""
            return result
        raw = event.get("rawQueryString", "")
        if len(raw) > 512:
            raise ValueError("Query too long")
        params = parse_qs(raw, keep_blank_values=True, max_num_fields=1)
        if set(params) - {"location_id"}:
            raise ValueError("Unknown filter")
        location = None
        if "location_id" in params:
            values = params["location_id"]
            if (
                len(values) != 1
                or not values[0].isdecimal()
                or int(values[0]) not in PILOTS
            ):
                raise ValueError("Invalid station")
            location = int(values[0])
        result = reply(
            200, latest(store or AWSStore(clients()), now or now_utc(), location)
        )
        if request["http"]["method"] == "HEAD":
            result["body"] = ""
        return result
    except ValueError:
        return reply(400, {"error": "invalid_parameters"})
    except Exception:  # noqa: BLE001 -- never expose raw SDK details
        return reply(503, {"error": "latest_state_unavailable"})
