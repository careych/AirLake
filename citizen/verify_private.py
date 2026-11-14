"""Three explicitly approved signed reads after promotion; no query or retry."""

import csv
import io
import json
from urllib.parse import urlsplit

from citizen.promotion import EXPORT_SHA, QUERY
from final_demo.control import Receipt
from live_data.contract import NAME, REGION


def verify(c, packet, digest):
    import boto3
    import requests
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    config = c["lambda"].get_function_url_config(FunctionName=NAME + "-api")
    if config["AuthType"] != "AWS_IAM" or not urlsplit(
        config["FunctionUrl"]
    ).hostname.endswith(".lambda-url.ap-southeast-2.on.aws"):
        raise ValueError("Private URL scope mismatch")
    r = Receipt(packet / "http-receipt.json", digest)
    r.value["requests"] = []
    session = boto3.Session(profile_name="airlake", region_name=REGION)
    http = requests.Session()
    http.trust_env = False
    responses = []
    for path in (
        "/api/v1/latest",
        "/api/v1/current/hourly?limit=100",
        "/api/v1/current/hourly?limit=100&format=csv",
    ):
        item = {"path": path, "status": "UNCERTAIN"}
        r.value["requests"].append(item)
        r.save()
        req = AWSRequest(method="GET", url=config["FunctionUrl"].rstrip("/") + path)
        SigV4Auth(
            session.get_credentials().get_frozen_credentials(), "lambda", REGION
        ).add_auth(req)
        response = http.get(
            req.url, headers=dict(req.headers), timeout=20, allow_redirects=False
        )
        item["status"] = response.status_code
        r.save()
        if response.status_code != 200:
            raise ValueError("Private HTTP failed; no retry")
        responses.append(response)
    latest = responses[0].json()
    hourly = responses[1].json()
    rows = list(csv.DictReader(io.StringIO(responses[2].text)))
    if (
        latest["count"] != 3
        or hourly["total"] != 14
        or hourly["count"] != 14
        or hourly["next_offset"] is not None
        or hourly["metadata"]["export_sha256"] != EXPORT_SHA
        or hourly["metadata"]["athena_query_id"] != QUERY
    ):
        raise ValueError("JSON/version mismatch")
    expected = {
        (str(r["location_id"]), r["hour_start"]): (r["pm25_avg"], r["aqi"])
        for r in hourly["data"]
    }
    if (
        len(rows) != 14
        or {(r["location_id"], r["hour_start"]) for r in rows} != set(expected)
        or any(
            abs(float(r["pm25_avg"]) - expected[(r["location_id"], r["hour_start"])][0])
            > 1e-10
            or int(r["aqi"]) != expected[(r["location_id"], r["hour_start"])][1]
            for r in rows
        )
    ):
        raise ValueError("CSV mismatch")
    for name, value in [("latest-api.json", latest), ("hourly-api.json", hourly)]:
        with (packet / name).open("x") as f:
            json.dump(value, f, indent=2, ensure_ascii=False)
        (packet / name).chmod(0o600)
    r.value.update(status="PASS_THREE_SIGNED_READS", hourly_rows=14, query_id=QUERY)
    r.save()
    return r.value
