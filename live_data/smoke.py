"""Separately approved eight HTTP reads; render the same latest response offline."""

import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit

from final_demo.control import Receipt
from live_data.contract import NAME, REGION
from live_data.deploy import clients, identity, load_packet
from live_data.view import render


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--packet", type=Path, required=True)
    a.add_argument("--execute", action="store_true")
    a.add_argument("--confirm-plan-sha256")
    args = a.parse_args()
    _, digest = load_packet(args.packet)
    if not args.execute or digest != args.confirm_plan_sha256:
        a.exit(1, "STOP: separate bounded HTTP approval required\n")
    c = clients()
    identity(c)
    config = c["lambda"].get_function_url_config(FunctionName=NAME + "-api")
    url = config["FunctionUrl"].rstrip("/")
    if config["AuthType"] != "AWS_IAM" or not urlsplit(url).hostname.endswith(
        ".lambda-url.ap-southeast-2.on.aws"
    ):
        raise ValueError("Unexpected API URL")
    import boto3
    import requests
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    session = boto3.Session(profile_name="airlake", region_name=REGION)
    http = requests.Session()
    http.trust_env = False
    r = Receipt(args.packet / "http-receipt.json", digest)
    r.value["requests"] = []

    def get(path, signed=True):
        if len(r.value["requests"]) >= 8:
            raise ValueError("HTTP budget exhausted")
        item = {"path": path, "signed": signed, "status": "UNCERTAIN"}
        r.value["requests"].append(item)
        r.save()
        headers = {}
        if signed:
            request = AWSRequest(method="GET", url=url + path)
            SigV4Auth(
                session.get_credentials().get_frozen_credentials(), "lambda", REGION
            ).add_auth(request)
            headers = dict(request.headers)
        response = http.get(
            url + path, headers=headers, allow_redirects=False, timeout=20
        )
        item["status"] = response.status_code
        r.save()
        if response.status_code != (200 if signed else 403):
            raise ValueError("HTTP acceptance failed")
        return response

    get("/api/v1/health", False)
    get("/api/v1/health")
    latest = get("/api/v1/latest").json()
    if latest["count"] != 3 or not any(
        x["freshness"]["status"] == "recent" and x.get("pm25") is not None
        for x in latest["data"]
    ):
        raise ValueError("No genuinely recent accepted reading")
    for loc in (4946812, 4946813, 6123215):
        data = get("/api/v1/latest?location_id=" + str(loc)).json()
        if data["count"] != 1 or data["data"][0]["location_id"] != loc:
            raise ValueError("Station filter failed")
    current = get("/api/v1/current/hourly?limit=100").json()
    promotion = json.loads((args.packet / "athena-promotion-receipt.json").read_text())
    if (
        current["total"] != promotion["rows"]
        or current["metadata"]["athena_query_id"] != promotion["query_id"]
    ):
        raise ValueError("Promoted export differs from independently verified output")
    old = get("/api/v1/gold/hourly?format=csv&limit=100")
    import csv
    import io

    if len(list(csv.DictReader(io.StringIO(old.text)))) != 72:
        raise ValueError("Historical API regression")
    page = args.packet / "latest-measurements.html"
    with page.open("x") as f:
        f.write(render(latest))
    (args.packet / "latest-response.json").write_text(
        json.dumps(latest, indent=2) + "\n"
    )
    r.value.update(
        status="PASS_HTTP_AND_REAL_LATEST_VIEW",
        latest_statuses=[
            {
                "location_id": x["location_id"],
                "observed_at": x.get("observed_at"),
                "freshness": x["freshness"],
            }
            for x in latest["data"]
        ],
        current_rows=current["total"],
        function_url=url,
    )
    r.save()
    print(
        json.dumps(
            {
                "status": r.value["status"],
                "http_requests": 8,
                "latest_view": "latest-measurements.html",
            }
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001 -- never log credentials or raw responses
        print(
            json.dumps(
                {
                    "status": "STOP_HTTP_FAILED_OR_UNCERTAIN",
                    "exception_type": type(e).__name__,
                }
            )
        )
        raise SystemExit(1) from None
