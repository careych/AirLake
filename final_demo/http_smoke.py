"""Opt-in, at most ten real signed Function URL requests; no Athena queries."""

import argparse
import csv
import io
import json
from pathlib import Path
from urllib.parse import urlsplit

from clean_api.data import Dataset
from clean_api.verify import same_rows
from final_demo.contract import ACCOUNT, REGION, digest, validate_plan


def require(condition):
    if not condition:
        raise ValueError("HTTP acceptance failed; retain evidence; no retry")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm-plan-sha256", required=True)
    a = parser.parse_args()
    raw = (a.packet / "plan.json").read_bytes()
    p = validate_plan(json.loads(raw))
    if not a.execute or digest(raw) != a.confirm_plan_sha256:
        parser.exit(
            1, "STOP: explicit HTTP-invocation approval and exact plan hash required\n"
        )
    output = a.packet / "http-result.json"
    if output.exists():
        parser.exit(1, "STOP: HTTP evidence already exists; no automatic retry\n")
    import boto3
    import requests
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.config import Config

    session = boto3.Session(profile_name="airlake", region_name=REGION)
    sdk = Config(retries={"total_max_attempts": 1})
    if session.client("sts", config=sdk).get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong account")
    config = session.client("lambda", config=sdk).get_function_url_config(
        FunctionName=p["api"]
    )
    if config["AuthType"] != "AWS_IAM":
        raise ValueError("Function URL must require IAM")
    base = config["FunctionUrl"].rstrip("/")
    u = urlsplit(base)
    if (
        u.scheme != "https"
        or not u.hostname.endswith(".lambda-url.ap-southeast-2.on.aws")
        or u.username
        or u.password
        or u.query
        or u.fragment
    ):
        raise ValueError("Unexpected Lambda URL")
    http = requests.Session()
    http.trust_env = False
    results = []

    def call(path, signed=True):
        if len(results) >= 10:
            raise ValueError("Approved HTTP request limit exhausted")
        item = {"path": path, "signed": signed, "status": "UNCERTAIN"}
        results.append(item)
        output.write_text(
            json.dumps({"status": "RUNNING", "requests": results}, indent=2)
        )
        output.chmod(0o600)
        headers = {}
        if signed:
            request = AWSRequest(method="GET", url=base + path)
            SigV4Auth(
                session.get_credentials().get_frozen_credentials(), "lambda", REGION
            ).add_auth(request)
            headers = dict(request.headers)
        response = http.get(
            base + path, headers=headers, timeout=15, allow_redirects=False
        )
        item["status"] = response.status_code
        return response

    try:
        require(call("/api/v1/health", False).status_code == 403)
        health = call("/api/v1/health")
        require(
            health.status_code == 200
            and health.json()["backend"] == "s3_verified_aws_glue_export"
        )
        facts = call("/api/v1/gold/hourly?limit=100")
        require(facts.status_code == 200 and facts.json()["total"] == 72)
        reference = json.loads((a.packet / "reference.json").read_text())
        expected = Dataset(
            reference["facts"],
            reference["dimensions"],
            evidence_sha256=reference["evidence_sha256"],
        )
        same_rows(facts.json()["data"], expected.rows)
        require(facts.json()["metadata"]["provenance"]["database"] == p["database"])
        csv_response = call("/api/v1/gold/hourly?format=csv&limit=100")
        require(
            csv_response.status_code == 200
            and len(list(csv.DictReader(io.StringIO(csv_response.text)))) == 72
        )
        csv_rows = [
            {
                "location_id": int(r["location_id"]),
                "station_name": r["station_name"],
                "hour_start": r["hour_start"],
                "pm25_avg": float(r["pm25_avg"]),
                "aqi": int(r["aqi"]),
            }
            for r in csv.DictReader(io.StringIO(csv_response.text))
        ]
        same_rows(csv_rows, expected.rows)
        stations = call("/api/v1/stations")
        require(stations.status_code == 200 and stations.json()["count"] == 3)
        same_rows(stations.json()["data"], expected.stations)
        for location, expected in ((4946812, 25), (4946813, 23), (6123215, 24)):
            response = call(f"/api/v1/gold/hourly?location_id={location}&limit=100")
            require(
                response.status_code == 200 and response.json()["total"] == expected
            )
        output.write_text(
            json.dumps(
                {
                    "status": "PASS",
                    "requests": results,
                    "function_url": base,
                    "data_provenance": facts.json()["metadata"]["provenance"],
                },
                indent=2,
            )
            + "\n"
        )
        print(
            "PASS: authenticated real AWS HTTP JSON/CSV/stations/filter smoke; 8 requests; no Athena queries"
        )
    except Exception as error:  # noqa: BLE001 -- never log signed headers or raw SDK exceptions
        output.write_text(
            json.dumps(
                {
                    "status": "FAILED_OR_UNCERTAIN",
                    "requests": results,
                    "exception_type": type(error).__name__,
                },
                indent=2,
            )
            + "\n"
        )
        output.chmod(0o600)
        raise ValueError(
            "HTTP smoke stopped; inspect private receipt; no retry"
        ) from None


if __name__ == "__main__":
    main()
