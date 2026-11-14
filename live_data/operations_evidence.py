"""Scoped status and separately approved three-read API verification; no SQL."""

import csv
import io
import json
from urllib.parse import urlsplit

from final_demo.control import Receipt
from live_data.api import plain
from live_data.contract import ACCOUNT, NAME, REGION, freshness, now_utc
from live_data.refresh import CONTROLLER
from live_data.store import AWSStore
from live_data.view import render


def status(c, window):
    from ingestion.openaq import PILOTS

    store = AWSStore(c)
    schedule = c["scheduler"].get_schedule(Name=NAME + "-5m", GroupName="default")
    state = store.read("OPS", window)
    result = {
        "mode": "READ_ONLY",
        "scheduler": schedule["State"],
        "concurrency": {
            n: c["lambda"].get_function_concurrency(FunctionName=n)[
                "ReservedConcurrentExecutions"
            ]
            for n in (NAME + "-poller", CONTROLLER)
        },
        "run": {
            k: v
            for k, v in store.read("RUN", "LEASE").items()
            if k in ("active", "halted", "attempts", "lease_until")
        },
        "controller_lock_active": store.read("OPS", "LOCK").get("active", False),
        "refresh": {
            k: v
            for k, v in state.items()
            if k
            in (
                "phase",
                "ticks",
                "last_poll",
                "last_controller_success_at",
                "manifest_sha256",
                "input_watermark",
                "source_count",
                "job_run_id",
                "query_id",
                "snapshots",
                "verified_rows",
                "query_statistics",
                "result_object",
            )
        },
        "export": {
            k: v
            for k, v in store.read("EXPORT", "CURRENT").items()
            if k not in ("previous", "pk", "sk")
        },
        "stations": [],
    }
    for loc in PILOTS:
        row = store.read("LATEST", str(loc))
        result["stations"].append(
            {
                "location_id": loc,
                "observed_at": row.get("observed_at"),
                "ingested_at": row.get("ingested_at"),
                "freshness": freshness(
                    row.get("observed_at"), now_utc(), row.get("cadence_seconds")
                ),
            }
        )
    if state.get("job_run_id"):
        run = c["glue"].get_job_run(
            JobName=NAME + "-gold",
            RunId=state["job_run_id"],
            PredecessorsIncluded=False,
        )["JobRun"]
        result["glue"] = {
            k: run[k]
            for k in ("Id", "JobRunState", "ExecutionTime", "DPUSeconds")
            if k in run
        }
    return plain(result)


def smoke(c, packet, digest, get=None):
    """Exactly three signed GETs, no retries. The caller enforces write/cost approval."""
    fn = c["lambda"].get_function_url_config(FunctionName=NAME + "-api")
    host = urlsplit(fn["FunctionUrl"])
    if (
        fn["AuthType"] != "AWS_IAM"
        or fn.get("Cors")
        or host.scheme != "https"
        or not host.hostname.endswith(".lambda-url.ap-southeast-2.on.aws")
    ):
        raise ValueError("Private API URL/authentication differs")
    if get is None:
        import boto3
        import requests
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest

        session = boto3.Session(profile_name="airlake", region_name=REGION)
        http = requests.Session()
        http.trust_env = False

        def get(path):
            url = fn["FunctionUrl"].rstrip("/") + path
            req = AWSRequest(method="GET", url=url)
            SigV4Auth(
                session.get_credentials().get_frozen_credentials(), "lambda", REGION
            ).add_auth(req)
            return http.get(
                url, headers=dict(req.headers), timeout=20, allow_redirects=False
            )

    r = Receipt(packet / "operations-http-receipt.json", digest)
    r.value["requests"] = []
    responses = []
    for path in (
        "/api/v1/latest",
        "/api/v1/current/hourly?limit=100",
        "/api/v1/current/hourly?limit=100&format=csv",
    ):
        item = {"path": path, "status": "UNCERTAIN"}
        r.value["requests"].append(item)
        r.save()
        response = get(path)
        item["status"] = response.status_code
        r.save()
        if response.status_code != 200:
            raise ValueError("HTTP check failed; no automatic retry")
        responses.append(response)
    latest, current = responses[0].json(), responses[1].json()
    if (
        latest["count"] != 3
        or latest["metadata"]["poll_interval_is_not_source_freshness"] is not True
    ):
        raise ValueError("Latest schema/classification mismatch")
    from ingestion.openaq import PILOTS

    if (
        len(latest["data"]) != 3
        or {r["location_id"] for r in latest["data"]} != set(PILOTS)
        or any(
            r["freshness"]["status"]
            not in {"recent", "stale", "no_data", "invalid_future"}
            for r in latest["data"]
        )
    ):
        raise ValueError("Latest station coverage/status mismatch")
    pointer = AWSStore(c).read("EXPORT", "CURRENT")
    if (
        current["metadata"]["export_sha256"] != pointer["sha256"]
        or current["metadata"]["athena_query_id"] != pointer["athena_query_id"]
    ):
        raise ValueError("API did not serve independently verified pointer")
    # The bounded API pages at 100. Larger exports require another approved page budget.
    if (
        current["next_offset"] is not None
        or current["count"] != current["total"]
        or not current["data"]
    ):
        raise ValueError(
            "More than one API page; request separate verification approval"
        )
    rows = list(csv.DictReader(io.StringIO(responses[2].text)))
    expected = {(str(x["location_id"]), x["hour_start"]): x for x in current["data"]}
    if (
        len(rows) != len(expected)
        or {(x["location_id"], x["hour_start"]) for x in rows} != set(expected)
        or any(
            (x["location_id"], x["hour_start"]) not in expected
            or abs(
                float(x["pm25_avg"])
                - expected[(x["location_id"], x["hour_start"])]["pm25_avg"]
            )
            > 1e-10
            or int(x["aqi"]) != expected[(x["location_id"], x["hour_start"])]["aqi"]
            for x in rows
        )
    ):
        raise ValueError("CSV/JSON values differ")
    (packet / "latest-api.json").write_text(json.dumps(latest, indent=2))
    (packet / "latest-api.html").write_text(render(latest))
    (packet / "latest-api.json").chmod(0o600)
    (packet / "latest-api.html").chmod(0o600)
    r.value.update(
        status="PASS_THREE_API_READS",
        export_sha256=pointer["sha256"],
        query_id=pointer["athena_query_id"],
        hourly_count=current["count"],
        aws_account=ACCOUNT,
    )
    r.save()
    return r.value
