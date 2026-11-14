"""Read-only scoped runtime evidence; never reads secrets, Athena or notifications."""

import json

from live_data.contract import BUCKET, NAME, PREFIX
from live_data.store import AWSStore


def collect(c):
    from ingestion.openaq import PILOTS

    result = {"mode": "READ_ONLY", "functions": {}, "state": {}, "s3_objects": []}
    for suffix in ("poller", "api"):
        f = c["lambda"].get_function_configuration(FunctionName=NAME + "-" + suffix)
        result["functions"][suffix] = {
            k: f[k]
            for k in (
                "FunctionName",
                "State",
                "LastUpdateStatus",
                "Runtime",
                "Timeout",
                "MemorySize",
                "CodeSha256",
                "Role",
            )
        }
        result["functions"][suffix]["environment_names"] = sorted(
            f.get("Environment", {}).get("Variables", {})
        )
        result["functions"][suffix]["concurrency"] = c[
            "lambda"
        ].get_function_concurrency(FunctionName=NAME + "-" + suffix)[
            "ReservedConcurrentExecutions"
        ]
    schedule = c["scheduler"].get_schedule(Name=NAME + "-5m", GroupName="default")
    result["scheduler"] = {
        k: schedule[k]
        for k in ("Name", "State", "ScheduleExpression", "StartDate", "EndDate")
    }
    store = AWSStore(c)
    run = store.read("RUN", "LEASE")
    result["run"] = {
        k: run[k] for k in ("attempts", "lease_until", "halted", "active") if k in run
    }
    for loc in PILOTS:
        latest = store.read("LATEST", str(loc))
        poll = store.read("POLL", str(loc))
        result["state"][str(loc)] = {
            "observed_at": latest.get("observed_at"),
            "ingested_at": latest.get("ingested_at"),
            "poll_status": poll.get("status"),
            "last_poll_at": poll.get("last_poll_at"),
        }
    result["api_url"] = c["lambda"].get_function_url_config(FunctionName=NAME + "-api")[
        "FunctionUrl"
    ]
    for page in (
        c["s3"].get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX)
    ):
        result["s3_objects"].extend(
            {"key": x["Key"], "bytes": x["Size"]} for x in page.get("Contents", [])
        )
        if len(result["s3_objects"]) > 2000:
            raise ValueError("Unexpected object inventory size")
    log = c["logs"].filter_log_events(
        logGroupName="/aws/lambda/" + NAME + "-poller",
        filterPattern='"AirLakePoll"',
        limit=30,
    )
    result["poll_log_statuses"] = []
    for event in log.get("events", []):
        try:
            value = json.loads(event["message"])
        except ValueError:
            continue
        result["poll_log_statuses"].append(
            {
                k: value[k]
                for k in (
                    "metric",
                    "accepted_new",
                    "replayed",
                    "rejected",
                    "status",
                    "exception_type",
                )
                if k in value
            }
        )
    return result
