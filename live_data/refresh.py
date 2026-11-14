"""Serialized, bounded refresh controller; uncertain writes require operator recovery."""

import json
import math
import os
import re
from datetime import timedelta

from ingestion.openaq import PILOTS, utc
from live_data.contract import (
    ACCOUNT,
    BUCKET,
    DATABASE,
    MAX_INVOCATIONS,
    MAX_RECORDS,
    NAME,
    PREFIX,
    REGION,
    encode,
    now_utc,
    sha,
)
from live_data.store import AWSStore, pack

CONTROLLER = NAME + "-refresh"
RESULT_BUCKET = "airlake-701364614078-athena-results"
ACTIVE_JOBS = {"STARTING", "RUNNING", "STOPPING", "WAITING"}


def version_database(digest):
    if not re.fullmatch(r"[a-f0-9]{64}", digest):
        raise ValueError("Invalid immutable input digest")
    return DATABASE + "_r" + digest[:16]


def allowed_database(value):
    return value == DATABASE or bool(
        isinstance(value, str)
        and re.fullmatch(re.escape(DATABASE) + r"_r[a-f0-9]{16}", value)
    )


def validate_export(raw, digest):
    if len(raw) > 500000:
        raise ValueError("Export size bound")
    value = json.loads(raw)
    p = value["provenance"]
    if (
        p.get("database") != version_database(digest)
        or p.get("execution") != "AWS_GLUE"
        or p.get("input_manifest_sha256") != digest
    ):
        raise ValueError("Export provenance mismatch")
    if p.get("aqi_method") != "simplified_hourly_mean_mvp; NOT official VN_AQI":
        raise ValueError("Unexpected AQI contract")
    utc(p["computed_at"])
    snapshots = p["snapshots"]
    if set(snapshots) != {
        "bronze",
        "silver",
        "breakpoints",
        "dimensions",
        "hourly",
    } or any(
        not isinstance(v, list) or len(v) != 1 or type(v[0]) is not int or v[0] <= 0
        for v in snapshots.values()
    ):
        raise ValueError("Unverified snapshot metadata")
    dimensions = value["dimensions"]
    if not 1 <= len(dimensions) <= len(PILOTS):
        raise ValueError("Dimension bound")
    names = {}
    for row in dimensions:
        loc, name = row["location_id"], row["name"]
        if (
            type(loc) is not int
            or loc not in PILOTS
            or loc in names
            or not isinstance(name, str)
            or not name.strip()
        ):
            raise ValueError("Invalid dimension key/name")
        names[loc] = name
    facts = value["facts"]
    if not 1 <= len(facts) <= 1000:
        raise ValueError("Fact bound")
    expected = {}
    for row in facts:
        if set(row) != {"location_id", "hour_start", "pm25_avg", "aqi"}:
            raise ValueError("Gold schema")
        loc, hour = row["location_id"], utc(row["hour_start"])
        key = (loc, hour)
        mean, aqi = row["pm25_avg"], row["aqi"]
        if (
            type(loc) is not int
            or loc not in names
            or key in expected
            or hour.minute
            or hour.second
            or hour.microsecond
        ):
            raise ValueError("Gold grain or join")
        if (
            type(mean) not in (int, float)
            or not math.isfinite(mean)
            or mean < 0
            or type(aqi) is not int
            or not 0 <= aqi <= 500
        ):
            raise ValueError("Invalid finite Gold value")
        expected[key] = (mean, aqi, names[loc])
    return value, expected


def query_sql(value, digest):
    _, expected = validate_export(encode(value), digest)
    start = min(k[1] for k in expected).strftime("%Y-%m-%d %H:%M:%S UTC")
    end = (max(k[1] for k in expected) + timedelta(hours=1)).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    db = version_database(digest)
    sn = value["provenance"]["snapshots"]
    return (
        "SELECT f.location_id, to_iso8601(f.hour_start AT TIME ZONE 'UTC') AS hour_start, f.pm25_avg, f.aqi, d.name "
        f'FROM "AwsDataCatalog"."{db}"."pm25_aqi_hourly" FOR VERSION AS OF {sn["hourly"][0]} f '
        f'LEFT JOIN "AwsDataCatalog"."{db}"."dim_location" FOR VERSION AS OF {sn["dimensions"][0]} d ON f.location_id=d.location_id '
        f"WHERE f.hour_start >= TIMESTAMP '{start}' AND f.hour_start < TIMESTAMP '{end}' "
        "AND f.location_id IN (4946812,4946813,6123215) ORDER BY f.location_id, f.hour_start LIMIT 1001"
    )


def reconcile(c, query_id, expected):
    actual = {}
    header = True
    for page in (
        c["athena"]
        .get_paginator("get_query_results")
        .paginate(QueryExecutionId=query_id)
    ):
        columns = page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        if [(x["Name"], x["Type"]) for x in columns] != [
            ("location_id", "integer"),
            ("hour_start", "varchar"),
            ("pm25_avg", "double"),
            ("aqi", "integer"),
            ("name", "varchar"),
        ]:
            raise ValueError("Athena schema mismatch")
        for row in page["ResultSet"]["Rows"]:
            if header:
                header = False
                continue
            if len(actual) >= 1000:
                raise ValueError("Athena row bound")
            cols = [d.get("VarCharValue") for d in row["Data"]]
            key = (int(cols[0]), utc(cols[1]))
            mean, aqi = float(cols[2]), int(cols[3])
            if key in actual or not math.isfinite(mean) or mean < 0:
                raise ValueError("Athena duplicate/nonfinite row")
            actual[key] = (mean, aqi, cols[4])
    if set(actual) != set(expected) or any(
        abs(actual[k][0] - expected[k][0]) > 1e-10 or actual[k][1:] != expected[k][1:]
        for k in expected
    ):
        raise ValueError("Independent Athena reconciliation failed")


def schedule_args(schedule):
    if (
        schedule["Name"] != NAME + "-5m"
        or schedule.get("GroupName", "default") != "default"
    ):
        raise ValueError("Wrong schedule")
    return {
        k: schedule[k]
        for k in (
            "Name",
            "GroupName",
            "ScheduleExpression",
            "ScheduleExpressionTimezone",
            "FlexibleTimeWindow",
            "Target",
            "StartDate",
            "EndDate",
        )
    }


def disable(c):
    """Attempt every stop even if another fails; never delete or reset state."""
    failures = []
    try:
        s = c["scheduler"].get_schedule(Name=NAME + "-5m", GroupName="default")
        c["scheduler"].update_schedule(**schedule_args(s), State="DISABLED")
    except Exception:  # noqa: BLE001 -- preserve all stop attempts and sanitize SDK errors
        failures.append("schedule")
    for function in (NAME + "-poller", CONTROLLER):
        try:
            c["lambda"].put_function_concurrency(
                FunctionName=function, ReservedConcurrentExecutions=0
            )
        except Exception:  # noqa: BLE001 -- preserve all stop attempts and sanitize SDK errors
            failures.append(function)
    if failures:
        raise RuntimeError(
            "Some stop operations unconfirmed; inspect exact scoped states"
        )


class Controller:
    def __init__(self, c, window, deadline, production):
        if not re.fullmatch(r"[a-f0-9]{64}", window):
            raise ValueError("Window must be the approved operations plan hash")
        self.c, self.window, self.deadline, self.production = (
            c,
            window,
            utc(deadline),
            production,
        )
        self.store = AWSStore(c)

    def read(self, sk):
        return self.store.read("OPS", sk)

    def save(self, state):
        self.c["dynamodb"].put_item(
            TableName=NAME + "-state",
            Item=pack({"pk": "OPS", "sk": self.window, **state}),
        )

    def freeze(self):
        objects = []
        observed = []
        for page in (
            self.c["s3"]
            .get_paginator("list_objects_v2")
            .paginate(
                Bucket=BUCKET, Prefix=PREFIX + "bronze/", ExpectedBucketOwner=ACCOUNT
            )
        ):
            for obj in page.get("Contents", []):
                if len(objects) >= MAX_INVOCATIONS * MAX_RECORDS:
                    raise ValueError("Raw count exceeds original approved bound")
                key = obj["Key"]
                if not re.fullmatch(
                    re.escape(PREFIX) + r"bronze/[a-f0-9]{64}\.json", key
                ):
                    raise ValueError("Unexpected raw object")
                raw = (
                    self.c["s3"]
                    .get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)[
                        "Body"
                    ]
                    .read(100001)
                )
                if len(raw) > 100000:
                    raise ValueError("Raw size cap")
                envelope = json.loads(raw)
                if (
                    envelope["record_sha256"] != sha(encode(envelope["record"]))
                    or envelope["observed_at"] != envelope["record"]["datetime"]
                ):
                    raise ValueError("Accepted raw provenance mismatch")
                observed.append(utc(envelope["observed_at"]))
                utc(envelope["ingested_at"])
                objects.append({"key": key, "sha256": sha(raw)})
        if not objects:
            return None
        self.input_watermark = max(observed).isoformat()
        raw = encode(
            {
                "schema_version": 1,
                "objects": sorted(objects, key=lambda x: x["key"]),
                "production_sha256": self.production,
            }
        )
        return raw, sha(raw)

    def step(self, state, now):
        # Never repeat a write whose outcome was not durably recorded.
        if state.get("phase") in {
            "STARTING_GLUE",
            "STARTING_QUERY",
            "PROMOTING",
            "FAILED_OR_UNCERTAIN",
        }:
            raise ValueError(
                "Uncertain previous operation; operator reconciliation required"
            )
        if state.get("poll_in_progress"):
            raise ValueError("Previous poll outcome uncertain")
        lease = self.store.read("RUN", "LEASE")
        if (
            lease.get("active")
            or lease.get("halted")
            or lease.get("lease_until", 0) != 0
        ):
            raise ValueError("Poller active/halted/uncertain; no automatic recovery")
        # Once complete, no further Glue job or Athena query in this window.
        if state.get("phase") == "COMPLETE":
            return state
        if lease.get("attempts", 0) < MAX_INVOCATIONS:
            for sensor in PILOTS.values():
                cursor = self.store.cursor(sensor)
                if (
                    not cursor.get("pending_end")
                    and cursor.get("watermark")
                    and utc(cursor["watermark"]) < now - timedelta(minutes=1, hours=24)
                ):
                    raise ValueError(
                        "Watermark gap exceeds bounded lookback; approved backfill required"
                    )
            state["poll_in_progress"] = True
            self.save(state)
            response = self.c["lambda"].invoke(
                FunctionName=NAME + "-poller",
                InvocationType="RequestResponse",
                Payload=b"{}",
            )
            report = json.loads(response["Payload"].read(100001))
            if response.get("FunctionError") or report.get("status") != "PASS":
                raise ValueError("Poll failed or uncertain; do not repeat")
            state["poll_in_progress"] = False
            state["last_poll"] = {
                k: report[k]
                for k in ("accepted_new", "replayed", "rejected", "polled_at")
            }
            # Fresh lock check prevents freezing an ambiguous/active poll.
            lease = self.store.read("RUN", "LEASE")
            if (
                lease.get("active")
                or lease.get("halted")
                or lease.get("lease_until", 0) != 0
            ):
                raise ValueError("Poll did not release its durable lease")
        if state.get("phase") in {"GLUE_RUNNING", "QUERY_RUNNING"}:
            return self.advance(state)
        frozen = self.freeze()
        current = self.store.read("EXPORT", "CURRENT")
        if (
            not frozen
            or current.get("key") == PREFIX + "exports/" + frozen[1] + "/gold.json"
        ):
            state["phase"] = "NO_NEW_DATA"
            return state
        if self.deadline - now < timedelta(minutes=15):
            state["phase"] = "DEFERRED_NEAR_DEADLINE"
            return state
        runs = self.c["glue"].get_job_runs(JobName=NAME + "-gold", MaxResults=10)[
            "JobRuns"
        ]
        if any(r["JobRunState"] in ACTIVE_JOBS for r in runs):
            raise ValueError("Existing Glue execution; no overlapping run")
        raw, digest = frozen
        database = version_database(digest)
        from final_demo.control import absent

        absent(
            lambda: self.c["glue"].get_database(Name=database),
            {"EntityNotFoundException"},
        )
        # Input key is specific to this window. A prior attempted upload is not overwritten.
        key = PREFIX + "operations/" + self.window + "/manifest.json"
        state.update(
            input_watermark=self.input_watermark,
            phase="STARTING_GLUE",
            manifest_sha256=digest,
            manifest_key=key,
            previous=current,
            source_count=len(json.loads(raw)["objects"]),
        )
        self.save(state)
        self.c["s3"].put_object(
            Bucket=BUCKET,
            Key=key,
            Body=raw,
            ExpectedBucketOwner=ACCOUNT,
            ServerSideEncryption="AES256",
        )
        run = self.c["glue"].start_job_run(
            JobName=NAME + "-gold",
            Arguments={
                "--manifest_key": key,
                "--manifest_sha256": digest,
                "--refresh_database": database,
            },
        )
        state.update(phase="GLUE_RUNNING", job_run_id=run["JobRunId"])
        return state

    def advance(self, state):
        digest = state["manifest_sha256"]
        if state["phase"] == "GLUE_RUNNING":
            job = self.c["glue"].get_job_run(
                JobName=NAME + "-gold",
                RunId=state["job_run_id"],
                PredecessorsIncluded=False,
            )["JobRun"]
            if job["JobRunState"] in ACTIVE_JOBS:
                return state
            if (
                job["JobRunState"] != "SUCCEEDED"
                or job.get("Arguments", {}).get("--manifest_sha256") != digest
                or job.get("Arguments", {}).get("--refresh_database")
                != version_database(digest)
            ):
                raise ValueError("Glue failed or scope differs")
            key = PREFIX + "exports/" + digest + "/gold.json"
            raw = (
                self.c["s3"]
                .get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)["Body"]
                .read(500001)
            )
            value, _ = validate_export(raw, digest)
            proof_raw = (
                self.c["s3"]
                .get_object(
                    Bucket=BUCKET,
                    Key=PREFIX + "evidence/" + digest + ".json",
                    ExpectedBucketOwner=ACCOUNT,
                )["Body"]
                .read(100001)
            )
            if len(proof_raw) > 100000:
                raise ValueError("Glue validation evidence too large")
            proof = json.loads(proof_raw)
            gold = proof["gold"]
            if (
                proof.get("status") != "PASS"
                or proof.get("source_count") != state["source_count"]
                or proof["silver"]["output_count"] != state["source_count"]
                or proof["snapshots_before"] != proof["snapshots_after"]
                or proof["snapshots_after"] != value["provenance"]["snapshots"]
                or proof["export_sha256"] != sha(raw)
                or gold["hourly_output_count"] != len(value["facts"])
                or gold["dimension_output_count"] != len(value["dimensions"])
                or any(
                    gold[k]
                    for k in (
                        "orphan_location_count",
                        "duplicate_hourly_key_count",
                        "duplicate_dimension_key_count",
                        "invalid_pm25_count",
                    )
                )
            ):
                raise ValueError("Glue count/integrity/idempotency evidence mismatch")
            sql = query_sql(value, digest)
            group = self.c["athena"].get_work_group(WorkGroup="airlake")["WorkGroup"]
            cfg = group["Configuration"]
            if (
                group["State"] != "ENABLED"
                or not cfg["EnforceWorkGroupConfiguration"]
                or cfg["ResultConfiguration"]["OutputLocation"]
                != f"s3://{RESULT_BUCKET}/"
                or cfg["EngineVersion"]["EffectiveEngineVersion"]
                != "Athena engine version 3"
            ):
                raise ValueError("Unexpected Athena workgroup")
            state.update(phase="STARTING_QUERY", export_sha256=sha(raw), export_key=key)
            self.save(state)
            q = self.c["athena"].start_query_execution(
                QueryString=sql,
                WorkGroup="airlake",
                QueryExecutionContext={
                    "Catalog": "AwsDataCatalog",
                    "Database": version_database(digest),
                },
                ClientRequestToken=sha((self.window + digest).encode()),
                ResultReuseConfiguration={
                    "ResultReuseByAgeConfiguration": {"Enabled": False}
                },
            )
            state.update(phase="QUERY_RUNNING", query_id=q["QueryExecutionId"])
            return state
        query = self.c["athena"].get_query_execution(
            QueryExecutionId=state["query_id"]
        )["QueryExecution"]
        status = query["Status"]["State"]
        if status in {"RUNNING", "QUEUED"}:
            return state
        raw = (
            self.c["s3"]
            .get_object(
                Bucket=BUCKET, Key=state["export_key"], ExpectedBucketOwner=ACCOUNT
            )["Body"]
            .read(500001)
        )
        value, expected = validate_export(raw, digest)
        if (
            status != "SUCCEEDED"
            or sha(raw) != state["export_sha256"]
            or query.get("Query") != query_sql(value, digest)
            or query.get("WorkGroup") != "airlake"
            or query.get("QueryExecutionContext", {}).get("Database")
            != version_database(digest)
        ):
            raise ValueError("Athena failed or scope/export changed")
        reconcile(self.c, state["query_id"], expected)
        p = state["previous"]
        if not p.get("key") or not p.get("sha256"):
            raise ValueError("Expected verified previous pointer")
        pointer = {
            "pk": "EXPORT",
            "sk": "CURRENT",
            "key": state["export_key"],
            "sha256": sha(raw),
            "database": version_database(digest),
            "athena_query_id": state["query_id"],
            "snapshots": value["provenance"]["snapshots"],
            "previous": p,
            "refresh_window": self.window,
        }
        # Keep a bounded one-level predecessor; older versions remain in OPS history and S3.
        pointer["previous"] = {k: v for k, v in p.items() if k != "previous"}
        state["phase"] = "PROMOTING"
        self.save(state)
        self.c["dynamodb"].put_item(
            TableName=NAME + "-state",
            Item=pack(pointer),
            ConditionExpression="#key=:key AND #sha=:sha",
            ExpressionAttributeNames={"#key": "key", "#sha": "sha256"},
            ExpressionAttributeValues=pack({":key": p["key"], ":sha": p["sha256"]}),
        )
        state.update(
            phase="COMPLETE",
            snapshots=value["provenance"]["snapshots"],
            verified_rows=len(expected),
            query_statistics=query["Statistics"],
            result_object=query["ResultConfiguration"]["OutputLocation"],
        )
        return state

    def tick(self, now):
        if now >= self.deadline:
            disable(self.c)
            return {"phase": "STOP_DEADLINE"}
        owner = self.window
        # No lease-expiry takeover: a timed-out controller is uncertain, not permission to repeat writes.
        try:
            self.c["dynamodb"].update_item(
                TableName=NAME + "-state",
                Key=pack({"pk": "OPS", "sk": "LOCK"}),
                UpdateExpression="SET active=:yes, owner_id=:owner",
                ConditionExpression="attribute_not_exists(active) OR active=:no",
                ExpressionAttributeValues=pack(
                    {":yes": True, ":no": False, ":owner": owner}
                ),
            )
        except Exception:  # noqa: BLE001 -- an active lock is uncertainty, not permission to retry
            disable(self.c)
            raise RuntimeError(
                "Controller lock unavailable; stopped without retry"
            ) from None
        try:
            state = self.read(self.window)
            if int(state.get("ticks", 0)) >= 12:
                raise ValueError("Controller invocation budget exhausted")
            state = {k: v for k, v in state.items() if k not in ("pk", "sk")}
            state["ticks"] = int(state.get("ticks", 0)) + 1
            self.save(state)
            state = self.step(state, now)
            state["last_controller_success_at"] = now.isoformat()
            self.save(state)
            if (
                state.get("phase") == "COMPLETE"
                or (
                    state.get("phase") == "NO_NEW_DATA"
                    and self.store.read("RUN", "LEASE").get("attempts", 0)
                    >= MAX_INVOCATIONS
                )
                or now + timedelta(minutes=5) >= self.deadline
            ):
                disable(self.c)
            self.c["dynamodb"].update_item(
                TableName=NAME + "-state",
                Key=pack({"pk": "OPS", "sk": "LOCK"}),
                UpdateExpression="SET active=:no",
                ConditionExpression="owner_id=:owner",
                ExpressionAttributeValues=pack({":no": False, ":owner": owner}),
            )
            return {
                k: state[k]
                for k in (
                    "phase",
                    "ticks",
                    "source_count",
                    "verified_rows",
                    "job_run_id",
                    "query_id",
                )
                if k in state
            }
        except Exception:  # noqa: BLE001 -- preserve all stop attempts and sanitize SDK errors
            # The saved phase survives; lock remains active for explicit recovery.
            try:
                disable(self.c)
            finally:
                print(
                    json.dumps(
                        {
                            "metric": "AirLakeRefresh",
                            "status": "FAILED_OR_UNCERTAIN",
                            "window": self.window,
                        }
                    )
                )
            raise RuntimeError(
                "Refresh stopped; retain ledger and inspect before recovery"
            ) from None


def handler(event, context):
    import boto3
    from botocore.config import Config

    if event != {} or os.environ.get("AIRLAKE_LIVE_RUN") != NAME.removeprefix(
        "airlake-live-"
    ):
        raise ValueError("Wrong invocation scope")
    now = now_utc()
    session = boto3.Session(region_name=REGION)
    cfg = Config(retries={"total_max_attempts": 1}, connect_timeout=3, read_timeout=200)
    c = {
        name: session.client(name, config=cfg)
        for name in ("dynamodb", "s3", "lambda", "scheduler", "glue", "athena")
    }
    value = Controller(
        c,
        os.environ["AIRLAKE_OPS_WINDOW"],
        os.environ["AIRLAKE_OPS_DEADLINE"],
        json.loads(os.environ["AIRLAKE_PRODUCTION_SHA256"]),
    ).tick(now)
    print(
        json.dumps(
            {
                "_aws": {
                    "Timestamp": int(now.timestamp() * 1000),
                    "CloudWatchMetrics": [
                        {
                            "Namespace": "AirLake/LiveDemo",
                            "Dimensions": [["Run"]],
                            "Metrics": [{"Name": "RefreshComplete", "Unit": "Count"}],
                        }
                    ],
                },
                "Run": NAME.removeprefix("airlake-live-"),
                "RefreshComplete": int(value.get("phase") == "COMPLETE"),
                "metric": "AirLakeRefresh",
                **value,
            }
        )
    )
    return value
