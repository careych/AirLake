"""One frozen batch and one Athena acceptance; export pointer changes last."""

import json
import math
import time
from datetime import datetime, timedelta

from final_demo.control import Receipt
from final_demo.prepare import job_definition
from live_data.contract import (
    ACCOUNT,
    BUCKET,
    DATABASE,
    MAX_INVOCATIONS,
    MAX_RECORDS,
    NAME,
    PREFIX,
    RUN,
    encode,
    sha,
)
from live_data.store import pack, unpack


def frozen_scope(c):
    schedule = c["scheduler"].get_schedule(Name=NAME + "-5m", GroupName="default")
    concurrency = c["lambda"].get_function_concurrency(FunctionName=NAME + "-poller")
    if (
        schedule["State"] != "DISABLED"
        or concurrency["ReservedConcurrentExecutions"] != 0
    ):
        raise ValueError("Disable polling before freezing source")
    lease = (
        c["dynamodb"]
        .get_item(
            TableName=NAME + "-state",
            Key=pack({"pk": "RUN", "sk": "LEASE"}),
            ConsistentRead=True,
        )
        .get("Item")
    )
    if not lease or unpack(lease).get("lease_until") != 0:
        raise ValueError(
            "An invocation may still be active or uncertain; inspect first"
        )


def freeze(c, packet, p, digest):
    frozen_scope(c)
    r = Receipt(packet / "glue-receipt.json", digest)
    objects = []
    for page in (
        c["s3"]
        .get_paginator("list_objects_v2")
        .paginate(Bucket=BUCKET, Prefix=PREFIX + "bronze/", ExpectedBucketOwner=ACCOUNT)
    ):
        for obj in page.get("Contents", []):
            if len(objects) >= MAX_RECORDS * MAX_INVOCATIONS:
                raise ValueError("Raw record cap exceeded")
            raw = (
                c["s3"]
                .get_object(Bucket=BUCKET, Key=obj["Key"], ExpectedBucketOwner=ACCOUNT)[
                    "Body"
                ]
                .read(100001)
            )
            if len(raw) > 100000:
                raise ValueError("Raw record size cap")
            objects.append({"key": obj["Key"], "sha256": sha(raw)})
    if not objects:
        raise ValueError("No accepted real source data; no empty Glue run")
    manifest = {
        "schema_version": 1,
        "objects": objects,
        "production_sha256": p["production_sha256"],
    }
    raw = encode(manifest)
    h = sha(raw)
    key = PREFIX + "manifests/" + h + ".json"
    (packet / "raw-manifest.json").write_bytes(raw)
    r.write(
        "freeze_manifest",
        lambda: c["s3"].put_object(
            Bucket=BUCKET,
            Key=key,
            Body=raw,
            ExpectedBucketOwner=ACCOUNT,
            IfNoneMatch="*",
            ServerSideEncryption="AES256",
        ),
    )
    for name in ["payload.zip", "runtime.jar", "aws-bundle.jar", "glue_job.py"]:
        r.write(
            "upload_" + name,
            lambda n=name: c["s3"].put_object(
                Bucket=BUCKET,
                Key=PREFIX + "artifacts/" + n,
                Body=(packet / n).read_bytes(),
                ExpectedBucketOwner=ACCOUNT,
                IfNoneMatch="*",
                ServerSideEncryption="AES256",
            ),
        )
    definition = job_definition(
        {
            "job": NAME + "-gold",
            "glue_role": NAME + "-glue",
            "run": RUN,
            "database": DATABASE,
            "prefix": PREFIX,
            "bucket": BUCKET,
            "log_prefix": "/airlake/live-demo/" + RUN,
        },
        digest,
    )
    args = definition["NonOverridableArguments"]
    args.pop("--plan_uri")
    args.pop("--plan_sha256")
    args.update({"--manifest_key": key, "--manifest_sha256": h})
    args["--conf"] = args["--conf"].replace(
        "spark.sql.catalog.demo", "spark.sql.catalog.live"
    )
    r.write("create_live_glue_job", lambda: c["glue"].create_job(**definition))
    result = r.write(
        "one_glue_run", lambda: c["glue"].start_job_run(JobName=NAME + "-gold")
    )
    r.value.update(
        job_run_id=result["JobRunId"],
        manifest_key=key,
        manifest_sha256=h,
        source_count=len(objects),
        status="STARTED",
    )
    r.save()
    return r.value


def verify(c, packet, p, digest):
    receipt = json.loads((packet / "glue-receipt.json").read_text())
    job = c["glue"].get_job_run(
        JobName=NAME + "-gold", RunId=receipt["job_run_id"], PredecessorsIncluded=False
    )["JobRun"]
    if job["JobRunState"] != "SUCCEEDED":
        raise ValueError("Glue is not SUCCEEDED; preserve evidence; no retry")
    key = PREFIX + "exports/" + receipt["manifest_sha256"] + "/gold.json"
    raw = (
        c["s3"]
        .get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)["Body"]
        .read(500001)
    )
    if len(raw) > 500000:
        raise ValueError("Export too large")
    value = json.loads(raw)
    facts = value["facts"]
    dims = value["dimensions"]
    sn = value["provenance"]["snapshots"]
    expected = {
        (x["location_id"], datetime.fromisoformat(x["hour_start"])): (
            x["pm25_avg"],
            x["aqi"],
        )
        for x in facts
    }
    if not expected or len(expected) != len(facts) or len(facts) > 1000:
        raise ValueError("Gold count/grain")
    if (
        value["provenance"].get("database") != DATABASE
        or value["provenance"].get("execution") != "AWS_GLUE"
    ):
        raise ValueError("Unexpected export provenance")
    for table in ("hourly", "dimensions"):
        if len(sn[table]) != 1 or type(sn[table][0]) is not int or sn[table][0] <= 0:
            raise ValueError("Invalid Iceberg snapshot")
    start = min(k[1] for k in expected).strftime("%Y-%m-%d %H:%M:%S UTC")
    end = (max(k[1] for k in expected) + timedelta(hours=1)).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    sql = (
        f"SELECT f.location_id, to_iso8601(f.hour_start AT TIME ZONE 'UTC') AS hour_start, f.pm25_avg, f.aqi, d.name "
        f'FROM "AwsDataCatalog"."{DATABASE}"."pm25_aqi_hourly" FOR VERSION AS OF {sn["hourly"][0]} f '
        f'LEFT JOIN "AwsDataCatalog"."{DATABASE}"."dim_location" FOR VERSION AS OF {sn["dimensions"][0]} d ON f.location_id=d.location_id '
        f"WHERE f.hour_start >= TIMESTAMP '{start}' AND f.hour_start < TIMESTAMP '{end}' "
        "AND f.location_id IN (4946812,4946813,6123215) ORDER BY f.location_id, f.hour_start LIMIT 1001"
    )
    group = c["athena"].get_work_group(WorkGroup="airlake")["WorkGroup"]
    if (
        group["State"] != "ENABLED"
        or not group["Configuration"]["EnforceWorkGroupConfiguration"]
        or group["Configuration"]["ResultConfiguration"]["OutputLocation"]
        != "s3://airlake-701364614078-athena-results/"
    ):
        raise ValueError("Unexpected Athena workgroup")
    r = Receipt(packet / "athena-promotion-receipt.json", digest)
    q = r.write(
        "one_athena_select",
        lambda: c["athena"].start_query_execution(
            QueryString=sql,
            WorkGroup="airlake",
            QueryExecutionContext={"Catalog": "AwsDataCatalog", "Database": DATABASE},
            ResultReuseConfiguration={
                "ResultReuseByAgeConfiguration": {"Enabled": False}
            },
        ),
    )
    r.value["query_id"] = q["QueryExecutionId"]
    r.save()
    for _ in range(60):
        query = c["athena"].get_query_execution(QueryExecutionId=r.value["query_id"])[
            "QueryExecution"
        ]
        state = query["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state not in ("RUNNING", "QUEUED"):
            raise ValueError("Athena failed; no retry")
        time.sleep(1)
    else:
        raise TimeoutError("Query may still run; do not resubmit")
    if (
        query.get("Query") != sql
        or query.get("WorkGroup") != "airlake"
        or query.get("QueryExecutionContext", {}).get("Database") != DATABASE
    ):
        raise ValueError("Returned Athena query scope differs")
    found = []
    for page in (
        c["athena"]
        .get_paginator("get_query_results")
        .paginate(QueryExecutionId=r.value["query_id"])
    ):
        if len(found) > 1001:
            raise ValueError("Athena row bound")
        columns = page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        if [(x["Name"], x["Type"]) for x in columns] != [
            ("location_id", "integer"),
            ("hour_start", "varchar"),
            ("pm25_avg", "double"),
            ("aqi", "integer"),
            ("name", "varchar"),
        ]:
            raise ValueError("Athena schema mismatch")
        found.extend(page["ResultSet"]["Rows"])
    names = {d["location_id"]: d["name"] for d in dims}
    actual = {}
    for row in found[1:]:
        cols = [d.get("VarCharValue") for d in row["Data"]]
        loc = int(cols[0])
        hour = datetime.fromisoformat(cols[1])
        if cols[4] != names.get(loc) or (loc, hour) in actual:
            raise ValueError("Athena join or grain mismatch")
        mean = float(cols[2])
        if not math.isfinite(mean) or mean < 0:
            raise ValueError("Athena returned invalid PM2.5")
        actual[(loc, hour)] = (mean, int(cols[3]))
    if set(actual) != set(expected) or any(
        abs(actual[k][0] - expected[k][0]) > 1e-10 or actual[k][1] != expected[k][1]
        for k in expected
    ):
        raise ValueError("Athena result mismatch")
    pointer = {
        "pk": "EXPORT",
        "sk": "CURRENT",
        "key": key,
        "sha256": sha(raw),
        "athena_query_id": r.value["query_id"],
        "snapshots": sn,
    }
    r.write(
        "promote_verified_version",
        lambda: c["dynamodb"].put_item(
            TableName=NAME + "-state",
            Item=pack(pointer),
            ConditionExpression="attribute_not_exists(pk)",
        ),
    )
    r.value.update(
        status="PASS_ATHENA_AND_ATOMIC_POINTER_PROMOTION",
        statistics=query["Statistics"],
        result_object=query["ResultConfiguration"]["OutputLocation"],
        rows=len(actual),
        source_count=receipt["source_count"],
        job_run_id=receipt["job_run_id"],
        snapshots=sn,
        export_sha256=sha(raw),
    )
    r.save()
    return r.value
