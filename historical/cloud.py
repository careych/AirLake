"""One isolated historical Glue run and two explicit Athena checks; no default writes."""

import argparse
import io
import json
import math
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from final_demo.control import Receipt, absent
from final_demo.prepare import job_definition, trust
from live_data.contract import ACCOUNT, BUCKET, REGION, sha
from live_data.deploy import clients

ROOT = Path(__file__).resolve().parents[1]
RUN = "20261114_7a6e91c2"
PREFIX = "historical-demo/" + RUN + "/"
DATABASE = "airlake_history_" + RUN
JOB = "airlake-history-20261114-7a6e91c2"
TABLES = (
    "bronze_measurements",
    "aq_measurements",
    "aqi_breakpoints",
    "dim_location",
    "pm25_aqi_hourly",
    "pm25_daily",
)
JARS = "s3://" + BUCKET + "/live-demo/20261114_9e2c740b/artifacts/"


def role_policy():
    bucket = "arn:aws:s3:::" + BUCKET
    glue = f"arn:aws:glue:{REGION}:{ACCOUNT}:"
    logs = f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/airlake/history-demo/{RUN}/*"

    def s(actions, resources, **kw):
        return {"Effect": "Allow", "Action": actions, "Resource": resources, **kw}

    return {
        "Version": "2012-10-17",
        "Statement": [
            s(
                ["s3:GetObject"],
                [
                    bucket + "/" + PREFIX + "*",
                    bucket + "/live-demo/20261114_9e2c740b/artifacts/runtime.jar",
                    bucket + "/live-demo/20261114_9e2c740b/artifacts/aws-bundle.jar",
                ],
            ),
            s(
                ["s3:PutObject"],
                [
                    bucket + "/" + PREFIX + x + "*"
                    for x in ("warehouse/", "outputs/", "temp/")
                ],
            ),
            s(
                ["s3:DeleteObject", "s3:AbortMultipartUpload"],
                [bucket + "/" + PREFIX + x + "*" for x in ("warehouse/", "temp/")],
            ),
            s(
                ["s3:ListBucket"],
                bucket,
                Condition={"StringLike": {"s3:prefix": [PREFIX + "*"]}},
            ),
            s(["s3:GetBucketLocation"], bucket),
            s(
                [
                    "glue:GetDatabase",
                    "glue:CreateDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:GetTableVersion",
                    "glue:GetTableVersions",
                ],
                [
                    glue + "catalog",
                    glue + "database/" + DATABASE,
                    *[glue + "table/" + DATABASE + "/" + t for t in TABLES],
                ],
            ),
            s(
                ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                [logs, logs + ":*"],
            ),
        ],
    }


def prepare(packet, source, publication_inputs=None, previous_api_zip=None):
    if packet.exists() or any(
        (p / ".git").exists() for p in (packet.resolve(), *packet.resolve().parents)
    ):
        raise ValueError("Use new private packet outside Git")
    audit = json.loads((source / "audit.json").read_bytes())
    raw = (source / "records.jsonl").read_bytes()
    if sha(raw) != audit["records_sha256"] or audit["accepted_unique_records"] != 17181:
        raise ValueError("Frozen real source changed")
    files = [
        *ROOT.glob("historical/*.py"),
        *ROOT.glob("final_demo/*.py"),
        *ROOT.glob("clean_api/*.py"),
        *ROOT.glob("live_data/*.py"),
        ROOT / "ingestion/__init__.py",
        ROOT / "ingestion/openaq.py",
        ROOT / "scripts/gold_batch.py",
        ROOT / "scripts/silver_batch.py",
    ]
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
        for p in sorted(files):
            info = zipfile.ZipInfo(str(p.relative_to(ROOT)), (2026, 11, 14, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, p.read_bytes())
        archive.writestr(
            zipfile.ZipInfo("scripts/__init__.py", (2026, 11, 14, 0, 0, 0)), b""
        )
    production = {
        str(p.relative_to(ROOT)): sha(p.read_bytes())
        for p in (ROOT / "scripts/gold_batch.py", ROOT / "scripts/silver_batch.py")
    }
    manifest = json.dumps(
        {
            "database": DATABASE,
            "prefix": PREFIX,
            "audit": audit,
            "production_sha256": production,
        },
        sort_keys=True,
    ).encode()
    artifacts = {
        "records.jsonl": raw,
        "manifest.json": manifest,
        "payload.zip": stream.getvalue(),
        "glue_job.py": (ROOT / "historical/glue_job.py").read_bytes(),
    }
    public_files = publication_inputs or {}
    if public_files:
        from historical.publish import api_zip

        previous = Path(previous_api_zip).read_bytes()
        artifacts["api-payload.zip"] = api_zip(previous, ROOT)
    website_sources = [
        *ROOT.glob("citizen/web/*"),
        *ROOT.glob("citizen/*.py"),
        *ROOT.glob("historical/*.py"),
    ]
    plan = {
        "account": ACCOUNT,
        "region": REGION,
        "run": RUN,
        "prefix": PREFIX,
        "database": DATABASE,
        "job": JOB,
        "role": JOB,
        "tables": list(TABLES),
        "artifacts": {k: sha(v) for k, v in artifacts.items()},
        "manifest_sha256": sha(manifest),
        "source_records": 17181,
        "expected_hourly_rows": 2133,
        "expected_daily_rows": 90,
        "expected_dimensions": 3,
        "glue_runs": 1,
        "glue_workers": 2,
        "worker_type": "G.1X",
        "timeout_minutes": 10,
        "retries": 0,
        "athena_selects": 2,
        "existing_jar_sources": [JARS + "runtime.jar", JARS + "aws-bundle.jar"],
        "policy": role_policy(),
        "production_sha256": production,
        "helper_sha256": sha(Path(__file__).read_bytes()),
        "no_polling_or_scheduler_changes": True,
        "no_historical_resource_changes": True,
        "status": "PREPARED_NOT_EXECUTED",
        "source_audit_sha256": sha(json.dumps(audit, sort_keys=True).encode()),
        "publication_inputs": public_files,
        "publication_input_sha256": {
            path: sha(Path(path).read_bytes()) for path in public_files.values()
        },
        "previous_api_sha256": sha(previous) if public_files else None,
        "website_source_sha256": {
            str(path.relative_to(ROOT)): sha(path.read_bytes())
            for path in website_sources
            if path.is_file()
        },
        "private_api_http_reads": 5 if public_files else 0,
        "new_website_get_budget": 40 if public_files else 0,
        "manual_amplify_deployments": 2 if public_files else 0,
        "website_app_id": "dhyvgpdsnj4zi",
        "public_rows": {"hourly": 720, "daily": 30},
        "protected_rows": {"hourly": 2133, "daily": 90},
        "api_function": "airlake-live-20261114_9e2c740b-api",
        "new_api_inline_policy": "HistoricalResearch20261114",
        "new_api_read_keys": [
            PREFIX + "outputs/" + name for name in ("export.json", "acceptance.json")
        ],
    }
    packet.mkdir(mode=0o700, parents=True)
    for n, v in artifacts.items():
        (packet / n).write_bytes(v)
    if public_files:
        (packet / "previous-api.zip").write_bytes(previous)
    (packet / "source-audit.json").write_text(json.dumps(audit, sort_keys=True))
    (packet / "plan.json").write_text(json.dumps(plan, sort_keys=True))
    for p in packet.iterdir():
        p.chmod(0o600)
    return sha((packet / "plan.json").read_bytes())


def load(packet):
    raw = (packet / "plan.json").read_bytes()
    p = json.loads(raw)
    if (
        (
            p["account"],
            p["region"],
            p["run"],
            p["prefix"],
            p["database"],
            p["job"],
            p["role"],
        )
        != (ACCOUNT, REGION, RUN, PREFIX, DATABASE, JOB, JOB)
        or p["tables"] != list(TABLES)
        or p["helper_sha256"] != sha(Path(__file__).read_bytes())
    ):
        raise ValueError("Historical frozen scope changed")
    if (
        p["publication_inputs"]
        and sha((packet / "previous-api.zip").read_bytes()) != p["previous_api_sha256"]
    ):
        raise ValueError("Rollback artifact changed")
    if sha((packet / "source-audit.json").read_bytes()) != p["source_audit_sha256"]:
        raise ValueError("Frozen source audit changed")
    required = {"records.jsonl", "manifest.json", "payload.zip", "glue_job.py"}
    if p["publication_inputs"]:
        required.add("api-payload.zip")
    if set(p["artifacts"]) != required:
        raise ValueError("Frozen artifact list changed")
    for n, h in p["artifacts"].items():
        if (
            n
            not in (
                "records.jsonl",
                "manifest.json",
                "payload.zip",
                "glue_job.py",
                "api-payload.zip",
            )
            or sha((packet / n).read_bytes()) != h
        ):
            raise ValueError("Frozen artifact changed")
    return p, sha(raw)


def preflight(c, p):
    if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong account")
    from live_data.contract import NAME
    from live_data.run_glue import frozen_scope

    frozen_scope(c)
    if (
        c["lambda"].get_function_concurrency(FunctionName=NAME + "-refresh")[
            "ReservedConcurrentExecutions"
        ]
        != 0
    ):
        raise ValueError("Keep the existing refresh worker disabled")
    block = c["s3"].get_public_access_block(Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT)[
        "PublicAccessBlockConfiguration"
    ]
    if not all(
        block.get(k)
        for k in (
            "BlockPublicAcls",
            "IgnorePublicAcls",
            "BlockPublicPolicy",
            "RestrictPublicBuckets",
        )
    ):
        raise ValueError("Lake bucket must remain private")
    for page in (
        c["s3"]
        .get_paginator("list_objects_v2")
        .paginate(Bucket=BUCKET, Prefix=PREFIX, ExpectedBucketOwner=ACCOUNT)
    ):
        if page.get("Contents"):
            raise ValueError("Historical prefix already used; never overwrite")
    absent(lambda: c["iam"].get_role(RoleName=JOB), {"NoSuchEntity"})
    absent(lambda: c["glue"].get_job(JobName=JOB), {"EntityNotFoundException"})
    absent(lambda: c["glue"].get_database(Name=DATABASE), {"EntityNotFoundException"})
    for jar in ("runtime.jar", "aws-bundle.jar"):
        c["s3"].head_object(
            Bucket=BUCKET,
            Key="live-demo/20261114_9e2c740b/artifacts/" + jar,
            ExpectedBucketOwner=ACCOUNT,
        )
    wg = c["athena"].get_work_group(WorkGroup="airlake")["WorkGroup"]
    if (
        wg["State"] != "ENABLED"
        or not wg["Configuration"]["EnforceWorkGroupConfiguration"]
        or wg["Configuration"]["ResultConfiguration"]["OutputLocation"]
        != "s3://airlake-701364614078-athena-results/"
    ):
        raise ValueError("Athena scope changed")
    if p.get("publication_inputs"):
        from citizen.release import clients as website_clients
        from citizen.release import preflight as website_preflight
        from historical.publish import api_preflight

        api_preflight(c, p)
        # The site stays at job 1 until consolidated approval. This reads metadata only.
        prior = Path(p["publication_inputs"]["previous_hosting_receipt"])
        retained = json.loads(prior.read_bytes())
        website_plan = {
            "previous_jobs": {
                b: str(int(retained["deployments"][b]["job_id"]))
                for b in ("public", "demo")
            }
        }
        credential_parent = Path(p["publication_inputs"]["credentials"]).parent
        website_preflight(website_clients(), credential_parent, website_plan)
    return {
        "status": "PASS_READ_ONLY_NO_WRITE_PERMISSION_INFERRED",
        "database": DATABASE,
        "job": JOB,
    }


def run_glue(c, p, packet, digest):
    if (packet / "glue-receipt.json").exists():
        raise ValueError(
            "An attempt exists; inspect before any separately approved recovery"
        )
    preflight(c, p)
    r = Receipt(packet / "glue-receipt.json", digest)
    r.write(
        "create_scoped_history_role",
        lambda: c["iam"].create_role(
            RoleName=JOB,
            AssumeRolePolicyDocument=json.dumps(trust("glue")),
            Tags=[{"Key": "Project", "Value": "AirLake-historical-demo"}],
        ),
    )
    r.write(
        "put_history_role_policy",
        lambda: c["iam"].put_role_policy(
            RoleName=JOB,
            PolicyName="BoundedHistoricalBackfill",
            PolicyDocument=json.dumps(p["policy"]),
        ),
    )

    def no_overwrite(params, **kwargs):
        params["headers"]["If-None-Match"] = "*"

    c["s3"].meta.events.register("before-call.s3.PutObject", no_overwrite)
    for n in p["artifacts"]:
        if n == "api-payload.zip":
            continue
        suffix = (
            "inputs/" + n
            if n in ("records.jsonl", "manifest.json")
            else "artifacts/" + n
        )
        r.write(
            "upload_" + n,
            lambda name=n, key=PREFIX + suffix: c["s3"].put_object(
                Bucket=BUCKET,
                Key=key,
                Body=(packet / name).read_bytes(),
                ExpectedBucketOwner=ACCOUNT,
                ServerSideEncryption="AES256",
            ),
        )
    definition = job_definition(
        {
            "job": JOB,
            "glue_role": JOB,
            "run": RUN,
            "database": DATABASE,
            "prefix": PREFIX,
            "bucket": BUCKET,
            "log_prefix": "/airlake/history-demo/" + RUN,
        },
        digest,
    )
    args = definition["NonOverridableArguments"]
    args.pop("--plan_uri")
    args.pop("--plan_sha256")
    args["--extra-jars"] = ",".join(p["existing_jar_sources"])
    args["--conf"] = args["--conf"].replace(
        "spark.sql.catalog.demo", "spark.sql.catalog.history"
    )
    args.update(
        {
            "--history_bucket": BUCKET,
            "--history_prefix": PREFIX,
            "--history_database": DATABASE,
            "--manifest_sha256": p["manifest_sha256"],
        }
    )
    # Read propagation, not repeated writes. Preserve a failed/uncertain attempt.
    policy = c["iam"].get_role_policy(
        RoleName=JOB, PolicyName="BoundedHistoricalBackfill"
    )["PolicyDocument"]
    if policy != p["policy"]:
        raise ValueError("Role policy not established")
    r.write("create_one_history_job", lambda: c["glue"].create_job(**definition))
    r.write("start_one_history_run", lambda: c["glue"].start_job_run(JobName=JOB))
    return r.value


def status(c, packet):
    r = json.loads((packet / "glue-receipt.json").read_bytes())
    job = r.get("job_run_id")
    if not job:
        raise ValueError(
            "No recorded run ID; inspect retained uncertain operation without starting again"
        )
    v = c["glue"].get_job_run(JobName=JOB, RunId=job, PredecessorsIncluded=False)[
        "JobRun"
    ]
    return {
        k: v[k] for k in ("Id", "JobRunState", "ExecutionTime", "DPUSeconds") if k in v
    }


def query_sql(export):
    snaps = export["provenance"]["snapshots"]
    f = snaps["hourly"][0]
    d = snaps["dimensions"][0]
    daily = snaps["daily"][0]
    first = f"SELECT f.location_id, date_format(f.hour_start, '%Y-%m-%dT%H:%i:%sZ') AS hour_start, f.pm25_avg, f.aqi, d.name AS station_name FROM {DATABASE}.pm25_aqi_hourly FOR VERSION AS OF {f} f JOIN {DATABASE}.dim_location FOR VERSION AS OF {d} d ON f.location_id=d.location_id WHERE f.location_id IN (4946812,4946813,6123215) AND f.hour_start>=TIMESTAMP '2026-08-31 17:00:00' AND f.hour_start<TIMESTAMP '2026-09-30 17:00:00' ORDER BY f.location_id,f.hour_start LIMIT 2134"
    second = f"SELECT location_id, CAST(date_local AS VARCHAR) AS date_local, pm25_avg, hours_present, hours_expected, coverage_percent, complete_24_hours FROM {DATABASE}.pm25_daily FOR VERSION AS OF {daily} WHERE location_id IN (4946812,4946813,6123215) AND date_local>=DATE '2026-09-01' AND date_local<DATE '2026-10-01' ORDER BY location_id,date_local LIMIT 91"
    return first, second


def query_rows(c, query):
    rows = []
    columns = None
    for page in (
        c["athena"]
        .get_paginator("get_query_results")
        .paginate(QueryExecutionId=query, PaginationConfig={"PageSize": 1000})
    ):
        result = page["ResultSet"]
        names = [v["Name"] for v in result["ResultSetMetadata"]["ColumnInfo"]]
        if columns is None:
            columns = names
        elif columns != names:
            raise ValueError("Athena pagination schema changed")
        for r in result["Rows"]:
            values = [v.get("VarCharValue") for v in r["Data"]]
            if not rows and values == columns:
                continue
            rows.append(dict(zip(columns, values, strict=True)))
    return rows


def verify(c, p, packet, digest):
    state = status(c, packet)
    if state["JobRunState"] != "SUCCEEDED":
        raise ValueError("Glue has not succeeded; do not query or publish")
    if (packet / "athena-receipt.json").exists():
        raise ValueError("Query attempt exists; never submit again automatically")

    def get(name):
        return (
            c["s3"]
            .get_object(
                Bucket=BUCKET,
                Key=PREFIX + "outputs/" + name,
                ExpectedBucketOwner=ACCOUNT,
            )["Body"]
            .read(4000001)
        )

    raw = get("export.json")
    evidence = json.loads(get("evidence.json"))
    export = json.loads(raw)
    if (
        evidence["status"] != "PASS"
        or evidence["export_sha256"] != sha(raw)
        or evidence["snapshots_before"] != evidence["snapshots_after"]
        or len(export["facts"]) != 2133
        or len(export["daily"]) != 90
    ):
        raise ValueError("Job output verification failed")
    r = Receipt(packet / "athena-receipt.json", digest)
    r.value["queries"] = []
    dims = {v["location_id"]: v["name"] for v in export["dimensions"]}
    for number, sql in enumerate(query_sql(export)):
        item = {
            "kind": "hourly" if number == 0 else "daily",
            "status": "SUBMISSION_UNCERTAIN",
            "sql_sha256": sha(sql.encode()),
        }
        r.value["queries"].append(item)
        r.save()
        result = r.write(
            "one_select_" + item["kind"],
            lambda q=sql, n=number: c["athena"].start_query_execution(
                QueryString=q,
                ClientRequestToken=sha((digest + str(n)).encode()),
                QueryExecutionContext={
                    "Catalog": "AwsDataCatalog",
                    "Database": DATABASE,
                },
                WorkGroup="airlake",
                ResultReuseConfiguration={
                    "ResultReuseByAgeConfiguration": {"Enabled": False}
                },
            ),
        )
        query = result["QueryExecutionId"]
        item["query_id"] = query
        r.save()
        for _ in range(90):
            execution = c["athena"].get_query_execution(QueryExecutionId=query)[
                "QueryExecution"
            ]
            phase = execution["Status"]["State"]
            if phase == "SUCCEEDED":
                break
            if phase in ("FAILED", "CANCELLED"):
                raise ValueError("Athena failed; no retry")
            time.sleep(2)
        else:
            raise TimeoutError("Query uncertain; inspect recorded ID; no resubmit")
        rows = query_rows(c, query)
        if number == 0:
            expected = {(v["location_id"], v["hour_start"]): v for v in export["facts"]}
            seen = set()
            for v in rows:
                key = (int(v["location_id"]), v["hour_start"])
                e = expected[key]
                if (
                    key in seen
                    or int(v["aqi"]) != e["aqi"]
                    or v["station_name"] != dims[key[0]]
                    or not math.isclose(
                        float(v["pm25_avg"]),
                        e["pm25_avg"],
                        rel_tol=1e-12,
                        abs_tol=1e-10,
                    )
                ):
                    raise ValueError("Independent hourly row mismatch")
                seen.add(key)
            if seen != set(expected) or len(rows) != 2133:
                raise ValueError("Independent hourly key/count mismatch")
        else:
            expected = {(v["location_id"], v["date_local"]): v for v in export["daily"]}
            seen = set()
            for v in rows:
                key = (int(v["location_id"]), v["date_local"])
                e = expected[key]
                if (
                    key in seen
                    or int(v["hours_present"]) != e["hours_present"]
                    or int(v["hours_expected"]) != 24
                    or (v["complete_24_hours"].lower() == "true")
                    != e["complete_24_hours"]
                    or not math.isclose(
                        float(v["pm25_avg"]),
                        e["pm25_avg"],
                        rel_tol=1e-12,
                        abs_tol=1e-10,
                    )
                    or not math.isclose(
                        float(v["coverage_percent"]),
                        e["coverage_percent"],
                        abs_tol=1e-9,
                    )
                ):
                    raise ValueError("Independent daily row mismatch")
                seen.add(key)
            if seen != set(expected) or len(rows) != 90:
                raise ValueError("Independent daily key/count mismatch")
        item.update(
            status="PASS_ALL_ROWS",
            rows=len(rows),
            scan_bytes=execution.get("Statistics", {}).get("DataScannedInBytes"),
            result_object=execution["ResultConfiguration"]["OutputLocation"],
        )
        r.save()
    acceptance = {
        "status": "PASS",
        "verified_at": datetime.now(UTC).isoformat(),
        "export_sha256": sha(raw),
        "hourly_all_rows_match": "PASS",
        "daily_all_rows_match": "PASS",
        "snapshots": export["provenance"]["snapshots"],
        "queries": r.value["queries"],
        "job_run_id": state["Id"],
    }
    (packet / "cloud-export.json").write_bytes(raw)
    (packet / "cloud-evidence.json").write_text(json.dumps(evidence, sort_keys=True))
    (packet / "acceptance.json").write_text(json.dumps(acceptance, sort_keys=True))
    r.value["status"] = "PASS_TWO_INDEPENDENT_SELECTS"
    r.save()
    return acceptance


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument(
        "command",
        choices=(
            "prepare",
            "preflight",
            "run-glue",
            "status",
            "verify-athena",
            "publish-api",
            "smoke-api",
            "publish-site",
        ),
    )
    a.add_argument("--packet", type=Path, required=True)
    a.add_argument("--source", type=Path)
    a.add_argument("--confirm-plan-sha256")
    a.add_argument("--previous-api-zip", type=Path)
    for key in (
        "metadata",
        "latest",
        "source-metadata",
        "credentials",
        "previous-hosting-receipt",
    ):
        a.add_argument("--" + key, type=Path)
    x = a.parse_args()
    if x.command == "prepare":
        inputs = {
            key: str(getattr(x, key))
            for key in (
                "metadata",
                "latest",
                "source_metadata",
                "credentials",
                "previous_hosting_receipt",
            )
            if getattr(x, key) is not None
        }
        print(
            json.dumps(
                {"plan_sha256": prepare(x.packet, x.source, inputs, x.previous_api_zip)}
            )
        )
        return
    p, h = load(x.packet)
    if (
        x.command
        in ("run-glue", "verify-athena", "publish-api", "smoke-api", "publish-site")
        and x.confirm_plan_sha256 != h
    ):
        a.exit(1, "STOP: new bounded historical AWS approval required\n")
    c = clients()
    if x.command in ("publish-api", "smoke-api", "publish-site"):
        from historical.publish import publish_api, publish_site, smoke_api

        if not p["publication_inputs"]:
            a.exit(1, "Publication was not included in the approved plan\n")
        if x.command == "publish-site":
            c["amplify"] = __import__(
                "citizen.hosting", fromlist=["clients"]
            ).clients()["amplify"]
        action = {
            "publish-api": publish_api,
            "smoke-api": smoke_api,
            "publish-site": publish_site,
        }[x.command]
        print(json.dumps(action(c, p, x.packet, h)))
        return
    result = (
        preflight(c, p)
        if x.command == "preflight"
        else status(c, x.packet)
        if x.command == "status"
        else run_glue(c, p, x.packet, h)
        if x.command == "run-glue"
        else verify(c, p, x.packet, h)
    )
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001 -- never expose SDK payloads, private paths or signed URLs
        print(
            json.dumps(
                {
                    "status": "STOP_FAILED_OR_UNCERTAIN",
                    "exception_type": type(e).__name__,
                }
            )
        )
        raise SystemExit(1) from None
