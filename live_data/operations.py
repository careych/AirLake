"""Prepare offline; read-only preflight; exact-plan-gated deployment and demo controls."""

import argparse
import base64
import json
import time
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from final_demo.control import Receipt, absent
from final_demo.prepare import trust
from live_data.contract import (
    ACCOUNT,
    BUCKET,
    MAX_INVOCATIONS,
    NAME,
    PREFIX,
    REGION,
    RUN,
    TABLE,
    encode,
    sha,
)
from live_data.deploy import ROOT, clients, identity, statement
from live_data.refresh import (
    ACTIVE_JOBS,
    CONTROLLER,
    RESULT_BUCKET,
    disable,
    schedule_args,
)
from live_data.store import AWSStore, pack, unpack

MUTABLE_JOB_FIELDS = (
    "Role",
    "Command",
    "DefaultArguments",
    "NonOverridableArguments",
    "Connections",
    "MaxRetries",
    "Timeout",
    "WorkerType",
    "NumberOfWorkers",
    "GlueVersion",
    "ExecutionClass",
    "ExecutionProperty",
    "SecurityConfiguration",
    "NotificationProperty",
    "JobRunQueuingEnabled",
    "JobMode",
)


def job_update(job):
    return {k: job[k] for k in MUTABLE_JOB_FIELDS if k in job}


def policies():
    arn = f"arn:aws:glue:{REGION}:{ACCOUNT}"
    db = "airlake_live_" + RUN + "_r*"
    log = f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/aws/lambda/{CONTROLLER}:*"
    controller = [
        statement(["logs:CreateLogStream", "logs:PutLogEvents"], log),
        statement(
            ["lambda:InvokeFunction"],
            f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NAME}-poller",
        ),
        statement(
            ["lambda:PutFunctionConcurrency"],
            [
                f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{n}"
                for n in (NAME + "-poller", CONTROLLER)
            ],
        ),
        statement(
            ["scheduler:GetSchedule", "scheduler:UpdateSchedule"],
            f"arn:aws:scheduler:{REGION}:{ACCOUNT}:schedule/default/{NAME}-5m",
        ),
        statement(
            ["iam:PassRole"],
            f"arn:aws:iam::{ACCOUNT}:role/{NAME}-scheduler",
            Condition={
                "StringEquals": {"iam:PassedToService": "scheduler.amazonaws.com"}
            },
        ),
        *[
            statement(
                actions,
                f"arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{TABLE}",
                Condition={"ForAllValues:StringEquals": {"dynamodb:LeadingKeys": keys}},
            )
            for actions, keys in (
                (["dynamodb:GetItem"], ["OPS", "RUN", "EXPORT", "CURSOR"]),
                (["dynamodb:PutItem"], ["OPS", "EXPORT"]),
                (["dynamodb:UpdateItem"], ["OPS"]),
            )
        ],
        statement(
            ["s3:ListBucket"],
            f"arn:aws:s3:::{BUCKET}",
            Condition={
                "StringLike": {
                    "s3:prefix": [PREFIX + "bronze/*", PREFIX + "warehouse/*"]
                }
            },
        ),
        statement(
            ["s3:GetObject"],
            [
                f"arn:aws:s3:::{BUCKET}/{PREFIX}{p}*"
                for p in ("bronze/", "exports/", "evidence/", "warehouse/")
            ],
        ),
        statement(["s3:PutObject"], f"arn:aws:s3:::{BUCKET}/{PREFIX}operations/*"),
        statement(
            ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"],
            arn + ":job/" + NAME + "-gold",
        ),
        statement(
            ["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"],
            [arn + ":catalog", arn + ":database/" + db, arn + ":table/" + db + "/*"],
        ),
        statement(
            [
                "athena:GetWorkGroup",
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
            ],
            f"arn:aws:athena:{REGION}:{ACCOUNT}:workgroup/airlake",
        ),
        statement(
            ["s3:GetBucketLocation", "s3:ListBucket"], f"arn:aws:s3:::{RESULT_BUCKET}"
        ),
        statement(["s3:GetObject", "s3:PutObject"], f"arn:aws:s3:::{RESULT_BUCKET}/*"),
    ]
    glue = [
        statement(
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
            [arn + ":catalog", arn + ":database/" + db, arn + ":table/" + db + "/*"],
        )
    ]
    scheduler = [
        statement(
            ["lambda:InvokeFunction"],
            f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{CONTROLLER}",
        )
    ]
    return {
        n: {"Version": "2012-10-17", "Statement": v}
        for n, v in (("refresh", controller), ("glue", glue), ("scheduler", scheduler))
    }


def build_payload():
    import io

    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
        files = [
            *ROOT.glob("live_data/*.py"),
            *ROOT.glob("final_demo/*.py"),
            *ROOT.glob("clean_api/*.py"),
            ROOT / "ingestion/__init__.py",
            ROOT / "ingestion/openaq.py",
            ROOT / "scripts/gold_batch.py",
            ROOT / "scripts/silver_batch.py",
        ]
        for path in sorted(files):
            info = zipfile.ZipInfo(str(path.relative_to(ROOT)), (2026, 11, 14, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
        archive.writestr(
            zipfile.ZipInfo("scripts/__init__.py", (2026, 11, 14, 0, 0, 0)), b""
        )
    return stream.getvalue()


def prepare(packet, inventory_path, old_api):
    if packet.exists() or any(
        (p / ".git").exists() for p in (packet.resolve(), *packet.resolve().parents)
    ):
        raise ValueError("Use a new private packet outside Git")
    inventory = json.loads(inventory_path.read_bytes())
    if inventory["account"] != ACCOUNT or inventory["schedule"]["Name"] != NAME + "-5m":
        raise ValueError("Inventory scope mismatch")
    previous = old_api.read_bytes()
    if (
        base64.b64encode(bytes.fromhex(sha(previous))).decode()
        != inventory["functions"]["api"]["CodeSha256"]
    ):
        raise ValueError("Rollback ZIP does not match deployed API")
    payload = build_payload()
    artifacts = {
        "payload.zip": payload,
        "glue_job.py": (ROOT / "live_data/glue_job.py").read_bytes(),
        "previous-api.zip": previous,
    }
    production = {
        str(p.relative_to(ROOT)): sha(p.read_bytes())
        for p in (ROOT / "scripts/gold_batch.py", ROOT / "scripts/silver_batch.py")
    }
    plan = {
        "schema_version": 1,
        "account": ACCOUNT,
        "region": REGION,
        "run": RUN,
        "controller": CONTROLLER,
        "controller_role": CONTROLLER,
        "schedule": NAME + "-5m",
        "prefix": PREFIX,
        "job": NAME + "-gold",
        "minutes": 45,
        "max_controller_ticks": 12,
        "max_total_poll_attempts": MAX_INVOCATIONS,
        "max_new_glue_runs": 1,
        "max_new_athena_selects": 1,
        "max_api_http_checks": 3,
        "production_sha256": production,
        "artifacts": {n: sha(v) for n, v in artifacts.items()},
        "baseline": inventory,
        "policies": policies(),
        "new_resource_initial_state": "DISABLED; concurrency 0",
    }
    packet.mkdir(mode=0o700, parents=True)
    for name, value in artifacts.items():
        (packet / name).write_bytes(value)
    (packet / "plan.json").write_bytes(encode(plan))
    digest = sha(encode(plan))
    (packet / "plan.sha256").write_text(digest + "\n")
    for p in packet.iterdir():
        p.chmod(0o600)
    return {
        "status": "CODE_PREPARED_OFFLINE",
        "plan_sha256": digest,
        "packet": str(packet),
    }


def load(packet):
    raw = (packet / "plan.json").read_bytes()
    p = json.loads(raw)
    if (
        (
            p["account"],
            p["region"],
            p["run"],
            p["prefix"],
            p["controller"],
            p["schedule"],
            p["job"],
        )
        != (ACCOUNT, REGION, RUN, PREFIX, CONTROLLER, NAME + "-5m", NAME + "-gold")
        or p["minutes"] != 45
        or p["policies"] != policies()
    ):
        raise ValueError("Unapproved operations scope")
    if set(p["artifacts"]) != {"payload.zip", "glue_job.py", "previous-api.zip"}:
        raise ValueError("Artifact allowlist")
    for n, digest in p["artifacts"].items():
        if sha((packet / n).read_bytes()) != digest:
            raise ValueError("Artifact hash mismatch")
    if (packet / "payload.zip").read_bytes() != build_payload() or (
        packet / "glue_job.py"
    ).read_bytes() != (ROOT / "live_data/glue_job.py").read_bytes():
        raise ValueError("Source changed after preparation")
    return p, sha(raw)


def preflight(c, p, *, installed=False):
    identity(c)
    s = c["scheduler"].get_schedule(Name=NAME + "-5m", GroupName="default")
    if s["State"] != "DISABLED" or s["ScheduleExpression"] != "rate(5 minutes)":
        raise ValueError("Stop existing schedule first")
    expected_target = CONTROLLER if installed else NAME + "-poller"
    target = json.loads(s["Target"]["Input"])
    if (
        target
        != {"FunctionName": expected_target, "InvocationType": "Event", "Payload": "{}"}
        or s["Target"]["Arn"] != "arn:aws:scheduler:::aws-sdk:lambda:invoke"
        or s["Target"]["RoleArn"] != f"arn:aws:iam::{ACCOUNT}:role/{NAME}-scheduler"
    ):
        raise ValueError("Unexpected schedule target")
    if (
        c["lambda"].get_function_concurrency(FunctionName=NAME + "-poller")[
            "ReservedConcurrentExecutions"
        ]
        != 0
    ):
        raise ValueError("Stop poller before preparation")
    if (
        s["Target"].get("RetryPolicy")
        != {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 60}
        or s.get("FlexibleTimeWindow") != {"Mode": "OFF"}
        or s.get("ScheduleExpressionTimezone") != "UTC"
    ):
        raise ValueError("Unexpected schedule retry/window settings")
    for name in [NAME + "-poller", CONTROLLER] if installed else [NAME + "-poller"]:
        cfg = c["lambda"].get_function_event_invoke_config(FunctionName=name)
        if (
            cfg.get("MaximumRetryAttempts") != 0
            or cfg.get("MaximumEventAgeInSeconds") != 60
        ):
            raise ValueError("Unexpected Lambda async retry settings")
    if installed:
        for suffix, policy in p["policies"].items():
            actual = c["iam"].get_role_policy(
                RoleName=NAME + "-" + suffix, PolicyName="LiveOperationsV1"
            )["PolicyDocument"]
            if actual != policy:
                raise ValueError("Installed inline IAM policy mismatch")
    store = AWSStore(c)
    lease = store.read("RUN", "LEASE")
    if (
        lease.get("active") is not False
        or lease.get("halted")
        or lease.get("lease_until") != 0
        or lease.get("attempts") != unpack(p["baseline"]["run"]).get("attempts")
        or not 0 < lease.get("attempts", MAX_INVOCATIONS) <= MAX_INVOCATIONS - 2
    ):
        raise ValueError(
            "Need inactive, unhalted lease and at least two unused poll attempts; never reset counters"
        )
    lock = store.read("OPS", "LOCK")
    if lock.get("active"):
        raise ValueError("Controller outcome uncertain; reconcile before continuation")
    if store.read("EXPORT", "CURRENT") != unpack(p["baseline"]["pointer"]):
        raise ValueError("Verified baseline pointer changed")
    if any(
        r["JobRunState"] in ACTIVE_JOBS
        for r in c["glue"].get_job_runs(JobName=NAME + "-gold", MaxResults=10)[
            "JobRuns"
        ]
    ):
        raise ValueError("Existing Glue run active")
    job = c["glue"].get_job(JobName=NAME + "-gold")["Job"]
    if installed:
        controller = c["lambda"].get_function_configuration(FunctionName=CONTROLLER)
        if (
            controller["CodeSha256"]
            != base64.b64encode(bytes.fromhex(p["artifacts"]["payload.zip"])).decode()
            or controller["State"] != "Active"
            or controller["LastUpdateStatus"] != "Successful"
        ):
            raise ValueError("Controller code/state mismatch")
        env = controller["Environment"]["Variables"]
        if (
            env.get("AIRLAKE_OPS_WINDOW") != sha(encode(p))
            or env.get("AIRLAKE_LIVE_RUN") != RUN
            or json.loads(env.get("AIRLAKE_PRODUCTION_SHA256", "{}"))
            != p["production_sha256"]
        ):
            raise ValueError("Controller environment scope mismatch")
        if (
            c["lambda"].get_function_concurrency(FunctionName=CONTROLLER)[
                "ReservedConcurrentExecutions"
            ]
            != 0
        ):
            raise ValueError("Controller must start disabled")
        if job_update(job) != desired_job(p, sha(encode(p))):
            raise ValueError("Installed job configuration differs")
    elif job_update(job) != job_update(p["baseline"]["job"]):
        raise ValueError("Existing Glue job changed since review")
    for suffix in ("poller", "api"):
        fn = c["lambda"].get_function_configuration(FunctionName=NAME + "-" + suffix)
        expected = (
            p["artifacts"]["payload.zip"]
            if installed and suffix == "api"
            else base64.b64decode(
                p["baseline"]["functions"][suffix]["CodeSha256"]
            ).hex()
        )
        if (
            fn["CodeSha256"] != base64.b64encode(bytes.fromhex(expected)).decode()
            or fn["Role"] != f"arn:aws:iam::{ACCOUNT}:role/{NAME}-{suffix}"
            or fn["State"] != "Active"
            or fn["LastUpdateStatus"] != "Successful"
        ):
            raise ValueError("Function code/role/readiness changed")
    if not installed:
        absent(
            lambda: c["lambda"].get_function(FunctionName=CONTROLLER),
            {"ResourceNotFoundException"},
        )
        absent(lambda: c["iam"].get_role(RoleName=CONTROLLER), {"NoSuchEntity"})
    url = c["lambda"].get_function_url_config(FunctionName=NAME + "-api")
    if url["AuthType"] != "AWS_IAM" or url.get("Cors"):
        raise ValueError("Existing API must remain private without CORS")
    if not installed:
        for suffix in ("glue", "scheduler"):
            names = c["iam"].list_role_policies(RoleName=NAME + "-" + suffix)[
                "PolicyNames"
            ]
            if "LiveOperationsV1" in names:
                raise ValueError("Proposed inline policy name already exists")
        for page in (
            c["s3"]
            .get_paginator("list_objects_v2")
            .paginate(
                Bucket=BUCKET,
                Prefix=PREFIX + "operations/" + sha(encode(p)) + "/",
                ExpectedBucketOwner=ACCOUNT,
            )
        ):
            if page.get("Contents"):
                raise ValueError(
                    "Operations artifact prefix already used; preserve prior attempts"
                )
    c["s3"].head_bucket(Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT)
    return {
        "status": "PASS_READ_ONLY",
        "remaining_poll_attempts": MAX_INVOCATIONS - int(lease["attempts"]),
        "writes_authorized": False,
    }


def desired_job(p, digest):
    update = job_update(p["baseline"]["job"])
    update = json.loads(json.dumps(update))
    update["Command"]["ScriptLocation"] = (
        f"s3://{BUCKET}/{PREFIX}operations/{digest}/glue_job.py"
    )
    args = update["NonOverridableArguments"]
    args.pop("--manifest_key", None)
    args.pop("--manifest_sha256", None)
    args["--extra-py-files"] = f"s3://{BUCKET}/{PREFIX}operations/{digest}/payload.zip"
    update["MaxRetries"] = 0
    update["Timeout"] = 10
    update["ExecutionProperty"] = {"MaxConcurrentRuns": 1}
    return update


def deploy(c, packet, p, digest):
    preflight(c, p)
    r = Receipt(packet / "install-receipt.json", digest)
    for n in ("payload.zip", "glue_job.py"):
        r.write(
            "upload_" + n,
            lambda name=n: c["s3"].put_object(
                Bucket=BUCKET,
                Key=PREFIX + "operations/" + digest + "/" + name,
                Body=(packet / name).read_bytes(),
                ExpectedBucketOwner=ACCOUNT,
                IfNoneMatch="*",
                ServerSideEncryption="AES256",
            ),
        )
    r.write(
        "create_controller_role",
        lambda: c["iam"].create_role(
            RoleName=CONTROLLER, AssumeRolePolicyDocument=json.dumps(trust("lambda"))
        ),
    )
    for suffix, policy in p["policies"].items():
        r.write(
            "policy_" + suffix,
            lambda s=suffix, pol=policy: c["iam"].put_role_policy(
                RoleName=NAME + "-" + s,
                PolicyName="LiveOperationsV1",
                PolicyDocument=json.dumps(pol),
            ),
        )
    r.write(
        "controller_log_group",
        lambda: c["logs"].create_log_group(logGroupName="/aws/lambda/" + CONTROLLER),
    )
    r.write(
        "controller_log_retention",
        lambda: c["logs"].put_retention_policy(
            logGroupName="/aws/lambda/" + CONTROLLER, retentionInDays=7
        ),
    )
    # IAM propagation can lag. No automatic mutation retry; stop and inspect if creation fails.
    time.sleep(10)
    r.write(
        "create_controller",
        lambda: c["lambda"].create_function(
            FunctionName=CONTROLLER,
            Role=f"arn:aws:iam::{ACCOUNT}:role/{CONTROLLER}",
            Runtime="python3.12",
            Handler="live_data.refresh.handler",
            Architectures=["arm64"],
            Timeout=240,
            MemorySize=256,
            Code={"ZipFile": (packet / "payload.zip").read_bytes()},
            Environment={
                "Variables": {
                    "AIRLAKE_LIVE_RUN": RUN,
                    "AIRLAKE_OPS_WINDOW": digest,
                    "AIRLAKE_OPS_DEADLINE": "2000-01-01T00:00:00Z",
                    "AIRLAKE_PRODUCTION_SHA256": json.dumps(p["production_sha256"]),
                }
            },
        ),
    )
    c["lambda"].get_waiter("function_active_v2").wait(
        FunctionName=CONTROLLER, WaiterConfig={"Delay": 2, "MaxAttempts": 45}
    )
    r.write(
        "controller_concurrency_zero",
        lambda: c["lambda"].put_function_concurrency(
            FunctionName=CONTROLLER, ReservedConcurrentExecutions=0
        ),
    )
    r.write(
        "controller_async_limits",
        lambda: c["lambda"].put_function_event_invoke_config(
            FunctionName=CONTROLLER, MaximumRetryAttempts=0, MaximumEventAgeInSeconds=60
        ),
    )
    api = c["lambda"].get_function_configuration(FunctionName=NAME + "-api")
    r.write(
        "api_backward_compatible_loader",
        lambda: c["lambda"].update_function_code(
            FunctionName=NAME + "-api",
            RevisionId=api["RevisionId"],
            ZipFile=(packet / "payload.zip").read_bytes(),
        ),
    )
    c["lambda"].get_waiter("function_updated_v2").wait(
        FunctionName=NAME + "-api", WaiterConfig={"Delay": 2, "MaxAttempts": 45}
    )
    r.write(
        "update_existing_job",
        lambda: c["glue"].update_job(
            JobName=NAME + "-gold", JobUpdate=desired_job(p, digest)
        ),
    )
    s = c["scheduler"].get_schedule(Name=NAME + "-5m", GroupName="default")
    args = schedule_args(s)
    args["Target"]["Input"] = json.dumps(
        {"FunctionName": CONTROLLER, "InvocationType": "Event", "Payload": "{}"}
    )
    r.write(
        "retarget_disabled_schedule",
        lambda: c["scheduler"].update_schedule(**args, State="DISABLED"),
    )
    r.value["status"] = "INSTALLED_DISABLED_NOT_LIVE_VERIFIED"
    r.save()
    return preflight(c, p, installed=True)


def enable(c, packet, p, digest):
    preflight(c, p, installed=True)
    r = Receipt(packet / "enable-receipt.json", digest)
    now = datetime.now(UTC).replace(microsecond=0)
    end = now + timedelta(minutes=p["minutes"])
    # Preserve existing variables, including the secret reference. Do not log them.
    for name, key in (
        (NAME + "-poller", "AIRLAKE_LIVE_DEADLINE"),
        (CONTROLLER, "AIRLAKE_OPS_DEADLINE"),
    ):
        fn = c["lambda"].get_function_configuration(FunctionName=name)
        env = dict(fn["Environment"]["Variables"])
        env[key] = end.isoformat()
        r.write(
            "deadline_" + name,
            lambda n=name, f=fn, e=env: c["lambda"].update_function_configuration(
                FunctionName=n, RevisionId=f["RevisionId"], Environment={"Variables": e}
            ),
        )
        c["lambda"].get_waiter("function_updated_v2").wait(
            FunctionName=name, WaiterConfig={"Delay": 2, "MaxAttempts": 45}
        )
        r.write(
            "concurrency_one_" + name,
            lambda n=name: c["lambda"].put_function_concurrency(
                FunctionName=n, ReservedConcurrentExecutions=1
            ),
        )
    args = schedule_args(
        c["scheduler"].get_schedule(Name=NAME + "-5m", GroupName="default")
    )
    args.update(StartDate=now, EndDate=end)
    r.write(
        "enable_bounded_schedule",
        lambda: c["scheduler"].update_schedule(**args, State="ENABLED"),
    )
    r.value.update(status="ENABLED_BOUNDED_NOT_ACCEPTED", deadline=end.isoformat())
    r.save()
    return {"status": r.value["status"], "deadline": end.isoformat()}


def stop_verify(c):
    disable(c)
    state = c["scheduler"].get_schedule(Name=NAME + "-5m", GroupName="default")["State"]
    values = [
        c["lambda"].get_function_concurrency(FunctionName=n)[
            "ReservedConcurrentExecutions"
        ]
        for n in (NAME + "-poller", CONTROLLER)
    ]
    if state != "DISABLED" or values != [0, 0]:
        raise ValueError("Final stop not verified")
    store = AWSStore(c)
    active = bool(
        store.read("RUN", "LEASE").get("active")
        or store.read("OPS", "LOCK").get("active")
    )
    return {
        "status": "DISABLED_RECONCILIATION_REQUIRED" if active else "PASS_DISABLED",
        "active_or_uncertain_lease": active,
        "scheduler": state,
        "poller_concurrency": 0,
        "controller_concurrency": 0,
    }


def rollback(c, packet, p, digest):
    if stop_verify(c)["status"] != "PASS_DISABLED":
        raise ValueError(
            "Active or uncertain controller/poller; do not roll back while running"
        )
    r = Receipt(packet / ("rollback-" + str(time.time_ns()) + ".json"), digest)
    store = AWSStore(c)
    current = store.read("EXPORT", "CURRENT")
    if current.get("refresh_window") not in (None, digest):
        raise ValueError("Pointer belongs to another operations window")
    if any(
        r["JobRunState"] in ACTIVE_JOBS
        for r in c["glue"].get_job_runs(JobName=NAME + "-gold", MaxResults=10)[
            "JobRuns"
        ]
    ):
        raise ValueError(
            "Stop/inspect the specific Glue run before configuration rollback"
        )
    previous = current.get("previous")
    if previous:
        from live_data.export import load as load_export

        # Validate the exact prior immutable object using the serving path before CAS.
        adapter = SimpleNamespace(s3=c["s3"], read=lambda pk, sk: previous)
        load_export(adapter)
        r.write(
            "restore_previous_verified_pointer",
            lambda: c["dynamodb"].put_item(
                TableName=TABLE,
                Item=pack(previous),
                ConditionExpression="#key=:key AND #sha=:sha",
                ExpressionAttributeNames={"#key": "key", "#sha": "sha256"},
                ExpressionAttributeValues=pack(
                    {":key": current["key"], ":sha": current["sha256"]}
                ),
            ),
        )
    r.write(
        "restore_previous_job_definition",
        lambda: c["glue"].update_job(
            JobName=NAME + "-gold", JobUpdate=job_update(p["baseline"]["job"])
        ),
    )
    api = c["lambda"].get_function_configuration(FunctionName=NAME + "-api")
    r.write(
        "restore_previous_api_code",
        lambda: c["lambda"].update_function_code(
            FunctionName=NAME + "-api",
            RevisionId=api["RevisionId"],
            ZipFile=(packet / "previous-api.zip").read_bytes(),
        ),
    )
    c["lambda"].get_waiter("function_updated_v2").wait(
        FunctionName=NAME + "-api", WaiterConfig={"Delay": 2, "MaxAttempts": 45}
    )
    r.value["status"] = "ROLLBACK_DISABLED_RESOURCES_RETAINED"
    r.save()
    return {"status": r.value["status"]}


def inventory(c):
    """Metadata only; never retrieve secrets or function environment values."""
    identity(c)
    functions = {}
    for suffix in ("poller", "api"):
        f = c["lambda"].get_function_configuration(FunctionName=NAME + "-" + suffix)
        functions[suffix] = {
            k: f[k]
            for k in (
                "FunctionName",
                "CodeSha256",
                "Role",
                "Timeout",
                "State",
                "LastUpdateStatus",
                "RevisionId",
            )
        }
        functions[suffix]["concurrency"] = (
            c["lambda"]
            .get_function_concurrency(FunctionName=NAME + "-" + suffix)
            .get("ReservedConcurrentExecutions")
        )
    s = c["scheduler"].get_schedule(Name=NAME + "-5m", GroupName="default")
    schedule = {**schedule_args(s), "State": s["State"]}
    for k in ("StartDate", "EndDate"):
        schedule[k] = schedule[k].isoformat()

    def item(pk, sk):
        return (
            c["dynamodb"]
            .get_item(
                TableName=TABLE, Key=pack({"pk": pk, "sk": sk}), ConsistentRead=True
            )
            .get("Item", {})
        )

    return {
        "account": ACCOUNT,
        "functions": functions,
        "schedule": schedule,
        "job": job_update(c["glue"].get_job(JobName=NAME + "-gold")["Job"]),
        "run": item("RUN", "LEASE"),
        "pointer": item("EXPORT", "CURRENT"),
    }


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument(
        "command",
        choices=[
            "inventory",
            "prepare",
            "preflight",
            "status",
            "http",
            "deploy",
            "enable",
            "stop",
            "watch",
            "rollback",
        ],
    )
    a.add_argument("--packet", type=Path)
    a.add_argument("--inventory", type=Path)
    a.add_argument("--previous-api-zip", type=Path)
    a.add_argument("--installed", action="store_true")
    a.add_argument("--execute", action="store_true")
    a.add_argument("--confirm-plan-sha256")
    args = a.parse_args()
    if args.command == "inventory":
        if (
            args.inventory is None
            or args.inventory.exists()
            or args.inventory.is_symlink()
            or args.inventory.parent.stat().st_mode & 0o077
        ):
            a.exit(
                1, "STOP: choose a new inventory file in a private directory (0700)\n"
            )
        for parent in (
            args.inventory.resolve().parent,
            *args.inventory.resolve().parents,
        ):
            if (parent / ".git").exists():
                a.exit(1, "STOP: private inventory must stay outside Git\n")
        result = inventory(clients())
        with args.inventory.open("x") as f:
            args.inventory.chmod(0o600)
            f.write(json.dumps(result, indent=2))
        print(
            json.dumps(
                {"status": "PASS_READ_ONLY_INVENTORY", "credential_values_read": False}
            )
        )
        return
    if args.packet is None:
        a.error("--packet is required")
    if args.command == "prepare":
        print(json.dumps(prepare(args.packet, args.inventory, args.previous_api_zip)))
        return
    p, digest = load(args.packet)
    if args.command not in ("preflight", "status") and (
        not args.execute or args.confirm_plan_sha256 != digest
    ):
        a.exit(
            1, "STOP: explicit write approval and exact operations plan hash required\n"
        )
    c = clients()
    identity(c)
    try:
        if args.command == "preflight":
            result = preflight(c, p, installed=args.installed)
        elif args.command == "status":
            from live_data.operations_evidence import status

            result = status(c, digest)
        elif args.command == "http":
            from live_data.operations_evidence import smoke

            result = smoke(c, args.packet, digest)
        elif args.command == "deploy":
            result = deploy(c, args.packet, p, digest)
        elif args.command == "enable":
            result = enable(c, args.packet, p, digest)
        elif args.command == "stop":
            result = stop_verify(c)
        elif args.command == "rollback":
            result = rollback(c, args.packet, p, digest)
        else:
            receipt = json.loads((args.packet / "enable-receipt.json").read_bytes())
            deadline = datetime.fromisoformat(receipt["deadline"])
            try:
                while datetime.now(UTC) < deadline:
                    if (
                        c["scheduler"].get_schedule(
                            Name=NAME + "-5m", GroupName="default"
                        )["State"]
                        == "DISABLED"
                    ):
                        break
                    time.sleep(10)
            finally:
                result = stop_verify(c)
        print(json.dumps(result))
    except Exception:
        if args.command not in ("preflight", "status", "stop", "rollback"):
            try:
                disable(c)
            except Exception:  # noqa: BLE001 -- attempt every scoped stop, without leaking SDK arguments
                print(
                    json.dumps(
                        {
                            "status": "STOP_UNCONFIRMED",
                            "action": "verify schedule and both reserved concurrencies manually",
                        }
                    )
                )
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001 -- never print credential-bearing SDK details
        print(
            json.dumps(
                {
                    "status": "FAILED_OR_UNCERTAIN",
                    "exception_type": type(e).__name__,
                    "action": "preserve receipts; stop and reconcile; no automatic write retry",
                }
            )
        )
        raise SystemExit(1) from None
