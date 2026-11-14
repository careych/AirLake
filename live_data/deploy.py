"""Offline packet, read-only preflight, then separately approved exact-scope operations."""

import argparse
import json
import os
import time
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from final_demo.control import Receipt, absent
from final_demo.prepare import JARS, trust
from ingestion.openaq import key_from_environment
from live_data.contract import (
    ACCOUNT,
    BUCKET,
    DATABASE,
    MAX_INVOCATIONS,
    MAX_RECORDS,
    NAME,
    PREFIX,
    REGION,
    RUN,
    SECRET_NAME,
    TABLE,
    encode,
    sha,
)

ROOT = Path(__file__).resolve().parents[1]
SERVICES = (
    "sts",
    "s3",
    "iam",
    "lambda",
    "dynamodb",
    "secretsmanager",
    "scheduler",
    "glue",
    "logs",
    "athena",
)


def clients():
    import boto3
    from botocore.config import Config

    if any(k.startswith("AWS_ENDPOINT_URL") for k in os.environ):
        raise ValueError("Custom endpoints forbidden")
    s = boto3.Session(profile_name="airlake", region_name=REGION)
    c = Config(retries={"total_max_attempts": 1}, connect_timeout=5, read_timeout=210)
    return {n: s.client(n, config=c) for n in SERVICES}


def statement(actions, resources, **extra):
    return {"Effect": "Allow", "Action": actions, "Resource": resources, **extra}


def policies(secret_arn):
    table = f"arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{TABLE}"

    def logs(suffix):
        group = (
            f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/aws/lambda/{NAME}-{suffix}:*"
        )
        return statement(["logs:CreateLogStream", "logs:PutLogEvents"], group)

    poller = [
        logs("poller"),
        statement(["secretsmanager:GetSecretValue"], secret_arn),
        statement(
            ["s3:PutObject", "s3:GetObject"], f"arn:aws:s3:::{BUCKET}/{PREFIX}bronze/*"
        ),
        statement(
            [
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:UpdateItem",
                "dynamodb:ConditionCheckItem",
            ],
            table,
            Condition={
                "ForAllValues:StringEquals": {
                    "dynamodb:LeadingKeys": ["RUN", "CURSOR", "LATEST", "POLL"]
                }
            },
        ),
    ]
    api = [
        logs("api"),
        statement(
            ["dynamodb:GetItem"],
            table,
            Condition={
                "ForAllValues:StringEquals": {
                    "dynamodb:LeadingKeys": ["LATEST", "POLL", "EXPORT"]
                }
            },
        ),
        statement(
            ["s3:GetObject"],
            [
                f"arn:aws:s3:::{BUCKET}/{PREFIX}exports/*",
                f"arn:aws:s3:::{BUCKET}/final-demo/20261114_34207148/exports/gold.json",
            ],
        ),
    ]
    scheduler = [
        statement(
            ["lambda:InvokeFunction"],
            f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NAME}-poller",
        )
    ]
    log = f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/airlake/live-demo/{RUN}/*"
    glue = [
        statement(
            ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            [log, log + ":*"],
        ),
        statement(["s3:GetObject"], f"arn:aws:s3:::{BUCKET}/{PREFIX}*"),
        statement(
            ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
            [
                f"arn:aws:s3:::{BUCKET}/{PREFIX}{s}*"
                for s in ("warehouse/", "exports/", "evidence/", "temp/")
            ],
        ),
        statement(
            ["s3:ListBucket"],
            f"arn:aws:s3:::{BUCKET}",
            Condition={"StringLike": {"s3:prefix": [PREFIX + "*"]}},
        ),
        statement(["s3:GetBucketLocation"], f"arn:aws:s3:::{BUCKET}"),
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
            [
                f"arn:aws:glue:{REGION}:{ACCOUNT}:catalog",
                f"arn:aws:glue:{REGION}:{ACCOUNT}:database/{DATABASE}",
                f"arn:aws:glue:{REGION}:{ACCOUNT}:table/{DATABASE}/*",
            ],
        ),
    ]
    return {
        n: {"Version": "2012-10-17", "Statement": v}
        for n, v in [
            ("poller", poller),
            ("api", api),
            ("scheduler", scheduler),
            ("glue", glue),
        ]
    }


def prepare(output, runtime, bundle):
    output = Path(output)
    if output.exists() or any(
        (p / ".git").exists() for p in (output.resolve(), *output.resolve().parents)
    ):
        raise ValueError("Choose a new private packet directory outside Git")
    # Verify first, before creating any packet.
    jars = {}
    for name, path in [("runtime.jar", runtime), ("aws-bundle.jar", bundle)]:
        raw = Path(path).read_bytes()
        if sha(raw) != JARS[name]:
            raise ValueError("JAR checksum mismatch")
        jars[name] = raw
    output.mkdir(mode=0o700, parents=True)
    with zipfile.ZipFile(output / "payload.zip", "w", zipfile.ZIP_DEFLATED) as z:
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
            z.writestr(info, path.read_bytes())
        generated = zipfile.ZipInfo("scripts/__init__.py", (2026, 11, 14, 0, 0, 0))
        z.writestr(generated, b"")
    for name, raw in jars.items():
        (output / name).write_bytes(raw)
    (output / "glue_job.py").write_bytes((ROOT / "live_data/glue_job.py").read_bytes())
    artifacts = {
        n: sha((output / n).read_bytes())
        for n in ["payload.zip", "runtime.jar", "aws-bundle.jar", "glue_job.py"]
    }
    plan = {
        "run": RUN,
        "account": ACCOUNT,
        "region": REGION,
        "prefix": f"s3://{BUCKET}/{PREFIX}",
        "state_table": TABLE,
        "new_functions": [NAME + "-poller", NAME + "-api"],
        "new_roles": [NAME + "-" + s for s in ["poller", "api", "scheduler", "glue"]],
        "scheduler": NAME + "-5m",
        "scheduler_initial_state": "DISABLED",
        "secret_name": SECRET_NAME,
        "glue_job": NAME + "-gold",
        "glue_database": DATABASE,
        "max_invocations": MAX_INVOCATIONS,
        "max_raw_records_per_invocation": MAX_RECORDS,
        "test_minutes": 30,
        "glue_runs": 1,
        "athena_selects": 1,
        "http_requests": 8,
        "artifacts": artifacts,
        "production_sha256": {
            str(f.relative_to(ROOT)): sha(f.read_bytes())
            for f in [ROOT / "scripts/gold_batch.py", ROOT / "scripts/silver_batch.py"]
        },
        "classification": "Recent OpenAQ measurements only; never substitute historical replay as live",
    }
    (output / "plan.json").write_bytes(encode(plan))
    (output / "plan.sha256").write_text(sha(encode(plan)) + "\n")
    for f in output.iterdir():
        f.chmod(0o600)
    return plan


def load_packet(packet, verify_artifacts=True):
    raw = (packet / "plan.json").read_bytes()
    p = json.loads(raw)
    if (
        p["run"] != RUN
        or p["account"] != ACCOUNT
        or p["region"] != REGION
        or p["state_table"] != TABLE
        or p["prefix"] != f"s3://{BUCKET}/{PREFIX}"
    ):
        raise ValueError("Packet scope mismatch")
    for name, digest in p["artifacts"].items() if verify_artifacts else []:
        if (
            name not in ("payload.zip", "runtime.jar", "aws-bundle.jar", "glue_job.py")
            or sha((packet / name).read_bytes()) != digest
        ):
            raise ValueError("Artifact mismatch")
    if verify_artifacts:
        with zipfile.ZipFile(packet / "payload.zip") as z:
            for name in z.namelist():
                if name == "scripts/__init__.py":
                    if z.read(name) != b"":
                        raise ValueError("Generated package initializer mismatch")
                    continue
                if not (ROOT / name).resolve().is_relative_to(ROOT):
                    raise ValueError("Unsafe archive path")
                if z.read(name) != (ROOT / name).read_bytes():
                    raise ValueError("Source changed after preparation")
    return p, sha(raw)


def identity(c):
    if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong account")


def preflight(c):
    identity(c)
    c["s3"].head_bucket(Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT)
    block = c["s3"].get_public_access_block(Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT)[
        "PublicAccessBlockConfiguration"
    ]
    if not all(
        block.get(n)
        for n in [
            "BlockPublicAcls",
            "IgnorePublicAcls",
            "BlockPublicPolicy",
            "RestrictPublicBuckets",
        ]
    ):
        raise ValueError("Bucket is not fully private")
    for page in (
        c["s3"]
        .get_paginator("list_objects_v2")
        .paginate(Bucket=BUCKET, Prefix=PREFIX, ExpectedBucketOwner=ACCOUNT)
    ):
        if page.get("Contents"):
            raise ValueError("New prefix is not empty")
    absent(
        lambda: c["dynamodb"].describe_table(TableName=TABLE),
        {"ResourceNotFoundException"},
    )
    absent(
        lambda: c["secretsmanager"].describe_secret(SecretId=SECRET_NAME),
        {"ResourceNotFoundException"},
    )
    absent(lambda: c["glue"].get_database(Name=DATABASE), {"EntityNotFoundException"})
    absent(
        lambda: c["glue"].get_job(JobName=NAME + "-gold"), {"EntityNotFoundException"}
    )
    absent(
        lambda: c["scheduler"].get_schedule(Name=NAME + "-5m", GroupName="default"),
        {"ResourceNotFoundException"},
    )
    for suffix in ["poller", "api"]:
        absent(
            lambda s=suffix: c["lambda"].get_function(FunctionName=NAME + "-" + s),
            {"ResourceNotFoundException"},
        )
    for suffix in ["poller", "api", "scheduler", "glue"]:
        absent(
            lambda s=suffix: c["iam"].get_role(RoleName=NAME + "-" + s),
            {"NoSuchEntity"},
        )
    group = c["athena"].get_work_group(WorkGroup="airlake")["WorkGroup"]
    if (
        group["State"] != "ENABLED"
        or not group["Configuration"]["EnforceWorkGroupConfiguration"]
        or group["Configuration"]["ResultConfiguration"]["OutputLocation"]
        != "s3://airlake-701364614078-athena-results/"
        or group["Configuration"]["EngineVersion"]["EffectiveEngineVersion"]
        != "Athena engine version 3"
    ):
        raise ValueError("Athena workgroup scope or engine differs")
    legacy = (
        c["s3"]
        .get_object(
            Bucket=BUCKET,
            Key="final-demo/20261114_34207148/exports/gold.json",
            ExpectedBucketOwner=ACCOUNT,
        )["Body"]
        .read(100001)
    )
    if (
        sha(legacy)
        != "8775e0daf60b6000f44af460ca680d56a182a3c51c89fbe032ff7eb4194c8826"
    ):
        raise ValueError("Preserved historical export hash differs")
    settings = c["lambda"].get_account_settings()
    if settings["AccountLimit"]["UnreservedConcurrentExecutions"] < 2:
        raise ValueError("Not enough unreserved concurrency for two isolated functions")
    for suffix in ("poller", "api"):
        log_name = "/aws/lambda/" + NAME + "-" + suffix
        groups = (
            c["logs"]
            .describe_log_groups(logGroupNamePrefix=log_name)
            .get("logGroups", [])
        )
        if any(g["logGroupName"] == log_name for g in groups):
            raise ValueError("New log group already exists")
    return {
        "status": "PASS_READ_ONLY",
        "scope": RUN,
        "write_permissions": "not established by reads",
    }


def deploy(c, packet, digest, env_file):
    preflight(c)
    ledger = Receipt(packet / "deployment-receipt.json", digest)
    write = ledger.write
    key = key_from_environment(env_file)
    secret = write(
        "create_scoped_openaq_secret",
        lambda secret_value=key: c["secretsmanager"].create_secret(
            Name=SECRET_NAME, SecretString=secret_value
        ),
    )
    secret_arn = secret["ARN"]
    del key
    from live_data.contract import config

    deadline = datetime.now(UTC).replace(microsecond=0) + timedelta(minutes=30)
    env = {
        "AIRLAKE_LIVE_RUN": RUN,
        "OPENAQ_SECRET_ARN": secret_arn,
        "AIRLAKE_LIVE_DEADLINE": deadline.isoformat(),
    }
    config(env)
    write(
        "create_state_table",
        lambda: c["dynamodb"].create_table(
            TableName=TABLE,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        ),
    )
    c["dynamodb"].get_waiter("table_exists").wait(
        TableName=TABLE, WaiterConfig={"Delay": 2, "MaxAttempts": 30}
    )
    for suffix, pol in policies(secret_arn).items():
        name = NAME + "-" + suffix
        service = (
            "glue"
            if suffix == "glue"
            else "scheduler"
            if suffix == "scheduler"
            else "lambda"
        )
        tr = trust(service)
        if suffix == "scheduler":
            tr["Statement"][0]["Condition"] = {
                "StringEquals": {"aws:SourceAccount": ACCOUNT},
                "ArnEquals": {
                    "aws:SourceArn": f"arn:aws:scheduler:{REGION}:{ACCOUNT}:schedule-group/default"
                },
            }
        write(
            "create_role_" + suffix,
            lambda n=name, t=tr: c["iam"].create_role(
                RoleName=n, AssumeRolePolicyDocument=json.dumps(t)
            ),
        )
        write(
            "scope_role_" + suffix,
            lambda n=name, p=pol: c["iam"].put_role_policy(
                RoleName=n,
                PolicyName="isolated-live-demo",
                PolicyDocument=json.dumps(p),
            ),
        )
    for suffix in ["poller", "api"]:
        write(
            "log_group_" + suffix,
            lambda s=suffix: c["logs"].create_log_group(
                logGroupName="/aws/lambda/" + NAME + "-" + s
            ),
        )
        write(
            "log_retention_" + suffix,
            lambda s=suffix: c["logs"].put_retention_policy(
                logGroupName="/aws/lambda/" + NAME + "-" + s, retentionInDays=3
            ),
        )
    time.sleep(10)  # One propagation delay, never retry an ambiguous creation.
    for suffix in ["poller", "api"]:
        values = (
            env
            if suffix == "poller"
            else {
                "AIRLAKE_LIVE_RUN": RUN,
                "AIRLAKE_FINAL_RUN": "20261114_34207148",
                "AIRLAKE_EXPORT_SHA256": "8775e0daf60b6000f44af460ca680d56a182a3c51c89fbe032ff7eb4194c8826",
            }
        )
        write(
            "create_function_" + suffix,
            lambda s=suffix, v=values: c["lambda"].create_function(
                FunctionName=NAME + "-" + s,
                Role=f"arn:aws:iam::{ACCOUNT}:role/{NAME}-{s}",
                Runtime="python3.12",
                Architectures=["arm64"],
                Handler="live_data."
                + ("poller" if s == "poller" else "api")
                + ".handler",
                Code={"ZipFile": (packet / "payload.zip").read_bytes()},
                Timeout=180 if s == "poller" else 15,
                MemorySize=128,
                Environment={"Variables": v},
                Publish=True,
            ),
        )
        c["lambda"].get_waiter("function_active_v2").wait(
            FunctionName=NAME + "-" + suffix,
            WaiterConfig={"Delay": 2, "MaxAttempts": 30},
        )
        write(
            "concurrency_" + suffix,
            lambda s=suffix: c["lambda"].put_function_concurrency(
                FunctionName=NAME + "-" + s, ReservedConcurrentExecutions=1
            ),
        )
    write(
        "no_async_retry",
        lambda: c["lambda"].put_function_event_invoke_config(
            FunctionName=NAME + "-poller",
            MaximumRetryAttempts=0,
            MaximumEventAgeInSeconds=60,
        ),
    )
    url = write(
        "private_api_url",
        lambda: c["lambda"].create_function_url_config(
            FunctionName=NAME + "-api", AuthType="AWS_IAM"
        ),
    )
    schedule = {
        "Name": NAME + "-5m",
        "GroupName": "default",
        "ScheduleExpression": "rate(5 minutes)",
        "ScheduleExpressionTimezone": "UTC",
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "State": "DISABLED",
        "StartDate": datetime.now(UTC),
        "EndDate": deadline,
        "Target": {
            "Arn": "arn:aws:scheduler:::aws-sdk:lambda:invoke",
            "RoleArn": f"arn:aws:iam::{ACCOUNT}:role/{NAME}-scheduler",
            "Input": json.dumps(
                {
                    "FunctionName": NAME + "-poller",
                    "InvocationType": "Event",
                    "Payload": "{}",
                }
            ),
            "RetryPolicy": {"MaximumEventAgeInSeconds": 60, "MaximumRetryAttempts": 0},
        },
    }
    write("disabled_schedule", lambda: c["scheduler"].create_schedule(**schedule))
    ledger.value.update(
        status="PASS_DEPLOYED_DISABLED",
        deadline=deadline.isoformat(),
        function_url=url["FunctionUrl"],
        secret_arn=secret_arn,
    )
    ledger.save()
    return ledger.value


def switch(c, packet, digest, enable):
    name = NAME + "-5m"
    s = c["scheduler"].get_schedule(Name=name, GroupName="default")
    if enable:
        if s["EndDate"] <= datetime.now(UTC):
            raise ValueError("Window expired; do not extend silently")
        fn = c["lambda"].get_function_configuration(FunctionName=NAME + "-poller")
        from live_data.contract import config

        _, deadline = config(fn.get("Environment", {}).get("Variables", {}))
        if (
            deadline != s["EndDate"]
            or c["lambda"].get_function_concurrency(FunctionName=NAME + "-poller")[
                "ReservedConcurrentExecutions"
            ]
            != 1
        ):
            raise ValueError("Deadline or concurrency guard differs")
        manual = json.loads((packet / "manual-poll-receipt.json").read_text())
        if (
            manual.get("status") != "PASS"
            or manual.get("result", {}).get("accepted_new", 0) < 1
        ):
            raise ValueError(
                "One accepted manual source poll is required before enabling"
            )
        if (
            fn["Role"] != f"arn:aws:iam::{ACCOUNT}:role/{NAME}-poller"
            or fn["CodeSha256"]
            != __import__("base64")
            .b64encode(
                __import__("hashlib")
                .sha256((packet / "payload.zip").read_bytes())
                .digest()
            )
            .decode()
        ):
            raise ValueError("Poller role or code hash differs")
        if fn["State"] != "Active":
            raise ValueError("Poller unavailable")
    args = {
        k: s[k]
        for k in [
            "Name",
            "GroupName",
            "ScheduleExpression",
            "ScheduleExpressionTimezone",
            "FlexibleTimeWindow",
            "Target",
            "StartDate",
            "EndDate",
        ]
    }
    args["State"] = "ENABLED" if enable else "DISABLED"
    receipt = Receipt(
        packet
        / (
            "enable-receipt.json"
            if enable
            else "disable-receipt-" + str(time.time_ns()) + ".json"
        ),
        digest,
    )
    try:
        receipt.write(
            "schedule_" + args["State"].lower(),
            lambda: c["scheduler"].update_schedule(**args),
        )
    finally:
        if not enable:
            # Emergency stopping must still block invocation if Scheduler is unavailable.
            receipt.write(
                "stop_poller",
                lambda: c["lambda"].put_function_concurrency(
                    FunctionName=NAME + "-poller", ReservedConcurrentExecutions=0
                ),
            )
    final = c["scheduler"].get_schedule(Name=name, GroupName="default")
    if final["State"] != args["State"]:
        raise ValueError("Scheduler final state not verified")
    if (
        not enable
        and c["lambda"].get_function_concurrency(FunctionName=NAME + "-poller")[
            "ReservedConcurrentExecutions"
        ]
        != 0
    ):
        raise ValueError("Poller stop not verified")
    receipt.value["status"] = "PASS"
    receipt.save()
    return {"status": "PASS", "schedule_state": args["State"]}


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument(
        "command",
        choices=[
            "prepare",
            "preflight",
            "status",
            "deploy",
            "poll-once",
            "enable",
            "disable",
            "freeze-glue",
            "verify-glue",
        ],
    )
    a.add_argument("--packet", type=Path, required=True)
    a.add_argument("--runtime-jar")
    a.add_argument("--aws-bundle")
    a.add_argument("--env-file")
    a.add_argument("--execute", action="store_true")
    a.add_argument("--confirm-plan-sha256")
    args = a.parse_args()
    if args.command == "prepare":
        p = prepare(args.packet, args.runtime_jar, args.aws_bundle)
        print(
            json.dumps(
                {
                    "status": "PREPARED_OFFLINE",
                    "run": p["run"],
                    "plan_sha256": sha(encode(p)),
                }
            )
        )
        return
    p, digest = load_packet(args.packet, verify_artifacts=args.command != "disable")
    if args.command not in ("preflight", "status") and (
        not args.execute or args.confirm_plan_sha256 != digest
    ):
        a.exit(1, "STOP: exact write approval and plan hash required\n")
    c = clients()
    identity(c)
    if args.command == "preflight":
        result = preflight(c)
    elif args.command == "status":
        from live_data.status import collect

        result = collect(c)
    elif args.command == "deploy":
        result = deploy(c, args.packet, digest, args.env_file)
    elif args.command in ("enable", "disable"):
        result = switch(c, args.packet, digest, args.command == "enable")
    elif args.command == "poll-once":
        r = Receipt(args.packet / "manual-poll-receipt.json", digest)
        response = r.write(
            "invoke_once",
            lambda: c["lambda"].invoke(
                FunctionName=NAME + "-poller",
                InvocationType="RequestResponse",
                Payload=b"{}",
            ),
        )
        body = json.loads(response["Payload"].read())
        if response.get("FunctionError") or body.get("status") != "PASS":
            raise ValueError("Poll failed; no automatic retry")
        r.value.update(status="PASS", result=body)
        r.save()
        result = r.value
    else:
        from live_data.run_glue import freeze, verify

        result = (freeze if args.command == "freeze-glue" else verify)(
            c, args.packet, p, digest
        )
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001 -- never log credential-bearing request arguments
        print(
            json.dumps(
                {
                    "status": "FAILED_OR_UNCERTAIN",
                    "exception_type": type(e).__name__,
                    "action": "inspect private receipts; no automatic retry",
                }
            )
        )
        raise SystemExit(1) from None
