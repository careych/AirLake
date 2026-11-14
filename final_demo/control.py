"""Exact-scope commands. Mutations require --execute and the frozen plan hash."""

import argparse
import json
import os
import time
import zipfile
from pathlib import Path

from final_demo.contract import (
    ACCOUNT,
    REGION,
    SOURCE_ROOT,
    SOURCES,
    digest,
    encode,
    s3_parts,
    validate_plan,
)
from final_demo.prepare import job_definition, policy, trust


def clients():
    import boto3
    from botocore.config import Config

    if any(k.startswith("AWS_ENDPOINT_URL") for k in os.environ):
        raise ValueError("Custom AWS endpoints forbidden")
    s = boto3.Session(profile_name="airlake", region_name=REGION)
    cfg = Config(connect_timeout=10, read_timeout=30, retries={"total_max_attempts": 1})
    return {
        name: s.client(name, config=cfg)
        for name in ("sts", "s3", "glue", "iam", "lambda", "logs")
    }


def absent(call, codes):
    from botocore.exceptions import ClientError

    try:
        call()
    except ClientError as e:
        if e.response["Error"]["Code"] in codes:
            return
        raise
    raise ValueError("A proposed resource already exists")


def empty(s3, p, suffix=""):
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=p["bucket"], Prefix=p["prefix"] + suffix, ExpectedBucketOwner=ACCOUNT
    ):
        if page.get("Contents"):
            raise ValueError("Proposed S3 output is not empty")


def preflight(c, p, *, deployed=False):
    if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong account")
    c["s3"].head_bucket(Bucket=p["bucket"], ExpectedBucketOwner=ACCOUNT)
    public = c["s3"].get_public_access_block(
        Bucket=p["bucket"], ExpectedBucketOwner=ACCOUNT
    )["PublicAccessBlockConfiguration"]
    if not all(
        public.get(k) is True
        for k in (
            "BlockPublicAcls",
            "IgnorePublicAcls",
            "BlockPublicPolicy",
            "RestrictPublicBuckets",
        )
    ):
        raise ValueError("Private final-demo bucket access is not established")
    for source in SOURCES.values():
        table = c["glue"].get_table(
            DatabaseName=source["database"], Name=source["table"]
        )["Table"]
        if table.get("IsRegisteredWithLakeFormation"):
            raise ValueError("Lake Formation grants require a separately reviewed plan")
        bucket, key = s3_parts(table["Parameters"]["metadata_location"])
        expected_location = (
            f"s3://{p['bucket']}/{SOURCE_ROOT}{source['database']}.db/{source['table']}"
        )
        if table["StorageDescriptor"]["Location"].rstrip(
            "/"
        ) != expected_location or not key.startswith(
            expected_location.split("/", 3)[3] + "/metadata/"
        ):
            raise ValueError("Source metadata escaped its validated prefix")
        response = c["s3"].get_object(
            Bucket=bucket, Key=key, ExpectedBucketOwner=ACCOUNT
        )
        if response.get("SSEKMSKeyId"):
            raise ValueError("Source KMS policy must be reviewed before execution")
        metadata = json.loads(response["Body"].read())
        if int(source["snapshot"]) not in {
            s["snapshot-id"] for s in metadata["snapshots"]
        }:
            raise ValueError("Pinned source snapshot missing")
    absent(
        lambda: c["glue"].get_database(Name=p["database"]), {"EntityNotFoundException"}
    )
    if deployed:
        for suffix in ("warehouse/", "exports/", "evidence/"):
            empty(c["s3"], p, suffix)
        if c["glue"].get_job_runs(JobName=p["job"], MaxResults=1).get("JobRuns"):
            raise ValueError("A job was already attempted; no automatic retry")
    else:
        empty(c["s3"], p)
        absent(lambda: c["glue"].get_job(JobName=p["job"]), {"EntityNotFoundException"})
        absent(lambda: c["iam"].get_role(RoleName=p["glue_role"]), {"NoSuchEntity"})
        absent(lambda: c["iam"].get_role(RoleName=p["api_role"]), {"NoSuchEntity"})
        absent(
            lambda: c["lambda"].get_function(FunctionName=p["api"]),
            {"ResourceNotFoundException"},
        )
    return {
        "status": "PASS",
        "source_snapshots_present": True,
        "write_permissions": "NOT PROVEN by read checks",
    }


class Receipt:
    def __init__(self, path, plan_sha):
        self.path = Path(path)
        if self.path.exists() or self.path.is_symlink():
            raise ValueError("Receipt already exists; do not overwrite or retry")
        self.value = {"status": "RUNNING", "plan_sha256": plan_sha, "operations": []}
        os.close(os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600))
        self.save()

    def save(self):
        temporary = self.path.with_suffix(".tmp")
        with os.fdopen(
            os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb"
        ) as f:
            f.write(encode(self.value))
            f.flush()
            os.fsync(f.fileno())
        temporary.replace(self.path)

    def write(self, name, call):
        item = {"operation": name, "status": "UNCERTAIN"}
        self.value["operations"].append(item)
        self.save()
        result = call()
        if isinstance(result, dict):
            item["identifiers"] = {
                k: result[k]
                for k in (
                    "JobRunId",
                    "FunctionArn",
                    "FunctionUrl",
                    "CodeSha256",
                    "Version",
                )
                if k in result
            }
            if "JobRunId" in result:
                self.value["job_run_id"] = result["JobRunId"]
        item["status"] = "RETURNED_SUCCESS"
        self.save()
        return result


def deploy_glue(c, p, packet, sha, receipt):
    preflight(c, p)
    w = receipt.write
    w(
        "iam.create_glue_role",
        lambda: c["iam"].create_role(
            RoleName=p["glue_role"],
            AssumeRolePolicyDocument=json.dumps(trust("glue")),
            Tags=[{"Key": "Scope", "Value": p["run"]}],
        ),
    )
    w(
        "iam.put_glue_inline_policy",
        lambda: c["iam"].put_role_policy(
            RoleName=p["glue_role"],
            PolicyName="isolated-final-demo",
            PolicyDocument=json.dumps(policy(p)),
        ),
    )
    for name in (
        "runtime.jar",
        "aws-bundle.jar",
        "glue_job.py",
        "payload.zip",
        "reference.json",
        "plan.json",
    ):
        raw = (packet / name).read_bytes()
        w(
            "s3.put_artifact." + name,
            lambda name=name, raw=raw: c["s3"].put_object(
                Bucket=p["bucket"],
                Key=p["prefix"] + "artifacts/" + name,
                Body=raw,
                ServerSideEncryption="AES256",
                IfNoneMatch="*",
                ExpectedBucketOwner=ACCOUNT,
            ),
        )
    w("glue.create_job", lambda: c["glue"].create_job(**job_definition(p, sha)))


def run_glue(c, p, packet, sha, receipt):
    previous = json.loads((packet / "deploy-glue-receipt.json").read_text())
    if previous["status"] != "PASS" or previous["plan_sha256"] != sha:
        raise ValueError("Successful deployment receipt required")
    preflight(c, p, deployed=True)
    actual = c["glue"].get_job(JobName=p["job"])["Job"]
    expected = job_definition(p, sha)
    for key in (
        "Role",
        "Command",
        "GlueVersion",
        "WorkerType",
        "NumberOfWorkers",
        "Timeout",
        "MaxRetries",
        "NonOverridableArguments",
        "ExecutionProperty",
    ):
        if actual[key] != expected[key]:
            raise ValueError("Deployed job differs from frozen definition")
    response = receipt.write(
        "glue.start_job_run", lambda: c["glue"].start_job_run(JobName=p["job"])
    )
    receipt.value["job_run_id"] = response["JobRunId"]


def watch(c, p, packet):
    receipt = json.loads((packet / "run-glue-receipt.json").read_text())
    job_id = receipt["job_run_id"]
    target = packet / "job-result.json"
    if target.exists():
        raise ValueError("Result already retained")
    for _ in range(65):
        value = c["glue"].get_job_run(JobName=p["job"], RunId=job_id)["JobRun"]
        state = value["JobRunState"]
        if state in {"SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED", "ERROR", "EXPIRED"}:
            result = {
                k: value.get(k)
                for k in (
                    "Id",
                    "JobRunState",
                    "ExecutionTime",
                    "DPUSeconds",
                    "GlueVersion",
                    "WorkerType",
                    "NumberOfWorkers",
                )
            }
            result["job_parameters"] = job_definition(
                p, digest((packet / "plan.json").read_bytes())
            )["NonOverridableArguments"]
            if state == "SUCCEEDED":
                obj = c["s3"].get_object(
                    Bucket=p["bucket"],
                    Key=p["prefix"] + "evidence/glue-result.json",
                    ExpectedBucketOwner=ACCOUNT,
                )
                result["validation"] = json.loads(obj["Body"].read())
                if result["validation"]["status"] != "PASS":
                    raise ValueError("Job validation was not PASS")
            result["cloudwatch_logs"] = []
            for suffix in ("error", "output"):
                group = p["log_prefix"] + "/" + suffix
                try:
                    streams = (
                        c["logs"]
                        .describe_log_streams(
                            logGroupName=group, logStreamNamePrefix=job_id
                        )
                        .get("logStreams", [])
                    )
                    result["cloudwatch_logs"].append(
                        {
                            "group": group,
                            "streams": [s["logStreamName"] for s in streams],
                        }
                    )
                except c["logs"].exceptions.ResourceNotFoundException:
                    result["cloudwatch_logs"].append(
                        {"group": group, "streams": [], "status": "not_found"}
                    )
                except Exception as error:  # noqa: BLE001 -- preserve the Glue outcome if log access fails
                    result["cloudwatch_logs"].append(
                        {
                            "group": group,
                            "status": "read_failed",
                            "exception_type": type(error).__name__,
                        }
                    )
            target.write_bytes(encode(result))
            target.chmod(0o600)
            print(json.dumps(result, indent=2))
            if state != "SUCCEEDED":
                raise ValueError("Glue failed; retain resources and evidence; no retry")
            return
        time.sleep(10)
    raise TimeoutError("Watch bounded; job may still run; inspect/stop separately")


def deploy_api(c, p, packet, receipt):
    if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong account")
    result = json.loads((packet / "job-result.json").read_text())
    if result["JobRunState"] != "SUCCEEDED" or result["validation"]["status"] != "PASS":
        raise ValueError("AWS-computed export must pass before API deployment")
    actual_run = c["glue"].get_job_run(JobName=p["job"], RunId=result["Id"])["JobRun"]
    if actual_run["JobRunState"] != "SUCCEEDED":
        raise ValueError("AWS does not confirm a successful Glue run")
    obj = c["s3"].get_object(
        Bucket=p["bucket"],
        Key=p["prefix"] + "exports/gold.json",
        ExpectedBucketOwner=ACCOUNT,
    )
    raw = obj["Body"].read()
    if digest(raw) != result["validation"]["export_sha256"]:
        raise ValueError("Export hash mismatch")
    absent(
        lambda: c["lambda"].get_function(FunctionName=p["api"]),
        {"ResourceNotFoundException"},
    )
    absent(lambda: c["iam"].get_role(RoleName=p["api_role"]), {"NoSuchEntity"})
    w = receipt.write
    w(
        "iam.create_api_role",
        lambda: c["iam"].create_role(
            RoleName=p["api_role"], AssumeRolePolicyDocument=json.dumps(trust("lambda"))
        ),
    )
    w(
        "iam.put_api_policy",
        lambda: c["iam"].put_role_policy(
            RoleName=p["api_role"],
            PolicyName="read-one-verified-export",
            PolicyDocument=json.dumps(policy(p, True)),
        ),
    )
    w(
        "lambda.create_function",
        lambda: c["lambda"].create_function(
            FunctionName=p["api"],
            Runtime="python3.12",
            Role=f"arn:aws:iam::{ACCOUNT}:role/{p['api_role']}",
            Handler="final_demo.lambda_api.handler",
            Code={"ZipFile": (packet / "api.zip").read_bytes()},
            Timeout=10,
            MemorySize=128,
            Architectures=["arm64"],
            Publish=True,
            Environment={
                "Variables": {
                    "AIRLAKE_FINAL_RUN": p["run"],
                    "AIRLAKE_EXPORT_SHA256": digest(raw),
                }
            },
        ),
    )
    c["lambda"].get_waiter("function_active_v2").wait(
        FunctionName=p["api"], WaiterConfig={"Delay": 2, "MaxAttempts": 30}
    )
    w(
        "lambda.set_reserved_concurrency",
        lambda: c["lambda"].put_function_concurrency(
            FunctionName=p["api"], ReservedConcurrentExecutions=1
        ),
    )
    response = w(
        "lambda.create_iam_url",
        lambda: c["lambda"].create_function_url_config(
            FunctionName=p["api"], AuthType="AWS_IAM", InvokeMode="BUFFERED"
        ),
    )
    receipt.value["function_url"] = response["FunctionUrl"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=["preflight", "deploy-glue", "run-glue", "watch", "deploy-api"],
    )
    parser.add_argument("--packet", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm-plan-sha256")
    a = parser.parse_args()
    packet = a.packet.resolve()
    if (
        packet.stat().st_uid != os.getuid()
        or packet.stat().st_mode & 0o077
        or any((parent / ".git").exists() for parent in (packet, *packet.parents))
    ):
        raise ValueError("Deployment packet must remain private and outside Git")
    raw = (packet / "plan.json").read_bytes()
    sha = digest(raw)
    p = validate_plan(json.loads(raw))
    for name, expected in p["artifacts"].items():
        if Path(name).name != name or digest((packet / name).read_bytes()) != expected:
            raise ValueError("Artifact integrity mismatch")
    with zipfile.ZipFile(packet / "payload.zip") as archive:
        for name in ("control.py", "prepare.py", "contract.py"):
            if (
                archive.read("final_demo/" + name)
                != Path(__file__).with_name(name).read_bytes()
            ):
                raise ValueError(
                    "Control code changed after packet freeze; rebuild before approval"
                )
    mutating = a.command in ("deploy-glue", "run-glue", "deploy-api")
    if mutating and (not a.execute or a.confirm_plan_sha256 != sha):
        parser.exit(
            1,
            "STOP: separate exact-scope write approval, --execute and exact plan SHA-256 required\n",
        )
    c = clients()
    if a.command == "preflight":
        print(json.dumps(preflight(c, p)))
    elif a.command == "watch":
        watch(c, p, packet)
    else:
        receipt = Receipt(packet / (a.command + "-receipt.json"), sha)
        try:
            {
                "deploy-glue": deploy_glue,
                "run-glue": run_glue,
                "deploy-api": lambda c, p, d, s, r: deploy_api(c, p, d, r),
            }[a.command](c, p, packet, sha, receipt)
            receipt.value["status"] = "PASS"
            receipt.save()
            print(json.dumps(receipt.value))
        except Exception as e:  # noqa: BLE001 -- preserve uncertain write evidence, no retry/cleanup
            receipt.value.update(
                status="FAILED_OR_UNCERTAIN", exception_type=type(e).__name__
            )
            receipt.save()
            parser.exit(
                1, "STOP: inspect private receipt; no retry or automatic cleanup\n"
            )


if __name__ == "__main__":
    main()
