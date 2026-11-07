"""Read-only staging checks. Never queries Athena, reads secrets or consumes SQS."""

import argparse
import hashlib
import json
import re
import subprocess
import tempfile
from pathlib import Path

from alerts.runtime import ROOT, load_plan


class CLI:
    def __init__(self, profile, region, executable="aws"):
        self.profile, self.region, self.executable = profile, region, executable

    def call(self, service, operation, *arguments):
        result = subprocess.run(
            [
                self.executable,
                "--profile",
                self.profile,
                "--region",
                self.region,
                "--no-cli-pager",
                service,
                operation,
                *arguments,
                "--output",
                "json",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode:
            code = re.search(r"An error occurred \(([^)]+)\)", result.stderr)
            return {
                "error": code.group(1)
                if code
                else (
                    "ExpiredSession"
                    if "expired" in result.stderr.lower()
                    else "CLIError"
                ),
                "not_found": bool(
                    re.search(
                        r"not found|does not exist|nonexistent|not exist",
                        result.stderr,
                        re.IGNORECASE,
                    )
                ),
            }
        return json.loads(result.stdout)


def preflight(plan, client):
    account, region, n = plan["account"], plan["region"], plan["names"]
    identity = client.call("sts", "get-caller-identity")
    report = {
        "account": identity.get("Account"),
        "region": region,
        "status": "BLOCKED",
        "absence": {},
        "snapshots": [],
    }
    if report["account"] != account:
        report["blocker"] = "STS identity failed: " + identity.get(
            "error", "WrongAccount"
        )
        return report
    checks = [
        (
            "sns_topic",
            "sns",
            "get-topic-attributes",
            ["--topic-arn", f"arn:aws:sns:{region}:{account}:{n['sns_topic']}"],
            {"NotFound", "NotFoundException"},
        ),
        (
            "lambda",
            "lambda",
            "get-function",
            ["--function-name", n["lambda"]],
            {"ResourceNotFoundException"},
        ),
        (
            "workgroup",
            "athena",
            "get-work-group",
            ["--work-group", n["workgroup"]],
            {"InvalidRequestException", "ResourceNotFoundException"},
        ),
    ]
    checks += [
        (
            key,
            "sqs",
            "get-queue-url",
            ["--queue-name", n[key], "--queue-owner-aws-account-id", account],
            {"AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"},
        )
        for key in ["queue", "dlq"]
    ]
    checks += [
        (
            key,
            "dynamodb",
            "describe-table",
            ["--table-name", n[key]],
            {"ResourceNotFoundException"},
        )
        for key in ["subscriptions", "delivery_ledger"]
    ]
    checks += [
        (key, "iam", "get-role", ["--role-name", n[key]], {"NoSuchEntity"})
        for key in ["consumer_role", "reader_role"]
    ]
    for key, service, operation, args, codes in checks:
        result = client.call(service, operation, *args)
        missing = result.get("error") in codes
        if result.get("error") == "InvalidRequestException":
            missing = missing and result.get("not_found", False)
        report["absence"][key] = (
            "ABSENT"
            if missing
            else ("UNKNOWN: " + result["error"] if "error" in result else "EXISTS")
        )
    logs = client.call(
        "logs", "describe-log-groups", "--log-group-name-prefix", n["log_group"]
    )
    report["absence"]["log_group"] = (
        "UNKNOWN"
        if "error" in logs
        else (
            "EXISTS"
            if any(
                g["logGroupName"] == n["log_group"] for g in logs.get("logGroups", [])
            )
            else "ABSENT"
        )
    )
    reader = plan["reader"]
    count = 0
    token = None
    seen_tokens = set()
    for _ in range(100):
        args = [
            "--bucket",
            reader["results_bucket"],
            "--prefix",
            reader["results_prefix"],
            "--expected-bucket-owner",
            account,
            "--no-paginate",
        ]
        if token:
            args += ["--continuation-token", token]
        result = client.call("s3api", "list-objects-v2", *args)
        if "error" in result:
            count = None
            break
        count += len(result.get("Contents", []))
        if not result.get("IsTruncated"):
            break
        next_token = result.get("NextContinuationToken")
        if not next_token or next_token in seen_tokens:
            count = None
            break
        seen_tokens.add(next_token)
        token = next_token
    else:
        count = None
    report["results_prefix_objects"] = count
    report["existing_workgroup"] = client.call(
        "athena", "get-work-group", "--work-group", "airlake"
    ).get("WorkGroup", {})
    secret = client.call(
        "secretsmanager", "describe-secret", "--secret-id", plan["secrets"]["name"]
    )
    report["test_secret"] = {
        "metadata_exists": "ARN" in secret,
        "arn": secret.get("ARN"),
        "error": secret.get("error"),
        "value_requested": False,
    }
    with tempfile.TemporaryDirectory(prefix="airlake-gold-metadata-") as directory:
        for table, expected in [
            (reader["hourly_table"], reader["hourly_snapshot"]),
            (reader["dimension_table"], reader["dimension_snapshot"]),
        ]:
            response = client.call(
                "glue",
                "get-table",
                "--database-name",
                reader["database"],
                "--name",
                table,
            )
            entry = {
                "table": table,
                "snapshot": expected,
                "present": False,
                "error": response.get("error"),
            }
            t = response.get("Table", {})
            uri = t.get("Parameters", {}).get("metadata_location", "")
            entry["lake_formation_registered"] = t.get("IsRegisteredWithLakeFormation")
            prefix = "s3://airlake-701364614078-lake/integration-real/20261106_f3b3b3e0677d4a6790e4a54faf68e4aa/"
            if uri.startswith(prefix):
                bucket, key = uri[5:].split("/", 1)
                output = Path(directory) / f"{table}.json"
                response = client.call(
                    "s3api",
                    "get-object",
                    "--bucket",
                    bucket,
                    "--key",
                    key,
                    "--expected-bucket-owner",
                    account,
                    str(output),
                )
                if "error" not in response:
                    raw = output.read_bytes()
                    metadata = json.loads(raw)
                    entry.update(
                        present=any(
                            str(s["snapshot-id"]) == expected
                            for s in metadata.get("snapshots", [])
                        ),
                        metadata_sha256=hashlib.sha256(raw).hexdigest(),
                        metadata_object=uri,
                    )
                else:
                    entry["error"] = response["error"]
            report["snapshots"].append(entry)
    role = identity.get("Arn", "").split(":assumed-role/")[-1].split("/")[0]
    report["publisher_principal_role_arn"] = f"arn:aws:iam::{account}:role/{role}"
    simulation = client.call(
        "iam",
        "simulate-principal-policy",
        "--policy-source-arn",
        report["publisher_principal_role_arn"],
        "--action-names",
        "lambda:CreateFunction",
        "iam:CreateRole",
        "iam:PassRole",
        "sns:CreateTopic",
        "sqs:CreateQueue",
        "dynamodb:CreateTable",
        "athena:CreateWorkGroup",
    )
    report["iam_simulation"] = {
        "error": simulation.get("error"),
        "decisions": [
            {"action": x["EvalActionName"], "decision": x["EvalDecision"]}
            for x in simulation.get("EvaluationResults", [])
        ],
    }
    report["limitations"] = [
        "Simulation is not effective authorization. Resource policies, boundaries, Lake Formation, KMS and SCPs require owner review.",
        "No deployed staging roles exist yet; no write permission is proven by read success.",
        "Secret identity as a distinct test bot requires owner confirmation; its value was not requested.",
    ]
    safe = (
        all(v == "ABSENT" for v in report["absence"].values())
        and count == 0
        and all(s["present"] for s in report["snapshots"])
    )
    report["status"] = (
        "READ_ONLY_CHECKS_PASS; APPROVAL_AND_SECRET_REQUIRED"
        if safe
        else "BLOCKED; INSPECT_CHECKS"
    )
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--plan", type=Path, default=ROOT / "docs/plans/athena-alert-staging.json"
    )
    parser.add_argument("--profile", default="airlake")
    parser.add_argument("--aws-cli", default="aws")
    args = parser.parse_args(argv)
    plan, digest = load_plan(args.plan)
    try:
        result = preflight(plan, CLI(args.profile, plan["region"], args.aws_cli))
    except (OSError, subprocess.TimeoutExpired, ValueError):
        result = {
            "status": "BLOCKED",
            "blocker": "CLI, timeout or malformed metadata; no mutation attempted",
        }
    result["plan_sha256"] = digest
    print(json.dumps(result, indent=2))
    return 0 if result["status"].startswith("READ_ONLY_CHECKS_PASS") else 1


if __name__ == "__main__":
    raise SystemExit(main())
