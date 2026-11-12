"""Prepare private AWS CLI deployment files; never executes AWS commands."""

import argparse
import base64
import hashlib
import json
import re
import shlex
import sys
from pathlib import Path

from alerts.delivery import chat_id
from alerts.runtime import ROOT, load_plan
from scripts.verify_telegram_staging_identity import load_identity


def prepare(
    plan_path,
    artifact,
    manifest_path,
    secret_arn,
    recipient_file,
    principal,
    output,
    *,
    test_bot_id=None,
    identity_file=None,
):
    plan, digest = load_plan(plan_path)
    if identity_file is not None:
        if test_bot_id is not None or recipient_file is not None:
            raise ValueError(
                "Choose private identity evidence or manual identity inputs"
            )
        identity = load_identity(identity_file)
        test_bot_id = identity["bot_id"]
        recipient_data = {"chat_id": identity["chat_id"]}
    else:
        recipient_data = json.loads(recipient_file.read_text())
    test_bot_id = chat_id(test_bot_id)
    if test_bot_id.startswith("-"):
        raise ValueError("Approved staging bot identity must be positive")
    if not re.fullmatch(
        r"arn:aws:secretsmanager:ap-southeast-2:701364614078:secret:airlake/alert-staging-v1/telegram-[A-Za-z0-9]{6}",
        secret_arn,
    ):
        raise ValueError("Exact separately approved test-bot secret ARN required")
    if not re.fullmatch(
        r"arn:aws:iam::701364614078:role/[A-Za-z0-9_+=,.@/-]+", principal
    ):
        raise ValueError("An approved existing IAM role principal is required")
    if set(recipient_data) != {"chat_id"}:
        raise ValueError("Recipient file must contain only chat_id")
    recipient = chat_id(recipient_data["chat_id"])
    manifest = json.loads(manifest_path.read_text())
    artifact_hash = hashlib.sha256(artifact.read_bytes()).hexdigest()
    if manifest["archive_sha256"] != artifact_hash:
        raise ValueError("Artifact hash mismatch")
    for path, expected in manifest["source_sha256"].items():
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != expected:
            raise ValueError("Artifact source is stale")
    output = output.resolve()
    if output == ROOT or ROOT in output.parents:
        raise ValueError("Private deployment files must be outside Git")
    output.mkdir(parents=True, exist_ok=False)
    output.chmod(0o700)

    def write(name, data):
        path = output / name
        path.write_text(json.dumps(data, indent=2) + "\n")
        path.chmod(0o600)
        return path

    n = plan["names"]
    account, region = plan["account"], plan["region"]
    scope = n["sns_topic"]
    arn = lambda service, resource: f"arn:aws:{service}:{region}:{account}:{resource}"
    write(
        "consumer-trust.json",
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        },
    )
    write(
        "reader-trust.json",
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": principal},
                    "Action": "sts:AssumeRole",
                }
            ],
        },
    )
    consumer_policy = json.loads(json.dumps(plan["consumer_policy_proposal"]))
    for statement in consumer_policy["Statement"]:
        if statement["Action"] == ["secretsmanager:GetSecretValue"]:
            statement["Resource"] = [secret_arn]
        if statement["Resource"] == [arn("dynamodb", "table/" + n["subscriptions"])]:
            statement["Condition"] = {
                "ForAllValues:StringEquals": {"dynamodb:LeadingKeys": [recipient]}
            }
    write("consumer-policy.json", consumer_policy)
    write("reader-policy.json", plan["reader_publisher_policy_proposal"])
    write(
        "dlq-attributes.json",
        {
            "MessageRetentionPeriod": str(plan["dlq"]["MessageRetentionPeriod"]),
            "SqsManagedSseEnabled": "true",
        },
    )
    attributes = {
        k: (
            json.dumps(v)
            if isinstance(v, dict)
            else str(v).lower()
            if isinstance(v, bool)
            else str(v)
        )
        for k, v in plan["sqs"].items()
    }
    attributes["Policy"] = json.dumps(plan["sns"]["queue_policy"])
    write("queue-attributes.json", attributes)
    write(
        "dlq-redrive.json",
        {"RedriveAllowPolicy": json.dumps(plan["dlq"]["RedriveAllowPolicy"])},
    )
    for kind, key in [("subscriptions", "chat_id"), ("delivery_ledger", "delivery_id")]:
        write(
            kind + ".json",
            {
                "TableName": n[kind],
                "KeySchema": [{"AttributeName": key, "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": key, "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST",
            },
        )
    write(
        "recipient-item.json",
        {
            "chat_id": {"S": recipient},
            "station_ids": {"L": [{"S": str(plan["reader"]["location_ids"][0])}]},
            "enabled": {"BOOL": True},
        },
    )
    write(
        "workgroup.json",
        {
            "EnforceWorkGroupConfiguration": True,
            "ResultConfiguration": {
                "OutputLocation": plan["workgroup"]["OutputLocation"],
                "ExpectedBucketOwner": account,
                "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
            },
            "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3"},
            "BytesScannedCutoffPerQuery": plan["workgroup"][
                "BytesScannedCutoffPerQuery"
            ],
        },
    )
    variables = {
        "AIRLAKE_ALERT_DELIVERY_ENABLED": "0",
        "AIRLAKE_ACCOUNT_ID": account,
        "AIRLAKE_ALERT_STATIONS": json.dumps(plan["stations"], ensure_ascii=False),
        "AIRLAKE_ALERT_RULES": json.dumps([plan["historical_test"]["rule"]]),
        "AIRLAKE_ALERT_TOPIC_ARN": arn("sns", scope),
        "AIRLAKE_ALERT_QUEUE_ARN": arn("sqs", scope),
        "AIRLAKE_SUBSCRIPTION_TABLE": n["subscriptions"],
        "AIRLAKE_DELIVERY_TABLE": n["delivery_ledger"],
        "AIRLAKE_DELIVERY_LEASE_SECONDS": str(plan["ledger"]["lease_seconds"]),
        "AIRLAKE_DEDUPE_RETENTION_SECONDS": str(plan["ledger"]["retention_seconds"]),
        "AIRLAKE_PARTIAL_BATCH_FAILURES": "1",
        "AIRLAKE_ALERT_TEST_MODE": "1",
        "AIRLAKE_ALERT_TEST_RECIPIENT": recipient,
        "AIRLAKE_ALERT_TEST_BOT_ID": test_bot_id,
        "AIRLAKE_ALERT_TEST_REFERENCE_TIME": plan["historical_test"]["reference_time"],
        "AIRLAKE_ALERT_STAGING_NAME": scope,
        "TELEGRAM_TOKEN_SECRET_ARN": secret_arn,
    }
    write(
        "function.json",
        {
            "FunctionName": n["lambda"],
            "Runtime": "python3.12",
            "Role": f"arn:aws:iam::{account}:role/{n['consumer_role']}",
            "Handler": plan["lambda"]["Handler"],
            "Timeout": plan["lambda"]["Timeout"],
            "MemorySize": 256,
            "Environment": {"Variables": variables},
            "Publish": True,
        },
    )
    enabled = dict(variables, AIRLAKE_ALERT_DELIVERY_ENABLED="1")
    write("disabled-environment.json", {"Variables": variables})
    write("enabled-environment.json", {"Variables": enabled})
    q = lambda x: shlex.quote(str(x))
    file = lambda name: "file://" + str(output / name)
    cli = lambda *args: (
        "aws --profile airlake --region "
        + region
        + " --no-cli-pager "
        + " ".join(q(x) for x in args)
    )
    queue_url = f"https://sqs.{region}.amazonaws.com/{account}/{n['queue']}"
    dlq_url = queue_url + "-dlq"
    identity_check = []
    if identity_file is not None:
        identity_check = [
            f"{q(sys.executable)} -B -c "
            + q(
                "import json,sys; from pathlib import Path; "
                "from scripts.verify_telegram_staging_identity import load_identity; "
                "i=load_identity(Path(sys.argv[1])); "
                "e=json.load(open(sys.argv[2]))['Environment']['Variables']; "
                "sys.exit(0 if i['bot_id']==e['AIRLAKE_ALERT_TEST_BOT_ID'] "
                "and i['chat_id']==e['AIRLAKE_ALERT_TEST_RECIPIENT'] "
                "else 'Private identity/configuration mismatch')"
            )
            + " "
            + q(identity_file.resolve())
            + " "
            + q(output / "function.json")
        ]
    steps = [
        "#!/bin/sh",
        "set -eu",
        "umask 077",
        'export AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard AWS_PAGER=""',
        f'test "${{AIRLAKE_APPROVED_PLAN_SHA256:-}}" = {q(digest)}',
        f"cd {q(ROOT)}",
        f"{q(sys.executable)} -B -m alerts.preflight --plan {q(plan_path.resolve())} > {q(output / 'fresh-preflight.json')}",
        f"{q(sys.executable)} -c "
        + q(
            "import json; p=json.load(open("
            + repr(str(output / "fresh-preflight.json"))
            + ")); import sys; sys.exit(0 if p['status'].startswith('READ_ONLY_CHECKS_PASS') and p['plan_sha256']=="
            + repr(digest)
            + " and p['test_secret']['arn']=="
            + repr(secret_arn)
            + " else 'Fresh staging preflight mismatch')"
        ),
    ]
    steps += identity_check
    steps += [
        "# FIRST AWS MUTATION BELOW: requires separate exact-scope approval. No automatic retry/cleanup."
    ]
    for role, kind in [(n["consumer_role"], "consumer"), (n["reader_role"], "reader")]:
        steps += [
            cli(
                "iam",
                "create-role",
                "--role-name",
                role,
                "--assume-role-policy-document",
                file(kind + "-trust.json"),
                "--query",
                "Role.Arn",
                "--output",
                "text",
            ),
            cli(
                "iam",
                "put-role-policy",
                "--role-name",
                role,
                "--policy-name",
                scope,
                "--policy-document",
                file(kind + "-policy.json"),
            ),
        ]
    steps += [
        cli("logs", "create-log-group", "--log-group-name", n["log_group"]),
        cli(
            "logs",
            "put-retention-policy",
            "--log-group-name",
            n["log_group"],
            "--retention-in-days",
            "7",
        ),
        cli(
            "sns",
            "create-topic",
            "--name",
            scope,
            "--query",
            "TopicArn",
            "--output",
            "text",
        ),
        cli(
            "sqs",
            "create-queue",
            "--queue-name",
            n["dlq"],
            "--attributes",
            file("dlq-attributes.json"),
            "--query",
            "QueueUrl",
            "--output",
            "text",
        ),
        cli(
            "sqs",
            "create-queue",
            "--queue-name",
            n["queue"],
            "--attributes",
            file("queue-attributes.json"),
            "--query",
            "QueueUrl",
            "--output",
            "text",
        ),
        cli(
            "sqs",
            "set-queue-attributes",
            "--queue-url",
            dlq_url,
            "--attributes",
            file("dlq-redrive.json"),
        ),
        cli(
            "sns",
            "subscribe",
            "--topic-arn",
            arn("sns", scope),
            "--protocol",
            "sqs",
            "--notification-endpoint",
            arn("sqs", scope),
            "--attributes",
            "RawMessageDelivery=false",
            "--query",
            "SubscriptionArn",
            "--output",
            "text",
        ),
    ]
    for kind in ["subscriptions", "delivery_ledger"]:
        steps += [
            cli(
                "dynamodb",
                "create-table",
                "--cli-input-json",
                file(kind + ".json"),
                "--query",
                "TableDescription.TableArn",
                "--output",
                "text",
            ),
            cli("dynamodb", "wait", "table-exists", "--table-name", n[kind]),
        ]
    steps += [
        cli(
            "dynamodb",
            "update-time-to-live",
            "--table-name",
            n["delivery_ledger"],
            "--time-to-live-specification",
            "Enabled=true,AttributeName=expires_at",
        ),
        cli(
            "dynamodb",
            "put-item",
            "--table-name",
            n["subscriptions"],
            "--item",
            file("recipient-item.json"),
            "--condition-expression",
            "attribute_not_exists(chat_id)",
        ),
        cli(
            "athena",
            "create-work-group",
            "--name",
            n["workgroup"],
            "--configuration",
            file("workgroup.json"),
        ),
        cli(
            "lambda",
            "create-function",
            "--cli-input-json",
            file("function.json"),
            "--zip-file",
            "fileb://" + str(artifact.resolve()),
            "--query",
            "Version",
            "--output",
            "text",
        )
        + " > "
        + q(output / "disabled-version.txt"),
        cli("lambda", "wait", "function-active-v2", "--function-name", scope),
        cli(
            "lambda",
            "put-function-concurrency",
            "--function-name",
            scope,
            "--reserved-concurrent-executions",
            "2",
        ),
        cli(
            "lambda",
            "create-alias",
            "--function-name",
            scope,
            "--name",
            "staging",
            "--function-version",
            "1",
            "--query",
            "AliasArn",
            "--output",
            "text",
        ),
        cli(
            "lambda",
            "create-event-source-mapping",
            "--function-name",
            scope + ":staging",
            "--event-source-arn",
            arn("sqs", scope),
            "--batch-size",
            "1",
            "--maximum-batching-window-in-seconds",
            "0",
            "--function-response-types",
            "ReportBatchItemFailures",
            "--scaling-config",
            "MaximumConcurrency=2",
            "--no-enabled",
            "--query",
            "UUID",
            "--output",
            "text",
        )
        + " > "
        + q(output / "mapping-uuid.txt"),
    ]
    script = output / "deploy-disabled.sh"
    script.write_text("\n".join(steps) + "\n")
    script.chmod(0o600)
    # Later enabling is a separate reviewed phase, not part of deploy-disabled.sh.
    enable = [
        "#!/bin/sh",
        "set -eu",
        "umask 077",
        'export AWS_MAX_ATTEMPTS=1 AWS_PAGER=""',
        f'test "${{AIRLAKE_APPROVED_PLAN_SHA256:-}}" = {q(digest)}',
        f"cd {q(ROOT)}",
        *identity_check,
        cli(
            "lambda",
            "update-function-configuration",
            "--function-name",
            scope,
            "--environment",
            file("enabled-environment.json"),
            "--query",
            "LastUpdateStatus",
            "--output",
            "text",
        ),
        cli("lambda", "wait", "function-updated-v2", "--function-name", scope),
        cli(
            "lambda",
            "publish-version",
            "--function-name",
            scope,
            "--code-sha256",
            base64.b64encode(bytes.fromhex(artifact_hash)).decode(),
            "--query",
            "Version",
            "--output",
            "text",
        )
        + " > "
        + q(output / "enabled-version.txt"),
        cli(
            "lambda",
            "update-alias",
            "--function-name",
            scope,
            "--name",
            "staging",
            "--function-version",
            "$(cat " + q(output / "enabled-version.txt") + ")",
            "--query",
            "AliasArn",
            "--output",
            "text",
        ),
    ]
    enable[-1] = enable[-1].replace(
        q("$(cat " + q(output / "enabled-version.txt") + ")"),
        '"$(cat ' + q(output / "enabled-version.txt") + ')"',
    )
    enable += [
        cli(
            "lambda",
            "update-event-source-mapping",
            "--uuid",
            "$(cat " + q(output / "mapping-uuid.txt") + ")",
            "--enabled",
            "--query",
            "State",
            "--output",
            "text",
        )
    ]
    enable[-1] = enable[-1].replace(
        q("$(cat " + q(output / "mapping-uuid.txt") + ")"),
        '"$(cat ' + q(output / "mapping-uuid.txt") + ')"',
    )
    (output / "enable-test.sh").write_text("\n".join(enable) + "\n")
    (output / "enable-test.sh").chmod(0o600)
    disable = [
        "#!/bin/sh",
        "set -eu",
        "umask 077",
        'export AWS_MAX_ATTEMPTS=1 AWS_PAGER=""',
        f'test "${{AIRLAKE_APPROVED_PLAN_SHA256:-}}" = {q(digest)}',
        cli(
            "lambda",
            "update-event-source-mapping",
            "--uuid",
            "$(cat " + q(output / "mapping-uuid.txt") + ")",
            "--no-enabled",
            "--query",
            "State",
            "--output",
            "text",
        ),
        cli(
            "lambda",
            "update-alias",
            "--function-name",
            scope,
            "--name",
            "staging",
            "--function-version",
            "$(cat " + q(output / "disabled-version.txt") + ")",
            "--query",
            "AliasArn",
            "--output",
            "text",
        ),
    ]
    for index in (5, 6):
        for name in ("mapping-uuid.txt", "disabled-version.txt"):
            substitution = "$(cat " + q(output / name) + ")"
            disable[index] = disable[index].replace(
                q(substitution), '"' + substitution + '"'
            )
    disable.append(
        cli(
            "lambda",
            "update-function-configuration",
            "--function-name",
            scope,
            "--environment",
            file("disabled-environment.json"),
            "--query",
            "LastUpdateStatus",
            "--output",
            "text",
        )
    )
    (output / "disable-test.sh").write_text("\n".join(disable) + "\n")
    (output / "disable-test.sh").chmod(0o600)
    summary = {
        "status": "PREPARED ONLY; NO AWS OPERATIONS EXECUTED",
        "plan_sha256": digest,
        "artifact_sha256": artifact_hash,
        "inventory": n,
        "secret_reference_only": True,
        "one_recipient_configured": True,
        "private_identity_evidence_supplied": identity_file is not None,
    }
    write("preparation.json", summary)
    return summary


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--plan", type=Path, default=ROOT / "docs/plans/athena-alert-staging.json"
    )
    for flag in ["artifact", "manifest", "output"]:
        p.add_argument("--" + flag, type=Path, required=True)
    p.add_argument("--recipient-file", type=Path)
    p.add_argument("--identity-file", type=Path)
    p.add_argument("--secret-arn", required=True)
    p.add_argument("--test-bot-id")
    p.add_argument("--publisher-principal", required=True)
    a = p.parse_args()
    if (a.identity_file is not None) == (
        a.recipient_file is not None or a.test_bot_id is not None
    ) or (
        a.identity_file is None and (a.recipient_file is None or a.test_bot_id is None)
    ):
        p.error("Supply --identity-file OR both --recipient-file and --test-bot-id")
    print(
        json.dumps(
            prepare(
                a.plan,
                a.artifact,
                a.manifest,
                a.secret_arn,
                a.recipient_file,
                a.publisher_principal,
                a.output,
                test_bot_id=a.test_bot_id,
                identity_file=a.identity_file,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
