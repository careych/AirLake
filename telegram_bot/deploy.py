"""Frozen, receipted release of the Telegram bot stack.

prepare -> plan.json + lambda.zip (local)   preflight -> read-only
execute -> table, queues, webhook secret, role, Lambda, URL, SQS mapping
configure-bot -> getMe, setWebhook (secret token), setMyCommands
smoke -> unsigned webhook 403, one evaluation with no subscribers
enable-schedule -> evaluation every 15 minutes, offset after the refresh
contain -> pause evaluation, block the webhook (concurrency 0), keep data
"""

import argparse
import io
import json
import re
import secrets
import time
import zipfile
from pathlib import Path

from final_demo.control import Receipt, absent
from live_data.contract import ACCOUNT, BUCKET, REGION, sha

ROOT = Path(__file__).resolve().parents[1]
NAME = "airlake-telegram-20261115"
FUNCTION, TABLE, QUEUE, DLQ = (
    NAME + "-bot",
    NAME,
    NAME + "-alerts",
    NAME + "-alerts-dlq",
)
SCHEDULE, SCHEDULER_ROLE = NAME + "-evaluate", NAME + "-scheduler"
WEBHOOK_SECRET_NAME = "airlake/telegram-20261115/webhook"
PACKAGES = ("telegram_bot", "ingestion")
ZIP_TIME = (2026, 11, 15, 0, 0, 0)
COMMANDS = [
    ("start", "Bắt đầu"),
    ("latest", "Số đo mới nhất"),
    ("subscribe", "Đăng ký cảnh báo"),
    ("status", "Cảnh báo của tôi"),
    ("settings", "Đổi trạm, ngưỡng hoặc tạm dừng"),
    ("hours", "Giờ thường ít bụi hơn"),
    ("unsubscribe", "Hủy cảnh báo"),
    ("help", "Hướng dẫn"),
]


def lambda_zip():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        for package in PACKAGES:
            for path in sorted((ROOT / package).rglob("*")):
                if (
                    path.suffix not in (".py", ".json")
                    or "__pycache__" in path.parts
                    or path.name == "deploy.py"
                ):
                    continue
                info = zipfile.ZipInfo(str(path.relative_to(ROOT)), ZIP_TIME)
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o644 << 16
                z.writestr(info, path.read_bytes())
    return buffer.getvalue()


def arn(service, resource):
    return f"arn:aws:{service}:{REGION}:{ACCOUNT}:{resource}"


def policies(token_arn, webhook_arn):
    return {
        FUNCTION: {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:UpdateItem",
                        "dynamodb:DeleteItem",
                        "dynamodb:Scan",
                    ],
                    "Resource": arn("dynamodb", "table/" + TABLE),
                },
                {
                    "Effect": "Allow",
                    "Action": "sqs:SendMessage",
                    "Resource": arn("sqs", QUEUE),
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "sqs:ReceiveMessage",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                    ],
                    "Resource": arn("sqs", QUEUE),
                },
                {
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{BUCKET}/nowcast/v1/current.json",
                },
                {
                    "Effect": "Allow",
                    "Action": "secretsmanager:GetSecretValue",
                    "Resource": [token_arn, webhook_arn],
                },
                {
                    "Effect": "Allow",
                    "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                    "Resource": arn("logs", f"log-group:/aws/lambda/{FUNCTION}:*"),
                },
            ],
        },
        SCHEDULER_ROLE: {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "lambda:InvokeFunction",
                    "Resource": arn("lambda", "function:" + FUNCTION),
                }
            ],
        },
    }


def trust(service):
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": service},
                    "Action": "sts:AssumeRole",
                    "Condition": {"StringEquals": {"aws:SourceAccount": ACCOUNT}},
                }
            ],
        }
    )


def prepare(packet, token_arn):
    if not re.fullmatch(
        rf"arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret:[A-Za-z0-9/_+=.@-]+-[A-Za-z0-9]{{6}}",
        token_arn,
    ):
        raise ValueError("Unexpected bot token secret ARN")
    packet = Path(packet)
    packet.mkdir(mode=0o700)
    code = lambda_zip()
    (packet / "lambda.zip").write_bytes(code)
    plan = {
        "account": ACCOUNT,
        "region": REGION,
        "lambda_zip_sha256": sha(code),
        "lambda_zip_files": sorted(zipfile.ZipFile(io.BytesIO(code)).namelist()),
        "token_secret_arn": token_arn,
        "webhook_secret_name": WEBHOOK_SECRET_NAME,
        "table": {
            "name": TABLE,
            "billing": "PAY_PER_REQUEST",
            "ttl_attribute": "expires",
            "key": "pk (S)",
        },
        "queues": {
            "alerts": QUEUE,
            "dlq": DLQ,
            "max_receive_count": 3,
            "visibility_timeout_s": 60,
            "dlq_retention_days": 14,
            "encryption": "SQS-managed",
        },
        "function": {
            "name": FUNCTION,
            "runtime": "python3.12",
            "architecture": "arm64",
            "handler": "telegram_bot.app.lambda_handler",
            "memory_mb": 256,
            "timeout_s": 30,
            "reserved_concurrency": 3,
        },
        "function_url": {
            "auth": "NONE",
            "protection": "X-Telegram-Bot-Api-Secret-Token checked in the handler; POST only",
        },
        "sqs_mapping": {"batch_size": 10, "report_batch_item_failures": True},
        "roles": {
            FUNCTION: trust("lambda.amazonaws.com"),
            SCHEDULER_ROLE: trust("scheduler.amazonaws.com"),
        },
        "policies": policies(token_arn, "<webhook secret ARN created in execute>"),
        "telegram": {
            "setWebhook": {
                "allowed_updates": ["message", "callback_query"],
                "max_connections": 5,
            },
            "setMyCommands": COMMANDS,
        },
        "schedule": {
            "name": SCHEDULE,
            "expression": "cron(5/15 * * * ? *)",
            "input": {"action": "evaluate"},
            "retry": {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 600},
        },
        "sns": "not used: one consumer, no fan-out; evaluation enqueues directly to SQS with a DLQ",
        "existing_untouched": [
            "AirLake_Bot_API (legacy bot, own token/webhook)",
            "airlake-alert-staging-v1* (Khuê's staging bot and resources)",
        ],
        "e2e": {
            "recipient": "owner's consenting Telegram account only",
            "manual_evaluations_max": 4,
            "trigger": "a real live observation at or above the owner's chosen threshold; no historical replay",
            "checks": [
                "message received",
                "value/time match /v1/now",
                "no duplicate on re-evaluation",
                "no message after /unsubscribe",
            ],
        },
        "order": [
            "execute",
            "configure-bot",
            "smoke",
            "consenting E2E (owner's Telegram account)",
            "enable-schedule",
        ],
        "cost_estimate_not_a_cap": "DynamoDB on-demand, SQS and Lambda requests at a few thousand per month; webhook traffic scales with users; Secrets Manager: one new secret",
        "rollback": [
            "contain: disable schedule, Lambda concurrency 0, deleteWebhook",
            "data kept; delete resources only on owner instruction",
        ],
    }
    raw = json.dumps(plan, indent=1, sort_keys=True).encode()
    (packet / "plan.json").write_bytes(raw)
    return sha(raw)


def load(packet, confirm):
    raw = (Path(packet) / "plan.json").read_bytes()
    if sha(raw) != confirm:
        raise ValueError("STOP: plan SHA-256 not approved")
    plan = json.loads(raw)
    if (
        lambda_zip() != (Path(packet) / "lambda.zip").read_bytes()
        or sha((Path(packet) / "lambda.zip").read_bytes()) != plan["lambda_zip_sha256"]
    ):
        raise ValueError("Bot source drifted from the frozen plan")
    return plan


def clients():
    import boto3
    from botocore.config import Config

    s = boto3.Session(profile_name="airlake", region_name=REGION)
    cfg = Config(retries={"total_max_attempts": 1}, connect_timeout=5, read_timeout=40)
    return {
        n: s.client(n, config=cfg)
        for n in (
            "sts",
            "iam",
            "lambda",
            "logs",
            "dynamodb",
            "sqs",
            "secretsmanager",
            "scheduler",
        )
    }


def preflight(c, plan):
    if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong AWS account")
    absent(
        lambda: c["dynamodb"].describe_table(TableName=TABLE),
        {"ResourceNotFoundException"},
    )
    absent(
        lambda: c["lambda"].get_function(FunctionName=FUNCTION),
        {"ResourceNotFoundException"},
    )
    for role in plan["roles"]:
        absent(lambda r=role: c["iam"].get_role(RoleName=r), {"NoSuchEntity"})
    for q in (QUEUE, DLQ):
        absent(
            lambda q=q: c["sqs"].get_queue_url(QueueName=q),
            {"AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"},
        )
    absent(
        lambda: c["secretsmanager"].describe_secret(SecretId=WEBHOOK_SECRET_NAME),
        {"ResourceNotFoundException"},
    )
    meta = c["secretsmanager"].describe_secret(SecretId=plan["token_secret_arn"])
    if meta["ARN"] != plan["token_secret_arn"]:
        raise ValueError("Token secret ARN mismatch")
    return {"status": "PASS_READ_ONLY"}


def execute(c, packet, plan, digest):
    packet = Path(packet)
    preflight(c, plan)
    r = Receipt(packet / "receipt.json", digest)
    r.write(
        "create_table",
        lambda: c["dynamodb"].create_table(
            TableName=TABLE,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        ),
    )
    c["dynamodb"].get_waiter("table_exists").wait(TableName=TABLE)
    r.write(
        "enable_ttl",
        lambda: c["dynamodb"].update_time_to_live(
            TableName=TABLE,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires"},
        ),
    )
    q = plan["queues"]
    dlq_url = r.write(
        "create_dlq",
        lambda: c["sqs"].create_queue(
            QueueName=DLQ,
            Attributes={
                "MessageRetentionPeriod": str(q["dlq_retention_days"] * 86400),
                "SqsManagedSseEnabled": "true",
            },
        ),
    )["QueueUrl"]
    dlq_arn = c["sqs"].get_queue_attributes(
        QueueUrl=dlq_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    redrive = json.dumps(
        {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": q["max_receive_count"]}
    )
    queue_url = r.write(
        "create_queue",
        lambda: c["sqs"].create_queue(
            QueueName=QUEUE,
            Attributes={
                "VisibilityTimeout": str(q["visibility_timeout_s"]),
                "RedrivePolicy": redrive,
                "SqsManagedSseEnabled": "true",
            },
        ),
    )["QueueUrl"]
    webhook_arn = r.write(
        "create_webhook_secret",
        lambda: c["secretsmanager"].create_secret(
            Name=WEBHOOK_SECRET_NAME, SecretString=secrets.token_urlsafe(48)
        ),
    )["ARN"]
    r.value["webhook_secret_arn"] = webhook_arn
    r.save()
    r.write(
        "create_log_group",
        lambda: c["logs"].create_log_group(logGroupName="/aws/lambda/" + FUNCTION),
    )
    r.write(
        "retention",
        lambda: c["logs"].put_retention_policy(
            logGroupName="/aws/lambda/" + FUNCTION, retentionInDays=14
        ),
    )
    docs = policies(plan["token_secret_arn"], webhook_arn)
    arns = {}
    for role, doc in plan["roles"].items():
        arns[role] = r.write(
            "create_role_" + role,
            lambda n=role, d=doc: c["iam"].create_role(
                RoleName=n, AssumeRolePolicyDocument=d
            ),
        )["Role"]["Arn"]
        r.write(
            "put_role_policy_" + role,
            lambda n=role: c["iam"].put_role_policy(
                RoleName=n, PolicyName="Scoped", PolicyDocument=json.dumps(docs[n])
            ),
        )
    time.sleep(30)
    f = plan["function"]
    env = {
        "AIRLAKE_TELEGRAM_TABLE": TABLE,
        "AIRLAKE_TELEGRAM_QUEUE_URL": queue_url,
        "AIRLAKE_BUCKET": BUCKET,
        "AIRLAKE_TELEGRAM_TOKEN_SECRET_ARN": plan["token_secret_arn"],
        "AIRLAKE_TELEGRAM_WEBHOOK_SECRET_ARN": webhook_arn,
    }
    r.write(
        "create_function",
        lambda: c["lambda"].create_function(
            FunctionName=FUNCTION,
            Runtime=f["runtime"],
            Architectures=[f["architecture"]],
            Role=arns[FUNCTION],
            Handler=f["handler"],
            Code={"ZipFile": (packet / "lambda.zip").read_bytes()},
            MemorySize=f["memory_mb"],
            Timeout=f["timeout_s"],
            Environment={"Variables": env},
            LoggingConfig={"LogFormat": "Text", "LogGroup": "/aws/lambda/" + FUNCTION},
        ),
    )
    c["lambda"].get_waiter("function_active_v2").wait(FunctionName=FUNCTION)
    r.write(
        "concurrency",
        lambda: c["lambda"].put_function_concurrency(
            FunctionName=FUNCTION,
            ReservedConcurrentExecutions=f["reserved_concurrency"],
        ),
    )
    url = r.write(
        "create_function_url",
        lambda: c["lambda"].create_function_url_config(
            FunctionName=FUNCTION, AuthType="NONE"
        ),
    )["FunctionUrl"]
    if not re.fullmatch(
        r"https://[a-z0-9]+\.lambda-url\.ap-southeast-2\.on\.aws/", url
    ):
        raise ValueError("Unexpected function URL")
    r.write(
        "permit_url",
        lambda: c["lambda"].add_permission(
            FunctionName=FUNCTION,
            StatementId="TelegramWebhook",
            Action="lambda:InvokeFunctionUrl",
            Principal="*",
            FunctionUrlAuthType="NONE",
        ),
    )
    r.write(
        "permit_url_invoke",
        lambda: c["lambda"].add_permission(
            FunctionName=FUNCTION,
            StatementId="TelegramWebhookInvoke",
            Action="lambda:InvokeFunction",
            Principal="*",
            InvokedViaFunctionUrl=True,
        ),
    )
    queue_arn = c["sqs"].get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    r.write(
        "sqs_mapping",
        lambda: c["lambda"].create_event_source_mapping(
            FunctionName=FUNCTION,
            EventSourceArn=queue_arn,
            BatchSize=10,
            FunctionResponseTypes=["ReportBatchItemFailures"],
            Enabled=True,
        ),
    )
    r.value.update(
        status="DEPLOYED_BOT_NOT_CONFIGURED", function_url=url, queue_url=queue_url
    )
    r.save()
    return {k: r.value[k] for k in ("status", "function_url")}


def telegram_call(token, method, payload):
    from telegram_bot.client import Telegram

    return Telegram(token).call(method, payload)


def configure_bot(c, packet, plan, digest):
    """Uses both secrets in memory only; prints the public bot username."""
    packet = Path(packet)
    backend = json.loads((packet / "receipt.json").read_text())
    if (
        backend["status"] != "DEPLOYED_BOT_NOT_CONFIGURED"
        or backend["plan_sha256"] != digest
    ):
        raise ValueError("Deploy the stack first")
    token = (
        c["secretsmanager"]
        .get_secret_value(SecretId=plan["token_secret_arn"])["SecretString"]
        .strip()
    )
    webhook = (
        c["secretsmanager"]
        .get_secret_value(SecretId=backend["webhook_secret_arn"])["SecretString"]
        .strip()
    )
    r = Receipt(packet / "bot-receipt.json", digest)
    me = r.write("get_me", lambda: {"result": telegram_call(token, "getMe", {})})[
        "result"
    ]
    r.write(
        "set_webhook",
        lambda: (
            telegram_call(
                token,
                "setWebhook",
                {
                    "url": backend["function_url"],
                    "secret_token": webhook,
                    **plan["telegram"]["setWebhook"],
                },
            )
            and {}
        ),
    )
    r.write(
        "set_commands",
        lambda: (
            telegram_call(
                token,
                "setMyCommands",
                {"commands": [{"command": a, "description": b} for a, b in COMMANDS]},
            )
            and {}
        ),
    )
    info = telegram_call(token, "getWebhookInfo", {})
    if info.get("url") != backend["function_url"]:
        raise ValueError("Webhook URL not applied")
    r.value.update(
        status="BOT_CONFIGURED",
        bot_username=me["username"],
        bot_link="https://t.me/" + me["username"],
        pending_updates=info.get("pending_update_count"),
    )
    r.save()
    return {k: r.value[k] for k in ("status", "bot_username", "bot_link")}


def smoke(c, packet, plan, digest, get=None):
    packet = Path(packet)
    backend = json.loads((packet / "receipt.json").read_text())
    r = Receipt(packet / "smoke-receipt.json", digest)
    if get is None:
        import requests

        def get(url):
            return requests.post(url, json={"update_id": 1}, timeout=15)

    unsigned = get(backend["function_url"])
    r.value["unsigned_webhook_status"] = unsigned.status_code
    r.save()
    if unsigned.status_code != 403:
        raise ValueError("Webhook accepted an unsigned request")
    result = r.write(
        "evaluate_once",
        lambda: c["lambda"].invoke(
            FunctionName=FUNCTION, Payload=json.dumps({"action": "evaluate"}).encode()
        ),
    )
    payload = json.loads(result["Payload"].read())
    if result.get("FunctionError") or payload.get("status") != "EVALUATED":
        raise ValueError("Evaluation failed")
    r.value.update(status="PASS", evaluate=payload)
    r.save()
    return r.value


def evaluate(c, packet, plan, digest):
    """One manual evaluation for the consenting E2E test (max 4)."""
    packet = Path(packet)
    if json.loads((packet / "smoke-receipt.json").read_text())["status"] != "PASS":
        raise ValueError("Smoke must pass first")
    n = len(list(packet.glob("e2e-evaluate-*.json"))) + 1
    if n > plan["e2e"]["manual_evaluations_max"]:
        raise ValueError("E2E evaluation budget used")
    r = Receipt(packet / f"e2e-evaluate-{n}.json", digest)
    out = r.write(
        "evaluate",
        lambda: c["lambda"].invoke(
            FunctionName=FUNCTION, Payload=json.dumps({"action": "evaluate"}).encode()
        ),
    )
    payload = json.loads(out["Payload"].read())
    r.value.update(
        status="FAIL" if out.get("FunctionError") else "PASS", result=payload
    )
    r.save()
    return r.value


def e2e_check():
    """Counts only; never prints chat identifiers."""
    import boto3

    table = (
        boto3.Session(profile_name="airlake", region_name=REGION)
        .resource("dynamodb")
        .Table(TABLE)
    )
    kinds = {}
    for item in table.scan(ProjectionExpression="pk")["Items"]:
        parts = item["pk"].split("#")
        kind = parts[0] + ("#" + parts[1] if parts[0] == "once" else "")
        kinds[kind] = kinds.get(kind, 0) + 1
    return {"items_by_kind": kinds}


def enable_schedule(c, packet, plan, digest):
    packet = Path(packet)
    if json.loads((packet / "smoke-receipt.json").read_text())["status"] != "PASS":
        raise ValueError("Smoke must pass first")
    s = plan["schedule"]
    r = Receipt(packet / "schedule-receipt.json", digest)
    r.write(
        "create_schedule",
        lambda: c["scheduler"].create_schedule(
            Name=SCHEDULE,
            ScheduleExpression=s["expression"],
            State="ENABLED",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": arn("lambda", "function:" + FUNCTION),
                "RoleArn": f"arn:aws:iam::{ACCOUNT}:role/{SCHEDULER_ROLE}",
                "Input": json.dumps(s["input"]),
                "RetryPolicy": s["retry"],
            },
        ),
    )
    r.value["status"] = (
        "SCHEDULE_" + c["scheduler"].get_schedule(Name=SCHEDULE)["State"]
    )
    r.save()
    return r.value


def contain(c, plan):
    from botocore.exceptions import ClientError

    c["lambda"].put_function_concurrency(
        FunctionName=FUNCTION, ReservedConcurrentExecutions=0
    )
    try:
        cur = c["scheduler"].get_schedule(Name=SCHEDULE)
        c["scheduler"].update_schedule(
            Name=SCHEDULE,
            ScheduleExpression=cur["ScheduleExpression"],
            FlexibleTimeWindow=cur["FlexibleTimeWindow"],
            Target=cur["Target"],
            State="DISABLED",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    token = (
        c["secretsmanager"]
        .get_secret_value(SecretId=plan["token_secret_arn"])["SecretString"]
        .strip()
    )
    telegram_call(token, "deleteWebhook", {"drop_pending_updates": False})
    return {"status": "CONTAINED"}


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument(
        "command",
        choices=(
            "prepare",
            "preflight",
            "execute",
            "configure-bot",
            "smoke",
            "evaluate",
            "e2e-check",
            "enable-schedule",
            "contain",
        ),
    )
    a.add_argument("--packet", type=Path, required=True)
    a.add_argument("--confirm-plan-sha256")
    a.add_argument("--token-secret-arn")
    x = a.parse_args()
    if x.command == "prepare":
        print(json.dumps({"plan_sha256": prepare(x.packet, x.token_secret_arn)}))
        return
    plan = load(x.packet, x.confirm_plan_sha256)
    c = clients()
    actions = {
        "preflight": lambda: preflight(c, plan),
        "execute": lambda: execute(c, x.packet, plan, x.confirm_plan_sha256),
        "configure-bot": lambda: configure_bot(
            c, x.packet, plan, x.confirm_plan_sha256
        ),
        "smoke": lambda: smoke(c, x.packet, plan, x.confirm_plan_sha256),
        "evaluate": lambda: evaluate(c, x.packet, plan, x.confirm_plan_sha256),
        "e2e-check": e2e_check,
        "enable-schedule": lambda: enable_schedule(
            c, x.packet, plan, x.confirm_plan_sha256
        ),
        "contain": lambda: contain(c, plan),
    }
    print(json.dumps(actions[x.command]()))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001 -- never print tokens, chat ids or SDK payloads
        print(
            json.dumps(
                {
                    "status": "STOP_FAILED_OR_UNCERTAIN",
                    "exception_type": type(e).__name__,
                }
            )
        )
        raise SystemExit(1) from None
