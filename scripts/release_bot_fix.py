"""Frozen bot-only code update: alert policy accepts the 'fresh' state.

prepare   -> plan.json + bot zip + rollback copy (no AWS calls)
preflight -> read-only: deployed bot code, schedules, queue mapping
execute   -> one UpdateFunctionCode (RevisionId pinned) and a CodeSha256 check.
             No manual evaluation and no Telegram message: the next scheduled
             evaluation exercises the code. No retry.
"""

import argparse
import json
from pathlib import Path

from final_demo.control import Receipt
from live_data.contract import ACCOUNT, REGION, sha
from scripts.release_rolling_history import SCHEDULES, code_sha, update_code

FUNCTION = "airlake-telegram-20261115-bot"
DEPLOYED = "ym3nARy2wDmZrMuXsMOFvvSvFQJxj8TD4dLph9P0nMY="
ROLLBACK = "/private/tmp/airlake-rolling-release-20261116/bot.zip"


def prepare(packet):
    from telegram_bot import deploy as telegram

    packet = Path(packet)
    packet.mkdir(mode=0o700)
    bot = telegram.lambda_zip()
    (packet / "bot.zip").write_bytes(bot)
    previous = Path(ROLLBACK).read_bytes()
    if code_sha(previous) != DEPLOYED:
        raise ValueError("Rollback copy is not the deployed bot code")
    (packet / "previous-bot.zip").write_bytes(previous)
    plan = {
        "account": ACCOUNT,
        "region": REGION,
        "functions": {"bot": FUNCTION},
        "deployed_code_sha256_b64": {"bot": DEPLOYED},
        "new_code_sha256_b64": {"bot": code_sha(bot)},
        "change": "telegram_bot/policy.py: alertable freshness states {'fresh', 'recent'} instead of 'recent' only",
        "unchanged": "webhook, secrets, DynamoDB table and subscriptions, SQS/DLQ and mapping, schedules, all other Lambdas, Amplify",
        "no_manual_evaluation": True,
        "rollback": "UpdateFunctionCode with previous-bot.zip",
    }
    raw = json.dumps(plan, indent=1, sort_keys=True).encode()
    (packet / "plan.json").write_bytes(raw)
    return sha(raw)


def load(packet, confirm):
    from telegram_bot import deploy as telegram

    packet = Path(packet)
    raw = (packet / "plan.json").read_bytes()
    if sha(raw) != confirm:
        raise ValueError("STOP: plan SHA-256 not approved")
    if telegram.lambda_zip() != (packet / "bot.zip").read_bytes():
        raise ValueError("Bot code drifted from the plan")
    return json.loads(raw)


def clients():
    import boto3
    from botocore.config import Config

    s = boto3.Session(profile_name="airlake", region_name=REGION)
    cfg = Config(retries={"total_max_attempts": 1}, connect_timeout=5, read_timeout=60)
    return {n: s.client(n, config=cfg) for n in ("sts", "lambda", "scheduler")}


def preflight(c, plan):
    if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong AWS account")
    f = c["lambda"].get_function_configuration(FunctionName=FUNCTION)
    if f["CodeSha256"] != DEPLOYED or f["State"] != "Active":
        raise ValueError("Deployed bot code changed; review first")
    for name in SCHEDULES:
        if c["scheduler"].get_schedule(Name=name)["State"] != "ENABLED":
            raise ValueError("A schedule is not enabled")
    mappings = c["lambda"].list_event_source_mappings(FunctionName=FUNCTION)[
        "EventSourceMappings"
    ]
    if [m["State"] for m in mappings] != ["Enabled"]:
        raise ValueError("Queue mapping changed; review first")
    return {"status": "PASS_READ_ONLY", "revision": f["RevisionId"]}


def execute(c, packet, plan, digest):
    packet = Path(packet)
    if (packet / "receipt.json").exists():
        raise ValueError("Preserve the prior attempt; never retry automatically")
    revision = preflight(c, plan)["revision"]
    r = Receipt(packet / "receipt.json", digest)
    update_code(c, r, plan, packet, "bot", "bot.zip", revision)
    r.value["status"] = "DEPLOYED_AWAITING_SCHEDULED_EVALUATION"
    r.save()
    return r.value


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("command", choices=("prepare", "preflight", "execute"))
    a.add_argument("--packet", type=Path, required=True)
    a.add_argument("--confirm-plan-sha256")
    x = a.parse_args()
    if x.command == "prepare":
        print(json.dumps({"plan_sha256": prepare(x.packet)}))
        return
    plan = load(x.packet, x.confirm_plan_sha256)
    c = clients()
    print(
        json.dumps(
            preflight(c, plan)
            if x.command == "preflight"
            else execute(c, x.packet, plan, x.confirm_plan_sha256)
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001 -- never print SDK payloads or secrets
        print(
            json.dumps(
                {
                    "status": "STOP_FAILED_OR_UNCERTAIN",
                    "exception_type": type(e).__name__,
                }
            )
        )
        raise SystemExit(1) from None
