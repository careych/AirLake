"""Frozen release: rolling history, location-first website, freshness states.

prepare   -> plan.json + code zips + history series + site inputs (no AWS calls)
preflight -> read-only: deployed code, schedules, IAM policy, absent history keys
execute   -> in order, one receipt entry per write, stop on the first failure:
   1 bot code            (understands fresh/delayed/stale before refresh emits them)
   2 public-api IAM      (+ s3:GetObject on the two exact history keys)
   3 history series      (If-None-Match; plus immutable backfill copies)
   4 refresh code        (+ one invoke; projection and history must check out)
   5 public-api code     (+ bounded GET checks of the new routes)
   6 Amplify             (geolocation=(self) header; one deployment per branch;
                          demo Basic Auth preserved and verified)
No retries; rollback zips and the previous IAM document stay in the packet.
"""

import argparse
import base64
import hashlib
import json
import time
from pathlib import Path

from final_demo.control import Receipt
from live_data.contract import ACCOUNT, BUCKET, REGION, sha
from nowcast import deploy as nowcast
from nowcast import history
from nowcast.public_api import CURRENT_KEY

DEPLOYED = {
    "refresh": "LqhGCUfNrxxV2W0+6PAIRKzkvb/chhEbQQ+RGm3dhn0=",
    "bot": "eOANIU7Wxd0noSnNqZAeAhnNWqL/zfWP0HYioIfs7e0=",
    "api": "5HH3RWWQpi1tDf9vjEUtDh4Tr0ZP/fHXLlaay9mbquE=",
}
ROLLBACK_ZIPS = {
    "refresh": "/private/tmp/airlake-live-update-20261116/refresh.zip",
    "bot": "/private/tmp/airlake-live-update-20261116/bot.zip",
    "api": "/private/tmp/airlake-nowcast-20261115/lambda.zip",
}
FUNCTIONS = {
    "refresh": nowcast.REFRESH,
    "bot": "airlake-telegram-20261115-bot",
    "api": nowcast.API,
}
API_ROLE, API_POLICY = nowcast.API, "Scoped"
PUBLIC_API = "https://npe6uhbknh4qojnmylv3w436mi0epxfb.lambda-url.ap-southeast-2.on.aws"
BOT_URL = "https://t.me/AirLakeHanoiBot"
PREVIOUS_SITE_RECEIPT = (
    "/private/tmp/airlake-live-update-20261116/website-release/release-receipt.json"
)
SCHEDULES = ("airlake-nowcast-20261115-15m", "airlake-telegram-20261115-evaluate")
CHECKS = [  # (path, expected status) against the public API after step 5
    ("/v1/stations", 200),
    ("/v1/history?station_id=6123215", 200),
    ("/v1/history?station_id=99385&granularity=daily&format=csv", 200),
    ("/v1/history?station_id=4946812", 404),
    ("/v1/archive/september-2026?limit=1", 200),
    ("/v1/now", 200),
]


def code_sha(raw):
    return base64.b64encode(hashlib.sha256(raw).digest()).decode()


def iam_document(current):
    doc = json.loads(json.dumps(current))
    bucket = f"arn:aws:s3:::{BUCKET}"
    extra = [f"{bucket}/{history.key(s)}" for s in (history.DRAGON, history.COMMUNITY)]
    statement = next(s for s in doc["Statement"] if s["Action"] == "s3:GetObject")
    if any(r in statement["Resource"] for r in extra):
        raise ValueError("History keys already granted; review first")
    statement["Resource"] = [*statement["Resource"], *extra]
    return doc


def prepare(packet, backfill, site_inputs, current_iam_path):
    from telegram_bot import deploy as telegram

    packet = Path(packet)
    packet.mkdir(mode=0o700)
    code = nowcast.lambda_zip()
    (packet / "nowcast.zip").write_bytes(code)
    bot = telegram.lambda_zip()
    (packet / "bot.zip").write_bytes(bot)
    for name, path in ROLLBACK_ZIPS.items():
        raw = Path(path).read_bytes()
        if code_sha(raw) != DEPLOYED[name]:
            raise ValueError(f"Rollback copy is not the deployed {name} code")
        (packet / f"previous-{name}.zip").write_bytes(raw)
    manifest = json.loads((Path(backfill) / "manifest.json").read_text())
    series = {}
    for sid in (history.DRAGON, history.COMMUNITY):
        raw = (Path(backfill) / f"series-{sid}.json").read_bytes()
        if sha(raw) != manifest["series"][str(sid)]["sha256"]:
            raise ValueError("Backfill series differs from its manifest")
        value = json.loads(raw)
        if value["station_id"] != sid or value["schema_version"] != 1:
            raise ValueError("Unexpected series")
        (packet / f"series-{sid}.json").write_bytes(raw)
        series[str(sid)] = {
            "key": history.key(sid),
            "sha256": sha(raw),
            "hours": manifest["series"][str(sid)]["hours"],
            "first_hour": manifest["series"][str(sid)]["first_hour"],
            "last_hour": manifest["series"][str(sid)]["last_hour"],
        }
    current_iam = json.loads(Path(current_iam_path).read_text())
    (packet / "previous-api-policy.json").write_text(json.dumps(current_iam, indent=1))
    (packet / "api-policy.json").write_text(
        json.dumps(iam_document(current_iam), indent=1)
    )
    root = Path(__file__).resolve().parents[1]
    plan = {
        "account": ACCOUNT,
        "region": REGION,
        "functions": FUNCTIONS,
        "deployed_code_sha256_b64": DEPLOYED,
        "new_code_sha256_b64": {
            "refresh": code_sha(code),
            "api": code_sha(code),
            "bot": code_sha(bot),
        },
        "iam": {
            "role": API_ROLE,
            "policy": API_POLICY,
            "previous_sha256": sha((packet / "previous-api-policy.json").read_bytes()),
            "new_sha256": sha((packet / "api-policy.json").read_bytes()),
            "change": "add s3:GetObject on exactly nowcast/v1/history/6123215.json and nowcast/v1/history/99385.json",
        },
        "history": series,
        "backfill_requests": manifest["requests"],
        "public_api": PUBLIC_API,
        "bot_url": BOT_URL,
        "site_inputs": site_inputs,
        "site_inputs_sha256": {
            k: sha(Path(v).read_bytes()) for k, v in site_inputs.items()
        },
        "site_source_sha256": {
            str(p.relative_to(root)): sha(p.read_bytes())
            for p in sorted((root / "citizen/web").iterdir())
            if p.is_file()
        },
        "previous_site_receipt": PREVIOUS_SITE_RECEIPT,
        "schedules_unchanged": list(SCHEDULES),
        "public_api_checks": CHECKS,
        "new_public_data": "hourly/daily PM2.5 history since 2026-09-01 for Dragon 6123215 (CC BY 4.0) and Sensor.Community 99385 (ODbL 1.0, derived database offered under ODbL)",
        "security_header_change": "Permissions-Policy geolocation=() -> geolocation=(self)",
        "unchanged": "schedules, alarms, Telegram webhook/table/queue/secrets, subscriptions, historical Glue/Athena data, private IAM API, demo Basic Auth, restricted stations",
        "new_resources": 0,
        "browser_get_budget": 60,
        "rollback": [
            "lambda UpdateFunctionCode with previous-bot.zip / previous-refresh.zip / previous-api.zip",
            "iam PutRolePolicy with previous-api-policy.json",
            "history series objects are new keys; leaving them is harmless (deleting needs separate approval)",
            "Amplify redeploy of the job 4 zips in the previous website-release packet; headers back to geolocation=()",
        ],
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
    plan = json.loads(raw)
    root = Path(__file__).resolve().parents[1]
    if nowcast.lambda_zip() != (packet / "nowcast.zip").read_bytes():
        raise ValueError("Nowcast code drifted from the plan")
    if telegram.lambda_zip() != (packet / "bot.zip").read_bytes():
        raise ValueError("Bot code drifted from the plan")
    for name, h in plan["site_source_sha256"].items():
        if sha((root / name).read_bytes()) != h:
            raise ValueError("Website source drifted from the plan")
    for k, h in plan["site_inputs_sha256"].items():
        if sha(Path(plan["site_inputs"][k]).read_bytes()) != h:
            raise ValueError("Website input drifted from the plan")
    for sid, item in plan["history"].items():
        if sha((packet / f"series-{sid}.json").read_bytes()) != item["sha256"]:
            raise ValueError("History series drifted from the plan")
    if sha((packet / "api-policy.json").read_bytes()) != plan["iam"]["new_sha256"]:
        raise ValueError("IAM document drifted from the plan")
    return plan


def clients():
    import boto3
    from botocore.config import Config

    s = boto3.Session(profile_name="airlake", region_name=REGION)
    cfg = Config(retries={"total_max_attempts": 1}, connect_timeout=5, read_timeout=70)
    return {
        n: s.client(n, config=cfg)
        for n in ("sts", "lambda", "s3", "iam", "scheduler", "amplify")
    }


def preflight(c, packet, plan):
    if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong AWS account")
    revisions = {}
    for name, fn in plan["functions"].items():
        f = c["lambda"].get_function_configuration(FunctionName=fn)
        if (
            f["CodeSha256"] != plan["deployed_code_sha256_b64"][name]
            or f["State"] != "Active"
        ):
            raise ValueError(f"Deployed {name} code changed; review first")
        revisions[name] = f["RevisionId"]
    for name in plan["schedules_unchanged"]:
        if c["scheduler"].get_schedule(Name=name)["State"] != "ENABLED":
            raise ValueError("A schedule is not enabled")
    doc = c["iam"].get_role_policy(RoleName=API_ROLE, PolicyName=API_POLICY)[
        "PolicyDocument"
    ]
    if doc != json.loads((Path(packet) / "previous-api-policy.json").read_text()):
        raise ValueError("Public API IAM policy changed; review first")
    for sid in plan["history"]:
        try:
            c["s3"].head_object(
                Bucket=BUCKET, Key=history.key(int(sid)), ExpectedBucketOwner=ACCOUNT
            )
            raise ValueError("History series already exists; review first")
        except c["s3"].exceptions.ClientError as e:
            if e.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
                raise
    return {"status": "PASS_READ_ONLY", "revisions": revisions}


def update_code(c, r, plan, packet, name, zip_name, revision):
    fn = plan["functions"][name]
    body = (Path(packet) / zip_name).read_bytes()
    r.write(
        f"update_{name}_code",
        lambda: c["lambda"].update_function_code(
            FunctionName=fn, ZipFile=body, RevisionId=revision
        ),
    )
    c["lambda"].get_waiter("function_updated_v2").wait(FunctionName=fn)
    if (
        c["lambda"].get_function_configuration(FunctionName=fn)["CodeSha256"]
        != plan["new_code_sha256_b64"][name]
    ):
        raise ValueError(f"Updated {name} code does not match")


def execute(c, packet, plan, digest, get=None):
    import requests

    from citizen import release
    from citizen.hosting import headers
    from historical.build import build
    from nowcast.refresh import Store

    packet = Path(packet)
    if (packet / "receipt.json").exists():
        raise ValueError("Preserve the prior attempt; never retry automatically")
    checked = preflight(c, packet, plan)
    rev = checked["revisions"]
    r = Receipt(packet / "receipt.json", digest)
    # 1 bot first: it reads both the old and the new freshness states
    update_code(c, r, plan, packet, "bot", "bot.zip", rev["bot"])
    # 2 IAM: two exact object keys for the public API
    policy = (packet / "api-policy.json").read_text()
    r.write(
        "put_api_policy",
        lambda: c["iam"].put_role_policy(
            RoleName=API_ROLE, PolicyName=API_POLICY, PolicyDocument=policy
        ),
    )
    # 3 history series: created once, never overwritten here
    store = Store(c["s3"])
    for sid, item in plan["history"].items():
        body = (packet / f"series-{sid}.json").read_bytes()
        r.write(
            f"put_history_{sid}",
            lambda b=body, k=item["key"]: store.put(k, b, {"If-None-Match": "*"}),
        )
        r.write(
            f"put_history_backfill_copy_{sid}",
            lambda b=body, s=sid, h=item["sha256"]: store.put_new(
                f"nowcast/v1/history/versions/{s}/backfill-{h[:16]}.json", b
            ),
        )
    # 4 refresh code + one run
    update_code(c, r, plan, packet, "refresh", "nowcast.zip", rev["refresh"])
    out = r.write(
        "invoke_refresh", lambda: c["lambda"].invoke(FunctionName=FUNCTIONS["refresh"])
    )
    result = json.loads(out["Payload"].read())
    r.value["refresh"] = result
    r.save()
    if out.get("FunctionError") or result.get("status") != "PUBLISHED":
        raise ValueError("Refresh with new code did not publish")
    if not isinstance(result.get("history"), dict) or "error" in result["history"]:
        raise ValueError("History update raised")
    current = json.loads(
        c["s3"]
        .get_object(Bucket=BUCKET, Key=CURRENT_KEY, ExpectedBucketOwner=ACCOUNT)["Body"]
        .read()
    )
    if current["published_at"] != result["published_at"] or any(
        o["freshness"]["status"] not in ("fresh", "delayed", "stale", "no_data")
        for o in current["observations"]
    ):
        raise ValueError("Projection lacks the new freshness states")
    # 5 public API code + bounded checks
    update_code(c, r, plan, packet, "api", "nowcast.zip", rev["api"])
    time.sleep(15)  # IAM propagation before the first read of the new keys
    http = get or requests.Session().get
    r.value["api_checks"] = []
    for path, expected in plan["public_api_checks"]:
        response = http(plan["public_api"] + path, timeout=20, allow_redirects=False)
        r.value["api_checks"].append({"path": path, "status": response.status_code})
        r.save()
        if response.status_code != expected:
            raise ValueError("Public API check failed; stop")
    # 6 website: both branches, demo Basic Auth preserved
    i = plan["site_inputs"]
    for branch in ("public", "demo"):
        build(
            packet / ("site-" + branch),
            i["export"],
            i["audit"],
            Path(i["metadata"]),
            Path(i["latest"]),
            Path(i["source_metadata"]),
            i["evidence"],
            "public",
            athena_proof_path=i["acceptance"],
            api_base=plan["public_api"],
            bot_url=plan["bot_url"],
        )
    child = packet / "website-release"
    release.prepare(
        child,
        packet / "site-public",
        packet / "site-demo",
        None,  # keep the existing demo password; never read or change it
        Path(plan["previous_site_receipt"]),
        demo_mode="public",
        failure_containment="stop_only",
        custom_headers=headers(plan["public_api"], geolocation=True),
        previous_headers=headers(plan["public_api"]),
    )
    child_plan, child_hash = release.load(child)
    for b in ("public", "demo"):
        if (
            child_plan["branches"][b]["stations"] != 1
            or child_plan["branches"][b]["hourly"] != 720
        ):
            raise ValueError("Website bundle escaped the licensed scope")
    r.value["derived_site_plan_sha256"] = child_hash
    r.save()
    deployed = release.execute(
        {"sts": c["sts"], "amplify": c["amplify"]}, child, child_plan, child_hash
    )
    r.value.update(
        status="DEPLOYED_BROWSER_VERIFICATION_REQUIRED",
        deployments=deployed["deployments"],
    )
    r.save()
    return r.value


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("command", choices=("prepare", "preflight", "execute"))
    a.add_argument("--packet", type=Path, required=True)
    a.add_argument("--confirm-plan-sha256")
    a.add_argument("--backfill", type=Path)
    a.add_argument("--site-inputs", type=Path)
    a.add_argument("--current-iam", type=Path)
    x = a.parse_args()
    if x.command == "prepare":
        digest = prepare(
            x.packet, x.backfill, json.loads(x.site_inputs.read_text()), x.current_iam
        )
        print(json.dumps({"plan_sha256": digest}))
        return
    plan = load(x.packet, x.confirm_plan_sha256)
    c = clients()
    print(
        json.dumps(
            preflight(c, x.packet, plan)
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
