"""Frozen update of the live refresh code and both Amplify branches.

prepare  -> plan.json, new refresh zip, rollback copy of the deployed zip
preflight -> read-only: deployed code, schedule, headers, branches
execute  -> update refresh code (pinned RevisionId), one refresh, check the
            new projection, build both sites (API + optional bot link),
            deploy each branch once with unchanged security headers
"""

import argparse
import base64
import hashlib
import json
from pathlib import Path

from final_demo.control import Receipt
from live_data.contract import ACCOUNT, BUCKET, REGION, sha
from nowcast import deploy as nowcast
from nowcast.public_api import CURRENT_KEY

DEPLOYED_REFRESH_CODE = "5HH3RWWQpi1tDf9vjEUtDh4Tr0ZP/fHXLlaay9mbquE="
PUBLIC_API = "https://npe6uhbknh4qojnmylv3w436mi0epxfb.lambda-url.ap-southeast-2.on.aws"
PREVIOUS_SITE_RECEIPT = (
    "/private/tmp/airlake-nowcast-20261115/website-release/release-receipt.json"
)


def code_sha(raw):
    return base64.b64encode(hashlib.sha256(raw).digest()).decode()


def prepare(packet, site_inputs, bot_url, previous_zip):
    packet = Path(packet)
    packet.mkdir(mode=0o700)
    code = nowcast.lambda_zip()
    (packet / "refresh.zip").write_bytes(code)
    previous = Path(previous_zip).read_bytes()
    if code_sha(previous) != DEPLOYED_REFRESH_CODE:
        raise ValueError("Rollback copy is not the deployed refresh code")
    (packet / "previous-refresh.zip").write_bytes(previous)
    root = Path(__file__).resolve().parents[1]
    plan = {
        "account": ACCOUNT,
        "region": REGION,
        "refresh_function": nowcast.REFRESH,
        "refresh_code_sha256_b64": code_sha(code),
        "rollback_code_sha256_b64": DEPLOYED_REFRESH_CODE,
        "public_api": PUBLIC_API,
        "bot_url": bot_url,
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
        "operations": [
            "lambda:UpdateFunctionCode refresh (RevisionId pinned), wait, verify CodeSha256",
            "lambda:Invoke refresh once; s3:GetObject current.json must contain 'recent'",
            "Amplify: update_app with unchanged headers, one deployment per branch (public, demo)",
        ],
        "unchanged": "public API code and URL, schedules, IAM, alarms, historical data, private API",
        "browser_get_budget": 40,
        "rollback": [
            "UpdateFunctionCode with previous-refresh.zip",
            "Amplify redeploy of job 3 zips",
        ],
    }
    raw = json.dumps(plan, indent=1, sort_keys=True).encode()
    (packet / "plan.json").write_bytes(raw)
    return sha(raw)


def load(packet, confirm):
    packet = Path(packet)
    raw = (packet / "plan.json").read_bytes()
    if sha(raw) != confirm:
        raise ValueError("STOP: plan SHA-256 not approved")
    plan = json.loads(raw)
    root = Path(__file__).resolve().parents[1]
    if (
        code_sha((packet / "refresh.zip").read_bytes())
        != plan["refresh_code_sha256_b64"]
        or nowcast.lambda_zip() != (packet / "refresh.zip").read_bytes()
    ):
        raise ValueError("Refresh code drifted from the plan")
    for name, h in plan["site_source_sha256"].items():
        if sha((root / name).read_bytes()) != h:
            raise ValueError("Website source drifted from the plan")
    for k, h in plan["site_inputs_sha256"].items():
        if sha(Path(plan["site_inputs"][k]).read_bytes()) != h:
            raise ValueError("Website input drifted from the plan")
    return plan


def preflight(c, plan):
    from citizen import release
    from citizen.hosting import headers

    if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong AWS account")
    f = c["lambda"].get_function_configuration(FunctionName=nowcast.REFRESH)
    if f["CodeSha256"] != plan["rollback_code_sha256_b64"] or f["State"] != "Active":
        raise ValueError("Deployed refresh code changed; review first")
    if c["scheduler"].get_schedule(Name=nowcast.SCHEDULE)["State"] != "ENABLED":
        raise ValueError("Refresh schedule is not enabled")
    app = c["amplify"].get_app(appId=release.APP)["app"]
    if not release.security_headers_match(
        app.get("customHeaders"), headers(plan["public_api"])
    ):
        raise ValueError("Amplify headers differ from the live release")
    return {"status": "PASS_READ_ONLY", "revision": f["RevisionId"]}


def execute(c, packet, plan, digest):
    from citizen import release
    from citizen.hosting import headers
    from historical.build import build

    packet = Path(packet)
    revision = preflight(c, plan)["revision"]
    r = Receipt(packet / "receipt.json", digest)
    code = (packet / "refresh.zip").read_bytes()
    r.write(
        "update_refresh_code",
        lambda: c["lambda"].update_function_code(
            FunctionName=nowcast.REFRESH, ZipFile=code, RevisionId=revision
        ),
    )
    c["lambda"].get_waiter("function_updated_v2").wait(FunctionName=nowcast.REFRESH)
    if (
        c["lambda"].get_function_configuration(FunctionName=nowcast.REFRESH)[
            "CodeSha256"
        ]
        != plan["refresh_code_sha256_b64"]
    ):
        raise ValueError("Updated refresh code does not match")
    out = r.write(
        "invoke_refresh", lambda: c["lambda"].invoke(FunctionName=nowcast.REFRESH)
    )
    result = json.loads(out["Payload"].read())
    if out.get("FunctionError") or result.get("status") != "PUBLISHED":
        raise ValueError("Refresh with new code did not publish")
    current = json.loads(
        c["s3"]
        .get_object(Bucket=BUCKET, Key=CURRENT_KEY, ExpectedBucketOwner=ACCOUNT)["Body"]
        .read()
    )
    if current["published_at"] != result["published_at"] or "recent" not in current:
        raise ValueError("Projection lacks the 24-hour series")
    r.value["refresh"] = result
    r.save()
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
        Path(i["credentials"]),
        Path(plan["previous_site_receipt"]),
        demo_mode="public",
        failure_containment="stop_only",
        custom_headers=headers(plan["public_api"]),
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
    result = release.execute(
        {"sts": c["sts"], "amplify": c["amplify"]}, child, child_plan, child_hash
    )
    r.value.update(
        status="DEPLOYED_BROWSER_VERIFICATION_REQUIRED",
        deployments=result["deployments"],
    )
    r.save()
    return r.value


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("command", choices=("prepare", "preflight", "execute"))
    a.add_argument("--packet", type=Path, required=True)
    a.add_argument("--confirm-plan-sha256")
    a.add_argument("--site-inputs", type=Path)
    a.add_argument("--bot-url")
    a.add_argument("--previous-zip", type=Path)
    x = a.parse_args()
    if x.command == "prepare":
        print(
            json.dumps(
                {
                    "plan_sha256": prepare(
                        x.packet,
                        json.loads(x.site_inputs.read_text()),
                        x.bot_url,
                        x.previous_zip,
                    )
                }
            )
        )
        return
    plan = load(x.packet, x.confirm_plan_sha256)
    c = nowcast.clients()
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
