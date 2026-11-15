"""Frozen static-site-only release to the existing Amplify branches.

prepare   -> build public/demo bundles and a child Amplify plan (no AWS calls)
preflight -> read-only: Lambda code and public API concurrency are what the
             final release left (this tool never writes them), Amplify app,
             headers, demo Basic Auth and last jobs unchanged
execute   -> one deployment per branch; demo Basic Auth checked before and
             after; headers unchanged. No retry on failure.
"""

import argparse
import json
from pathlib import Path

from final_demo.control import Receipt
from live_data.contract import ACCOUNT, REGION, sha
from scripts.release_rolling_history import BOT_URL, PUBLIC_API

PREVIOUS_SITE_RECEIPT = (
    "/private/tmp/airlake-final-release-20261116/website-release/release-receipt.json"
)
LAMBDA_EXPECTED = {
    "airlake-nowcast-20261115-refresh": "OfJOlM2sl3+v6xcFh7ZahZh2nyeoyt59Jnrqv14yYAo=",
    "airlake-nowcast-20261115-public-api": "OfJOlM2sl3+v6xcFh7ZahZh2nyeoyt59Jnrqv14yYAo=",
    "airlake-telegram-20261115-bot": "ym3nARy2wDmZrMuXsMOFvvSvFQJxj8TD4dLph9P0nMY=",
}
API_CONCURRENCY = 10


def prepare(packet, site_inputs):
    from citizen import release
    from citizen.hosting import headers
    from historical.build import build

    packet = Path(packet)
    packet.mkdir(mode=0o700)
    for branch in ("public", "demo"):
        build(
            packet / ("site-" + branch),
            site_inputs["export"],
            site_inputs["audit"],
            Path(site_inputs["metadata"]),
            Path(site_inputs["latest"]),
            Path(site_inputs["source_metadata"]),
            site_inputs["evidence"],
            "public",
            athena_proof_path=site_inputs["acceptance"],
            api_base=PUBLIC_API,
            bot_url=BOT_URL,
        )
    live_headers = headers(PUBLIC_API, geolocation=True)
    child_hash = release.prepare(
        packet / "website-release",
        packet / "site-public",
        packet / "site-demo",
        None,  # keep the existing demo password; never read or change it
        Path(PREVIOUS_SITE_RECEIPT),
        demo_mode="public",
        failure_containment="stop_only",
        custom_headers=None,  # no update_app call
        previous_headers=live_headers,
    )
    child = json.loads((packet / "website-release/plan.json").read_text())
    root = Path(__file__).resolve().parents[1]
    plan = {
        "account": ACCOUNT,
        "region": REGION,
        "scope": "Amplify static site only: one deployment each to public and demo",
        "child_site_plan_sha256": child_hash,
        "zip_sha256": child["zip_sha256"],
        "previous_jobs": child["previous_jobs"],
        "site_source_sha256": {
            str(p.relative_to(root)): sha(p.read_bytes())
            for p in sorted((root / "citizen/web").iterdir())
            if p.is_file()
        },
        "read_only_guards": {
            "lambda_code_sha256_b64": LAMBDA_EXPECTED,
            "public_api_reserved_concurrency": API_CONCURRENCY,
        },
        "unchanged": "Lambda code, concurrency, IAM, S3 history, schedulers, Telegram, security headers, demo Basic Auth",
        "browser_get_budget": 30,
        "rollback": "Amplify redeploy of the job 6 zips in /private/tmp/airlake-final-release-20261116/website-release",
    }
    raw = json.dumps(plan, indent=1, sort_keys=True).encode()
    (packet / "plan.json").write_bytes(raw)
    return sha(raw)


def load(packet, confirm):
    from citizen import release

    packet = Path(packet)
    raw = (packet / "plan.json").read_bytes()
    if sha(raw) != confirm:
        raise ValueError("STOP: plan SHA-256 not approved")
    plan = json.loads(raw)
    root = Path(__file__).resolve().parents[1]
    for name, h in plan["site_source_sha256"].items():
        if sha((root / name).read_bytes()) != h:
            raise ValueError("Website source drifted from the plan")
    child, child_hash = release.load(packet / "website-release")
    if (
        child_hash != plan["child_site_plan_sha256"]
        or child["zip_sha256"] != plan["zip_sha256"]
    ):
        raise ValueError("Site bundle drifted from the plan")
    return plan, child, child_hash


def clients():
    import boto3
    from botocore.config import Config

    s = boto3.Session(profile_name="airlake", region_name=REGION)
    cfg = Config(retries={"total_max_attempts": 1}, connect_timeout=5, read_timeout=30)
    return {n: s.client(n, config=cfg) for n in ("sts", "lambda", "amplify")}


def preflight(c, packet, plan, child):
    from citizen import release

    if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong AWS account")
    for fn, code in plan["read_only_guards"]["lambda_code_sha256_b64"].items():
        if (
            c["lambda"].get_function_configuration(FunctionName=fn)["CodeSha256"]
            != code
        ):
            raise ValueError(
                "Lambda code changed since the final release; review first"
            )
    reserved = (
        c["lambda"]
        .get_function_concurrency(FunctionName="airlake-nowcast-20261115-public-api")
        .get("ReservedConcurrentExecutions")
    )
    if reserved != plan["read_only_guards"]["public_api_reserved_concurrency"]:
        raise ValueError("Public API concurrency changed; review first")
    site = release.preflight(
        {"sts": c["sts"], "amplify": c["amplify"]},
        Path(packet) / "website-release",
        child,
    )
    return {"status": "PASS_READ_ONLY", "site": site["status"]}


def execute(c, packet, plan, child, child_hash, digest):
    from citizen import release

    packet = Path(packet)
    if (packet / "receipt.json").exists():
        raise ValueError("Preserve the prior attempt; never retry automatically")
    preflight(c, packet, plan, child)
    r = Receipt(packet / "receipt.json", digest)
    result = release.execute(
        {"sts": c["sts"], "amplify": c["amplify"]},
        packet / "website-release",
        child,
        child_hash,
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
    x = a.parse_args()
    if x.command == "prepare":
        print(
            json.dumps(
                {
                    "plan_sha256": prepare(
                        x.packet, json.loads(x.site_inputs.read_text())
                    )
                }
            )
        )
        return
    plan, child, child_hash = load(x.packet, x.confirm_plan_sha256)
    c = clients()
    print(
        json.dumps(
            preflight(c, x.packet, plan, child)
            if x.command == "preflight"
            else execute(c, x.packet, plan, child, child_hash, x.confirm_plan_sha256)
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
