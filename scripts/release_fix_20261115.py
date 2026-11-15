"""Frozen follow-up release after hosted acceptance of plan 19df0056.

Fixes found on the deployed site, plus the per-area CAMS estimate for all
126 Hanoi areas (model_areas in the projection):
- the public API's reserved concurrency of 2 throttled the new pages'
  parallel history requests (throttle replies carry no CORS header);
- Dragon's latest hour was missing from history because OpenAQ /hours lags
  /latest.

prepare   -> plan.json + code zip + site inputs (no AWS calls)
preflight -> read-only: deployed code, concurrency 2, schedules, headers
execute   -> 1 refresh code + one invoke, 2 public-api code, 3 public-api
             reserved concurrency 2 -> 10, 4 Amplify one deployment per branch
             (headers unchanged, demo Basic Auth preserved). Stop on failure.
"""

import argparse
import json
from pathlib import Path

from final_demo.control import Receipt
from live_data.contract import ACCOUNT, BUCKET, REGION, sha
from nowcast import deploy as nowcast
from nowcast.public_api import CURRENT_KEY
from scripts.release_rolling_history import (
    BOT_URL,
    PUBLIC_API,
    SCHEDULES,
    clients,
    code_sha,
    update_code,
)

DEPLOYED = "DN5zwUfw2DuIEEIny3B5nRJpDDFSQLYnWi/+tvGUFKk="
PREVIOUS = "/private/tmp/airlake-rolling-release-20261116"
FUNCTIONS = {"refresh": nowcast.REFRESH, "api": nowcast.API}
CONCURRENCY = {"previous": 2, "new": 10}


def prepare(packet, site_inputs):
    packet = Path(packet)
    packet.mkdir(mode=0o700)
    code = nowcast.lambda_zip()
    (packet / "nowcast.zip").write_bytes(code)
    previous = (Path(PREVIOUS) / "nowcast.zip").read_bytes()
    if code_sha(previous) != DEPLOYED:
        raise ValueError("Rollback copy is not the deployed nowcast code")
    (packet / "previous-nowcast.zip").write_bytes(previous)
    root = Path(__file__).resolve().parents[1]
    plan = {
        "account": ACCOUNT,
        "region": REGION,
        "functions": FUNCTIONS,
        "deployed_code_sha256_b64": {"refresh": DEPLOYED, "api": DEPLOYED},
        "new_code_sha256_b64": {"refresh": code_sha(code), "api": code_sha(code)},
        "api_reserved_concurrency": CONCURRENCY,
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
        "previous_site_receipt": f"{PREVIOUS}/website-release/release-receipt.json",
        "schedules_unchanged": list(SCHEDULES),
        "unchanged": "bot code, IAM, S3 history objects (only the scheduled refresh extends them), schedules, Telegram state, demo Basic Auth, security headers",
        "new_resources": 0,
        "browser_get_budget": 60,
        "rollback": [
            "lambda UpdateFunctionCode refresh and public-api with previous-nowcast.zip",
            "lambda PutFunctionConcurrency public-api 2",
            "Amplify redeploy of the job 5 zips in the previous website-release packet",
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
    if nowcast.lambda_zip() != (packet / "nowcast.zip").read_bytes():
        raise ValueError("Nowcast code drifted from the plan")
    for name, h in plan["site_source_sha256"].items():
        if sha((root / name).read_bytes()) != h:
            raise ValueError("Website source drifted from the plan")
    for k, h in plan["site_inputs_sha256"].items():
        if sha(Path(plan["site_inputs"][k]).read_bytes()) != h:
            raise ValueError("Website input drifted from the plan")
    return plan


def preflight(c, plan):
    if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong AWS account")
    revisions = {}
    for name, fn in plan["functions"].items():
        f = c["lambda"].get_function_configuration(FunctionName=fn)
        if f["CodeSha256"] != DEPLOYED or f["State"] != "Active":
            raise ValueError(f"Deployed {name} code changed; review first")
        revisions[name] = f["RevisionId"]
    reserved = c["lambda"].get_function_concurrency(FunctionName=FUNCTIONS["api"])
    if reserved.get("ReservedConcurrentExecutions") != CONCURRENCY["previous"]:
        raise ValueError("Public API concurrency changed; review first")
    for name in plan["schedules_unchanged"]:
        if c["scheduler"].get_schedule(Name=name)["State"] != "ENABLED":
            raise ValueError("A schedule is not enabled")
    return {"status": "PASS_READ_ONLY", "revisions": revisions}


def execute(c, packet, plan, digest):
    from citizen import release
    from citizen.hosting import headers
    from historical.build import build

    packet = Path(packet)
    if (packet / "receipt.json").exists():
        raise ValueError("Preserve the prior attempt; never retry automatically")
    rev = preflight(c, plan)["revisions"]
    r = Receipt(packet / "receipt.json", digest)
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
    if current["published_at"] != result["published_at"]:
        raise ValueError("Projection pointer did not move")
    areas = current.get("model_areas") or {}
    r.value["model_areas"] = {
        k: areas.get(k) for k in ("fetch_status", "model_run_at", "group_count")
    }
    r.save()
    if (
        areas.get("fetch_status") != "ok"
        or sum(len(g["areas"]) for g in areas.get("groups", [])) != 126
    ):
        raise ValueError("Per-area model estimate missing or incomplete")
    update_code(c, r, plan, packet, "api", "nowcast.zip", rev["api"])
    r.write(
        "put_api_concurrency",
        lambda: c["lambda"].put_function_concurrency(
            FunctionName=FUNCTIONS["api"],
            ReservedConcurrentExecutions=CONCURRENCY["new"],
        ),
    )
    if (
        c["lambda"]
        .get_function_concurrency(FunctionName=FUNCTIONS["api"])
        .get("ReservedConcurrentExecutions")
        != CONCURRENCY["new"]
    ):
        raise ValueError("Concurrency not applied as planned")
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
    live_headers = headers(plan["public_api"], geolocation=True)
    release.prepare(
        child,
        packet / "site-public",
        packet / "site-demo",
        None,  # keep the existing demo password; never read or change it
        Path(plan["previous_site_receipt"]),
        demo_mode="public",
        failure_containment="stop_only",
        custom_headers=None,  # headers already live; no update_app call
        previous_headers=live_headers,
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
