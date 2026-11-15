"""Resume the approved nowcast release after the smoke-check defect.

The frozen plan pins every nowcast module (they are in the Lambda bundle), so
this resume lives outside that package. The first attempt completed every
write through the public URL permissions and all seven smoke requests returned
their expected status; the checker then applied the /v1/now body check to the
POST 405 response and stopped. This script accepts exactly that recorded state
and runs the remaining plan steps with their own receipts.
"""

import argparse
import json
from pathlib import Path

from final_demo.control import Receipt
from nowcast import deploy

RECORDED_OPERATIONS = [
    "create_log_group_" + deploy.API,
    "retention_" + deploy.API,
    "create_log_group_" + deploy.REFRESH,
    "retention_" + deploy.REFRESH,
    "create_role_" + deploy.API,
    "put_role_policy_" + deploy.API,
    "create_role_" + deploy.REFRESH,
    "put_role_policy_" + deploy.REFRESH,
    "create_role_" + deploy.SCHEDULER_ROLE,
    "put_role_policy_" + deploy.SCHEDULER_ROLE,
    "create_function_" + deploy.API,
    "concurrency_" + deploy.API,
    "create_function_" + deploy.REFRESH,
    "concurrency_" + deploy.REFRESH,
    "invoke_refresh_once",
    "create_function_url",
    "permit_url",
    "permit_url_invoke",
]


def recorded_state(packet, digest):
    """The first attempt's receipt must match the known stop exactly."""
    backend = json.loads((Path(packet) / "receipt.json").read_text())
    ops = backend["operations"]
    if (
        backend["plan_sha256"] != digest
        or backend["status"] != "RUNNING"
        or [o["operation"] for o in ops] != RECORDED_OPERATIONS
        or any(o["status"] != "RETURNED_SUCCESS" for o in ops)
        or [(s["method"], s["path"], s["status"]) for s in backend["smoke"]]
        != deploy.API_SMOKE
        or backend["first_refresh"]["status"] != "PUBLISHED"
    ):
        raise ValueError("Recorded state differs from the known smoke-check stop")
    return backend


def alarms(c, packet, plan, digest):
    recorded_state(packet, digest)
    r = Receipt(Path(packet) / "resume-receipt.json", digest)
    r.value["resumes"] = "POST /v1/now 405 body was checked as a projection"
    for name, a in plan["alarms"].items():
        r.write(
            "alarm_" + name,
            lambda n=name, a=a: c["cloudwatch"].put_metric_alarm(
                AlarmName=n,
                Namespace="AWS/Lambda",
                MetricName=a["metric"],
                Dimensions=[{"Name": "FunctionName", "Value": a["function"]}],
                Statistic="Sum",
                Period=a["period_s"],
                EvaluationPeriods=1,
                Threshold=a["threshold"],
                ComparisonOperator="GreaterThanThreshold",
                TreatMissingData="notBreaching",
                ActionsEnabled=False,
            ),
        )
    r.value["status"] = "BACKEND_LIVE_SITE_RELEASE_NEXT"
    r.save()
    return r.value


def release_site(c, packet, plan, digest):
    """deploy.release_site with the gate moved to the resume receipt."""
    from urllib.parse import urlsplit

    from citizen import release
    from citizen.hosting import headers
    from historical.build import build

    packet = Path(packet)
    backend = recorded_state(packet, digest)
    resume = json.loads((packet / "resume-receipt.json").read_text())
    if resume["status"] != "BACKEND_LIVE_SITE_RELEASE_NEXT":
        raise ValueError("Alarms must be created first")
    if (packet / "site-receipt.json").exists():
        raise ValueError("Site release already attempted; no retry")
    url = backend["function_url"].rstrip("/")
    origin = "https://" + urlsplit(url).hostname
    i = plan["site"]["inputs"]
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
            api_base=url,
        )
    child = packet / "website-release"
    release.prepare(
        child,
        packet / "site-public",
        packet / "site-demo",
        Path(i["credentials"]),
        Path(i["previous_hosting_receipt"]),
        demo_mode="public",
        failure_containment="stop_only",
        custom_headers=headers(origin),
    )
    child_plan, child_hash = release.load(child)
    for b in ("public", "demo"):
        if (
            child_plan["branches"][b]["stations"] != 1
            or child_plan["branches"][b]["hourly"] != 720
        ):
            raise ValueError("Website bundle escaped the licensed scope")
    r = Receipt(packet / "site-receipt.json", digest)
    r.value.update(derived_plan_sha256=child_hash, api_origin=origin)
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


def fix_cors(c, packet, plan, digest, get=None):
    """Remove the Function URL CORS block; the handler already sets one header."""
    packet = Path(packet)
    verify = json.loads((packet / "verify-receipt.json").read_text())
    browser = json.loads((packet / "browser-evidence.json").read_text())
    if (
        verify["status"] != "PASS_LIVE_UPDATE_WITHOUT_REDEPLOYMENT"
        or browser.get("status") != "FAIL"
        or "duplicate Access-Control-Allow-Origin" not in browser.get("reason", "")
    ):
        raise ValueError("Not the recorded CORS failure")
    url = recorded_state(packet, digest)["function_url"].rstrip("/")
    r = Receipt(packet / "cors-receipt.json", digest)
    r.write(
        "remove_url_cors",
        lambda: c["lambda"].update_function_url_config(
            FunctionName=deploy.API, AuthType="NONE", Cors={}
        ),
    )
    config = c["lambda"].get_function_url_config(FunctionName=deploy.API)
    if config["AuthType"] != "NONE" or (config.get("Cors") or {}).get("AllowOrigins"):
        raise ValueError("Function URL CORS was not cleared; stop")
    if get is None:
        import requests

        http = requests.Session()
        http.trust_env = False

        def get(path, origin):
            return http.get(url + path, headers={"Origin": origin}, timeout=15)

    origin = "https://public." + __import__("citizen.release", fromlist=["x"]).DOMAIN
    resp = get("/v1/now", origin)
    r.value["check"] = {
        "status": resp.status_code,
        "allow_origin": resp.headers.get("access-control-allow-origin"),
    }
    r.save()
    if (
        resp.status_code != 200
        or resp.headers.get("access-control-allow-origin") != origin
    ):
        raise ValueError("CORS header is not exactly one allowed origin")
    r.value["status"] = "PASS_SINGLE_CORS_HEADER"
    r.save()
    return r.value


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument(
        "command",
        choices=(
            "check",
            "alarms",
            "release-site",
            "verify-live",
            "fix-cors",
            "enable-schedule",
        ),
    )
    a.add_argument("--packet", type=Path, required=True)
    a.add_argument("--confirm-plan-sha256", required=True)
    x = a.parse_args()
    plan = deploy.load(x.packet, x.confirm_plan_sha256)
    if x.command == "check":
        recorded_state(x.packet, x.confirm_plan_sha256)
        print(json.dumps({"status": "RECORDED_STATE_MATCHES"}))
        return
    c = deploy.clients()
    action = {
        "alarms": alarms,
        "release-site": release_site,
        "verify-live": deploy.verify_live,
        "fix-cors": fix_cors,
        "enable-schedule": deploy.enable_schedule,
    }[x.command]
    print(json.dumps(action(c, x.packet, plan, x.confirm_plan_sha256)))


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
