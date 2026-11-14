"""Update the existing Amplify branches once under an approved immutable plan."""

import argparse
import base64
import json
import stat
import time
from pathlib import Path
from urllib.parse import urlsplit

from citizen.hosting import HEADERS, NAME, clients, zip_site
from final_demo.control import Receipt
from live_data.contract import ACCOUNT, REGION, sha

APP = "dhyvgpdsnj4zi"
DOMAIN = APP + ".amplifyapp.com"


def security_headers_match(value):
    if value == HEADERS:
        return True
    # Amplify canonicalizes the submitted YAML as a JSON array in GetApp.
    expected = {}
    key = None
    for line in HEADERS.splitlines():
        line = line.strip()
        if line.startswith("- key:"):
            key = line.split(":", 1)[1].strip()
        elif line.startswith("value:"):
            item = line.split(":", 1)[1].strip()
            if item[:1] in ("'", '"') and item[-1:] == item[:1]:
                item = item[1:-1]
            expected[key] = item
    try:
        rules = json.loads(value)
        if (
            len(rules) != 1
            or set(rules[0]) != {"pattern", "headers"}
            or rules[0]["pattern"] != "**/*"
        ):
            return False
        headers = rules[0]["headers"]
        return (
            len(headers) == len(expected)
            and all(set(h) == {"key", "value"} for h in headers)
            and {h["key"]: h["value"] for h in headers} == expected
        )
    except (ValueError, TypeError, KeyError):
        return False


def private_credentials(path):
    if path.is_symlink() or stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise ValueError("Use the existing private 0600 credential file")
    raw = path.read_bytes()
    data = json.loads(raw)
    if (
        set(data) != {"username", "password"}
        or data["username"] != "airlake-demo"
        or not isinstance(data["password"], str)
        or len(data["password"]) < 20
    ):
        raise ValueError("Unexpected private credential schema")
    encoded = base64.b64encode(
        (data["username"] + ":" + data["password"]).encode()
    ).decode()
    return raw, data, encoded


def prepare(packet, public, instructor, credential_path, previous_receipt):
    if packet.exists() or any(
        (p / ".git").exists() for p in (packet.resolve(), *packet.resolve().parents)
    ):
        raise ValueError("Choose a new private packet outside Git")
    previous = json.loads(previous_receipt.read_bytes())
    if previous.get("app_id") != APP or previous.get("domain") != DOMAIN:
        raise ValueError("Existing deployment receipt required")
    old_jobs = {}
    for branch in ("public", "demo"):
        job = previous["deployments"][branch]
        if job["status"] != "SUCCEED":
            raise ValueError("Do not replace an uncertain deployment")
        old_jobs[branch] = str(int(job["job_id"]))
    raw, _, _ = private_credentials(credential_path)
    old_plan_bytes = previous_receipt.with_name("plan.json").read_bytes()
    old_plan = json.loads(old_plan_bytes)
    if sha(old_plan_bytes) != previous["plan_sha256"] or old_plan[
        "credential_sha256"
    ] != sha(raw):
        raise ValueError(
            "Private credential does not match the retained deployment plan"
        )
    bundles, details = {}, {}
    for b, source, mode in (
        ("public", public, "public"),
        ("demo", instructor, "instructor"),
    ):
        bundles[b], details[b] = zip_site(source, mode)
    plan = {
        "account": ACCOUNT,
        "region": REGION,
        "app_id": APP,
        "domain": DOMAIN,
        "previous_jobs": old_jobs,
        "branches": details,
        "zip_sha256": {b: sha(body) for b, body in bundles.items()},
        "credential_sha256": sha(raw),
        "helper_sha256": sha(Path(__file__).read_bytes()),
        "max_deployments": 2,
        "new_website_get_budget": 40,
        "new_athena_queries": 0,
        "new_polling_or_glue_runs": 0,
        "new_resources": 0,
        "credential_rotation": False,
        "containment": "existing app-wide password protection only; then STOP",
    }
    packet.mkdir(mode=0o700, parents=True)
    (packet / "preview-login.json").write_bytes(raw)
    for b, body in bundles.items():
        (packet / (b + ".zip")).write_bytes(body)
    (packet / "plan.json").write_text(json.dumps(plan, sort_keys=True))
    for p in packet.iterdir():
        p.chmod(0o600)
    return sha((packet / "plan.json").read_bytes())


def load(packet):
    if (packet / "CANCELLED_BY_OWNER.json").exists():
        raise ValueError("This release was cancelled by the owner")
    raw = (packet / "plan.json").read_bytes()
    p = json.loads(raw)
    credentials_raw, _, _ = private_credentials(packet / "preview-login.json")
    if (
        (p["account"], p["region"], p["app_id"], p["domain"])
        != (ACCOUNT, REGION, APP, DOMAIN)
        or p["helper_sha256"] != sha(Path(__file__).read_bytes())
        or p["credential_sha256"] != sha(credentials_raw)
        or set(p["branches"]) != {"public", "demo"}
        or p["max_deployments"] != 2
        or p["new_website_get_budget"] != 40
        or p["new_resources"] != 0
    ):
        raise ValueError("Frozen release scope changed")
    for b in ("public", "demo"):
        if sha((packet / (b + ".zip")).read_bytes()) != p["zip_sha256"][b]:
            raise ValueError("Release ZIP changed")
    return p, sha(raw)


def preflight(c, packet, plan):
    if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong AWS account")
    _, credentials, _ = private_credentials(packet / "preview-login.json")
    app = c["amplify"].get_app(appId=APP)["app"]
    if (
        app["name"] != NAME
        or app["defaultDomain"] != DOMAIN
        or app["platform"] != "WEB"
        or app.get("enableBasicAuth")
        or app.get("enableBranchAutoBuild")
        or not security_headers_match(app.get("customHeaders"))
    ):
        raise ValueError("Existing app changed; review before writing")
    for b in ("public", "demo"):
        actual = c["amplify"].get_branch(appId=APP, branchName=b)["branch"]
        if (
            actual["enableBasicAuth"] != (b == "demo")
            or actual["enableAutoBuild"]
            or (
                b == "demo"
                and base64.b64decode(
                    actual.get("basicAuthCredentials", ""), validate=True
                )
                .decode()
                .split(":", 1)[0]
                != credentials["username"]
            )
        ):
            raise ValueError("Existing authentication changed")
        jobs = c["amplify"].list_jobs(appId=APP, branchName=b, maxResults=1)[
            "jobSummaries"
        ]
        if (
            len(jobs) != 1
            or jobs[0]["status"] != "SUCCEED"
            or str(int(jobs[0]["jobId"])) != plan["previous_jobs"][b]
        ):
            raise ValueError("Deployment changed or is uncertain; stop")
    return {"status": "PASS_EXISTING_APP_AND_AUTH_UNCHANGED"}


def execute(c, packet, plan, digest):
    import requests

    if (packet / "release-receipt.json").exists():
        raise ValueError("Preserve prior attempt; never retry deployment automatically")
    preflight(c, packet, plan)
    _, credentials, encoded = private_credentials(packet / "preview-login.json")
    receipt = Receipt(packet / "release-receipt.json", digest)
    receipt.value.update(app_id=APP, domain=DOMAIN, deployments={})
    receipt.save()
    http = requests.Session()
    http.trust_env = False
    try:
        # AWS returns an encrypted password component. Validate the retained
        # password through HTTPS, never by comparing it with the API field.
        receipt.value["http_checks"] = []
        for authenticated, expected in ((False, 401), (True, 200)):
            item = {
                "branch": "demo",
                "authenticated": authenticated,
                "status": "UNCERTAIN",
            }
            receipt.value["http_checks"].append(item)
            receipt.save()
            response = http.get(
                "https://demo." + DOMAIN + "/",
                auth=(credentials["username"], credentials["password"])
                if authenticated
                else None,
                timeout=25,
                allow_redirects=False,
            )
            item["status"] = response.status_code
            receipt.save()
            if response.status_code != expected:
                raise ValueError("Existing demo credential/authentication not verified")
        for b in ("public", "demo"):
            d = receipt.write(
                "prepare_upload_" + b,
                lambda branch=b: c["amplify"].create_deployment(
                    appId=APP, branchName=branch
                ),
            )
            target = urlsplit(d["zipUploadUrl"])
            if (
                target.scheme != "https"
                or not target.hostname
                or not target.hostname.endswith(".amazonaws.com")
            ):
                raise ValueError("Unexpected managed upload endpoint")
            job = d["jobId"]
            receipt.value["deployments"][b] = {
                "job_id": job,
                "status": "UPLOAD_UNCERTAIN",
            }
            receipt.save()
            response = http.put(
                d["zipUploadUrl"],
                data=(packet / (b + ".zip")).read_bytes(),
                timeout=60,
                allow_redirects=False,
            )
            if response.status_code not in (200, 201, 204):
                raise ValueError("Upload failed; no retry")
            receipt.write(
                "deploy_once_" + b,
                lambda branch=b, j=job: c["amplify"].start_deployment(
                    appId=APP, branchName=branch, jobId=j
                ),
            )
            for _ in range(90):
                status = c["amplify"].get_job(appId=APP, branchName=b, jobId=job)[
                    "job"
                ]["summary"]["status"]
                if status == "SUCCEED":
                    break
                if status in ("FAILED", "CANCELLED"):
                    raise ValueError("Deployment failed; no retry")
                time.sleep(5)
            else:
                raise TimeoutError("Deployment uncertain; preserve receipt")
            receipt.value["deployments"][b].update(
                status="SUCCEED", url="https://" + b + "." + DOMAIN
            )
            receipt.save()
        receipt.value["status"] = "PASS_DEPLOYED_BROWSER_VERIFICATION_REQUIRED"
        receipt.save()
        return receipt.value
    except BaseException:
        # Authorized containment preserves data and does not retry a deployment.
        receipt.write(
            "contain_existing_app",
            lambda: c["amplify"].update_app(
                appId=APP, enableBasicAuth=True, basicAuthCredentials=encoded
            ),
        )
        raise


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("command", choices=("prepare", "preflight", "execute"))
    a.add_argument("--packet", required=True, type=Path)
    a.add_argument("--public", type=Path)
    a.add_argument("--instructor", type=Path)
    a.add_argument("--credentials", type=Path)
    a.add_argument("--previous-receipt", type=Path)
    a.add_argument("--confirm-plan-sha256")
    x = a.parse_args()
    if x.command == "prepare":
        print(
            json.dumps(
                {
                    "plan_sha256": prepare(
                        x.packet,
                        x.public,
                        x.instructor,
                        x.credentials,
                        x.previous_receipt,
                    )
                }
            )
        )
        return
    plan, digest = load(x.packet)
    if x.command == "execute" and x.confirm_plan_sha256 != digest:
        a.exit(
            1, "STOP: exact existing-app release and new GET budget approval required\n"
        )
    c = clients()
    result = (
        preflight(c, x.packet, plan)
        if x.command == "preflight"
        else execute(c, x.packet, plan, digest)
    )
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001 -- signed upload URLs and credentials stay private
        print(
            json.dumps(
                {
                    "status": "STOP_FAILED_OR_UNCERTAIN",
                    "exception_type": type(e).__name__,
                }
            )
        )
        raise SystemExit(1) from None
