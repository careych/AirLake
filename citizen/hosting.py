"""Manual Amplify static deployment, public licensed data and gated instructor data."""

import argparse
import base64
import io
import json
import secrets
import time
import zipfile
from pathlib import Path
from urllib.parse import urlsplit

from final_demo.control import Receipt
from live_data.contract import ACCOUNT, REGION, sha

NAME = "airlake-hanoi-citizen-20261114"
ROOT = Path(__file__).resolve().parents[1]
HEADERS = """customHeaders:
  - pattern: '**/*'
    headers:
      - key: X-Content-Type-Options
        value: nosniff
      - key: X-Frame-Options
        value: DENY
      - key: Referrer-Policy
        value: no-referrer
      - key: Cache-Control
        value: no-store
      - key: Permissions-Policy
        value: 'camera=(), microphone=(), geolocation=()'
      - key: Content-Security-Policy
        value: "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
"""


def clients():
    import os

    import boto3
    from botocore.config import Config

    if any(k.startswith("AWS_ENDPOINT_URL") for k in os.environ):
        raise ValueError("Custom endpoints forbidden")
    s = boto3.Session(profile_name="airlake", region_name=REGION)
    cfg = Config(retries={"total_max_attempts": 1}, connect_timeout=5, read_timeout=30)
    return {n: s.client(n, config=cfg) for n in ("sts", "amplify")}


def zip_site(path, mode):
    files = {str(p.relative_to(path)): p for p in path.rglob("*") if p.is_file()}
    if any(p.is_symlink() for p in path.rglob("*")):
        raise ValueError("No symlink artifacts")
    config = (path / "config.js").read_text()
    if not config.startswith("window.AIRLAKE_CONFIG=") or not config.endswith(";\n"):
        raise ValueError("Unexpected config")
    cfg = json.loads(config[len("window.AIRLAKE_CONFIG=") : -2])
    name = cfg["dataUrl"]
    if name != "data/" + cfg["sha256"] + ".json" or cfg["mode"] != mode:
        raise ValueError("Dataset path/mode mismatch")
    assets = ["index.html", "app.js", "styles.css", "icon.svg", "history.js"]
    if set(files) != {*assets, "config.js", name}:
        raise ValueError("Unapproved deployment files")
    data = json.loads(files[name].read_bytes())
    if sha(files[name].read_bytes()) != cfg["sha256"] or data["mode"] != mode:
        raise ValueError("Dataset hash mismatch")
    if data.get("schema_version") == 2 and data.get("cloud_verified") is not True:
        raise ValueError(
            "Historical preview is not independently AWS verified; deployment forbidden"
        )
    if mode == "public" and (
        {s["id"] for s in data["stations"]} != {6123215}
        or any(
            s["license"] != "CC BY 4.0" or not s["downloads_allowed"]
            for s in data["stations"]
        )
    ):
        raise ValueError("Public data license mismatch")
    if mode == "instructor" and {s["id"] for s in data["stations"]} != {
        4946812,
        4946813,
        6123215,
    }:
        raise ValueError("Instructor scope mismatch")
    for n in assets:
        if files[n].read_bytes() != (ROOT / "citizen/web" / n).read_bytes():
            raise ValueError("Source asset changed after build")
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as z:
        for n, p in sorted(files.items()):
            info = zipfile.ZipInfo(n, (2026, 11, 14, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            z.writestr(info, p.read_bytes())
    return output.getvalue(), {
        "data_sha256": cfg["sha256"],
        "data_path": name,
        "stations": len(data["stations"]),
        "hourly": len(data["hourly"]),
    }


def prepare(packet, public, instructor):
    if packet.exists() or any(
        (p / ".git").exists() for p in (packet.resolve(), *packet.resolve().parents)
    ):
        raise ValueError("New private packet outside Git required")
    bundles = {}
    details = {}
    for branch, path, mode in [
        ("public", public, "public"),
        ("demo", instructor, "instructor"),
    ]:
        bundles[branch], details[branch] = zip_site(path, mode)
    password = secrets.token_urlsafe(24)
    credentials = {"username": "airlake-demo", "password": password}
    packet.mkdir(mode=0o700, parents=True)
    auth = json.dumps(credentials).encode()
    (packet / "preview-login.json").write_bytes(auth)
    for b, raw in bundles.items():
        (packet / (b + ".zip")).write_bytes(raw)
    p = {
        "account": ACCOUNT,
        "region": REGION,
        "app_name": NAME,
        "platform": "WEB",
        "branches": {
            "public": {"password_protected": False, **details["public"]},
            "demo": {"password_protected": True, **details["demo"]},
        },
        "zip_sha256": {b: sha(raw) for b, raw in bundles.items()},
        "credential_sha256": sha(auth),
        "max_deployments": 2,
        "auto_build": False,
        "source_helper_sha256": sha(Path(__file__).read_bytes()),
        "max_http_verification_requests": 40,
        "containment": "Enable app-wide password protection if any restricted asset is anonymously accessible",
        "no_lake_policy_changes": True,
    }
    (packet / "plan.json").write_text(json.dumps(p, sort_keys=True))
    for f in packet.iterdir():
        f.chmod(0o600)
    return sha((packet / "plan.json").read_bytes())


def load(packet):
    raw = (packet / "plan.json").read_bytes()
    p = json.loads(raw)
    if (
        (p["account"], p["region"], p["app_name"], p["platform"])
        != (ACCOUNT, REGION, NAME, "WEB")
        or p["source_helper_sha256"] != sha(Path(__file__).read_bytes())
        or p["branches"]["public"]["password_protected"] is not False
        or p["branches"]["demo"]["password_protected"] is not True
    ):
        raise ValueError("Frozen hosting scope mismatch")
    for b, h in p["zip_sha256"].items():
        if (
            b not in ("public", "demo")
            or sha((packet / (b + ".zip")).read_bytes()) != h
        ):
            raise ValueError("ZIP integrity mismatch")
    if sha((packet / "preview-login.json").read_bytes()) != p["credential_sha256"]:
        raise ValueError("Private credential file changed")
    return p, sha(raw)


def preflight(c):
    if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong account")
    for page in c["amplify"].get_paginator("list_apps").paginate():
        if any(a["name"] == NAME for a in page.get("apps", [])):
            raise ValueError("App name already exists; do not recreate/retry")
    return {"status": "PASS_READ_ONLY_NO_EXISTING_APP"}


def deploy(c, packet, p, digest):
    import requests

    preflight(c)
    r = Receipt(packet / "hosting-receipt.json", digest)
    auth = json.loads((packet / "preview-login.json").read_bytes())
    encoded = base64.b64encode(
        (auth["username"] + ":" + auth["password"]).encode()
    ).decode()
    app_id = None
    try:
        a = r.write(
            "create_one_static_app",
            lambda: c["amplify"].create_app(
                name=NAME,
                platform="WEB",
                enableBranchAutoBuild=False,
                enableBasicAuth=False,
                customHeaders=HEADERS,
                tags={"Project": "AirLake", "Purpose": "citizen-final-demo"},
            ),
        )["app"]
        app_id = a["appId"]
        domain = a["defaultDomain"]
        if not domain.endswith(".amplifyapp.com"):
            raise ValueError("Unexpected Amplify domain")
        r.value.update(app_id=app_id, domain=domain)
        r.save()
        for b in ("public", "demo"):
            args = {
                "appId": app_id,
                "branchName": b,
                "stage": "PRODUCTION" if b == "public" else "DEVELOPMENT",
                "enableAutoBuild": False,
                "enableBasicAuth": b == "demo",
                "enablePerformanceMode": False,
            }
            if b == "demo":
                args["basicAuthCredentials"] = encoded
            r.write(
                "create_branch_" + b, lambda kw=args: c["amplify"].create_branch(**kw)
            )
            actual = c["amplify"].get_branch(appId=app_id, branchName=b)["branch"]
            if actual["enableBasicAuth"] != (b == "demo") or actual["enableAutoBuild"]:
                raise ValueError("Branch isolation not established")
        http = requests.Session()
        http.trust_env = False
        r.value["deployments"] = {}
        for b in ("public", "demo"):
            deployment = r.write(
                "prepare_upload_" + b,
                lambda branch=b: c["amplify"].create_deployment(
                    appId=app_id, branchName=branch
                ),
            )
            job_id = deployment["jobId"]
            url = deployment["zipUploadUrl"]
            host = urlsplit(url)
            if host.scheme != "https" or not host.hostname.endswith(".amazonaws.com"):
                raise ValueError("Unexpected managed upload target")
            r.value["deployments"][b] = {"job_id": job_id, "status": "UPLOAD_UNCERTAIN"}
            r.save()
            response = http.put(
                url,
                data=(packet / (b + ".zip")).read_bytes(),
                timeout=60,
                allow_redirects=False,
            )
            if response.status_code not in (200, 201, 204):
                raise ValueError("Managed upload failed; no retry")
            r.value["deployments"][b]["status"] = "UPLOADED"
            r.save()
            r.write(
                "start_once_" + b,
                lambda branch=b, j=job_id: c["amplify"].start_deployment(
                    appId=app_id, branchName=branch, jobId=j
                ),
            )
            for _ in range(90):
                job = c["amplify"].get_job(appId=app_id, branchName=b, jobId=job_id)[
                    "job"
                ]["summary"]
                state = job["status"]
                if state == "SUCCEED":
                    break
                if state in ("FAILED", "CANCELLED"):
                    raise ValueError("Amplify deployment failed; no retry")
                time.sleep(5)
            else:
                raise TimeoutError("Deployment still uncertain; preserve job id")
            r.value["deployments"][b].update(
                status="SUCCEED", url="https://" + b + "." + domain
            )
            r.save()
        r.value["http_checks"] = []

        def get(branch, path="", private=False):
            if len(r.value["http_checks"]) >= p["max_http_verification_requests"]:
                raise ValueError("Verification HTTP budget exhausted")
            item = {
                "branch": branch,
                "path": path,
                "authenticated": private,
                "status": "UNCERTAIN",
            }
            r.value["http_checks"].append(item)
            r.save()
            response = http.get(
                "https://" + branch + "." + domain + "/" + path,
                auth=(auth["username"], auth["password"]) if private else None,
                timeout=30,
                allow_redirects=False,
            )
            item["status"] = response.status_code
            r.save()
            return response

        # Protect every demo asset, not only its HTML route.
        for path in ("", p["branches"]["demo"]["data_path"], "app.js"):
            if get("demo", path).status_code != 401:
                raise PermissionError("Restricted branch protection failed")
        for branch, private in [("public", False), ("demo", True)]:
            if get(branch, private=private).status_code != 200:
                raise ValueError("HTTPS root not verified")
            response = get(branch, p["branches"][branch]["data_path"], private)
            if (
                response.status_code != 200
                or sha(response.content) != p["branches"][branch]["data_sha256"]
            ):
                raise ValueError("Hosted data integrity failed")
        r.value.update(
            status="PASS_HOSTED_HTTPS_AUTH_SCOPE_EXTERNAL_BROWSER_PENDING",
            public_url="https://public." + domain,
            preview_url="https://demo." + domain,
        )
        r.save()
        return r.value
    except BaseException:
        if app_id:
            # Authorized emergency containment preserves deployment/resources; no deletion.
            r.write(
                "contain_app_with_password",
                lambda: c["amplify"].update_app(
                    appId=app_id, enableBasicAuth=True, basicAuthCredentials=encoded
                ),
            )
        raise


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("command", choices=["prepare", "preflight", "deploy"])
    a.add_argument("--packet", type=Path, required=True)
    a.add_argument("--public", type=Path)
    a.add_argument("--instructor", type=Path)
    a.add_argument("--confirm-plan-sha256")
    x = a.parse_args()
    if x.command == "prepare":
        print(json.dumps({"plan_sha256": prepare(x.packet, x.public, x.instructor)}))
        return
    p, h = load(x.packet)
    if x.command == "deploy" and x.confirm_plan_sha256 != h:
        a.exit(1, "STOP: exact AWS hosting/exposure approval required\n")
    c = clients()
    result = preflight(c) if x.command == "preflight" else deploy(c, x.packet, p, h)
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001 -- never print credentials or signed upload URLs
        print(
            json.dumps(
                {
                    "status": "STOP_FAILED_OR_UNCERTAIN",
                    "exception_type": type(e).__name__,
                }
            )
        )
        raise SystemExit(1) from None
