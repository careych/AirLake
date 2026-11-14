"""Conditional private API and existing-site publication after full cloud acceptance."""

import base64
import io
import json
import zipfile
from pathlib import Path

from citizen import release
from final_demo.control import Receipt, absent
from historical.build import build
from live_data.contract import ACCOUNT, BUCKET, NAME, REGION, sha

API = NAME + "-api"
POLICY = "HistoricalResearch20261114"


def api_zip(previous, root):
    buffer = io.BytesIO()
    with (
        zipfile.ZipFile(io.BytesIO(previous)) as old,
        zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as new,
    ):
        if "live_data/api.py" not in old.namelist() or any(
            name.startswith("historical/") for name in old.namelist()
        ):
            raise ValueError("Expected original API packet without historical route")
        for info in old.infolist():
            new.writestr(
                info,
                (root / "live_data/api.py").read_bytes()
                if info.filename == "live_data/api.py"
                else old.read(info.filename),
            )
        for name in ("__init__.py", "api.py", "analytics.py", "data.py"):
            info = zipfile.ZipInfo("historical/" + name, (2026, 11, 14, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            new.writestr(info, (root / "historical" / name).read_bytes())
    return buffer.getvalue()


def api_preflight(c, p):
    f = c["lambda"].get_function_configuration(FunctionName=API)
    expected = base64.b64encode(bytes.fromhex(p["previous_api_sha256"])).decode()
    if (
        f["CodeSha256"] != expected
        or f["Role"] != f"arn:aws:iam::{ACCOUNT}:role/{API}"
        or f.get("Runtime") != "python3.12"
        or f.get("Handler") != "live_data.api.handler"
        or f.get("State") != "Active"
        or f.get("LastUpdateStatus") != "Successful"
        or any(
            k.startswith("AIRLAKE_HISTORY_")
            for k in f.get("Environment", {}).get("Variables", {})
        )
    ):
        raise ValueError("Existing private API changed; preserve its revision")
    if c["lambda"].get_function_url_config(FunctionName=API)["AuthType"] != "AWS_IAM":
        raise ValueError("Private API authentication changed")
    absent(
        lambda: c["iam"].get_role_policy(RoleName=API, PolicyName=POLICY),
        {"NoSuchEntity"},
    )
    return f


def publish_api(c, p, packet, digest):
    from historical.cloud import PREFIX

    if (packet / "api-publication.json").exists():
        raise ValueError("Publication already attempted; no retry")
    accepted = json.loads((packet / "acceptance.json").read_bytes())
    raw = (packet / "cloud-export.json").read_bytes()
    proof = (packet / "acceptance.json").read_bytes()
    if (
        accepted["status"] != "PASS"
        or accepted["export_sha256"] != sha(raw)
        or accepted["hourly_all_rows_match"] != "PASS"
        or accepted["daily_all_rows_match"] != "PASS"
    ):
        raise ValueError("Independent cloud acceptance required")
    f = api_preflight(c, p)
    r = Receipt(packet / "api-publication.json", digest)

    def no_overwrite(params, **kwargs):
        params["headers"]["If-None-Match"] = "*"

    c["s3"].meta.events.register("before-call.s3.PutObject", no_overwrite)
    r.write(
        "publish_immutable_acceptance",
        lambda: c["s3"].put_object(
            Bucket=BUCKET,
            Key=PREFIX + "outputs/acceptance.json",
            Body=proof,
            ExpectedBucketOwner=ACCOUNT,
            ServerSideEncryption="AES256",
        ),
    )
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": [
                    "arn:aws:s3:::" + BUCKET + "/" + PREFIX + "outputs/" + n
                    for n in ("export.json", "acceptance.json")
                ],
            }
        ],
    }
    r.write(
        "add_exact_two_object_read_policy",
        lambda: c["iam"].put_role_policy(
            RoleName=API, PolicyName=POLICY, PolicyDocument=json.dumps(policy)
        ),
    )
    payload = (packet / "api-payload.zip").read_bytes()
    r.write(
        "update_history_route_only",
        lambda: c["lambda"].update_function_code(
            FunctionName=API, ZipFile=payload, RevisionId=f["RevisionId"]
        ),
    )
    c["lambda"].get_waiter("function_updated_v2").wait(
        FunctionName=API, WaiterConfig={"Delay": 2, "MaxAttempts": 40}
    )
    fresh = c["lambda"].get_function_configuration(FunctionName=API)
    if (
        fresh["CodeSha256"]
        != base64.b64encode(bytes.fromhex(p["artifacts"]["api-payload.zip"])).decode()
    ):
        raise ValueError("Updated API code does not match")
    env = dict(fresh.get("Environment", {}).get("Variables", {}))
    env.update(
        AIRLAKE_HISTORY_EXPORT_KEY=PREFIX + "outputs/export.json",
        AIRLAKE_HISTORY_EXPORT_SHA256=sha(raw),
        AIRLAKE_HISTORY_ACCEPTANCE_SHA256=sha(proof),
    )
    r.write(
        "add_pinned_historical_configuration",
        lambda: c["lambda"].update_function_configuration(
            FunctionName=API,
            RevisionId=fresh["RevisionId"],
            Environment={"Variables": env},
        ),
    )
    c["lambda"].get_waiter("function_updated_v2").wait(
        FunctionName=API, WaiterConfig={"Delay": 2, "MaxAttempts": 40}
    )
    if c["lambda"].get_function_url_config(FunctionName=API)["AuthType"] != "AWS_IAM":
        raise ValueError("API must stay IAM-only")
    r.value.update(
        status="DEPLOYED_REQUIRES_FIVE_SIGNED_HTTP_CHECKS",
        history_export_sha256=sha(raw),
        previous_api_sha256=p["previous_api_sha256"],
    )
    r.save()
    return r.value


def smoke_api(c, p, packet, digest, get=None):
    import csv
    import math
    from datetime import timedelta
    from urllib.parse import urlsplit

    from ingestion.openaq import stamp, utc

    if (packet / "history-http.json").exists():
        raise ValueError("HTTP budget already attempted")
    ex = json.loads((packet / "cloud-export.json").read_text())
    proof = json.loads((packet / "acceptance.json").read_text())
    url = c["lambda"].get_function_url_config(FunctionName=API)
    host = urlsplit(url["FunctionUrl"])
    if (
        url["AuthType"] != "AWS_IAM"
        or host.scheme != "https"
        or not host.hostname.endswith(".lambda-url.ap-southeast-2.on.aws")
    ):
        raise ValueError("API URL scope changed")
    if get is None:
        import boto3
        import requests
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest

        session = boto3.Session(profile_name="airlake", region_name=REGION)
        http = requests.Session()
        http.trust_env = False

        def get(path):
            target = url["FunctionUrl"].rstrip("/") + path
            r = AWSRequest(method="GET", url=target)
            SigV4Auth(
                session.get_credentials().get_frozen_credentials(), "lambda", REGION
            ).add_auth(r)
            return http.get(
                target, headers=dict(r.headers), timeout=25, allow_redirects=False
            )

    r = Receipt(packet / "history-http.json", digest)
    r.value["requests"] = []
    start = stamp(utc(ex["provenance"]["window_end_utc"]) - timedelta(days=7))
    paths = [
        "/api/v1/history?station_id=6123215&limit=1000",
        "/api/v1/history?station_id=6123215&granularity=daily&format=csv&limit=100",
        "/api/v1/history?station_id=6123215&limit=1000&start=" + start,
        "/api/v1/current/hourly?limit=100",
        "/api/v1/latest",
    ]
    for i, path in enumerate(paths):
        item = {"path": path, "status": "UNCERTAIN"}
        r.value["requests"].append(item)
        r.save()
        resp = get(path)
        item["status"] = resp.status_code
        r.save()
        if resp.status_code != 200:
            raise ValueError("Signed API failed; no retry")
        if i == 0:
            body = resp.json()
            expected = {
                v["hour_start"]: v for v in ex["facts"] if v["location_id"] == 6123215
            }
            if (
                body["count"] != 720
                or body["total"] != 720
                or body["metadata"]["export_sha256"] != proof["export_sha256"]
            ):
                raise ValueError("Full API dataset mismatch")
            seen = set()
            for v in body["data"]:
                if (
                    v["hour_start"] in seen
                    or v["aqi"] != expected[v["hour_start"]]["aqi"]
                    or not math.isclose(
                        v["pm25_avg"],
                        expected[v["hour_start"]]["pm25_avg"],
                        rel_tol=1e-12,
                        abs_tol=1e-10,
                    )
                ):
                    raise ValueError("API row mismatch")
                seen.add(v["hour_start"])
            if seen != set(expected):
                raise ValueError("API key set mismatch")
        elif i == 1:
            rows = list(csv.DictReader(io.StringIO(resp.text)))
            expected = {
                v["date_local"]: v for v in ex["daily"] if v["location_id"] == 6123215
            }
            if len(rows) != 30 or len({v["date_local"] for v in rows}) != 30:
                raise ValueError("Daily CSV count/key mismatch")
            if any(
                not math.isclose(
                    float(v["pm25_avg"]),
                    expected[v["date_local"]]["pm25_avg"],
                    rel_tol=1e-12,
                    abs_tol=1e-10,
                )
                for v in rows
            ):
                raise ValueError("Daily API values differ")
        elif i == 2:
            if resp.json()["count"] != 168:
                raise ValueError("Seven-day filter mismatch")
        elif i == 3:
            if (
                resp.json()["total"] != 14
                or resp.json()["metadata"]["export_sha256"]
                != "d3226cf36a89bf6d543d4eb2f075da3c912a2f0ea8e12db48cd4fcb0dbc8b803"
            ):
                raise ValueError("Original current API changed")
        elif resp.json()["count"] != 3:
            raise ValueError("Original latest path changed")
    r.value["status"] = "PASS_FIVE_SIGNED_READS"
    r.save()
    return r.value


def publish_site(c, p, packet, digest):
    if (packet / "website-publication.json").exists():
        raise ValueError("Website publication already attempted")
    if (
        json.loads((packet / "history-http.json").read_text())["status"]
        != "PASS_FIVE_SIGNED_READS"
    ):
        raise ValueError("API acceptance required first")
    for name, h in p["website_source_sha256"].items():
        if sha((Path(__file__).resolve().parents[1] / name).read_bytes()) != h:
            raise ValueError("Website/verification source changed")
    pub = packet / "site-public"
    demo = packet / "site-demo"
    files = p["publication_inputs"]
    for path, h in p["publication_input_sha256"].items():
        if sha(Path(path).read_bytes()) != h:
            raise ValueError("Private prepared input changed")
    for output, mode in ((pub, "public"), (demo, "instructor")):
        build(
            output,
            packet / "cloud-export.json",
            packet / "source-audit.json",
            Path(files["metadata"]),
            Path(files["latest"]),
            Path(files["source_metadata"]),
            packet / "cloud-evidence.json",
            mode,
            athena_proof_path=packet / "acceptance.json",
        )
    child = packet / "website-release"
    release.prepare(
        child,
        pub,
        demo,
        Path(files["credentials"]),
        Path(files["previous_hosting_receipt"]),
    )
    child_plan, child_hash = release.load(child)
    if (
        child_plan["branches"]["public"]["stations"] != 1
        or child_plan["branches"]["public"]["hourly"] != 720
        or child_plan["branches"]["demo"]["stations"] != 3
        or child_plan["branches"]["demo"]["hourly"] != 2133
        or sum((child / (b + ".zip")).stat().st_size for b in ("public", "demo"))
        > 1000000
    ):
        raise ValueError("Derived website output escaped approved row/size scope")
    r = Receipt(packet / "website-publication.json", digest)
    r.value.update(
        derived_plan_sha256=child_hash,
        derivation="AWS export + two independent Athena comparisons + frozen code/license proof only",
    )
    r.save()
    result = release.execute(
        {"sts": c["sts"], "amplify": c["amplify"]}, child, child_plan, child_hash
    )
    r.value.update(
        status="DEPLOYED_EXTERNAL_BROWSER_ACCEPTANCE_REQUIRED",
        app_id=release.APP,
        deployments=result["deployments"],
    )
    r.save()
    return r.value
