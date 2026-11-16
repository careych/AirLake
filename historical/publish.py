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
POLICY = "HistoricalResearch20261115"  # run 1's HistoricalResearch20261114 is kept


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
    variables = f.get("Environment", {}).get("Variables", {})
    history = {k: v for k, v in variables.items() if k.startswith("AIRLAKE_HISTORY_")}
    superseded = p.get("superseded_api") or {}

    def code(hex_digest):
        return base64.b64encode(bytes.fromhex(hex_digest)).decode()

    # Either the original API, or exactly run 1's unaccepted history revision.
    original = f["CodeSha256"] == code(p["previous_api_sha256"]) and not history
    replaced = bool(superseded) and (
        f["CodeSha256"] == code(superseded["code_sha256"])
        and history.get("AIRLAKE_HISTORY_EXPORT_KEY") == superseded["export_key"]
    )
    if (
        not (original or replaced)
        or f["Role"] != f"arn:aws:iam::{ACCOUNT}:role/{API}"
        or f.get("Runtime") != "python3.12"
        or f.get("Handler") != "live_data.api.handler"
        or f.get("State") != "Active"
        or f.get("LastUpdateStatus") != "Successful"
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


RESUME = "history-http-resume.json"
AMENDMENT = "amendment-1.json"
# Exact state left by the first attempt: SigV4 signed the raw ':' in start=,
# Lambda URLs canonicalize it as %3A, so the third read was refused (403).
FIRST_ATTEMPT = (200, 200, 403)


def signed_url(target):
    """Percent-encode the query once so the signed and sent bytes are identical."""
    from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

    u = urlsplit(target)
    query = urlencode(
        parse_qsl(u.query, keep_blank_values=True, strict_parsing=bool(u.query)),
        quote_via=quote,
        safe="-_.~",
    )
    return urlunsplit((u.scheme, u.netloc, u.path, query, ""))


def smoke_paths(ex):
    from datetime import timedelta

    from ingestion.openaq import stamp, utc

    start = stamp(utc(ex["provenance"]["window_end_utc"]) - timedelta(days=7))
    return [
        "/api/v1/history?station_id=6123215&limit=1000",
        "/api/v1/history?station_id=6123215&granularity=daily&format=csv&limit=100",
        "/api/v1/history?station_id=6123215&limit=1000&start=" + start,
        "/api/v1/current/hourly?limit=100",
        "/api/v1/latest",
    ]


def check_response(i, resp, ex, proof):
    import csv
    import math

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


def api_url(c):
    from urllib.parse import urlsplit

    url = c["lambda"].get_function_url_config(FunctionName=API)
    host = urlsplit(url["FunctionUrl"])
    if (
        url["AuthType"] != "AWS_IAM"
        or host.scheme != "https"
        or not host.hostname.endswith(".lambda-url.ap-southeast-2.on.aws")
    ):
        raise ValueError("API URL scope changed")
    return url["FunctionUrl"]


def signed_getter(function_url):
    import boto3
    import requests
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    session = boto3.Session(profile_name="airlake", region_name=REGION)
    http = requests.Session()
    http.trust_env = False

    def get(path):
        target = signed_url(function_url.rstrip("/") + path)
        r = AWSRequest(method="GET", url=target)
        SigV4Auth(
            session.get_credentials().get_frozen_credentials(), "lambda", REGION
        ).add_auth(r)
        return http.get(
            target, headers=dict(r.headers), timeout=25, allow_redirects=False
        )

    return get


def run_reads(r, indexes, paths, get, ex, proof):
    r.value["requests"] = []
    for i in indexes:
        item = {"path": paths[i], "status": "UNCERTAIN"}
        r.value["requests"].append(item)
        r.save()
        resp = get(paths[i])
        item["status"] = resp.status_code
        r.save()
        check_response(i, resp, ex, proof)


def smoke_api(c, p, packet, digest, get=None):
    if (packet / "history-http.json").exists():
        raise ValueError("HTTP budget already attempted")
    ex = json.loads((packet / "cloud-export.json").read_text())
    proof = json.loads((packet / "acceptance.json").read_text())
    function_url = api_url(c)
    get = get or signed_getter(function_url)
    r = Receipt(packet / "history-http.json", digest)
    run_reads(r, range(5), smoke_paths(ex), get, ex, proof)
    r.value["status"] = "PASS_FIVE_SIGNED_READS"
    r.save()
    return r.value


def prepare_amendment(packet, digest):
    """Freeze the bounded recovery after the first attempt's signing failure."""
    if (packet / AMENDMENT).exists():
        raise ValueError("Amendment already frozen")
    first = (packet / "history-http.json").read_bytes()
    first_attempt_matches(
        json.loads(first), json.loads((packet / "cloud-export.json").read_text())
    )
    root = Path(__file__).resolve().parents[1]
    value = {
        "base_plan_sha256": digest,
        "reason": "SigV4 signed raw ':' in the start= query; Lambda URL expected %3A (HTTP 403)",
        "first_attempt_receipt_sha256": sha(first),
        "api_publication_receipt_sha256": sha(
            (packet / "api-publication.json").read_bytes()
        ),
        "remaining_signed_gets": 3,
        "remaining_paths": smoke_paths(
            json.loads((packet / "cloud-export.json").read_text())
        )[2:],
        "website_source_sha256_overrides": {
            name: sha((root / name).read_bytes()) for name in ("historical/publish.py",)
        },
        "unchanged": "Amplify phase: one deployment per branch, 40 GETs, Dragon-only, stop_only",
    }
    raw = json.dumps(value, indent=2, sort_keys=True).encode() + b"\n"
    (packet / AMENDMENT).write_bytes(raw)
    return sha(raw)


def load_amendment(packet, digest, confirm):
    path = packet / AMENDMENT
    if not path.exists():
        return None
    raw = path.read_bytes()
    a = json.loads(raw)
    if sha(raw) != confirm or a["base_plan_sha256"] != digest:
        raise ValueError("STOP: amendment approval required")
    if (
        sha((packet / "history-http.json").read_bytes())
        != a["first_attempt_receipt_sha256"]
        or sha((packet / "api-publication.json").read_bytes())
        != a["api_publication_receipt_sha256"]
    ):
        raise ValueError("Recorded attempt changed after amendment")
    return a


def first_attempt_matches(first, ex):
    paths = smoke_paths(ex)
    if first.get("status") != "RUNNING" or [
        (v["path"], v["status"]) for v in first.get("requests", [])
    ] != list(zip(paths, FIRST_ATTEMPT, strict=False)):
        raise ValueError("First attempt is not the recorded signing failure")


def smoke_api_resume(c, p, packet, digest, amendment, get=None):
    if amendment is None:
        raise ValueError("STOP: amendment approval required")
    if (packet / RESUME).exists():
        raise ValueError("Resume budget already attempted")
    ex = json.loads((packet / "cloud-export.json").read_text())
    proof = json.loads((packet / "acceptance.json").read_text())
    first_attempt_matches(json.loads((packet / "history-http.json").read_text()), ex)
    paths = smoke_paths(ex)
    if paths[2:] != amendment["remaining_paths"]:
        raise ValueError("Resume paths drifted from amendment")
    deployed = json.loads((packet / "api-publication.json").read_text())
    f = c["lambda"].get_function_configuration(FunctionName=API)
    if (
        f["CodeSha256"] != deployed["operations"][-1]["identifiers"]["CodeSha256"]
        or f["Environment"]["Variables"].get("AIRLAKE_HISTORY_EXPORT_SHA256")
        != proof["export_sha256"]
    ):
        raise ValueError("Private API changed since publication")
    function_url = api_url(c)
    get = get or signed_getter(function_url)
    r = Receipt(packet / RESUME, digest)
    r.value["amendment_sha256"] = sha((packet / AMENDMENT).read_bytes())
    run_reads(r, range(2, 5), paths, get, ex, proof)
    r.value["status"] = "PASS_REMAINING_THREE_SIGNED_READS"
    r.save()
    return r.value


def api_accepted(packet, amendment):
    if (
        json.loads((packet / "history-http.json").read_text())["status"]
        == "PASS_FIVE_SIGNED_READS"
    ):
        return True
    return (
        amendment is not None
        and (packet / RESUME).exists()
        and json.loads((packet / RESUME).read_text())["status"]
        == "PASS_REMAINING_THREE_SIGNED_READS"
    )


def publish_site(c, p, packet, digest, amendment=None):
    if (packet / "website-publication.json").exists():
        raise ValueError("Website publication already attempted")
    if not api_accepted(packet, amendment):
        raise ValueError("API acceptance required first")
    pinned = dict(p["website_source_sha256"])
    pinned.update((amendment or {}).get("website_source_sha256_overrides", {}))
    for name, h in pinned.items():
        if sha((Path(__file__).resolve().parents[1] / name).read_bytes()) != h:
            raise ValueError("Website/verification source changed")
    pub = packet / "site-public"
    demo = packet / "site-demo"
    files = p["publication_inputs"]
    for path, h in p["publication_input_sha256"].items():
        if sha(Path(path).read_bytes()) != h:
            raise ValueError("Private prepared input changed")
    # The protected demo stays Dragon-only until the
    # two network stations' display/redistribution rights are verified.
    for output, mode in ((pub, "public"), (demo, p["demo_mode"])):
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
        demo_mode=p["demo_mode"],
        failure_containment="stop_only",
    )
    child_plan, child_hash = release.load(child)
    if (
        child_plan["branches"]["public"]["stations"] != 1
        or child_plan["branches"]["public"]["hourly"] != 720
        or child_plan["demo_mode"] != "public"
        or child_plan["failure_containment"] != "stop_only"
        or child_plan["branches"]["demo"]["stations"] != 1
        or child_plan["branches"]["demo"]["hourly"] != 720
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
