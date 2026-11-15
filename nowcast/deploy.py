"""Frozen, receipted release of the live refresh, public API and website.

prepare (local)  -> plan.json + lambda.zip, printed SHA-256
preflight        -> read-only checks
execute          -> one pass, receipt per write, stop on any failure, no retry
contain          -> block the public API (reserved concurrency 0) and pause refresh
"""

import argparse
import io
import json
import re
import time
import zipfile
from pathlib import Path

from final_demo.control import Receipt, absent
from live_data.contract import ACCOUNT, BUCKET, REGION, sha
from nowcast.public_api import CURRENT_KEY, ORIGINS

ROOT = Path(__file__).resolve().parents[1]
NAME = "airlake-nowcast-20261115"
REFRESH, API, SCHEDULE = NAME + "-refresh", NAME + "-public-api", NAME + "-15m"
SCHEDULER_ROLE = NAME + "-scheduler"
POLLER = "airlake-live-20261114_9e2c740b-poller"
HISTORY_PREFIX = "historical-demo/20261115_90a13b9f/outputs/"
PACKAGES = (
    "nowcast",
    "ingestion",
    "live_data",
    "historical",
    "final_demo",
    "clean_api",
)
ZIP_TIME = (2026, 11, 15, 0, 0, 0)
LOG_RETENTION_DAYS = 14
API_CONCURRENCY = 5
API_SMOKE = [
    ("GET", "/v1", 200),
    ("GET", "/v1/now", 200),
    ("GET", "/v1/history?limit=100", 200),
    ("GET", "/v1/history?granularity=daily&format=csv&limit=100", 200),
    ("GET", "/v1/history?station_id=4946812", 404),
    ("GET", "/v1/now?x=1", 400),
    ("POST", "/v1/now", 405),
]


def lambda_zip():
    """Deterministic bundle of first-party, stdlib-only modules (boto3 is in the runtime)."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        for package in PACKAGES:
            for path in sorted((ROOT / package).rglob("*")):
                if path.suffix not in (".py", ".json") or "__pycache__" in path.parts:
                    continue
                info = zipfile.ZipInfo(str(path.relative_to(ROOT)), ZIP_TIME)
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o644 << 16
                z.writestr(info, path.read_bytes())
    return buffer.getvalue()


def trust(service):
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": service},
                    "Action": "sts:AssumeRole",
                    "Condition": {"StringEquals": {"aws:SourceAccount": ACCOUNT}},
                }
            ],
        }
    )


def logs_statement(function):
    arn = f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/aws/lambda/{function}"
    return {
        "Effect": "Allow",
        "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
        "Resource": [arn + ":*"],
    }


def policies(secret_arn, refresh_arn):
    bucket = f"arn:aws:s3:::{BUCKET}"
    return {
        REFRESH: {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject"],
                    "Resource": [f"{bucket}/nowcast/v1/*"],
                },
                {
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": bucket,
                    "Condition": {"StringLike": {"s3:prefix": ["nowcast/v1/*"]}},
                },
                {
                    "Effect": "Allow",
                    "Action": "secretsmanager:GetSecretValue",
                    "Resource": secret_arn,
                },
                logs_statement(REFRESH),
            ],
        },
        API: {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": [
                        f"{bucket}/{CURRENT_KEY}",
                        f"{bucket}/{HISTORY_PREFIX}export.json",
                        f"{bucket}/{HISTORY_PREFIX}acceptance.json",
                    ],
                },
                logs_statement(API),
            ],
        },
        SCHEDULER_ROLE: {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "lambda:InvokeFunction",
                    "Resource": refresh_arn,
                }
            ],
        },
    }


def prepare(packet, secret_arn, history_env, site_inputs):
    packet = Path(packet)
    packet.mkdir(mode=0o700)
    code = lambda_zip()
    (packet / "lambda.zip").write_bytes(code)
    refresh_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{REFRESH}"
    if not re.fullmatch(
        rf"arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret:airlake/live-demo/20261114_9e2c740b/openaq-[A-Za-z0-9]{{6}}",
        secret_arn,
    ):
        raise ValueError("Unexpected OpenAQ secret ARN")
    plan = {
        "account": ACCOUNT,
        "region": REGION,
        "bucket": BUCKET,
        "lambda_zip_sha256": sha(code),
        "lambda_zip_files": sorted(zipfile.ZipFile(io.BytesIO(code)).namelist()),
        "functions": {
            REFRESH: {
                "handler": "nowcast.refresh.handler",
                "memory_mb": 256,
                "timeout_s": 60,
                "reserved_concurrency": 1,
                "environment": {"AIRLAKE_OPENAQ_SECRET_ARN": secret_arn},
            },
            API: {
                "handler": "nowcast.public_api.handler",
                "memory_mb": 256,
                "timeout_s": 10,
                "reserved_concurrency": API_CONCURRENCY,
                "environment": history_env,
            },
        },
        "runtime": "python3.12",
        "architecture": "arm64",
        "roles": {
            REFRESH: trust("lambda.amazonaws.com"),
            API: trust("lambda.amazonaws.com"),
            SCHEDULER_ROLE: trust("scheduler.amazonaws.com"),
        },
        "policies": policies(secret_arn, refresh_arn),
        "log_retention_days": LOG_RETENTION_DAYS,
        "function_url": {
            "auth": "NONE",
            "cors": {
                "AllowOrigins": sorted(ORIGINS),
                "AllowMethods": ["GET", "HEAD"],
                "MaxAge": 86400,
            },
            "permissions": [
                "lambda:InvokeFunctionUrl (FunctionUrlAuthType NONE)",
                "lambda:InvokeFunction (InvokedViaFunctionUrl true)",
            ],
        },
        "schedule": {
            "name": SCHEDULE,
            "expression": "rate(15 minutes)",
            "retry": {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 600},
            "state_after_smoke": "ENABLED",
        },
        "alarms": {
            NAME + "-api-traffic": {
                "metric": "Invocations",
                "function": API,
                "period_s": 3600,
                "threshold": 2000,
                "actions": [],
            },
            NAME + "-api-throttles": {
                "metric": "Throttles",
                "function": API,
                "period_s": 300,
                "threshold": 0,
                "actions": [],
            },
            NAME + "-refresh-errors": {
                "metric": "Errors",
                "function": REFRESH,
                "period_s": 3600,
                "threshold": 2,
                "actions": [],
            },
        },
        "s3_prefixes": {
            "nowcast/v1/observations/{source}/{date}/{observed_at}.json": "immutable, one per source observation",
            "nowcast/v1/model/cams_global/{run_epoch}.json": "immutable raw Open-Meteo body per CAMS run (validated)",
            "nowcast/v1/projections/{published_at}.json": "immutable projection schema_version 2",
            CURRENT_KEY: "replaced pointer copy of the latest projection",
        },
        "api_smoke": API_SMOKE,
        "site": {
            "inputs_sha256": {
                k: sha(Path(v).read_bytes()) for k, v in site_inputs.items()
            },
            "inputs": site_inputs,
            "source_sha256": {
                str(p.relative_to(ROOT)): sha(p.read_bytes())
                for p in sorted((ROOT / "citizen/web").iterdir())
                if p.is_file()
            },
            "branches": ["public", "demo"],
            "demo_mode": "public",
            "failure_containment": "stop_only",
            "headers": "citizen.hosting.headers(<function URL origin>) replacing HEADERS_PREVIOUS",
            "browser_get_budget": 40,
        },
        "stop_conditions": "any exception, non-success status, smoke mismatch, unlicensed station id, or plan/source drift stops without retry or cleanup",
        "rollback": [
            "contain: put_function_concurrency(public API, 0); update_schedule(state DISABLED)",
            "Amplify: redeploy job 2 zips and update_app(customHeaders=HEADERS_PREVIOUS)",
            "remove: delete_function_url_config, delete_schedule (only on owner instruction)",
        ],
    }
    raw = json.dumps(plan, indent=1, sort_keys=True).encode()
    (packet / "plan.json").write_bytes(raw)
    return sha(raw)


def load(packet, confirm):
    raw = (Path(packet) / "plan.json").read_bytes()
    plan = json.loads(raw)
    if sha(raw) != confirm:
        raise ValueError("STOP: plan SHA-256 not approved")
    if (
        sha((Path(packet) / "lambda.zip").read_bytes()) != plan["lambda_zip_sha256"]
        or lambda_zip() != (Path(packet) / "lambda.zip").read_bytes()
    ):
        raise ValueError("Lambda source drifted from the frozen plan")
    for name, h in plan["site"]["source_sha256"].items():
        if sha((ROOT / name).read_bytes()) != h:
            raise ValueError("Website source drifted from the frozen plan")
    for k, h in plan["site"]["inputs_sha256"].items():
        if sha(Path(plan["site"]["inputs"][k]).read_bytes()) != h:
            raise ValueError("Website input drifted from the frozen plan")
    return plan


def clients():
    import boto3
    from botocore.config import Config

    s = boto3.Session(profile_name="airlake", region_name=REGION)
    cfg = Config(retries={"total_max_attempts": 1}, connect_timeout=5, read_timeout=70)
    return {
        n: s.client(n, config=cfg)
        for n in (
            "sts",
            "iam",
            "lambda",
            "logs",
            "scheduler",
            "cloudwatch",
            "s3",
            "amplify",
        )
    }


def preflight(c, plan):
    if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong AWS account")
    for role in plan["roles"]:
        absent(lambda r=role: c["iam"].get_role(RoleName=r), {"NoSuchEntity"})
    for fn in plan["functions"]:
        absent(
            lambda f=fn: c["lambda"].get_function(FunctionName=f),
            {"ResourceNotFoundException"},
        )
    absent(
        lambda: c["scheduler"].get_schedule(Name=SCHEDULE),
        {"ResourceNotFoundException"},
    )
    if (
        c["s3"]
        .list_objects_v2(
            Bucket=BUCKET, Prefix="nowcast/", MaxKeys=1, ExpectedBucketOwner=ACCOUNT
        )
        .get("KeyCount")
    ):
        raise ValueError("nowcast/ prefix is not empty")
    poller = c["lambda"].get_function_configuration(FunctionName=POLLER)
    if (
        poller["Environment"]["Variables"].get("OPENAQ_SECRET_ARN")
        != plan["functions"][REFRESH]["environment"]["AIRLAKE_OPENAQ_SECRET_ARN"]
    ):
        raise ValueError("OpenAQ secret ARN differs from the existing poller")
    from citizen import release
    from citizen.hosting import HEADERS_PREVIOUS

    app = c["amplify"].get_app(appId=release.APP)["app"]
    if not release.security_headers_match(app.get("customHeaders"), HEADERS_PREVIOUS):
        raise ValueError("Amplify headers are not the recorded previous headers")
    return {"status": "PASS_READ_ONLY", "account": ACCOUNT}


def execute(c, packet, plan, digest, get=None):
    packet = Path(packet)
    preflight(c, plan)
    r = Receipt(packet / "receipt.json", digest)
    code = (packet / "lambda.zip").read_bytes()
    for fn in plan["functions"]:
        r.write(
            "create_log_group_" + fn,
            lambda f=fn: c["logs"].create_log_group(logGroupName="/aws/lambda/" + f),
        )
        r.write(
            "retention_" + fn,
            lambda f=fn: c["logs"].put_retention_policy(
                logGroupName="/aws/lambda/" + f,
                retentionInDays=plan["log_retention_days"],
            ),
        )
    arns = {}
    for role, doc in plan["roles"].items():
        arns[role] = r.write(
            "create_role_" + role,
            lambda n=role, d=doc: c["iam"].create_role(
                RoleName=n, AssumeRolePolicyDocument=d, MaxSessionDuration=3600
            ),
        )["Role"]["Arn"]
        r.write(
            "put_role_policy_" + role,
            lambda n=role: c["iam"].put_role_policy(
                RoleName=n,
                PolicyName="Scoped",
                PolicyDocument=json.dumps(plan["policies"][n]),
            ),
        )
    time.sleep(30)  # IAM propagation before Lambda/Scheduler assume the roles
    functions = {}
    for fn, spec in plan["functions"].items():
        functions[fn] = r.write(
            "create_function_" + fn,
            lambda f=fn, s=spec: c["lambda"].create_function(
                FunctionName=f,
                Runtime=plan["runtime"],
                Architectures=[plan["architecture"]],
                Role=arns[f],
                Handler=s["handler"],
                Code={"ZipFile": code},
                MemorySize=s["memory_mb"],
                Timeout=s["timeout_s"],
                Environment={"Variables": s["environment"]},
                LoggingConfig={"LogFormat": "Text", "LogGroup": "/aws/lambda/" + f},
                Publish=False,
            ),
        )["FunctionArn"]
        c["lambda"].get_waiter("function_active_v2").wait(
            FunctionName=fn, WaiterConfig={"Delay": 2, "MaxAttempts": 30}
        )
        r.write(
            "concurrency_" + fn,
            lambda f=fn, s=spec: c["lambda"].put_function_concurrency(
                FunctionName=f, ReservedConcurrentExecutions=s["reserved_concurrency"]
            ),
        )
    first = r.write(
        "invoke_refresh_once",
        lambda: c["lambda"].invoke(
            FunctionName=REFRESH, InvocationType="RequestResponse"
        ),
    )
    result = json.loads(first["Payload"].read())
    if first.get("FunctionError") or result.get("status") != "PUBLISHED":
        raise ValueError("First refresh did not publish")
    r.value["first_refresh"] = result
    r.save()
    url = r.write(
        "create_function_url",
        lambda: c["lambda"].create_function_url_config(
            FunctionName=API, AuthType="NONE", Cors=plan["function_url"]["cors"]
        ),
    )["FunctionUrl"]
    if not re.fullmatch(
        r"https://[a-z0-9]+\.lambda-url\.ap-southeast-2\.on\.aws/", url
    ):
        raise ValueError("Unexpected function URL")
    r.write(
        "permit_url",
        lambda: c["lambda"].add_permission(
            FunctionName=API,
            StatementId="PublicUrl",
            Action="lambda:InvokeFunctionUrl",
            Principal="*",
            FunctionUrlAuthType="NONE",
        ),
    )
    r.write(
        "permit_url_invoke",
        lambda: c["lambda"].add_permission(
            FunctionName=API,
            StatementId="PublicUrlInvoke",
            Action="lambda:InvokeFunction",
            Principal="*",
            InvokedViaFunctionUrl=True,
        ),
    )
    r.value["function_url"] = url
    r.save()
    smoke(r, url.rstrip("/"), get)
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
    s = plan["schedule"]
    r.write(
        "create_schedule",
        lambda: c["scheduler"].create_schedule(
            Name=SCHEDULE,
            ScheduleExpression=s["expression"],
            State=s["state_after_smoke"],
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": functions[REFRESH],
                "RoleArn": arns[SCHEDULER_ROLE],
                "RetryPolicy": s["retry"],
            },
        ),
    )
    r.value.update(status="BACKEND_LIVE_SITE_RELEASE_NEXT")
    r.save()
    return r.value


def smoke(r, base, get=None):
    if get is None:
        import requests

        http = requests.Session()
        http.trust_env = False

        def get(method, url):
            return http.request(method, url, timeout=15, allow_redirects=False)

    r.value["smoke"] = []
    for method, path, expected in API_SMOKE:
        item = {"method": method, "path": path, "status": "UNCERTAIN"}
        r.value["smoke"].append(item)
        r.save()
        resp = get(method, base + path)
        item["status"] = resp.status_code
        r.save()
        if resp.status_code != expected:
            raise ValueError("Public API smoke mismatch; stop")
        if path == "/v1/now":
            body = resp.json()
            if body["schema_version"] != 2 or {
                o["station_id"] for o in body["observations"]
            } - {6123215, 99385}:
                raise ValueError("Projection contains unexpected stations")
        if path == "/v1/history?limit=100" and {
            d.get("location_id") for d in resp.json()["data"]
        } != {6123215}:
            raise ValueError("Public history leaked another station")


def release_site(c, packet, plan, digest):
    """Build both bundles against the live API and deploy them once each."""
    from urllib.parse import urlsplit

    from citizen import release
    from citizen.hosting import headers
    from historical.build import build

    packet = Path(packet)
    backend = json.loads((packet / "receipt.json").read_text())
    if (
        backend["status"] != "BACKEND_LIVE_SITE_RELEASE_NEXT"
        or backend["plan_sha256"] != digest
    ):
        raise ValueError("Backend release must succeed first")
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


def contain(c):
    """Owner-invoked containment: block public traffic, pause refresh; no deletion."""
    c["lambda"].put_function_concurrency(
        FunctionName=API, ReservedConcurrentExecutions=0
    )
    current = c["scheduler"].get_schedule(Name=SCHEDULE)
    c["scheduler"].update_schedule(
        Name=SCHEDULE,
        ScheduleExpression=current["ScheduleExpression"],
        FlexibleTimeWindow=current["FlexibleTimeWindow"],
        Target=current["Target"],
        State="DISABLED",
    )
    return {"status": "CONTAINED", "public_api_concurrency": 0, "schedule": "DISABLED"}


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument(
        "command",
        choices=("prepare", "preflight", "execute", "release-site", "contain"),
    )
    a.add_argument("--packet", type=Path, required=True)
    a.add_argument("--confirm-plan-sha256")
    a.add_argument("--secret-arn")
    a.add_argument("--history-env", type=Path)
    a.add_argument("--site-inputs", type=Path)
    x = a.parse_args()
    if x.command == "prepare":
        print(
            json.dumps(
                {
                    "plan_sha256": prepare(
                        x.packet,
                        x.secret_arn,
                        json.loads(x.history_env.read_text()),
                        json.loads(x.site_inputs.read_text()),
                    )
                }
            )
        )
        return
    plan = load(x.packet, x.confirm_plan_sha256)
    c = clients()
    action = {
        "preflight": lambda: preflight(c, plan),
        "execute": lambda: execute(c, x.packet, plan, x.confirm_plan_sha256),
        "release-site": lambda: release_site(c, x.packet, plan, x.confirm_plan_sha256),
        "contain": lambda: contain(c),
    }
    print(json.dumps(action[x.command]()))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001 -- never print SDK payloads, URLs with tokens or secrets
        print(
            json.dumps(
                {
                    "status": "STOP_FAILED_OR_UNCERTAIN",
                    "exception_type": type(e).__name__,
                }
            )
        )
        raise SystemExit(1) from None
