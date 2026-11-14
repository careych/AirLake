"""One guarded resume of a previously verified query; no new poll/job/SELECT."""

import argparse
import base64
import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from final_demo.control import Receipt
from live_data.contract import ACCOUNT, BUCKET, NAME, REGION, sha
from live_data.json_types import normalize
from live_data.refresh import CONTROLLER, disable, query_sql, reconcile, validate_export
from live_data.store import AWSStore

ROOT = Path(__file__).resolve().parents[1]
WINDOW = "03196a080157d893aac9b4aa8cd1b7c716c4f3c1d3f5ca72994cfc599b5a04ab"
QUERY = "e476332e-79e9-4b8a-8e59-c36b68437a77"
EXPORT_KEY = "live-demo/20261114_9e2c740b/exports/cf04952bb53c9e9367a822528896c23b250ef276ba455871e28b4948b40207be/gold.json"
EXPORT_SHA = "d3226cf36a89bf6d543d4eb2f075da3c912a2f0ea8e12db48cd4fcb0dbc8b803"
DEPLOYED_SHA = "e765ae4a6efc04e73a688d979daa6f5a778865a920d8b634217cee5d813b95c4"
PREVIOUS_SHA = "fd3ae933ebb13c3ef989eee809c2aebc586b4a18007a8d6c54977a05233b99d9"


def clients():
    from live_data.deploy import clients as base

    return base()


def prepare(packet, deployed_zip):
    if packet.exists() or any(
        (p / ".git").exists() for p in (packet.resolve(), *packet.resolve().parents)
    ):
        raise ValueError("Use new private packet outside Git")
    old_bytes = Path(deployed_zip).read_bytes()
    if sha(old_bytes) != DEPLOYED_SHA:
        raise ValueError("Deployed ZIP hash mismatch")
    stream = io.BytesIO()
    with (
        zipfile.ZipFile(io.BytesIO(old_bytes)) as old,
        zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as new,
    ):
        for info in old.infolist():
            value = (
                (ROOT / "live_data/refresh.py").read_bytes()
                if info.filename == "live_data/refresh.py"
                else old.read(info.filename)
            )
            new.writestr(info, value)
        new.writestr(
            "live_data/json_types.py", (ROOT / "live_data/json_types.py").read_bytes()
        )
    plan = {
        "account": ACCOUNT,
        "region": REGION,
        "window": WINDOW,
        "query_id": QUERY,
        "source_export_sha256": EXPORT_SHA,
        "previous_sha256": PREVIOUS_SHA,
        "deployed_sha256": DEPLOYED_SHA,
        "payload_sha256": sha(stream.getvalue()),
        "controller": CONTROLLER,
        "controller_deadline_minutes": 10,
        "controller_invocations": 1,
        "http_reads": 3,
        "new_selects": 0,
        "new_glue_runs": 0,
        "new_polls": 0,
        "changed_zip_entries": ["live_data/refresh.py", "live_data/json_types.py"],
        "helper_sha256": sha(Path(__file__).read_bytes()),
    }
    packet.mkdir(mode=0o700, parents=True)
    (packet / "payload.zip").write_bytes(stream.getvalue())
    (packet / "plan.json").write_text(json.dumps(plan, sort_keys=True))
    for f in packet.iterdir():
        f.chmod(0o600)
    return sha((packet / "plan.json").read_bytes())


def load(packet):
    raw = (packet / "plan.json").read_bytes()
    p = json.loads(raw)
    if (
        p["window"] != WINDOW
        or p["query_id"] != QUERY
        or p["source_export_sha256"] != EXPORT_SHA
        or p["controller"] != CONTROLLER
        or p["account"] != ACCOUNT
        or p["region"] != REGION
        or p["new_selects"] != 0
        or p["controller_invocations"] != 1
        or p["helper_sha256"] != sha(Path(__file__).read_bytes())
        or p["payload_sha256"] != sha((packet / "payload.zip").read_bytes())
    ):
        raise ValueError("Frozen promotion plan mismatch")
    return p, sha(raw)


def preflight(c):
    if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong account")
    if (
        c["scheduler"].get_schedule(Name=NAME + "-5m", GroupName="default")["State"]
        != "DISABLED"
    ):
        raise ValueError("Schedule must stay disabled")
    for suffix in ("poller", "refresh"):
        if (
            c["lambda"].get_function_concurrency(FunctionName=NAME + "-" + suffix)[
                "ReservedConcurrentExecutions"
            ]
            != 0
        ):
            raise ValueError("Compute must start disabled")
    f = c["lambda"].get_function_configuration(FunctionName=CONTROLLER)
    if (
        f["CodeSha256"] != base64.b64encode(bytes.fromhex(DEPLOYED_SHA)).decode()
        or f["Role"] != f"arn:aws:iam::{ACCOUNT}:role/{CONTROLLER}"
    ):
        raise ValueError("Controller changed")
    store = AWSStore(c)
    run = store.read("RUN", "LEASE")
    s = store.read("OPS", WINDOW)
    if (
        run.get("attempts") != 6
        or run.get("active") is not False
        or run.get("lease_until") != 0
        or store.read("OPS", "LOCK").get("active")
    ):
        raise ValueError("Run/lock mismatch")
    if (
        s.get("phase") != "QUERY_RUNNING"
        or s.get("ticks") != 3
        or s.get("query_id") != QUERY
        or s.get("poll_in_progress") is not False
        or store.read("EXPORT", "CURRENT").get("sha256") != PREVIOUS_SHA
    ):
        raise ValueError("Exact stopped state mismatch")
    raw = (
        c["s3"]
        .get_object(Bucket=BUCKET, Key=EXPORT_KEY, ExpectedBucketOwner=ACCOUNT)["Body"]
        .read(500001)
    )
    if sha(raw) != EXPORT_SHA:
        raise ValueError("Export changed")
    v, expected = validate_export(raw, s["manifest_sha256"])
    q = c["athena"].get_query_execution(QueryExecutionId=QUERY)["QueryExecution"]
    if q["Status"]["State"] != "SUCCEEDED" or q.get("Query") != query_sql(
        v, s["manifest_sha256"]
    ):
        raise ValueError("Existing verified query differs")
    reconcile(c, QUERY, expected)
    if len(expected) != 14:
        raise ValueError("Expected fourteen facts")
    return {
        "status": "PASS_READ_ONLY_EXISTING_QUERY",
        "rows": len(expected),
        "new_selects": 0,
    }


def execute(c, packet, p, digest):
    preflight(c)
    r = Receipt(packet / "promotion-receipt.json", digest)
    try:
        f = c["lambda"].get_function_configuration(FunctionName=CONTROLLER)
        r.write(
            "update_controller_reporting_only",
            lambda: c["lambda"].update_function_code(
                FunctionName=CONTROLLER,
                RevisionId=f["RevisionId"],
                ZipFile=(packet / "payload.zip").read_bytes(),
            ),
        )
        c["lambda"].get_waiter("function_updated_v2").wait(
            FunctionName=CONTROLLER, WaiterConfig={"Delay": 2, "MaxAttempts": 45}
        )
        f = c["lambda"].get_function_configuration(FunctionName=CONTROLLER)
        if (
            f["CodeSha256"]
            != base64.b64encode(bytes.fromhex(p["payload_sha256"])).decode()
        ):
            raise ValueError("Deployed repair hash mismatch")
        env = dict(f["Environment"]["Variables"])
        env["AIRLAKE_OPS_DEADLINE"] = (
            datetime.now(UTC).replace(microsecond=0) + timedelta(minutes=10)
        ).isoformat()
        r.write(
            "bounded_deadline_only",
            lambda: c["lambda"].update_function_configuration(
                FunctionName=CONTROLLER,
                RevisionId=f["RevisionId"],
                Environment={"Variables": env},
            ),
        )
        c["lambda"].get_waiter("function_updated_v2").wait(
            FunctionName=CONTROLLER, WaiterConfig={"Delay": 2, "MaxAttempts": 45}
        )
        r.write(
            "one_controller_slot",
            lambda: c["lambda"].put_function_concurrency(
                FunctionName=CONTROLLER, ReservedConcurrentExecutions=1
            ),
        )
        response = r.write(
            "one_resume_only",
            lambda: c["lambda"].invoke(
                FunctionName=CONTROLLER, InvocationType="RequestResponse", Payload=b"{}"
            ),
        )
        value = json.loads(response["Payload"].read(100001))
        if response.get("FunctionError") or value.get("phase") != "COMPLETE":
            raise RuntimeError("Controller failed; no retry")
        pointer = AWSStore(c).read("EXPORT", "CURRENT")
        if (
            pointer.get("sha256") != EXPORT_SHA
            or pointer.get("athena_query_id") != QUERY
            or pointer.get("previous", {}).get("sha256") != PREVIOUS_SHA
        ):
            raise ValueError("Conditional promotion mismatch")
        # Three signed requests only; no SELECT is submitted by this verifier.
        from citizen.verify_private import verify

        r.value.update(
            status="PASS_PROMOTED_AND_PRIVATE_HTTP",
            pointer=normalize(pointer),
            http=verify(c, packet, digest),
        )
        r.save()
        return r.value
    finally:
        disable(c)
        final = {
            "scheduler": c["scheduler"].get_schedule(
                Name=NAME + "-5m", GroupName="default"
            )["State"],
            "concurrency": {
                s: c["lambda"].get_function_concurrency(FunctionName=NAME + "-" + s)[
                    "ReservedConcurrentExecutions"
                ]
                for s in ("poller", "refresh")
            },
            "lock_active": AWSStore(c).read("OPS", "LOCK").get("active", False),
        }
        with (packet / "final-stop.json").open("x") as f:
            json.dump(final, f, indent=2)


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("command", choices=["prepare", "preflight", "execute"])
    a.add_argument("--packet", type=Path, required=True)
    a.add_argument("--deployed-zip", type=Path)
    a.add_argument("--confirm-plan-sha256")
    x = a.parse_args()
    if x.command == "prepare":
        print(json.dumps({"plan_sha256": prepare(x.packet, x.deployed_zip)}))
        return
    p, h = load(x.packet)
    if x.command == "execute" and x.confirm_plan_sha256 != h:
        a.exit(1, "STOP: new exact-scope AWS approval required\n")
    print(
        json.dumps(
            normalize(
                preflight(clients())
                if x.command == "preflight"
                else execute(clients(), x.packet, p, h)
            )
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001 -- never expose SDK response values
        print(
            json.dumps(
                {
                    "status": "STOP_FAILED_OR_UNCERTAIN",
                    "exception_type": type(e).__name__,
                }
            )
        )
        raise SystemExit(1) from None
