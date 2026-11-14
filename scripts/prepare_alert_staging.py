"""Prepare private AWS CLI deployment files; never executes AWS commands."""

import argparse
import hashlib
import inspect
import json
import os
import re
import shlex
import sys
from pathlib import Path

from alerts.delivery import chat_id
from alerts.runtime import ROOT, load_plan
from scripts.verify_telegram_staging_identity import load_identity


def _run_control(
    mode, directory, profile, artifact_hash, *, call=None, clock=None, sleep=None
):
    """Embedded in generated scripts; mutations are single attempts, polls are bounded."""
    import base64
    import json
    import os
    import re
    import shlex
    import subprocess
    import tempfile
    import time
    from pathlib import Path

    directory = Path(directory)
    scope = "airlake-alert-staging-v1"
    account, region = "701364614078", "ap-southeast-2"
    function_arn = f"arn:aws:lambda:{region}:{account}:function:{scope}"
    expected = json.loads((directory / "function.json").read_text())["Environment"][
        "Variables"
    ]
    mapping_id = (directory / "mapping-uuid.txt").read_text().strip()
    disabled_version = (directory / "disabled-version.txt").read_text().strip()
    if (
        mode not in {"enable", "disable"}
        or not re.fullmatch(r"[A-Za-z0-9_-]+", profile)
        or not re.fullmatch(r"[0-9a-fA-F-]{36}", mapping_id)
        or not re.fullmatch(r"[1-9][0-9]*", disabled_version)
        or not re.fullmatch(r"[0-9a-f]{64}", artifact_hash)
        or any(key.startswith("AWS_ENDPOINT_URL") for key in os.environ)
    ):
        raise ValueError("Invalid staging control inputs")
    code_hash = base64.b64encode(bytes.fromhex(artifact_hash)).decode()
    clock, sleep = clock or time.monotonic, sleep or time.sleep
    context = {"phase": "validate", "operation": "local inputs"}
    control_started = clock()

    class ControlFailure(RuntimeError):
        def __init__(
            self, message, *, phase, operation, exit_code, elapsed_ms
        ):
            super().__init__(message)
            self.phase = phase
            self.operation = operation
            self.exit_code = exit_code
            self.elapsed_ms = elapsed_ms

    def diagnostic(exc):
        if isinstance(exc, ControlFailure):
            return (
                f"phase={exc.phase}, operation={exc.operation}, "
                f"exit={exc.exit_code}, elapsed_ms={exc.elapsed_ms}"
            )
        return f"phase={context['phase']}, operation=state-check, detail={exc}"

    def state_failure(message):
        return ControlFailure(
            message,
            phase=context["phase"],
            operation=context["operation"],
            exit_code="state-check",
            elapsed_ms=round((clock() - control_started) * 1000),
        )

    if call is None:

        def raw_call(operation, **parameters):
            # CLI JSON is stored in a private 0600 file, never in process arguments.
            operation_started = clock()
            fd, name = tempfile.mkstemp(prefix=".aws-cli-input-", dir=directory)
            input_path = Path(name)
            try:
                os.fchmod(fd, 0o600)
                with os.fdopen(fd, "w", encoding="utf-8") as stream:
                    json.dump(parameters, stream, separators=(",", ":"))
                args = shlex.split(f"aws --profile {profile} --region {region}") + [
                    "--no-cli-pager",
                    "--output",
                    "json",
                    "--cli-connect-timeout",
                    "5",
                    "--cli-read-timeout",
                    "10",
                    "lambda",
                    operation,
                    "--cli-input-json",
                    input_path.as_uri(),
                ]
                result = subprocess.run(
                    args,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=20,
                    env=dict(
                        os.environ,
                        AWS_MAX_ATTEMPTS="1",
                        AWS_RETRY_MODE="standard",
                        AWS_PAGER="",
                    ),
                )
                if result.returncode:
                    raise ControlFailure(
                        "AWS operation failed",
                        phase=context["phase"],
                        operation=operation,
                        exit_code=result.returncode,
                        elapsed_ms=round((clock() - operation_started) * 1000),
                    )
                return json.loads(result.stdout)
            finally:
                input_path.unlink(missing_ok=True)

        call = raw_call

    raw_call = call

    def call(operation, **parameters):
        context["operation"] = operation
        operation_started = clock()
        try:
            return raw_call(operation, **parameters)
        except ControlFailure:
            raise
        except subprocess.TimeoutExpired as exc:
            raise ControlFailure(
                "AWS operation timed out",
                phase=context["phase"],
                operation=operation,
                exit_code="timeout",
                elapsed_ms=round((clock() - operation_started) * 1000),
            ) from exc
        except Exception as exc:  # noqa: BLE001 -- report type only; do not leak SDK text
            code = getattr(exc, "returncode", None)
            raise ControlFailure(
                "AWS operation failed",
                phase=context["phase"],
                operation=operation,
                exit_code=code if isinstance(code, int) else "unknown",
                elapsed_ms=round((clock() - operation_started) * 1000),
            ) from None

    def config(qualifier=None):
        args = {"FunctionName": scope}
        if qualifier is not None:
            args["Qualifier"] = qualifier
        result = call("get-function-configuration", **args)
        if result.get("FunctionArn") not in {
            function_arn,
            function_arn + ":" + str(qualifier),
        }:
            raise state_failure("Unexpected staging function scope")
        return result

    def ready():
        deadline = clock() + 300
        while clock() < deadline:
            value = config()
            if clock() >= deadline:
                raise state_failure("Lambda configuration wait timed out")
            if (
                value.get("State") == "Active"
                and value.get("LastUpdateStatus") == "Successful"
            ):
                return value
            if value.get("State") not in {"Active", "Pending"} or value.get(
                "LastUpdateStatus"
            ) not in {"InProgress", "Successful"}:
                raise state_failure("Unexpected Lambda configuration state")
            sleep(5)
        raise state_failure("Lambda configuration wait timed out")

    def mapping():
        deadline, previous = clock() + 300, None
        while clock() < deadline:
            value = call("get-event-source-mapping", UUID=mapping_id)
            if clock() >= deadline:
                raise state_failure("Mapping wait timed out")
            if (
                value.get("UUID") != mapping_id
                or value.get("FunctionArn") != function_arn + ":staging"
                or value.get("EventSourceArn")
                != f"arn:aws:sqs:{region}:{account}:{scope}"
            ):
                raise state_failure("Unexpected staging mapping scope")
            state = value.get("State")
            if state in {"Enabled", "Disabled"}:
                if state == previous:
                    return value
                previous = state
            elif state in {"Enabling", "Disabling", "Updating"}:
                previous = None
            else:
                raise state_failure("Unexpected mapping state")
            sleep(5)
        raise state_failure("Mapping wait timed out")

    def alias():
        value = call("get-alias", FunctionName=scope, Name="staging")
        if value.get("AliasArn") != function_arn + ":staging":
            raise state_failure("Unexpected staging alias scope")
        return value

    def delivery(value, flag):
        environment = value.get("Environment", {})
        variables = environment.get("Variables")
        if (
            environment.get("Error")
            or not isinstance(variables, dict)
            or any(
                not isinstance(k, str) or not isinstance(v, str)
                for k, v in variables.items()
            )
        ):
            raise state_failure("Existing environment cannot be safely preserved")
        if variables.get("AIRLAKE_ALERT_DELIVERY_ENABLED") != flag:
            merged = dict(variables, AIRLAKE_ALERT_DELIVERY_ENABLED=flag)
            call(
                "update-function-configuration",
                FunctionName=scope,
                RevisionId=value["RevisionId"],
                Environment={"Variables": merged},
            )
            value = ready()
        if (
            value.get("Environment", {})
            .get("Variables", {})
            .get("AIRLAKE_ALERT_DELIVERY_ENABLED")
            != flag
        ):
            raise state_failure("Delivery flag did not converge")
        return value

    if mode == "disable":
        errors = []
        # Each recovery step runs even if another step fails; never retry mutations.
        context["phase"] = "restore_alias"
        try:
            target = config(disabled_version)
            if (
                target.get("CodeSha256") != code_hash
                or target.get("Environment", {})
                .get("Variables", {})
                .get("AIRLAKE_ALERT_DELIVERY_ENABLED")
                != "0"
            ):
                raise state_failure("Recorded disabled version is not verified disabled")
            current = alias()
            if current.get("FunctionVersion") != disabled_version:
                call(
                    "update-alias",
                    FunctionName=scope,
                    Name="staging",
                    FunctionVersion=disabled_version,
                    RevisionId=current["RevisionId"],
                )
            if alias().get("FunctionVersion") != disabled_version:
                raise state_failure("Disabled alias restoration did not converge")
        except Exception as exc:  # noqa: BLE001 -- independent recovery
            errors.append("alias: " + diagnostic(exc))
        context["phase"] = "restore_delivery_flag"
        try:
            delivery(ready(), "0")
        except Exception as exc:  # noqa: BLE001 -- continue mapping recovery
            errors.append("configuration: " + diagnostic(exc))
        context["phase"] = "disable_mapping"
        try:
            if mapping()["State"] == "Enabled":
                call("update-event-source-mapping", UUID=mapping_id, Enabled=False)
                if mapping()["State"] != "Disabled":
                    raise state_failure("Mapping did not disable")
        except Exception as exc:  # noqa: BLE001 -- prior recovery steps were attempted
            errors.append("mapping: " + diagnostic(exc))
        if errors:
            raise state_failure("Staging recovery incomplete: " + ", ".join(errors))
        return "PASS: staging alias, delivery and mapping are disabled"

    context["phase"] = "validate_mapping"
    state = mapping()
    required = {
        "AIRLAKE_ACCOUNT_ID": account,
        "AIRLAKE_ALERT_STAGING_NAME": scope,
        "AIRLAKE_ALERT_TEST_MODE": "1",
        "AIRLAKE_PARTIAL_BATCH_FAILURES": "1",
        "AIRLAKE_ALERT_TOPIC_ARN": f"arn:aws:sns:{region}:{account}:{scope}",
        "AIRLAKE_ALERT_QUEUE_ARN": f"arn:aws:sqs:{region}:{account}:{scope}",
        "AIRLAKE_SUBSCRIPTION_TABLE": scope + "-subscriptions",
        "AIRLAKE_DELIVERY_TABLE": scope + "-ledger",
    }
    if any(expected.get(key) != value for key, value in required.items()):
        raise state_failure("Reviewed staging environment safeguards differ")
    if (
        state.get("BatchSize") != 1
        or state.get("MaximumBatchingWindowInSeconds") != 0
        or state.get("FunctionResponseTypes") != ["ReportBatchItemFailures"]
        or state.get("ScalingConfig", {}).get("MaximumConcurrency") != 2
    ):
        raise state_failure("Staging mapping safeguards differ")
    context["phase"] = "validate_alias_and_versions"
    current = alias()
    selected = config(current["FunctionVersion"])
    latest = ready()
    for value in (selected, latest):
        variables = value.get("Environment", {}).get("Variables", {})
        if (
            value.get("CodeSha256") != code_hash
            or value.get("Role") != f"arn:aws:iam::{account}:role/{scope}-consumer"
            or any(
                variables.get(key) != wanted
                for key, wanted in expected.items()
                if key != "AIRLAKE_ALERT_DELIVERY_ENABLED"
            )
            or any(
                key in variables
                for key in ("TELEGRAM_TOKEN", "TELEGRAM_WEBHOOK_SECRET")
            )
        ):
            raise state_failure("Staging code or environment safeguards differ")
    if state["State"] == "Enabled":
        if (
            selected["Environment"]["Variables"].get("AIRLAKE_ALERT_DELIVERY_ENABLED")
            != "1"
        ):
            raise state_failure("Enabled mapping has a disabled alias")
        return "PASS: staging mapping is stably Enabled"
    context["phase"] = "enable_latest_delivery"
    latest = delivery(latest, "1")
    if (
        selected["Environment"]["Variables"].get("AIRLAKE_ALERT_DELIVERY_ENABLED")
        != "1"
    ):
        context["phase"] = "publish_enabled_version"
        version = call("publish-version", FunctionName=scope, CodeSha256=code_hash)[
            "Version"
        ]
        if not re.fullmatch(r"[1-9][0-9]*", str(version)):
            raise state_failure("Invalid published version")
        receipt = directory / ("enabled-version-" + str(version) + ".txt")
        with receipt.open("x") as stream:
            receipt.chmod(0o600)
            stream.write(str(version) + "\n")
        context["phase"] = "move_alias_to_enabled_version"
        call(
            "update-alias",
            FunctionName=scope,
            Name="staging",
            FunctionVersion=str(version),
            RevisionId=current["RevisionId"],
        )
    context["phase"] = "enable_mapping"
    call("update-event-source-mapping", UUID=mapping_id, Enabled=True)
    if mapping()["State"] != "Enabled":
        raise state_failure("Mapping did not enable")
    return "PASS: staging mapping is stably Enabled"

def _control_script(
    mode, output, digest, artifact_hash, identity_check=(), profile="airlake"
):
    q = shlex.quote
    lines = [
        "#!/bin/sh",
        "set -eu",
        "umask 077",
        'export AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard AWS_PAGER=""',
        f'test "${{AIRLAKE_APPROVED_PLAN_SHA256:-}}" = {q(digest)}',
        f"cd {q(str(ROOT))}",
        *identity_check,
        f"{q(sys.executable)} -B - <<'AIRLAKE_STAGING_CONTROL'",
        inspect.getsource(_run_control),
        "try:",
        f"    print(_run_control({mode!r}, {str(output)!r}, {profile!r}, {artifact_hash!r}))",
        "except Exception as exc:",
        "    if all(hasattr(exc, key) for key in ('phase', 'operation', 'exit_code', 'elapsed_ms')):",
        "        print(f'FAIL: staging control phase={exc.phase} operation={exc.operation} exit={exc.exit_code} elapsed_ms={exc.elapsed_ms} detail={exc}; no automatic mutation retry.')",
        "    else:",
        "        print(f'FAIL: staging control failed: {type(exc).__name__}: {exc}; no automatic mutation retry.')",
        "    raise SystemExit(1) from None",
        "AIRLAKE_STAGING_CONTROL",
    ]
    return "\n".join(lines) + "\n"


def repair_control_scripts(directory, *, profile, identity_file):
    """Write new fixed scripts only; never overwrite deployed files or call AWS."""
    directory = directory.resolve(strict=True)
    if (
        directory == ROOT
        or ROOT in directory.parents
        or directory.stat().st_uid != os.getuid()
        or directory.stat().st_mode & 0o777 != 0o700
        or any((parent / ".git").exists() for parent in (directory, *directory.parents))
    ):
        raise ValueError("An owner-private external deployment directory is required")
    plan, digest = load_plan(ROOT / "docs/plans/athena-alert-staging.json")
    receipt = json.loads((directory / "preparation.json").read_text())
    function = json.loads((directory / "function.json").read_text())
    if (
        receipt["plan_sha256"] != digest
        or receipt["inventory"] != plan["names"]
        or function["FunctionName"] != plan["names"]["lambda"]
        or not re.fullmatch(r"[A-Za-z0-9_-]+", profile)
        or not re.fullmatch(r"[0-9a-f]{64}", receipt["artifact_sha256"])
    ):
        raise ValueError("Only the existing reviewed staging deployment is supported")
    identity_file = identity_file.resolve(strict=True)
    check = [
        f"{shlex.quote(sys.executable)} -B -c "
        + shlex.quote(
            "import json,sys; from pathlib import Path; from scripts.verify_telegram_staging_identity import load_identity; "
            "i=load_identity(Path(sys.argv[1])); e=json.load(open(sys.argv[2]))['Environment']['Variables']; "
            "sys.exit(0 if i['bot_id']==e['AIRLAKE_ALERT_TEST_BOT_ID'] and i['chat_id']==e['AIRLAKE_ALERT_TEST_RECIPIENT'] else 'Private identity/configuration mismatch')"
        )
        + " "
        + shlex.quote(str(identity_file))
        + " "
        + shlex.quote(str(directory / "function.json"))
    ]
    targets = [directory / "enable-test-fixed.sh", directory / "disable-test-fixed.sh"]
    if any(path.exists() or path.is_symlink() for path in targets):
        raise FileExistsError("Fixed scripts already exist; preserve and review them")
    for mode, target in zip(("enable", "disable"), targets, strict=True):
        text = _control_script(
            mode,
            directory,
            digest,
            receipt["artifact_sha256"],
            check if mode == "enable" else (),
            profile,
        )
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(text)
    return "PASS: fixed private scripts prepared only; no AWS operations executed"


def prepare(
    plan_path,
    artifact,
    manifest_path,
    secret_arn,
    recipient_file,
    principal,
    output,
    *,
    test_bot_id=None,
    identity_file=None,
):
    plan, digest = load_plan(plan_path)
    if identity_file is not None:
        if test_bot_id is not None or recipient_file is not None:
            raise ValueError(
                "Choose private identity evidence or manual identity inputs"
            )
        identity = load_identity(identity_file)
        test_bot_id = identity["bot_id"]
        recipient_data = {"chat_id": identity["chat_id"]}
    else:
        recipient_data = json.loads(recipient_file.read_text())
    test_bot_id = chat_id(test_bot_id)
    if test_bot_id.startswith("-"):
        raise ValueError("Approved staging bot identity must be positive")
    if not re.fullmatch(
        r"arn:aws:secretsmanager:ap-southeast-2:701364614078:secret:airlake/alert-staging-v1/telegram-[A-Za-z0-9]{6}",
        secret_arn,
    ):
        raise ValueError("Exact separately approved test-bot secret ARN required")
    if not re.fullmatch(
        r"arn:aws:iam::701364614078:role/[A-Za-z0-9_+=,.@/-]+", principal
    ):
        raise ValueError("An approved existing IAM role principal is required")
    if set(recipient_data) != {"chat_id"}:
        raise ValueError("Recipient file must contain only chat_id")
    recipient = chat_id(recipient_data["chat_id"])
    manifest = json.loads(manifest_path.read_text())
    artifact_hash = hashlib.sha256(artifact.read_bytes()).hexdigest()
    if manifest["archive_sha256"] != artifact_hash:
        raise ValueError("Artifact hash mismatch")
    for path, expected in manifest["source_sha256"].items():
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != expected:
            raise ValueError("Artifact source is stale")
    output = output.resolve()
    if output == ROOT or ROOT in output.parents:
        raise ValueError("Private deployment files must be outside Git")
    output.mkdir(parents=True, exist_ok=False)
    output.chmod(0o700)

    def write(name, data):
        path = output / name
        path.write_text(json.dumps(data, indent=2) + "\n")
        path.chmod(0o600)
        return path

    n = plan["names"]
    account, region = plan["account"], plan["region"]
    scope = n["sns_topic"]
    arn = lambda service, resource: f"arn:aws:{service}:{region}:{account}:{resource}"
    write(
        "consumer-trust.json",
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        },
    )
    write(
        "reader-trust.json",
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": principal},
                    "Action": "sts:AssumeRole",
                }
            ],
        },
    )
    consumer_policy = json.loads(json.dumps(plan["consumer_policy_proposal"]))
    for statement in consumer_policy["Statement"]:
        if statement["Action"] == ["secretsmanager:GetSecretValue"]:
            statement["Resource"] = [secret_arn]
        if statement["Resource"] == [arn("dynamodb", "table/" + n["subscriptions"])]:
            statement["Condition"] = {
                "ForAllValues:StringEquals": {"dynamodb:LeadingKeys": [recipient]}
            }
    write("consumer-policy.json", consumer_policy)
    write("reader-policy.json", plan["reader_publisher_policy_proposal"])
    write(
        "dlq-attributes.json",
        {
            "MessageRetentionPeriod": str(plan["dlq"]["MessageRetentionPeriod"]),
            "SqsManagedSseEnabled": "true",
        },
    )
    attributes = {
        k: (
            json.dumps(v)
            if isinstance(v, dict)
            else str(v).lower()
            if isinstance(v, bool)
            else str(v)
        )
        for k, v in plan["sqs"].items()
    }
    attributes["Policy"] = json.dumps(plan["sns"]["queue_policy"])
    write("queue-attributes.json", attributes)
    write(
        "dlq-redrive.json",
        {"RedriveAllowPolicy": json.dumps(plan["dlq"]["RedriveAllowPolicy"])},
    )
    for kind, key in [("subscriptions", "chat_id"), ("delivery_ledger", "delivery_id")]:
        write(
            kind + ".json",
            {
                "TableName": n[kind],
                "KeySchema": [{"AttributeName": key, "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": key, "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST",
            },
        )
    write(
        "recipient-item.json",
        {
            "chat_id": {"S": recipient},
            "station_ids": {"L": [{"S": str(plan["reader"]["location_ids"][0])}]},
            "enabled": {"BOOL": True},
        },
    )
    write(
        "workgroup.json",
        {
            "EnforceWorkGroupConfiguration": True,
            "ResultConfiguration": {
                "OutputLocation": plan["workgroup"]["OutputLocation"],
                "ExpectedBucketOwner": account,
                "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
            },
            "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3"},
            "BytesScannedCutoffPerQuery": plan["workgroup"][
                "BytesScannedCutoffPerQuery"
            ],
        },
    )
    variables = {
        "AIRLAKE_ALERT_DELIVERY_ENABLED": "0",
        "AIRLAKE_ACCOUNT_ID": account,
        "AIRLAKE_ALERT_STATIONS": json.dumps(plan["stations"], ensure_ascii=False),
        "AIRLAKE_ALERT_RULES": json.dumps([plan["historical_test"]["rule"]]),
        "AIRLAKE_ALERT_TOPIC_ARN": arn("sns", scope),
        "AIRLAKE_ALERT_QUEUE_ARN": arn("sqs", scope),
        "AIRLAKE_SUBSCRIPTION_TABLE": n["subscriptions"],
        "AIRLAKE_DELIVERY_TABLE": n["delivery_ledger"],
        "AIRLAKE_DELIVERY_LEASE_SECONDS": str(plan["ledger"]["lease_seconds"]),
        "AIRLAKE_DEDUPE_RETENTION_SECONDS": str(plan["ledger"]["retention_seconds"]),
        "AIRLAKE_PARTIAL_BATCH_FAILURES": "1",
        "AIRLAKE_ALERT_TEST_MODE": "1",
        "AIRLAKE_ALERT_TEST_RECIPIENT": recipient,
        "AIRLAKE_ALERT_TEST_BOT_ID": test_bot_id,
        "AIRLAKE_ALERT_TEST_REFERENCE_TIME": plan["historical_test"]["reference_time"],
        "AIRLAKE_ALERT_STAGING_NAME": scope,
        "TELEGRAM_TOKEN_SECRET_ARN": secret_arn,
    }
    write(
        "function.json",
        {
            "FunctionName": n["lambda"],
            "Runtime": "python3.12",
            "Role": f"arn:aws:iam::{account}:role/{n['consumer_role']}",
            "Handler": plan["lambda"]["Handler"],
            "Timeout": plan["lambda"]["Timeout"],
            "MemorySize": 256,
            "Environment": {"Variables": variables},
            "Publish": True,
        },
    )
    enabled = dict(variables, AIRLAKE_ALERT_DELIVERY_ENABLED="1")
    write("disabled-environment.json", {"Variables": variables})
    write("enabled-environment.json", {"Variables": enabled})
    q = lambda x: shlex.quote(str(x))
    file = lambda name: "file://" + str(output / name)
    cli = lambda *args: (
        "aws --profile airlake --region "
        + region
        + " --no-cli-pager "
        + " ".join(q(x) for x in args)
    )
    queue_url = f"https://sqs.{region}.amazonaws.com/{account}/{n['queue']}"
    dlq_url = queue_url + "-dlq"
    identity_check = []
    if identity_file is not None:
        identity_check = [
            f"{q(sys.executable)} -B -c "
            + q(
                "import json,sys; from pathlib import Path; "
                "from scripts.verify_telegram_staging_identity import load_identity; "
                "i=load_identity(Path(sys.argv[1])); "
                "e=json.load(open(sys.argv[2]))['Environment']['Variables']; "
                "sys.exit(0 if i['bot_id']==e['AIRLAKE_ALERT_TEST_BOT_ID'] "
                "and i['chat_id']==e['AIRLAKE_ALERT_TEST_RECIPIENT'] "
                "else 'Private identity/configuration mismatch')"
            )
            + " "
            + q(identity_file.resolve())
            + " "
            + q(output / "function.json")
        ]
    steps = [
        "#!/bin/sh",
        "set -eu",
        "umask 077",
        'export AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard AWS_PAGER=""',
        f'test "${{AIRLAKE_APPROVED_PLAN_SHA256:-}}" = {q(digest)}',
        f"cd {q(ROOT)}",
        f"{q(sys.executable)} -B -m alerts.preflight --plan {q(plan_path.resolve())} > {q(output / 'fresh-preflight.json')}",
        f"{q(sys.executable)} -c "
        + q(
            "import json; p=json.load(open("
            + repr(str(output / "fresh-preflight.json"))
            + ")); import sys; sys.exit(0 if p['status'].startswith('READ_ONLY_CHECKS_PASS') and p['plan_sha256']=="
            + repr(digest)
            + " and p['test_secret']['arn']=="
            + repr(secret_arn)
            + " else 'Fresh staging preflight mismatch')"
        ),
    ]
    steps += identity_check
    steps += [
        "# FIRST AWS MUTATION BELOW: requires separate exact-scope approval. No automatic retry/cleanup."
    ]
    for role, kind in [(n["consumer_role"], "consumer"), (n["reader_role"], "reader")]:
        steps += [
            cli(
                "iam",
                "create-role",
                "--role-name",
                role,
                "--assume-role-policy-document",
                file(kind + "-trust.json"),
                "--query",
                "Role.Arn",
                "--output",
                "text",
            ),
            cli(
                "iam",
                "put-role-policy",
                "--role-name",
                role,
                "--policy-name",
                scope,
                "--policy-document",
                file(kind + "-policy.json"),
            ),
        ]
    steps += [
        cli("logs", "create-log-group", "--log-group-name", n["log_group"]),
        cli(
            "logs",
            "put-retention-policy",
            "--log-group-name",
            n["log_group"],
            "--retention-in-days",
            "7",
        ),
        cli(
            "sns",
            "create-topic",
            "--name",
            scope,
            "--query",
            "TopicArn",
            "--output",
            "text",
        ),
        cli(
            "sqs",
            "create-queue",
            "--queue-name",
            n["dlq"],
            "--attributes",
            file("dlq-attributes.json"),
            "--query",
            "QueueUrl",
            "--output",
            "text",
        ),
        cli(
            "sqs",
            "create-queue",
            "--queue-name",
            n["queue"],
            "--attributes",
            file("queue-attributes.json"),
            "--query",
            "QueueUrl",
            "--output",
            "text",
        ),
        cli(
            "sqs",
            "set-queue-attributes",
            "--queue-url",
            dlq_url,
            "--attributes",
            file("dlq-redrive.json"),
        ),
        cli(
            "sns",
            "subscribe",
            "--topic-arn",
            arn("sns", scope),
            "--protocol",
            "sqs",
            "--notification-endpoint",
            arn("sqs", scope),
            "--attributes",
            "RawMessageDelivery=false",
            "--query",
            "SubscriptionArn",
            "--output",
            "text",
        ),
    ]
    for kind in ["subscriptions", "delivery_ledger"]:
        steps += [
            cli(
                "dynamodb",
                "create-table",
                "--cli-input-json",
                file(kind + ".json"),
                "--query",
                "TableDescription.TableArn",
                "--output",
                "text",
            ),
            cli("dynamodb", "wait", "table-exists", "--table-name", n[kind]),
        ]
    steps += [
        cli(
            "dynamodb",
            "update-time-to-live",
            "--table-name",
            n["delivery_ledger"],
            "--time-to-live-specification",
            "Enabled=true,AttributeName=expires_at",
        ),
        cli(
            "dynamodb",
            "put-item",
            "--table-name",
            n["subscriptions"],
            "--item",
            file("recipient-item.json"),
            "--condition-expression",
            "attribute_not_exists(chat_id)",
        ),
        cli(
            "athena",
            "create-work-group",
            "--name",
            n["workgroup"],
            "--configuration",
            file("workgroup.json"),
        ),
        cli(
            "lambda",
            "create-function",
            "--cli-input-json",
            file("function.json"),
            "--zip-file",
            "fileb://" + str(artifact.resolve()),
            "--query",
            "Version",
            "--output",
            "text",
        )
        + " > "
        + q(output / "disabled-version.txt"),
        cli("lambda", "wait", "function-active-v2", "--function-name", scope),
        cli(
            "lambda",
            "put-function-concurrency",
            "--function-name",
            scope,
            "--reserved-concurrent-executions",
            "2",
        ),
        cli(
            "lambda",
            "create-alias",
            "--function-name",
            scope,
            "--name",
            "staging",
            "--function-version",
            "1",
            "--query",
            "AliasArn",
            "--output",
            "text",
        ),
        cli(
            "lambda",
            "create-event-source-mapping",
            "--function-name",
            scope + ":staging",
            "--event-source-arn",
            arn("sqs", scope),
            "--batch-size",
            "1",
            "--maximum-batching-window-in-seconds",
            "0",
            "--function-response-types",
            "ReportBatchItemFailures",
            "--scaling-config",
            "MaximumConcurrency=2",
            "--no-enabled",
            "--query",
            "UUID",
            "--output",
            "text",
        )
        + " > "
        + q(output / "mapping-uuid.txt"),
    ]
    script = output / "deploy-disabled.sh"
    script.write_text("\n".join(steps) + "\n")
    script.chmod(0o600)
    # Control scripts never query Gold or publish; enable remains a separate approval.
    for mode in ("enable", "disable"):
        target = output / (mode + "-test.sh")
        target.write_text(
            _control_script(
                mode,
                output,
                digest,
                artifact_hash,
                identity_check if mode == "enable" else (),
            )
        )
        target.chmod(0o600)
    summary = {
        "status": "PREPARED ONLY; NO AWS OPERATIONS EXECUTED",
        "plan_sha256": digest,
        "artifact_sha256": artifact_hash,
        "inventory": n,
        "secret_reference_only": True,
        "one_recipient_configured": True,
        "private_identity_evidence_supplied": identity_file is not None,
    }
    write("preparation.json", summary)
    return summary


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--plan", type=Path, default=ROOT / "docs/plans/athena-alert-staging.json"
    )
    for flag in ["artifact", "manifest", "output"]:
        p.add_argument("--" + flag, type=Path, required=True)
    p.add_argument("--recipient-file", type=Path)
    p.add_argument("--identity-file", type=Path)
    p.add_argument("--secret-arn", required=True)
    p.add_argument("--test-bot-id")
    p.add_argument("--publisher-principal", required=True)
    a = p.parse_args()
    if (a.identity_file is not None) == (
        a.recipient_file is not None or a.test_bot_id is not None
    ) or (
        a.identity_file is None and (a.recipient_file is None or a.test_bot_id is None)
    ):
        p.error("Supply --identity-file OR both --recipient-file and --test-bot-id")
    print(
        json.dumps(
            prepare(
                a.plan,
                a.artifact,
                a.manifest,
                a.secret_arn,
                a.recipient_file,
                a.publisher_principal,
                a.output,
                test_bot_id=a.test_bot_id,
                identity_file=a.identity_file,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
