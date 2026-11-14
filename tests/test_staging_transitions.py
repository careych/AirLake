"""Exercise generated control logic against a local Lambda fake; no AWS access."""

import base64
import copy
import hashlib
import json
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from botocore.session import get_session
from botocore.validate import validate_parameters

from alerts.runtime import ROOT
from scripts.prepare_alert_staging import _run_control, prepare, repair_control_scripts

SCOPE = "airlake-alert-staging-v1"
FUNCTION = "arn:aws:lambda:ap-southeast-2:701364614078:function:" + SCOPE
MAPPING = "00000000-0000-0000-0000-000000000001"


class Clock:
    def __init__(self):
        self.now = 0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class LambdaFake:
    def __init__(self, expected, digest):
        self.model = get_session().get_service_model("lambda")
        self.calls = []
        self.failures = {}
        self.states = ["Disabled"]
        self.enable_states = ["Enabling", "Updating", "Enabled", "Enabled"]
        self.disable_states = ["Disabling", "Disabled", "Disabled"]
        self.config_states = ["Successful"]
        self.mapping = {
            "UUID": MAPPING,
            "FunctionArn": FUNCTION + ":staging",
            "EventSourceArn": "arn:aws:sqs:ap-southeast-2:701364614078:" + SCOPE,
            "BatchSize": 1,
            "MaximumBatchingWindowInSeconds": 0,
            "FunctionResponseTypes": ["ReportBatchItemFailures"],
            "ScalingConfig": {"MaximumConcurrency": 2},
        }
        self.latest = {
            "FunctionArn": FUNCTION,
            "Role": "arn:aws:iam::701364614078:role/" + SCOPE + "-consumer",
            "CodeSha256": base64.b64encode(bytes.fromhex(digest)).decode(),
            "RevisionId": "function-revision",
            "Version": "$LATEST",
            "State": "Active",
            "LastUpdateStatus": "Successful",
            "Environment": {"Variables": dict(expected, EXTRA_SETTING="preserve-me")},
        }
        self.versions = {
            "1": copy.deepcopy(self.latest),
            "2": copy.deepcopy(self.latest),
        }
        self.versions["2"]["Environment"]["Variables"][
            "AIRLAKE_ALERT_DELIVERY_ENABLED"
        ] = "1"
        self.current = "1"

    @staticmethod
    def next_value(values):
        return values.pop(0) if len(values) > 1 else values[0]

    def __call__(self, operation, **params):
        name = "".join(part.title() for part in operation.split("-"))
        validate_parameters(params, self.model.operation_model(name).input_shape)
        self.calls.append((operation, copy.deepcopy(params)))
        if "UUID" in params and params["UUID"] != MAPPING:
            raise AssertionError("Unexpected mapping identifier")
        if "FunctionName" in params and params["FunctionName"] != SCOPE:
            raise AssertionError("Unexpected function identifier")
        if operation in self.failures:
            raise RuntimeError(self.failures[operation])
        if operation == "get-event-source-mapping":
            return dict(self.mapping, State=self.next_value(self.states))
        if operation == "update-event-source-mapping":
            self.states = list(
                self.enable_states if params["Enabled"] else self.disable_states
            )
            return {"State": self.states[0]}
        if operation == "get-function-configuration":
            if "Qualifier" in params:
                result = copy.deepcopy(self.versions[params["Qualifier"]])
                return dict(result, FunctionArn=FUNCTION + ":" + params["Qualifier"])
            return dict(
                copy.deepcopy(self.latest),
                LastUpdateStatus=self.next_value(self.config_states),
            )
        if operation == "update-function-configuration":
            self.latest["Environment"] = copy.deepcopy(params["Environment"])
            self.config_states = ["InProgress", "Successful"]
            return {"LastUpdateStatus": "InProgress"}
        if operation == "get-alias":
            return {
                "AliasArn": FUNCTION + ":staging",
                "FunctionVersion": self.current,
                "RevisionId": "alias-revision",
            }
        if operation == "update-alias":
            self.current = params["FunctionVersion"]
            return {"AliasArn": FUNCTION + ":staging"}
        if operation == "publish-version":
            version = str(len(self.versions) + 1)
            self.versions[version] = copy.deepcopy(self.latest)
            return {"Version": version}
        raise AssertionError(operation)


class StagingTransitionTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(
                socket, "socket", side_effect=AssertionError("Offline tests only")
            )
        )
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        artifact = self.root / "fixture.zip"
        artifact.write_bytes(b"synthetic package fixture")
        manifest = self.root / "manifest.json"
        manifest.write_text(
            json.dumps(
                {
                    "archive_sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
                    "source_sha256": {},
                }
            )
        )
        recipient = self.root / "recipient.json"
        recipient.write_text('{"chat_id":"101"}')
        self.output = self.root / "prepared"
        prepare(
            ROOT / "docs/plans/athena-alert-staging.json",
            artifact,
            manifest,
            "arn:aws:secretsmanager:ap-southeast-2:701364614078:secret:airlake/alert-staging-v1/telegram-Ab1234",
            recipient,
            "arn:aws:iam::701364614078:role/managed/FixtureOperator",
            self.output,
            test_bot_id="900001",
        )
        self.digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
        (self.output / "mapping-uuid.txt").write_text(MAPPING)
        (self.output / "disabled-version.txt").write_text("1")
        (self.output / "enabled-version.txt").write_text("2")
        expected = json.loads((self.output / "function.json").read_text())[
            "Environment"
        ]["Variables"]
        self.aws = LambdaFake(expected, self.digest)
        self.clock = Clock()

    def run_control(self, mode):
        return _run_control(
            mode,
            self.output,
            "khue",
            self.digest,
            call=self.aws,
            clock=self.clock,
            sleep=self.clock.sleep,
        )

    def mutations(self):
        return [(op, p) for op, p in self.aws.calls if not op.startswith("get-")]

    def enabled(self):
        self.aws.current = "2"
        self.aws.latest["Environment"]["Variables"][
            "AIRLAKE_ALERT_DELIVERY_ENABLED"
        ] = "1"
        self.aws.states = ["Enabled"]

    def assert_recovered(self):
        self.assertEqual("1", self.aws.current)
        self.assertEqual(
            "0",
            self.aws.latest["Environment"]["Variables"][
                "AIRLAKE_ALERT_DELIVERY_ENABLED"
            ],
        )
        self.assertEqual(
            "preserve-me", self.aws.latest["Environment"]["Variables"]["EXTRA_SETTING"]
        )

    def test_enable_waits_for_two_enabled_reads_and_preserves_environment(self):
        result = self.run_control("enable")
        self.assertIn("stably Enabled", result)
        self.assertGreaterEqual(self.clock.now, 20)
        self.assertEqual(
            [True],
            [
                p["Enabled"]
                for op, p in self.mutations()
                if op == "update-event-source-mapping"
            ],
        )
        self.assertEqual(
            "preserve-me", self.aws.latest["Environment"]["Variables"]["EXTRA_SETTING"]
        )
        self.assertEqual("2", (self.output / "enabled-version.txt").read_text())
        self.assertTrue((self.output / "enabled-version-3.txt").exists())
        operations = [op for op, _ in self.aws.calls]
        self.assertEqual(["get-event-source-mapping"] * 2, operations[-2:])
        self.assertFalse(any("concurrency" in op for op in operations))

    def test_enable_timeout_and_unexpected_state_stop_without_repeated_mutation(self):
        for states in (["Enabling"], ["Deleting"]):
            with self.subTest(states=states):
                self.setUp_fake()
                self.aws.enable_states = list(states)
                with self.assertRaises(RuntimeError):
                    self.run_control("enable")
                self.assertEqual(
                    1,
                    sum(
                        op == "update-event-source-mapping"
                        for op, _ in self.mutations()
                    ),
                )
                self.assertLessEqual(self.clock.now, 310)

    def test_enable_failure_reports_first_aws_operation_without_service_text(self):
        self.aws.failures["publish-version"] = "AccessDeniedException: private detail"
        with self.assertRaisesRegex(RuntimeError, "AWS operation failed") as raised:
            self.run_control("enable")
        self.assertEqual("publish_enabled_version", raised.exception.phase)
        self.assertEqual("publish-version", raised.exception.operation)
        self.assertEqual("unknown", raised.exception.exit_code)
        self.assertEqual(0, raised.exception.elapsed_ms)
        self.assertNotIn("private detail", str(raised.exception))
        operations = [operation for operation, _ in self.aws.calls]
        self.assertEqual(1, operations.count("publish-version"))
        self.assertNotIn("update-event-source-mapping", operations)

    def setUp_fake(self):
        expected = json.loads((self.output / "function.json").read_text())[
            "Environment"
        ]["Variables"]
        self.aws = LambdaFake(expected, self.digest)
        self.clock = Clock()
        # New immutable version numbers in a real function cannot be reused.
        self.aws.versions.update(
            {
                str(i): copy.deepcopy(self.aws.latest)
                for i in range(
                    3, 3 + len(list(self.output.glob("enabled-version-*.txt")))
                )
            }
        )

    def test_disable_waits_through_all_transitions_and_is_idempotent(self):
        self.enabled()
        self.aws.states = ["Enabling", "Updating", "Enabled", "Enabled"]
        result = self.run_control("disable")
        self.assertIn("disabled", result)
        self.assert_recovered()
        self.assertEqual(
            [False],
            [
                p["Enabled"]
                for op, p in self.mutations()
                if op == "update-event-source-mapping"
            ],
        )
        count = len(self.mutations())
        self.run_control("disable")
        self.assertEqual(count, len(self.mutations()))

    def test_already_disabling_waits_without_redundant_disable_write(self):
        self.enabled()
        self.aws.states = ["Disabling", "Disabled", "Disabled"]
        self.run_control("disable")
        self.assert_recovered()
        self.assertFalse(
            any(op == "update-event-source-mapping" for op, _ in self.mutations())
        )

    def test_already_disabled_mapping_still_restores_alias_and_latest(self):
        self.enabled()
        self.aws.states = ["Disabled"]
        self.run_control("disable")
        self.assert_recovered()
        self.assertFalse(
            any(op == "update-event-source-mapping" for op, _ in self.mutations())
        )

    def test_temporary_enabled_state_does_not_count_as_stable(self):
        self.aws.enable_states = ["Enabled", "Updating", "Enabled", "Enabled"]
        self.run_control("enable")
        self.assertGreaterEqual(self.clock.now, 25)

    def test_enable_waits_for_initial_disabling_and_updating_before_writes(self):
        self.aws.states = ["Disabling", "Updating", "Disabled", "Disabled"]
        self.run_control("enable")
        first_write = next(
            i for i, (op, _) in enumerate(self.aws.calls) if not op.startswith("get-")
        )
        self.assertGreaterEqual(
            sum(
                op == "get-event-source-mapping"
                for op, _ in self.aws.calls[:first_write]
            ),
            4,
        )

    def test_mapping_conflict_does_not_skip_alias_or_latest_restoration(self):
        self.enabled()
        self.aws.failures["update-event-source-mapping"] = "ResourceInUseException"
        with self.assertRaisesRegex(RuntimeError, "incomplete: mapping"):
            self.run_control("disable")
        self.assert_recovered()
        self.assertEqual(
            1, sum(op == "update-event-source-mapping" for op, _ in self.mutations())
        )

    def test_mapping_timeout_or_read_failure_still_restores_delivery(self):
        for failure in ("timeout", "AccessDenied"):
            with self.subTest(failure=failure):
                self.setUp_fake()
                self.enabled()
                if failure == "timeout":
                    self.aws.states = ["Updating"]
                else:
                    self.aws.failures["get-event-source-mapping"] = failure
                with self.assertRaisesRegex(RuntimeError, "incomplete: mapping"):
                    self.run_control("disable")
                self.assert_recovered()
                self.assertLessEqual(self.clock.now, 310)

    def test_alias_failure_does_not_skip_configuration_or_mapping(self):
        self.enabled()
        self.aws.failures["update-alias"] = "ResourceInUseException"
        with self.assertRaisesRegex(RuntimeError, "incomplete: alias"):
            self.run_control("disable")
        self.assertEqual(
            "0",
            self.aws.latest["Environment"]["Variables"][
                "AIRLAKE_ALERT_DELIVERY_ENABLED"
            ],
        )
        self.assertEqual("Disabled", self.aws.states[-1])

    def test_config_failure_does_not_skip_alias_or_mapping(self):
        self.enabled()
        self.aws.failures["update-function-configuration"] = "ResourceInUseException"
        with self.assertRaisesRegex(RuntimeError, "incomplete: configuration"):
            self.run_control("disable")
        self.assertEqual("1", self.aws.current)
        self.assertEqual("Disabled", self.aws.states[-1])

    def test_config_timeout_still_attempts_alias_and_mapping_recovery(self):
        self.enabled()
        self.aws.config_states = ["InProgress"]
        with self.assertRaisesRegex(RuntimeError, "incomplete: configuration"):
            self.run_control("disable")
        self.assertEqual("1", self.aws.current)
        self.assertEqual("Disabled", self.aws.states[-1])
        # A 300s config timeout is followed by 15s of bounded mapping recovery.
        self.assertEqual(315, self.clock.now)

    def test_unreadable_environment_is_not_replaced_with_only_delivery_flag(self):
        self.enabled()
        self.aws.latest["Environment"] = {
            "Error": {"ErrorCode": "KMSAccessDeniedException"}
        }
        with self.assertRaisesRegex(RuntimeError, "incomplete: configuration"):
            self.run_control("disable")
        self.assertEqual("1", self.aws.current)
        self.assertEqual("Disabled", self.aws.states[-1])
        self.assertFalse(
            any(op == "update-function-configuration" for op, _ in self.mutations())
        )

    def test_enable_is_noop_when_already_enabled_and_checks_partial_batches(self):
        self.enabled()
        self.run_control("enable")
        self.assertEqual([], self.mutations())
        self.aws.mapping["FunctionResponseTypes"] = []
        with self.assertRaisesRegex(RuntimeError, "safeguards"):
            self.run_control("enable")
        self.assertEqual([], self.mutations())

    def test_wrong_mapping_scope_and_unsafe_disabled_version_rejected(self):
        self.aws.mapping["FunctionArn"] = FUNCTION.replace(SCOPE, "AirLake_Bot_API")
        with self.assertRaisesRegex(RuntimeError, "scope"):
            self.run_control("enable")
        self.assertEqual([], self.mutations())
        self.aws.versions["1"]["Environment"]["Variables"][
            "AIRLAKE_ALERT_DELIVERY_ENABLED"
        ] = "1"
        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            self.run_control("disable")
        self.assertFalse(any(op == "update-alias" for op, _ in self.mutations()))

    def test_transport_keeps_environment_out_of_argv_and_never_retries_errors(self):
        self.enabled()
        input_paths = []

        def run(args, **kwargs):
            self.assertEqual("1", kwargs["env"]["AWS_MAX_ATTEMPTS"])
            self.assertNotIn("input", kwargs)
            self.assertIn("khue", args)
            self.assertNotIn("101", args)
            input_arg = args[args.index("--cli-input-json") + 1]
            self.assertTrue(input_arg.startswith("file://"))
            input_path = Path(input_arg.removeprefix("file://"))
            input_paths.append(input_path)
            self.assertEqual(0o600, input_path.stat().st_mode & 0o777)
            payload = json.loads(input_path.read_text())
            operation = args[args.index("lambda") + 1]
            return subprocess.CompletedProcess(
                args,
                0,
                json.dumps(self.aws(operation, **payload)),
                "",
            )

        with patch("subprocess.run", side_effect=run):
            _run_control(
                "disable",
                self.output,
                "khue",
                self.digest,
                clock=self.clock,
                sleep=self.clock.sleep,
            )
        self.assert_recovered()
        self.assertTrue(input_paths)
        self.assertTrue(all(not path.exists() for path in input_paths))

    def test_patch_only_creates_new_private_scripts_and_leaves_all_evidence_unchanged(
        self,
    ):
        before = {p.name: p.read_bytes() for p in self.output.iterdir()}
        identity = self.root / "identity.json"
        identity.write_text("not opened during script patching")
        result = repair_control_scripts(
            self.output, profile="khue", identity_file=identity
        )
        self.assertIn("prepared only", result)
        self.assertTrue(
            all(
                (self.output / name).read_bytes() == data
                for name, data in before.items()
            )
        )
        for name in ("enable-test-fixed.sh", "disable-test-fixed.sh"):
            path = self.output / name
            self.assertEqual(0o600, path.stat().st_mode & 0o777)
            text = path.read_text()
            self.assertIn("'khue'", text)
            self.assertNotIn("'airlake'", text)
            subprocess.run(["sh", "-n", str(path)], check=True, capture_output=True)
            body = text.split("<<'AIRLAKE_STAGING_CONTROL'\n", 1)[1].rsplit(
                "\nAIRLAKE_STAGING_CONTROL", 1
            )[0]
            compile(body, "<generated staging control>", "exec")
        with self.assertRaises(FileExistsError):
            repair_control_scripts(self.output, profile="khue", identity_file=identity)


if __name__ == "__main__":
    unittest.main()
