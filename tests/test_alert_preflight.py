"""Read-only preflight and private command preparation are tested offline."""

import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from alerts.preflight import preflight
from alerts.runtime import ROOT, load_plan
from scripts.prepare_alert_staging import prepare


class FakeCLI:
    def __init__(self, plan):
        self.plan = plan
        self.calls = []
        self.override = {}
        self.pages = 0

    def call(self, service, operation, *args):
        self.calls.append((service, operation, args))
        if (service, operation) in self.override:
            return self.override[service, operation]
        if service == "sts":
            return {
                "Account": self.plan["account"],
                "Arn": "arn:aws:sts::701364614078:assumed-role/FixtureOperator/session",
            }
        if (service, operation) == ("sns", "get-topic-attributes"):
            return {"error": "NotFound"}
        if (service, operation) == ("sqs", "get-queue-url"):
            return {"error": "AWS.SimpleQueueService.NonExistentQueue"}
        if (service, operation) == ("lambda", "get-function"):
            return {"error": "ResourceNotFoundException"}
        if (service, operation) == ("dynamodb", "describe-table"):
            return {"error": "ResourceNotFoundException"}
        if (service, operation) == ("iam", "get-role"):
            return {"error": "NoSuchEntity"}
        if (service, operation) == ("athena", "get-work-group"):
            return (
                {"WorkGroup": {"State": "ENABLED"}}
                if args[-1] == "airlake"
                else {"error": "InvalidRequestException", "not_found": True}
            )
        if service == "logs":
            return {"logGroups": []}
        if (service, operation) == ("secretsmanager", "describe-secret"):
            return {
                "ARN": "arn:aws:secretsmanager:ap-southeast-2:701364614078:secret:airlake/alert-staging-v1/telegram-Ab1234"
            }
        if operation == "get-context-keys-for-principal-policy":
            return {"ContextKeyNames": []}
        if (service, operation) == ("glue", "get-table"):
            table = args[-1]
            return {
                "Table": {
                    "IsRegisteredWithLakeFormation": False,
                    "Parameters": {
                        "metadata_location": "s3://airlake-701364614078-lake/integration-real/20261106_f3b3b3e0677d4a6790e4a54faf68e4aa/"
                        + table
                        + ".json"
                    },
                }
            }
        if (service, operation) == ("s3api", "get-object"):
            expected = (
                self.plan["reader"]["hourly_snapshot"]
                if "pm25_aqi_hourly" in args[-1]
                else self.plan["reader"]["dimension_snapshot"]
            )
            Path(args[-1]).write_text(
                json.dumps({"snapshots": [{"snapshot-id": int(expected)}]})
            )
            return {}
        if (service, operation) == ("s3api", "list-objects-v2"):
            return {"Contents": [], "IsTruncated": False}
        if operation == "simulate-principal-policy":
            return {
                "EvaluationResults": [
                    {"EvalActionName": action, "EvalDecision": "allowed"}
                    for action in (
                        "lambda:CreateFunction",
                        "iam:CreateRole",
                        "iam:PassRole",
                        "sns:CreateTopic",
                        "sqs:CreateQueue",
                        "dynamodb:CreateTable",
                        "athena:CreateWorkGroup",
                    )
                ]
            }
        raise AssertionError((service, operation))


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.plan, _ = load_plan(ROOT / "docs/plans/athena-alert-staging.json")
        self.client = FakeCLI(self.plan)

    def test_read_only_check_covers_snapshots_and_never_reads_secrets_or_queries(self):
        report = preflight(self.plan, self.client)
        self.assertTrue(report["status"].startswith("READ_ONLY_CHECKS_PASS"))
        self.assertTrue(all(x["present"] for x in report["snapshots"]))
        self.assertFalse(report["test_secret"]["value_requested"])
        forbidden = {
            "get-secret-value",
            "start-query-execution",
            "publish",
            "receive-message",
            "create-table",
            "create-function",
        }
        self.assertFalse(any(op in forbidden for _, op, _ in self.client.calls))
        self.assertIn(
            "Simulation is not effective authorization.", report["limitations"][0]
        )

    def test_expired_identity_stops_before_inventory(self):
        self.client.override["sts", "get-caller-identity"] = {"error": "ExpiredSession"}
        self.assertEqual("BLOCKED", preflight(self.plan, self.client)["status"])
        self.assertEqual(1, len(self.client.calls))

    def test_access_denied_is_unknown_and_existing_resources_block(self):
        for response in ({"error": "AccessDeniedException"}, {"Configuration": {}}):
            self.client.override["lambda", "get-function"] = response
            self.assertTrue(
                preflight(self.plan, self.client)["status"].startswith("BLOCKED")
            )

    def test_invalid_request_is_not_assumed_to_mean_workgroup_absence(self):
        self.client.override["athena", "get-work-group"] = {
            "error": "InvalidRequestException",
            "not_found": False,
        }
        self.assertIn(
            "UNKNOWN", preflight(self.plan, self.client)["absence"]["workgroup"]
        )

    def test_nonempty_prefix_or_missing_snapshot_blocks(self):
        self.client.override["s3api", "list-objects-v2"] = {
            "Contents": [{"Key": "fixture"}]
        }
        self.assertTrue(
            preflight(self.plan, self.client)["status"].startswith("BLOCKED")
        )
        del self.client.override["s3api", "list-objects-v2"]
        self.client.override["glue", "get-table"] = {"error": "AccessDenied"}
        self.assertTrue(
            preflight(self.plan, self.client)["status"].startswith("BLOCKED")
        )

    def test_region_is_explicit_simulation_context_not_only_cli_region(self):
        report = preflight(self.plan, self.client)
        call = next(
            args
            for service, operation, args in self.client.calls
            if operation == "simulate-principal-policy"
        )
        self.assertIn(
            "ContextKeyName=aws:RequestedRegion,ContextKeyValues=ap-southeast-2,ContextKeyType=string",
            call,
        )
        self.assertEqual(
            {"aws:RequestedRegion": "ap-southeast-2"},
            report["iam_simulation"]["context"],
        )
        self.assertFalse(report["execution_authorized"])

    def test_missing_secret_blocks_even_when_inventory_and_simulation_pass(self):
        self.client.override["secretsmanager", "describe-secret"] = {
            "error": "ResourceNotFoundException"
        }
        report = preflight(self.plan, self.client)
        self.assertEqual("PASS", report["inventory_status"])
        self.assertTrue(report["iam_status"].startswith("SIMULATED_ALLOWED"))
        self.assertTrue(report["status"].startswith("BLOCKED"))
        self.assertEqual(["TEST_BOT_SECRET_MISSING_OR_UNVERIFIED"], report["blockers"])

    def test_denial_missing_context_and_incomplete_simulation_fail_closed(self):
        baseline = self.client.call("iam", "simulate-principal-policy")
        import copy

        cases = []
        denied = copy.deepcopy(baseline)
        denied["EvaluationResults"][0]["EvalDecision"] = "explicitDeny"
        cases.append(denied)
        missing = copy.deepcopy(baseline)
        missing["EvaluationResults"][0]["MissingContextValues"] = [
            "aws:RequestTag/owner"
        ]
        cases.append(missing)
        partial = copy.deepcopy(baseline)
        partial["EvaluationResults"].pop()
        cases.append(partial)
        cases += [{"error": "AccessDenied"}, {"EvaluationResults": []}]
        for response in cases:
            with self.subTest(response=response):
                self.client.override["iam", "simulate-principal-policy"] = response
                report = preflight(self.plan, self.client)
                self.assertIn(
                    "IAM_SIMULATION_DENIED_OR_INCONCLUSIVE", report["blockers"]
                )
                self.assertEqual("PASS", report["inventory_status"])
                self.assertTrue(report["status"].startswith("BLOCKED"))

    def test_cli_returns_nonzero_for_missing_secret_and_zero_only_when_ready_for_approval(
        self,
    ):
        from contextlib import redirect_stdout
        from io import StringIO
        from unittest.mock import patch

        from alerts.preflight import main

        with (
            patch("alerts.preflight.CLI", return_value=self.client),
            redirect_stdout(StringIO()),
        ):
            self.client.override["secretsmanager", "describe-secret"] = {
                "error": "ResourceNotFoundException"
            }
            self.assertEqual(1, main([]))
            del self.client.override["secretsmanager", "describe-secret"]
            self.assertEqual(0, main([]))


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.artifact = self.directory / "fixture.zip"
        self.artifact.write_bytes(b"synthetic fixture, not a deployment artifact")
        self.manifest = self.directory / "manifest.json"
        self.manifest.write_text(
            json.dumps(
                {
                    "archive_sha256": hashlib.sha256(
                        self.artifact.read_bytes()
                    ).hexdigest(),
                    "source_sha256": {},
                }
            )
        )
        self.recipient = self.directory / "private-recipient.json"
        self.recipient.write_text('{"chat_id":"101"}')
        self.secret = "arn:aws:secretsmanager:ap-southeast-2:701364614078:secret:airlake/alert-staging-v1/telegram-Ab1234"

    def run_prepare(self, **changes):
        kwargs = {
            "plan_path": ROOT / "docs/plans/athena-alert-staging.json",
            "artifact": self.artifact,
            "manifest_path": self.manifest,
            "secret_arn": self.secret,
            "recipient_file": self.recipient,
            "principal": "arn:aws:iam::701364614078:role/FixtureOperator",
            "output": self.directory / "prepared",
        }
        kwargs.update(changes)
        return prepare(**kwargs)

    def test_private_disabled_commands_are_valid_and_do_not_execute(self):
        result = self.run_prepare()
        output = self.directory / "prepared"
        self.assertTrue(result["status"].startswith("PREPARED ONLY"))
        self.assertEqual(0o700, output.stat().st_mode & 0o777)
        function = json.loads((output / "function.json").read_text())
        env = function["Environment"]["Variables"]
        self.assertEqual("0", env["AIRLAKE_ALERT_DELIVERY_ENABLED"])
        self.assertNotIn("TELEGRAM_TOKEN", env)
        self.assertEqual(self.secret, env["TELEGRAM_TOKEN_SECRET_ARN"])
        for name in ["deploy-disabled.sh", "enable-test.sh"]:
            subprocess.run(
                ["sh", "-n", str(output / name)], check=True, capture_output=True
            )
        script = (output / "deploy-disabled.sh").read_text()
        self.assertIn("--no-enabled", script)
        self.assertIn("ReportBatchItemFailures", script)
        self.assertLess(
            script.index("alerts.preflight"), script.index("iam create-role")
        )
        self.assertNotIn("101", json.dumps(result))
        with self.assertRaises(FileExistsError):
            self.run_prepare()

    def test_wrong_secret_scope_and_artifact_hash_fail_before_output(self):
        with self.assertRaises(ValueError):
            self.run_prepare(secret_arn="arn:wrong")
        self.artifact.write_bytes(b"changed")
        with self.assertRaises(ValueError):
            self.run_prepare()
        self.assertFalse((self.directory / "prepared").exists())
