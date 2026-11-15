"""Release tooling for the live layer; mocked AWS only."""

import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from nowcast import deploy

SECRET = "arn:aws:secretsmanager:ap-southeast-2:701364614078:secret:airlake/live-demo/20261114_9e2c740b/openaq-AbC123"


class DeployTests(unittest.TestCase):
    def packet(self, root):
        inputs = {}
        for k in (
            "export",
            "audit",
            "metadata",
            "latest",
            "source_metadata",
            "evidence",
            "acceptance",
            "credentials",
            "previous_hosting_receipt",
        ):
            p = root / (k + ".json")
            p.write_text("{}")
            inputs[k] = str(p)
        return deploy.prepare(
            root / "packet", SECRET, {"AIRLAKE_HISTORY_EXPORT_KEY": "k"}, inputs
        )

    def test_bundle_is_deterministic_and_imports_with_stdlib_only(self):
        a, b = deploy.lambda_zip(), deploy.lambda_zip()
        self.assertEqual(a, b)
        with tempfile.TemporaryDirectory() as tmp:
            z = Path(tmp) / "lambda.zip"
            z.write_bytes(a)
            out = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    "-S",
                    "-c",
                    f"import sys; sys.path.insert(0, {str(z)!r}); import nowcast.refresh, nowcast.public_api, nowcast.http; print('ok')",
                ],
                capture_output=True,
                check=False,
                text=True,
                cwd=tmp,
            )
            self.assertEqual(out.stdout.strip(), "ok", out.stderr[-500:])
        self.assertIn(
            "nowcast/validation_public.json",
            deploy.zipfile.ZipFile(io.BytesIO(a)).namelist(),
        )

    def test_policies_are_scoped_to_exact_resources(self):
        p = deploy.policies(
            SECRET,
            "arn:aws:lambda:ap-southeast-2:701364614078:function:" + deploy.REFRESH,
        )
        text = json.dumps(p)
        self.assertNotIn('"*"', text)
        self.assertNotIn("s3:*", text)
        api = p[deploy.API]["Statement"][0]
        self.assertEqual(api["Action"], "s3:GetObject")
        self.assertEqual(len(api["Resource"]), 3)
        self.assertTrue(
            all(
                r.startswith("arn:aws:s3:::airlake-701364614078-lake/nowcast/v1/")
                for r in p[deploy.REFRESH]["Statement"][0]["Resource"]
            )
        )
        self.assertEqual(
            p[deploy.SCHEDULER_ROLE]["Statement"][0]["Action"], "lambda:InvokeFunction"
        )

    def test_plan_hash_and_source_drift_stop_before_aws(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            digest = self.packet(root)
            plan = deploy.load(root / "packet", digest)
            self.assertEqual(plan["functions"][deploy.API]["reserved_concurrency"], 5)
            self.assertEqual(plan["schedule"]["retry"]["MaximumRetryAttempts"], 0)
            with self.assertRaises(ValueError):
                deploy.load(root / "packet", "0" * 64)
            Path(plan["site"]["inputs"]["latest"]).write_text('{"changed": true}')
            with self.assertRaises(ValueError):
                deploy.load(root / "packet", digest)
        with tempfile.TemporaryDirectory() as tmp, self.assertRaises(ValueError):
            deploy.prepare(
                Path(tmp) / "p", "arn:aws:secretsmanager:us-east-1:1:secret:x", {}, {}
            )

    def clients(self):
        c = {
            k: Mock()
            for k in (
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
        c["iam"].create_role.side_effect = lambda RoleName, **k: {
            "Role": {"Arn": "arn:aws:iam::701364614078:role/" + RoleName}
        }
        c["lambda"].create_function.side_effect = lambda FunctionName, **k: {
            "FunctionArn": "arn:aws:lambda:ap-southeast-2:701364614078:function:"
            + FunctionName
        }
        c["lambda"].invoke.return_value = {
            "Payload": io.BytesIO(json.dumps({"status": "PUBLISHED"}).encode())
        }
        c["lambda"].create_function_url_config.return_value = {
            "FunctionUrl": "https://abc123.lambda-url.ap-southeast-2.on.aws/"
        }
        return c

    def responses(self, override=None):
        def get(method, url):
            path = url.split(".on.aws", 1)[1]
            code = {(m, p): e for m, p, e in deploy.API_SMOKE}[(method, path)]
            if override and path in override:
                code = override[path]
            body = {
                "schema_version": 2,
                "observations": [{"station_id": 6123215}, {"station_id": 99385}],
                "data": [{"location_id": 6123215}],
            }
            return Mock(status_code=code, json=Mock(return_value=body))

        return get

    def test_execute_order_and_schedule_last(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            digest = self.packet(root)
            plan = deploy.load(root / "packet", digest)
            c = self.clients()
            with patch.object(deploy, "preflight"), patch.object(deploy.time, "sleep"):
                result = deploy.execute(
                    c, root / "packet", plan, digest, self.responses()
                )
            ops = [o["operation"] for o in result["operations"]]
            self.assertLess(
                ops.index("invoke_refresh_once"), ops.index("create_function_url")
            )
            self.assertEqual(ops[-1], "create_schedule")
            self.assertEqual(result["status"], "BACKEND_LIVE_SITE_RELEASE_NEXT")
            self.assertEqual(len(result["smoke"]), 7)
            c["lambda"].put_function_concurrency.assert_any_call(
                FunctionName=deploy.API, ReservedConcurrentExecutions=5
            )
            c["lambda"].create_function_url_config.assert_called_once()
            self.assertEqual(
                c["lambda"].create_function_url_config.call_args.kwargs["AuthType"],
                "NONE",
            )

    def test_smoke_mismatch_stops_before_alarms_and_schedule(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            digest = self.packet(root)
            plan = deploy.load(root / "packet", digest)
            c = self.clients()
            with (
                patch.object(deploy, "preflight"),
                patch.object(deploy.time, "sleep"),
                self.assertRaises(ValueError),
            ):
                deploy.execute(
                    c,
                    root / "packet",
                    plan,
                    digest,
                    self.responses({"/v1/history?station_id=4946812": 200}),
                )
            c["scheduler"].create_schedule.assert_not_called()
            c["cloudwatch"].put_metric_alarm.assert_not_called()

    def test_failed_first_refresh_stops_before_public_url(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            digest = self.packet(root)
            plan = deploy.load(root / "packet", digest)
            c = self.clients()
            c["lambda"].invoke.return_value = {
                "FunctionError": "Unhandled",
                "Payload": io.BytesIO(b"{}"),
            }
            with (
                patch.object(deploy, "preflight"),
                patch.object(deploy.time, "sleep"),
                self.assertRaises(ValueError),
            ):
                deploy.execute(c, root / "packet", plan, digest, self.responses())
            c["lambda"].create_function_url_config.assert_not_called()

    def test_contain_blocks_public_api_and_pauses_refresh_without_deleting(self):
        c = self.clients()
        c["scheduler"].get_schedule.return_value = {
            "ScheduleExpression": "rate(15 minutes)",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "Target": {"Arn": "a", "RoleArn": "r"},
        }
        self.assertEqual(deploy.contain(c)["status"], "CONTAINED")
        c["lambda"].put_function_concurrency.assert_called_once_with(
            FunctionName=deploy.API, ReservedConcurrentExecutions=0
        )
        self.assertEqual(
            c["scheduler"].update_schedule.call_args.kwargs["State"], "DISABLED"
        )
        c["lambda"].delete_function.assert_not_called()


if __name__ == "__main__":
    unittest.main()
