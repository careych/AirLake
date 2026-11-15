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
            self.assertEqual(plan["functions"][deploy.API]["reserved_concurrency"], 2)
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
            "Payload": io.BytesIO(
                json.dumps(
                    {
                        "status": "PUBLISHED",
                        "published_at": "2026-11-15T02:00:00Z",
                        "projection_key": "nowcast/v1/projections/x.json",
                    }
                ).encode()
            )
        }
        c["s3"].get_object.return_value = {
            "Body": io.BytesIO(b'{"published_at": "2026-11-15T02:00:00Z"}')
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
                "published_at": "2026-11-15T02:00:00Z",
                "observations": [
                    {
                        "station_id": i,
                        "observed_at": "t",
                        "fetched_at": "t",
                        "fetch_status": "ok",
                    }
                    for i in (6123215, 99385)
                ],
                "model": {
                    "model_run_at": "r",
                    "model_available_at": "a",
                    "current": {"valid_at": "v"},
                    "forecast": [{"valid_at": "v"}],
                },
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
            self.assertNotIn("create_schedule", ops)
            self.assertTrue(ops[-1].startswith("alarm_"))
            c["scheduler"].create_schedule.assert_not_called()
            self.assertEqual(result["status"], "BACKEND_LIVE_SITE_RELEASE_NEXT")
            self.assertEqual(len(result["smoke"]), 7)
            c["lambda"].put_function_concurrency.assert_any_call(
                FunctionName=deploy.API, ReservedConcurrentExecutions=2
            )
            c["lambda"].create_function_url_config.assert_called_once()
            kwargs = c["lambda"].create_function_url_config.call_args.kwargs
            self.assertEqual(kwargs["AuthType"], "NONE")
            self.assertNotIn("Cors", kwargs)  # handler-only CORS

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

    def test_wrong_served_version_or_missing_times_stop(self):
        good = {
            "schema_version": 2,
            "published_at": "p",
            "observations": [
                {
                    "station_id": 6123215,
                    "observed_at": "o",
                    "fetched_at": "f",
                    "fetch_status": "ok",
                }
            ],
            "model": {
                "model_run_at": "r",
                "model_available_at": "a",
                "current": {"valid_at": "v"},
                "forecast": [{"valid_at": "v"}],
            },
        }
        deploy.check_projection(good, "p")
        with self.assertRaises(ValueError):
            deploy.check_projection(good, "older")
        bad = json.loads(json.dumps(good))
        del bad["observations"][0]["fetched_at"]
        with self.assertRaises(ValueError):
            deploy.check_projection(bad, "p")

    def test_schedule_needs_live_verification_and_browser_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            digest = self.packet(root)
            plan = deploy.load(root / "packet", digest)
            packet = root / "packet"
            (packet / "verify-receipt.json").write_text(
                json.dumps(
                    {
                        "status": "PASS_LIVE_UPDATE_WITHOUT_REDEPLOYMENT",
                        "plan_sha256": digest,
                    }
                )
            )
            (packet / "browser-evidence.json").write_text('{"status": "FAIL"}')
            c = self.clients()
            with self.assertRaises(ValueError):
                deploy.enable_schedule(c, packet, plan, digest)
            c["scheduler"].create_schedule.assert_not_called()
            (packet / "browser-evidence.json").write_text('{"status": "PASS"}')
            c["scheduler"].get_schedule.return_value = {"State": "ENABLED"}
            self.assertEqual(
                deploy.enable_schedule(c, packet, plan, digest)["status"],
                "SCHEDULE_ENABLED",
            )
            self.assertEqual(
                c["scheduler"].create_schedule.call_args.kwargs["State"], "ENABLED"
            )

    def test_verify_live_requires_newer_version_from_api(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            digest = self.packet(root)
            plan = deploy.load(root / "packet", digest)
            packet = root / "packet"
            (packet / "receipt.json").write_text(
                json.dumps(
                    {
                        "function_url": "https://abc.lambda-url.ap-southeast-2.on.aws/",
                        "first_refresh": {"published_at": "2026-11-15T02:00:00Z"},
                    }
                )
            )
            (packet / "site-receipt.json").write_text(
                json.dumps(
                    {
                        "status": "DEPLOYED_BROWSER_VERIFICATION_REQUIRED",
                        "plan_sha256": digest,
                    }
                )
            )
            c = self.clients()
            c["lambda"].invoke.return_value = {
                "Payload": io.BytesIO(
                    json.dumps(
                        {"status": "PUBLISHED", "published_at": "2026-11-15T02:15:00Z"}
                    ).encode()
                )
            }
            served = {"published_at": "2026-11-15T02:00:00Z"}

            def get(method, url, auth=None):
                if url.endswith("config.js"):
                    return Mock(
                        status_code=200 if auth or "public." in url else 401,
                        text="https://abc.lambda-url.ap-southeast-2.on.aws/v1/now",
                    )
                return Mock(
                    status_code=200,
                    json=Mock(
                        return_value={
                            **json.loads(json.dumps(self._projection())),
                            **served,
                        }
                    ),
                )

            with (
                patch(
                    "citizen.release.private_credentials",
                    return_value=(None, {"username": "u", "password": "p"}, None),
                ),
                self.assertRaises(ValueError),
            ):
                deploy.verify_live(c, packet, plan, digest, get, wait=0)
            (packet / "verify-receipt.json").unlink()
            served["published_at"] = "2026-11-15T02:15:00Z"
            c["lambda"].invoke.return_value = {
                "Payload": io.BytesIO(
                    json.dumps(
                        {"status": "PUBLISHED", "published_at": "2026-11-15T02:15:00Z"}
                    ).encode()
                )
            }
            with patch(
                "citizen.release.private_credentials",
                return_value=(None, {"username": "u", "password": "p"}, None),
            ):
                self.assertEqual(
                    deploy.verify_live(c, packet, plan, digest, get, wait=0)["status"],
                    "PASS_LIVE_UPDATE_WITHOUT_REDEPLOYMENT",
                )

    def _projection(self):
        return {
            "schema_version": 2,
            "observations": [
                {
                    "station_id": 99385,
                    "observed_at": "o",
                    "fetched_at": "f",
                    "fetch_status": "fetch_failed",
                }
            ],
            "model": {
                "model_run_at": "r",
                "model_available_at": "a",
                "current": {"valid_at": "v"},
                "forecast": [{"valid_at": "v"}],
            },
        }

    def test_contain_works_before_the_schedule_exists(self):
        from botocore.exceptions import ClientError

        c = self.clients()
        c["scheduler"].get_schedule.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "GetSchedule"
        )
        self.assertEqual(deploy.contain(c)["schedule"], "ABSENT")
        c["lambda"].put_function_concurrency.assert_called_once_with(
            FunctionName=deploy.API, ReservedConcurrentExecutions=0
        )

    def test_smoke_does_not_parse_the_post_405_error_body(self):
        good = {
            "schema_version": 2,
            "published_at": "p",
            "observations": [
                {
                    "station_id": 6123215,
                    "observed_at": "o",
                    "fetched_at": "f",
                    "fetch_status": "ok",
                }
            ],
            "model": {
                "model_run_at": "r",
                "model_available_at": "a",
                "current": {"valid_at": "v"},
                "forecast": [{"valid_at": "v"}],
            },
            "data": [{"location_id": 6123215}],
        }

        def get(method, url):
            path = url.split(".on.aws", 1)[1]
            code = {(m, p): e for m, p, e in deploy.API_SMOKE}[(method, path)]
            body = {"error": "read_only"} if method == "POST" else good
            return Mock(status_code=code, json=Mock(return_value=body))

        with tempfile.TemporaryDirectory() as tmp:
            r = deploy.Receipt(Path(tmp) / "r.json", "D")
            deploy.smoke(r, "https://x.lambda-url.ap-southeast-2.on.aws", "p", get)
            self.assertEqual(len(r.value["smoke"]), 7)


if __name__ == "__main__":
    unittest.main()
