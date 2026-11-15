import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from nowcast import history
from scripts import release_rolling_history as rel
from tests.test_nowcast import FakeS3

POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": ["arn:aws:s3:::b/nowcast/v1/current.json"],
        },
        {"Effect": "Allow", "Action": ["logs:PutLogEvents"], "Resource": ["x"]},
    ],
}


class Lambda:
    def __init__(self, plan, log):
        self.code = dict(plan["deployed_code_sha256_b64"])
        self.names = {v: k for k, v in plan["functions"].items()}
        self.new = plan["new_code_sha256_b64"]
        self.log = log

    def get_function_configuration(self, FunctionName):
        return {
            "CodeSha256": self.code[self.names[FunctionName]],
            "State": "Active",
            "RevisionId": "r-" + FunctionName,
        }

    def update_function_code(self, FunctionName, ZipFile, RevisionId):
        self.log.append("update_" + self.names[FunctionName])
        self.code[self.names[FunctionName]] = self.new[self.names[FunctionName]]

    def get_waiter(self, name):
        return type("W", (), {"wait": lambda self, FunctionName: None})()

    def invoke(self, FunctionName):
        self.log.append("invoke")
        return {
            "Payload": io.BytesIO(
                json.dumps(
                    {
                        "status": "PUBLISHED",
                        "published_at": "2026-11-16T01:00:00Z",
                        "history": {"6123215": "updated"},
                    }
                ).encode()
            )
        }


class S3(FakeS3):
    class exceptions(FakeS3.exceptions):
        class ClientError(Exception):
            def __init__(self, code):
                self.response = {"Error": {"Code": code}}

    def head_object(self, Bucket, Key, ExpectedBucketOwner):
        if Key not in self.objects:
            raise self.exceptions.ClientError("404")
        return {}


class ReleaseTests(unittest.TestCase):
    def test_iam_adds_exactly_the_two_history_keys(self):
        doc = rel.iam_document(POLICY)
        added = set(doc["Statement"][0]["Resource"]) - set(
            POLICY["Statement"][0]["Resource"]
        )
        self.assertEqual(
            added,
            {f"arn:aws:s3:::{rel.BUCKET}/{history.key(s)}" for s in (6123215, 99385)},
        )
        self.assertEqual(doc["Statement"][1], POLICY["Statement"][1])
        with self.assertRaises(ValueError):
            rel.iam_document(doc)

    def test_execute_order_and_stop_on_failed_api_check(self):
        with tempfile.TemporaryDirectory() as root:
            packet = Path(root)
            plan = {
                "functions": rel.FUNCTIONS,
                "deployed_code_sha256_b64": rel.DEPLOYED,
                "new_code_sha256_b64": {"refresh": "N", "api": "N", "bot": "B"},
                "schedules_unchanged": list(rel.SCHEDULES),
                "history": {
                    str(s): {"key": history.key(s), "sha256": "a" * 64}
                    for s in (6123215, 99385)
                },
                "public_api": "https://api.example",
                "public_api_checks": rel.CHECKS,
            }
            for name in ("nowcast.zip", "bot.zip"):
                (packet / name).write_bytes(b"zip")
            for s in (6123215, 99385):
                (packet / f"series-{s}.json").write_bytes(b"{}")
            (packet / "previous-api-policy.json").write_text(json.dumps(POLICY))
            (packet / "api-policy.json").write_text(
                json.dumps(rel.iam_document(POLICY))
            )
            log = []
            s3 = S3()
            current = {
                "published_at": "2026-11-16T01:00:00Z",
                "observations": [{"freshness": {"status": "fresh"}}],
            }
            s3.objects[rel.CURRENT_KEY] = json.dumps(current).encode()
            iam = type(
                "I",
                (),
                {
                    "get_role_policy": lambda self, RoleName, PolicyName: {
                        "PolicyDocument": POLICY
                    },
                    "put_role_policy": lambda self, **kw: log.append("iam"),
                },
            )()
            c = {
                "sts": type(
                    "S",
                    (),
                    {"get_caller_identity": lambda self: {"Account": rel.ACCOUNT}},
                )(),
                "lambda": Lambda(plan, log),
                "s3": s3,
                "iam": iam,
                "scheduler": type(
                    "X", (), {"get_schedule": lambda self, Name: {"State": "ENABLED"}}
                )(),
            }

            def get(url, timeout, allow_redirects):
                log.append("GET " + url.split("/v1")[1])
                return type(
                    "R", (), {"status_code": 503 if "stations" in url else 200}
                )()

            with (
                mock.patch.object(rel.time, "sleep"),
                self.assertRaisesRegex(ValueError, "Public API check failed"),
            ):
                rel.execute(c, packet, plan, "d" * 64, get=get)
            self.assertEqual(
                log,
                [
                    "update_bot",
                    "iam",
                    "update_refresh",
                    "invoke",
                    "update_api",
                    "GET /stations",
                ],
            )
            self.assertIn(history.key(99385), s3.objects)
            receipt = json.loads((packet / "receipt.json").read_text())
            self.assertEqual(
                receipt["api_checks"], [{"path": "/v1/stations", "status": 503}]
            )
            with self.assertRaises(ValueError):  # never retried automatically
                rel.execute(c, packet, plan, "d" * 64, get=get)


if __name__ == "__main__":
    unittest.main()


class FixReleaseTests(unittest.TestCase):
    def test_order_concurrency_and_stop_before_site(self):
        from scripts import release_fix_20261115 as fix

        plan = {
            "functions": fix.FUNCTIONS,
            "deployed_code_sha256_b64": {"refresh": fix.DEPLOYED, "api": fix.DEPLOYED},
            "new_code_sha256_b64": {"refresh": "N", "api": "N"},
            "schedules_unchanged": list(rel.SCHEDULES),
            "site_inputs": dict.fromkeys(
                (
                    "export",
                    "audit",
                    "metadata",
                    "latest",
                    "source_metadata",
                    "evidence",
                    "acceptance",
                ),
                "x",
            ),
            "public_api": "https://api.example",
            "bot_url": "https://t.me/x",
        }
        log, reserved = [], {"value": 2}
        lam = Lambda(plan, log)
        lam.get_function_concurrency = lambda FunctionName: {
            "ReservedConcurrentExecutions": reserved["value"]
        }

        def put(FunctionName, ReservedConcurrentExecutions):
            log.append(f"concurrency {ReservedConcurrentExecutions}")
            reserved["value"] = ReservedConcurrentExecutions

        lam.put_function_concurrency = put
        s3 = S3()
        s3.objects[rel.CURRENT_KEY] = json.dumps(
            {
                "published_at": "2026-11-16T01:00:00Z",
                "model_areas": {
                    "fetch_status": "ok",
                    "groups": [{"areas": list(range(126))}],
                },
            }
        ).encode()
        c = {
            "sts": type(
                "S", (), {"get_caller_identity": lambda self: {"Account": rel.ACCOUNT}}
            )(),
            "lambda": lam,
            "s3": s3,
            "scheduler": type(
                "X", (), {"get_schedule": lambda self, Name: {"State": "ENABLED"}}
            )(),
        }
        with tempfile.TemporaryDirectory() as root:
            packet = Path(root)
            (packet / "nowcast.zip").write_bytes(b"zip")
            with (
                mock.patch("historical.build.build", side_effect=RuntimeError("stop")),
                self.assertRaises(RuntimeError),
            ):
                fix.execute(c, packet, plan, "d" * 64)
            self.assertEqual(
                log, ["update_refresh", "invoke", "update_api", "concurrency 10"]
            )
            with self.assertRaises(ValueError):  # never retried
                fix.execute(c, packet, plan, "d" * 64)
        lam.code = dict(plan["deployed_code_sha256_b64"])
        reserved["value"] = 5
        with self.assertRaisesRegex(ValueError, "concurrency changed"):
            fix.preflight(c, plan)
