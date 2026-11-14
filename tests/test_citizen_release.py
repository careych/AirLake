"""Existing-app release and offline refresh safety. Every service is mocked."""

import base64
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from test_citizen_data import fixtures

from citizen import refresh, release
from citizen.build import verified_data
from citizen.hosting import HEADERS, NAME
from live_data.contract import sha


def source_files(root):
    ex, inv, cov, counts = fixtures()
    paths = tuple(
        root / n
        for n in ("export.json", "inventory.json", "coverage.json", "evidence.json")
    )
    raw = json.dumps(ex).encode()
    proof = {
        "export_sha256": sha(raw),
        "athena": {
            "independent_stored_query_result_reconciliation": "PASS",
            "rows": 3,
            "query_id": "SYNTHETIC_VERIFIED_QUERY",
        },
        "source": {"station_counts": counts},
    }
    for p, value in zip(paths, (ex, inv, cov, proof), strict=True):
        p.write_text(json.dumps(value))
    return paths


class RefreshTests(unittest.TestCase):
    def test_build_cannot_put_datasets_inside_a_repository(self):
        from citizen.build import build

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".git").write_text("SYNTHETIC_WORKTREE_MARKER")
            with self.assertRaises(ValueError):
                build(root / "output", None, None, None, None, "instructor")
            self.assertFalse((root / "output").exists())

    def test_dry_run_does_not_create_state_and_two_runs_are_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inputs = source_files(root)
            work = root / "private"
            self.assertEqual(
                refresh.prepare(work, inputs)["status"],
                "WOULD_PREPARE_NEW_LOCAL_BUILDS",
            )
            self.assertFalse(work.exists())
            first = refresh.prepare(work, inputs, True)
            self.assertEqual(first["status"], "PREPARED_LOCAL_ONLY_NOT_DEPLOYED")
            before = (work / "current.json").read_bytes()
            self.assertEqual(
                refresh.prepare(work, inputs, True)["status"], "SKIP_UNCHANGED"
            )
            self.assertEqual((work / "current.json").read_bytes(), before)
            self.assertEqual(len(list(work.glob("*/preparation.json"))), 1)

    def test_changed_source_creates_version_and_preserves_predecessor(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inputs = source_files(root)
            work = root / "private"
            first = refresh.prepare(work, inputs, True)
            inv = json.loads(inputs[1].read_text())
            inv["latest"]["6123215"]["pm25"] = 12.5
            inputs[1].write_text(json.dumps(inv))
            second = refresh.prepare(work, inputs, True)
            self.assertNotEqual(first["fingerprint"], second["fingerprint"])
            self.assertEqual(second["previous_fingerprint"], first["fingerprint"])
            self.assertTrue((work / first["fingerprint"] / "public/config.js").exists())

    def test_unverified_source_or_count_cannot_build_or_change_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inputs = source_files(root)
            proof = json.loads(inputs[3].read_text())
            proof["athena"]["rows"] = 14
            inputs[3].write_text(json.dumps(proof))
            with self.assertRaises(ValueError):
                refresh.prepare(root / "private", inputs, True)
            self.assertFalse((root / "private").exists())

    def test_tampered_prior_artifact_is_detected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inputs = source_files(root)
            work = root / "private"
            first = refresh.prepare(work, inputs, True)
            (work / first["fingerprint"] / "public/app.js").write_text(
                "SYNTHETIC_TAMPER"
            )
            with self.assertRaises(ValueError):
                refresh.prepare(work, inputs, True)

    def test_operational_check_time_alone_does_not_publish_again(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inputs = source_files(root)
            work = root / "private"
            inv = json.loads(inputs[1].read_text())
            inv["operations_status"] = {
                "verified_at": "2026-11-14T11:00:00Z",
                "collection_enabled": False,
                "refresh_enabled": False,
            }
            inputs[1].write_text(json.dumps(inv))
            refresh.prepare(work, inputs, True)
            inv["operations_status"]["verified_at"] = "2026-11-14T12:00:00Z"
            inputs[1].write_text(json.dumps(inv))
            self.assertEqual(
                refresh.prepare(work, inputs, True)["status"], "SKIP_UNCHANGED"
            )

    def test_private_state_symlink_and_unknown_operational_fields_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inputs = source_files(root)
            alias = root / "alias"
            alias.symlink_to(root)
            with self.assertRaises(ValueError):
                refresh.prepare(alias, inputs, True)
            inv = json.loads(inputs[1].read_text())
            inv["operations_status"] = {"DO_NOT_EXPOSE": "SYNTHETIC"}
            inputs[1].write_text(json.dumps(inv))
            with self.assertRaises(ValueError):
                verified_data(*inputs, "public")


class ReleaseTests(unittest.TestCase):
    def setup_cloud(self, packet):
        credentials = {
            "username": "airlake-demo",
            "password": "SYNTHETIC_TEST_PASSWORD_NOT_REAL",
        }
        f = packet / "preview-login.json"
        f.write_text(json.dumps(credentials))
        f.chmod(0o600)
        cloud = {"sts": Mock(), "amplify": Mock()}
        cloud["sts"].get_caller_identity.return_value = {"Account": "701364614078"}
        cloud["amplify"].get_app.return_value = {
            "app": {
                "name": NAME,
                "defaultDomain": release.DOMAIN,
                "platform": "WEB",
                "enableBasicAuth": False,
                "enableBranchAutoBuild": False,
                "customHeaders": HEADERS,
            }
        }
        # Real AWS does not return the original plaintext password.
        encrypted = base64.b64encode(
            b"airlake-demo:SYNTHETIC_SERVICE_ENCRYPTED_COMPONENT"
        ).decode()
        cloud["amplify"].get_branch.side_effect = lambda **kw: {
            "branch": {
                "enableBasicAuth": kw["branchName"] == "demo",
                "enableAutoBuild": False,
                "basicAuthCredentials": encrypted,
            }
        }
        cloud["amplify"].list_jobs.return_value = {
            "jobSummaries": [{"jobId": "0000000001", "status": "SUCCEED"}]
        }
        plan = {"previous_jobs": {"public": "1", "demo": "1"}}
        return cloud, plan, credentials

    def test_amplify_json_headers_require_the_same_six_exact_security_values(self):
        expected = {}
        key = None
        for line in HEADERS.splitlines():
            line = line.strip()
            if line.startswith("- key:"):
                key = line.split(":", 1)[1].strip()
            elif line.startswith("value:"):
                value = line.split(":", 1)[1].strip()
                if value[:1] in ("'", '"') and value[-1:] == value[:1]:
                    value = value[1:-1]
                expected[key] = value
        rules = [
            {
                "pattern": "**/*",
                "headers": [{"key": k, "value": v} for k, v in expected.items()],
            }
        ]
        self.assertTrue(release.security_headers_match(json.dumps(rules)))
        rules[0]["headers"][0]["value"] = "unsafe-synthetic-value"
        self.assertFalse(release.security_headers_match(json.dumps(rules)))
        self.assertFalse(release.security_headers_match(None))
        self.assertFalse(release.security_headers_match("[]"))
        rules[0]["headers"].append({"key": "X-Extra", "value": "not-approved"})
        self.assertFalse(release.security_headers_match(json.dumps(rules)))

    def test_approval_is_required_before_clients(self):
        with (
            patch.object(release, "load", return_value=({}, "exact")),
            patch.object(release, "clients") as c,
            patch("sys.argv", ["release", "execute", "--packet", "/tmp/synthetic"]),
        ):
            with self.assertRaises(SystemExit):
                release.main()
            c.assert_not_called()

    def test_encrypted_metadata_is_not_compared_to_plaintext_password(self):
        with tempfile.TemporaryDirectory() as tmp:
            c, plan, _ = self.setup_cloud(Path(tmp))
            self.assertEqual(
                release.preflight(c, Path(tmp), plan)["status"],
                "PASS_EXISTING_APP_AND_AUTH_UNCHANGED",
            )
            c["amplify"].create_deployment.assert_not_called()

    def test_changed_job_auth_account_or_security_headers_stop_before_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            packet = Path(tmp)
            for kind in ("job", "account", "headers", "auth"):
                c, plan, _ = self.setup_cloud(packet)
                if kind == "job":
                    c["amplify"].list_jobs.return_value["jobSummaries"][0]["jobId"] = (
                        "2"
                    )
                if kind == "account":
                    c["sts"].get_caller_identity.return_value = {
                        "Account": "000000000000"
                    }
                if kind == "headers":
                    c["amplify"].get_app.return_value["app"]["customHeaders"] = ""
                if kind == "auth":
                    c["amplify"].get_branch.side_effect = lambda **kw: {
                        "branch": {
                            "enableBasicAuth": False,
                            "enableAutoBuild": False,
                            "basicAuthCredentials": "",
                        }
                    }
                with self.assertRaises(ValueError):
                    release.preflight(c, packet, plan)
                c["amplify"].create_deployment.assert_not_called()

    def test_ambiguous_upload_stops_and_contains_without_retry_or_recreation(self):
        with tempfile.TemporaryDirectory() as tmp:
            packet = Path(tmp)
            c, plan, credentials = self.setup_cloud(packet)
            c["amplify"].create_deployment.return_value = {
                "jobId": "2",
                "zipUploadUrl": "https://synthetic.s3.ap-southeast-2.amazonaws.com/unit-only",
            }
            (packet / "public.zip").write_bytes(b"SYNTHETIC_NOT_REAL_ZIP")
            http = Mock()
            http.get.side_effect = [Mock(status_code=401), Mock(status_code=200)]
            http.put.side_effect = TimeoutError("SYNTHETIC")
            with (
                patch("requests.Session", return_value=http),
                self.assertRaises(TimeoutError),
            ):
                release.execute(c, packet, plan, "synthetic-plan")
            self.assertEqual(c["amplify"].create_deployment.call_count, 1)
            c["amplify"].start_deployment.assert_not_called()
            c["amplify"].create_app.assert_not_called()
            c["amplify"].create_branch.assert_not_called()
            c["amplify"].update_app.assert_called_once()
            evidence = (packet / "release-receipt.json").read_text()
            self.assertNotIn(credentials["password"], evidence)
            self.assertNotIn("zipUploadUrl", evidence)
            with self.assertRaises(ValueError):
                release.execute(c, packet, plan, "synthetic-plan")

    def test_failed_password_check_causes_no_deployment(self):
        with tempfile.TemporaryDirectory() as tmp:
            packet = Path(tmp)
            c, plan, _ = self.setup_cloud(packet)
            http = Mock()
            http.get.side_effect = [Mock(status_code=401), Mock(status_code=401)]
            with (
                patch("requests.Session", return_value=http),
                self.assertRaises(ValueError),
            ):
                release.execute(c, packet, plan, "synthetic-plan")
            c["amplify"].create_deployment.assert_not_called()
