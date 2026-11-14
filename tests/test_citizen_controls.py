"""No real service access; review mutation and exposure gates."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from citizen import hosting, promotion


class ControlTests(unittest.TestCase):
    def test_promotion_confirmation_before_clients(self):
        with (
            patch.object(promotion, "load", return_value=({}, "exact")),
            patch.object(promotion, "clients") as clients,
            patch("sys.argv", ["promotion", "execute", "--packet", "/tmp/test"]),
        ):
            with self.assertRaises(SystemExit):
                promotion.main()
            clients.assert_not_called()

    def test_hosting_confirmation_before_clients(self):
        with (
            patch.object(hosting, "load", return_value=({}, "exact")),
            patch.object(hosting, "clients") as clients,
            patch("sys.argv", ["hosting", "deploy", "--packet", "/tmp/test"]),
        ):
            with self.assertRaises(SystemExit):
                hosting.main()
            clients.assert_not_called()

    def test_readonly_app_inventory_fails_on_existing_or_denied(self):
        c = {"sts": Mock(), "amplify": Mock()}
        c["sts"].get_caller_identity.return_value = {"Account": "701364614078"}
        c["amplify"].get_paginator.return_value.paginate.return_value = [
            {"apps": [{"name": hosting.NAME}]}
        ]
        with self.assertRaises(ValueError):
            hosting.preflight(c)
        c["amplify"].get_paginator.return_value.paginate.side_effect = PermissionError(
            "synthetic denied"
        )
        with self.assertRaises(PermissionError):
            hosting.preflight(c)
        c["amplify"].create_app.assert_not_called()

    def test_public_zip_cannot_contain_extra_private_file(self):
        with tempfile.TemporaryDirectory() as root:
            p = Path(root)
            (p / "config.js").write_text(
                "window.AIRLAKE_CONFIG="
                + json.dumps(
                    {
                        "dataUrl": "data/" + "a" * 64 + ".json",
                        "sha256": "a" * 64,
                        "mode": "public",
                    }
                )
                + ";\n"
            )
            (p / "private.json").write_text('{"test":"not allowed"}')
            with self.assertRaises(ValueError):
                hosting.zip_site(p, "public")

    def test_promotion_packet_rejects_wrong_deployed_artifact(self):
        with tempfile.TemporaryDirectory() as root:
            p = Path(root)
            (p / "wrong.zip").write_bytes(b"synthetic")
            with self.assertRaises(ValueError):
                promotion.prepare(p / "packet", p / "wrong.zip")
            self.assertFalse((p / "packet").exists())

    def test_static_security_headers_no_cloud_credentials(self):
        self.assertIn("script-src 'self'", hosting.HEADERS)
        self.assertIn("no-store", hosting.HEADERS)
        self.assertIn("connect-src 'self'", hosting.HEADERS)
        self.assertNotIn("amazonaws", hosting.HEADERS)


class HostingExecutionTests(unittest.TestCase):
    def test_two_branches_authentication_and_secrets_never_in_receipt(self):
        from unittest.mock import patch

        from live_data.contract import sha

        with tempfile.TemporaryDirectory() as root:
            packet = Path(root)
            secret = "SYNTHETIC_PRIVATE_PASSWORD_NOT_REAL"
            (packet / "preview-login.json").write_text(
                json.dumps({"username": "airlake-demo", "password": secret})
            )
            bodies = {
                b: json.dumps({"mode": b, "synthetic_unit_fixture": True}).encode()
                for b in ("public", "demo")
            }
            p = {
                "branches": {
                    b: {
                        "data_path": "data/" + sha(body) + ".json",
                        "data_sha256": sha(body),
                    }
                    for b, body in bodies.items()
                },
                "max_http_verification_requests": 40,
            }
            for b in bodies:
                (packet / (b + ".zip")).write_bytes(b"test-zip-not-a-real-deployment")
            c = {"sts": Mock(), "amplify": Mock()}
            c["sts"].get_caller_identity.return_value = {"Account": "701364614078"}
            c["amplify"].get_paginator.return_value.paginate.return_value = [
                {"apps": []}
            ]
            c["amplify"].create_app.return_value = {
                "app": {
                    "appId": "dtestonly",
                    "defaultDomain": "dtestonly.amplifyapp.com",
                }
            }
            c["amplify"].get_branch.side_effect = lambda **kw: {
                "branch": {
                    "enableBasicAuth": kw["branchName"] == "demo",
                    "enableAutoBuild": False,
                }
            }
            c["amplify"].create_deployment.side_effect = lambda **kw: {
                "jobId": kw["branchName"],
                "zipUploadUrl": "https://synthetic.s3.ap-southeast-2.amazonaws.com/unit-only",
            }
            c["amplify"].get_job.return_value = {
                "job": {"summary": {"status": "SUCCEED"}}
            }
            http = Mock()
            http.put.return_value = Mock(status_code=200)

            def get(url, **kw):
                branch = "demo" if "demo.dtestonly" in url else "public"
                if branch == "demo" and kw.get("auth") is None:
                    return Mock(status_code=401)
                return Mock(
                    status_code=200,
                    content=bodies[branch]
                    if "/data/" in url
                    else b'<html lang="vi">synthetic</html>',
                )

            http.get.side_effect = get
            with patch("requests.Session", return_value=http):
                result = hosting.deploy(c, packet, p, "synthetic-plan")
            self.assertEqual(c["amplify"].start_deployment.call_count, 2)
            self.assertEqual(
                [
                    x.kwargs["enableBasicAuth"]
                    for x in c["amplify"].create_branch.call_args_list
                ],
                [False, True],
            )
            self.assertEqual(http.get.call_count, 7)
            self.assertNotIn(secret, json.dumps(result))
            self.assertNotIn("basicAuthCredentials", json.dumps(result))
            self.assertNotIn("zipUploadUrl", json.dumps(result))
            c["amplify"].update_app.assert_not_called()
