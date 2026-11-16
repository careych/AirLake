"""Mocked release safety; no AWS calls or source measurements in fixtures."""

import base64
import hashlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from historical import cloud, publish
from live_data.contract import ACCOUNT, sha


class HistoricalReleaseTests(unittest.TestCase):
    def verification_fixture(self, root):
        facts = []
        daily = []
        for station, count in ((4946812, 720), (4946813, 693), (6123215, 720)):
            for i in range(count):
                facts.append(
                    {
                        "location_id": station,
                        "hour_start": (
                            datetime(2026, 8, 31, 17, tzinfo=UTC) + timedelta(hours=i)
                        )
                        .isoformat()
                        .replace("+00:00", "Z"),
                        "pm25_avg": 10.5,
                        "aqi": 20,
                    }
                )
            for i in range(30):
                daily.append(
                    {
                        "location_id": station,
                        "date_local": f"2026-09-{i + 1:02d}",
                        "pm25_avg": 10.5,
                        "hours_present": 24,
                        "hours_expected": 24,
                        "coverage_percent": 100.0,
                        "complete_24_hours": True,
                    }
                )
        snaps = {"hourly": [11], "dimensions": [12], "daily": [13]}
        ex = {
            "facts": facts,
            "daily": daily,
            "dimensions": [
                {"location_id": i, "name": "SYNTHETIC fixture"}
                for i in (4946812, 4946813, 6123215)
            ],
            "provenance": {"snapshots": snaps},
        }
        raw = json.dumps(ex).encode()
        evidence = {
            "status": "PASS",
            "export_sha256": sha(raw),
            "snapshots_before": snaps,
            "snapshots_after": snaps,
        }
        c = {"s3": Mock(), "athena": Mock()}

        def get(**kwargs):
            return {
                "Body": io.BytesIO(
                    raw
                    if kwargs["Key"].endswith("export.json")
                    else json.dumps(evidence).encode()
                )
            }

        c["s3"].get_object.side_effect = get
        c["athena"].start_query_execution.side_effect = [
            {"QueryExecutionId": "SYNTHETIC_HOUR"},
            {"QueryExecutionId": "SYNTHETIC_DAY"},
        ]
        c["athena"].get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {"DataScannedInBytes": 1},
                "ResultConfiguration": {"OutputLocation": "s3://SYNTHETIC-test-result"},
            }
        }
        hourly_rows = [
            {**{k: str(v) for k, v in f.items()}, "station_name": "SYNTHETIC fixture"}
            for f in facts
        ]
        daily_rows = [
            {k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in d.items()}
            for d in daily
        ]
        return c, hourly_rows, daily_rows

    def test_two_selects_compare_every_hour_day_and_refuse_resubmission(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c, hours, days = self.verification_fixture(root)
            with (
                patch.object(
                    cloud,
                    "status",
                    return_value={"Id": "SYNTHETIC_JOB", "JobRunState": "SUCCEEDED"},
                ),
                patch.object(cloud, "query_rows", side_effect=[hours, days]),
            ):
                result = cloud.verify(c, {}, root, "d" * 64)
                self.assertEqual(result["status"], "PASS")
                self.assertEqual([q["rows"] for q in result["queries"]], [2133, 90])
                with self.assertRaises(ValueError):
                    cloud.verify(c, {}, root, "d" * 64)
            self.assertEqual(c["athena"].start_query_execution.call_count, 2)

    def test_duplicate_hour_stops_before_daily_query_or_acceptance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c, hours, _ = self.verification_fixture(root)
            hours[-1] = hours[0]
            with (
                patch.object(
                    cloud,
                    "status",
                    return_value={"Id": "SYNTHETIC_JOB", "JobRunState": "SUCCEEDED"},
                ),
                patch.object(cloud, "query_rows", return_value=hours),
                self.assertRaises(ValueError),
            ):
                cloud.verify(c, {}, root, "d" * 64)
            self.assertFalse((root / "acceptance.json").exists())
            self.assertEqual(c["athena"].start_query_execution.call_count, 1)

    def packet(self, root):
        source = root / "source"
        source.mkdir()
        raw = b"SYNTHETIC packaging bytes; never an execution fixture\n"
        (source / "records.jsonl").write_bytes(raw)
        (source / "audit.json").write_text(
            json.dumps({"records_sha256": sha(raw), "accepted_unique_records": 17181})
        )
        packet = root / "packet"
        digest = cloud.prepare(packet, source)
        return packet, digest

    def test_packet_is_private_deterministic_and_importable_without_source_tree(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            packet, digest = self.packet(root)
            p, got = cloud.load(packet)
            self.assertEqual(got, digest)
            self.assertEqual(packet.stat().st_mode & 0o777, 0o700)
            self.assertTrue(
                all(v.stat().st_mode & 0o777 == 0o600 for v in packet.iterdir())
            )
            second = root / "second"
            cloud.prepare(second, root / "source")
            self.assertEqual(
                (packet / "payload.zip").read_bytes(),
                (second / "payload.zip").read_bytes(),
            )
            # -I excludes cwd and PYTHONPATH; catch dependencies masked by local imports.
            code = "import sys; sys.path.insert(0,sys.argv[1]); import historical.glue_job; print('PACKAGED_IMPORT_PASS')"
            result = subprocess.run(
                [sys.executable, "-I", "-B", "-c", code, str(packet / "payload.zip")],
                cwd=root,
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertIn("PACKAGED_IMPORT_PASS", result.stdout)
            self.assertEqual(p["athena_selects"], 2)
            (packet / "records.jsonl").write_bytes(b"tampered")
            with self.assertRaises(ValueError):
                cloud.load(packet)

    def test_no_approval_stops_before_sdk_construction(self):
        with tempfile.TemporaryDirectory() as tmp:
            packet, _ = self.packet(Path(tmp))
            with (
                patch.object(
                    sys,
                    "argv",
                    ["historical.cloud", "run-glue", "--packet", str(packet)],
                ),
                patch.object(cloud, "clients") as client,
            ):
                with self.assertRaises(SystemExit):
                    cloud.main()
                client.assert_not_called()

    def test_glue_policy_never_writes_old_data_or_reads_credentials(self):
        for stmt in cloud.role_policy()["Statement"]:
            actions = stmt["Action"]
            resources = stmt["Resource"]
            self.assertFalse(
                any(
                    a.startswith(("secretsmanager:", "sns:", "scheduler:", "iam:"))
                    for a in actions
                )
            )
            if any(
                a in ("s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload")
                for a in actions
            ):
                self.assertTrue(all(cloud.PREFIX in r for r in resources))
            if "glue:CreateTable" in actions:
                self.assertNotIn("*", resources)
                self.assertEqual(len(resources), 8)

    def test_single_job_keeps_compatible_runtime_and_zero_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            packet, digest = self.packet(Path(tmp))
            p, _ = cloud.load(packet)
            clients = {k: Mock() for k in ("iam", "s3", "glue")}
            clients["iam"].get_role_policy.return_value = {
                "PolicyDocument": p["policy"]
            }
            clients["glue"].start_job_run.return_value = {"JobRunId": "SYNTHETIC_JOB"}
            with patch.object(cloud, "preflight"), patch.object(cloud.time, "sleep"):
                cloud.run_glue(clients, p, packet, digest)
                with self.assertRaises(ValueError):
                    cloud.run_glue(clients, p, packet, digest)
            definition = clients["glue"].create_job.call_args.kwargs
            self.assertEqual(
                (
                    definition["GlueVersion"],
                    definition["MaxRetries"],
                    definition["Timeout"],
                    definition["NumberOfWorkers"],
                ),
                ("5.0", 0, 10, 2),
            )
            args = definition["NonOverridableArguments"]
            self.assertNotIn("spark.sql.catalog.demo", args["--conf"])
            self.assertIn("GlueCatalog", args["--conf"])
            self.assertIn("S3FileIO", args["--conf"])
            self.assertEqual(
                args["--extra-jars"],
                cloud.JARS + "runtime.jar," + cloud.JARS + "aws-bundle.jar",
            )
            clients["glue"].start_job_run.assert_called_once_with(JobName=cloud.JOB)
            self.assertEqual(len(clients["s3"].put_object.call_args_list), 4)

    def test_ambiguous_upload_keeps_receipt_and_blocks_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            packet, digest = self.packet(Path(tmp))
            p, _ = cloud.load(packet)
            clients = {k: Mock() for k in ("iam", "s3", "glue")}
            clients["s3"].put_object.side_effect = TimeoutError("SYNTHETIC")
            with patch.object(cloud, "preflight"), patch.object(cloud.time, "sleep"):
                with self.assertRaises(TimeoutError):
                    cloud.run_glue(clients, p, packet, digest)
                with self.assertRaises(ValueError):
                    cloud.run_glue(clients, p, packet, digest)
            value = json.loads((packet / "glue-receipt.json").read_text())
            self.assertEqual(value["operations"][-1]["status"], "UNCERTAIN")
            clients["glue"].start_job_run.assert_not_called()

    def test_athena_pagination_has_one_header_and_preserves_null(self):
        meta = {"ColumnInfo": [{"Name": "id"}, {"Name": "value"}]}

        def page(rows):
            return {
                "ResultSet": {
                    "ResultSetMetadata": meta,
                    "Rows": [
                        {"Data": [{} if x is None else {"VarCharValue": x} for x in r]}
                        for r in rows
                    ],
                }
            }

        client = Mock()
        client.get_paginator.return_value.paginate.return_value = [
            page([["id", "value"], ["1", "0"]]),
            page([["2", None]]),
        ]
        self.assertEqual(
            cloud.query_rows({"athena": client}, "SYNTHETIC"),
            [{"id": "1", "value": "0"}, {"id": "2", "value": None}],
        )

    def test_queries_are_pinned_bounded_and_do_not_touch_old_database(self):
        sql = cloud.query_sql(
            {
                "provenance": {
                    "snapshots": {"hourly": [11], "dimensions": [12], "daily": [13]}
                }
            }
        )
        self.assertTrue(
            all(s.startswith("SELECT ") and cloud.DATABASE in s for s in sql)
        )
        self.assertIn("FOR VERSION AS OF 11", sql[0])
        self.assertIn("FOR VERSION AS OF 12", sql[0])
        self.assertIn("LIMIT 2134", sql[0])
        self.assertIn("LIMIT 91", sql[1])
        self.assertIn("2026-10-01", sql[1])

    def test_api_patch_preserves_every_other_original_entry_and_is_deterministic(self):
        previous = io.BytesIO()
        with zipfile.ZipFile(previous, "w") as z:
            z.writestr("live_data/api.py", "SYNTHETIC original")
            z.writestr("retained/provenance.json", '{"original":true}')
        first = publish.api_zip(previous.getvalue(), cloud.ROOT)
        self.assertEqual(first, publish.api_zip(previous.getvalue(), cloud.ROOT))
        with zipfile.ZipFile(io.BytesIO(first)) as z:
            self.assertEqual(z.read("retained/provenance.json"), b'{"original":true}')
            self.assertEqual(
                z.read("live_data/api.py"),
                (cloud.ROOT / "live_data/api.py").read_bytes(),
            )
            self.assertEqual(len(z.namelist()), 6)
        with self.assertRaises(ValueError):
            publish.api_zip(first, cloud.ROOT)

    def test_api_requires_exact_original_revision_and_iam_auth(self):
        clients = {k: Mock() for k in ("lambda", "iam")}
        config = {
            "CodeSha256": base64.b64encode(bytes.fromhex("a" * 64)).decode(),
            "Role": f"arn:aws:iam::{ACCOUNT}:role/{publish.API}",
            "Runtime": "python3.12",
            "Handler": "live_data.api.handler",
            "State": "Active",
            "LastUpdateStatus": "Successful",
        }
        clients["lambda"].get_function_configuration.return_value = config
        clients["lambda"].get_function_url_config.return_value = {"AuthType": "AWS_IAM"}
        with patch.object(publish, "absent"):
            publish.api_preflight(clients, {"previous_api_sha256": "a" * 64})
            clients["lambda"].get_function_url_config.return_value = {
                "AuthType": "NONE"
            }
            with self.assertRaises(ValueError):
                publish.api_preflight(clients, {"previous_api_sha256": "a" * 64})

    def test_plan_names_the_policy_that_publication_writes(self):
        source = (cloud.ROOT / "historical/cloud.py").read_text()
        self.assertIn(f'"new_api_inline_policy": "{publish.POLICY}"', source)

    def test_api_accepts_only_exact_superseded_run_one_revision(self):
        clients = {k: Mock() for k in ("lambda", "iam")}
        old_key = "historical-demo/" + cloud.SUPERSEDED_RUN + "/outputs/export.json"
        config = {
            "CodeSha256": base64.b64encode(bytes.fromhex("b" * 64)).decode(),
            "Role": f"arn:aws:iam::{ACCOUNT}:role/{publish.API}",
            "Runtime": "python3.12",
            "Handler": "live_data.api.handler",
            "State": "Active",
            "LastUpdateStatus": "Successful",
            "Environment": {"Variables": {"AIRLAKE_HISTORY_EXPORT_KEY": old_key}},
        }
        plan = {
            "previous_api_sha256": "a" * 64,
            "superseded_api": {"code_sha256": "b" * 64, "export_key": old_key},
        }
        clients["lambda"].get_function_configuration.return_value = config
        clients["lambda"].get_function_url_config.return_value = {"AuthType": "AWS_IAM"}
        self.assertNotIn(cloud.SUPERSEDED_RUN, cloud.PREFIX)
        self.assertNotEqual("HistoricalResearch20261114", publish.POLICY)
        with patch.object(publish, "absent"):
            publish.api_preflight(clients, plan)
            for change in (
                {"AIRLAKE_HISTORY_EXPORT_KEY": cloud.PREFIX + "outputs/export.json"},
                {},
            ):
                config["Environment"]["Variables"] = change
                with self.assertRaises(ValueError):
                    publish.api_preflight(clients, plan)
            config["Environment"]["Variables"] = {}
            config["CodeSha256"] = base64.b64encode(bytes.fromhex("a" * 64)).decode()
            publish.api_preflight(clients, plan)  # original API, no history route

    def test_missing_cloud_acceptance_or_signed_http_blocks_publication(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "acceptance.json").write_text(
                json.dumps(
                    {
                        "status": "LOCAL_ONLY",
                        "export_sha256": sha(b"{}"),
                        "hourly_all_rows_match": "PASS",
                        "daily_all_rows_match": "PASS",
                    }
                )
            )
            (root / "cloud-export.json").write_text("{}")
            client = Mock()
            with self.assertRaises(ValueError):
                publish.publish_api(client, {}, root, "digest")
            (root / "history-http.json").write_text('{"status":"FAILED"}')
            with self.assertRaises(ValueError):
                publish.publish_site(client, {}, root, "digest")
            self.assertEqual(client.mock_calls, [])

    def test_signed_query_bytes_match_what_lambda_urls_canonicalize(self):
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        from botocore.credentials import Credentials

        url = publish.signed_url(
            "https://x.lambda-url.ap-southeast-2.on.aws/api/v1/history"
            "?station_id=6123215&limit=1000&start=2026-09-23T17:00:00Z"
        )
        self.assertTrue(url.endswith("start=2026-09-23T17%3A00%3A00Z"))
        signer = SigV4Auth(Credentials("SYNTHETIC", "SYNTHETIC"), "lambda", "x")
        self.assertEqual(
            signer.canonical_query_string(AWSRequest(method="GET", url=url)),
            "limit=1000&start=2026-09-23T17%3A00%3A00Z&station_id=6123215",
        )
        plain = "https://x.lambda-url.ap-southeast-2.on.aws/api/v1/latest"
        self.assertEqual(publish.signed_url(plain), plain)

    def resume_fixture(self, root, statuses=(200, 200, 403)):
        ex = {
            "provenance": {"window_end_utc": "2026-09-30T17:00:00Z"},
            "facts": [],
            "daily": [],
        }
        (root / "cloud-export.json").write_text(json.dumps(ex))
        (root / "acceptance.json").write_text(json.dumps({"export_sha256": "E"}))
        paths = publish.smoke_paths(ex)
        (root / "history-http.json").write_text(
            json.dumps(
                {
                    "status": "RUNNING",
                    "requests": [
                        {"path": a, "status": b}
                        for a, b in zip(paths, statuses, strict=False)
                    ],
                }
            )
        )
        (root / "api-publication.json").write_text(
            json.dumps({"operations": [{"identifiers": {"CodeSha256": "C"}}]})
        )
        c = {"lambda": Mock()}
        c["lambda"].get_function_configuration.return_value = {
            "CodeSha256": "C",
            "Environment": {"Variables": {"AIRLAKE_HISTORY_EXPORT_SHA256": "E"}},
        }
        c["lambda"].get_function_url_config.return_value = {
            "AuthType": "AWS_IAM",
            "FunctionUrl": "https://x.lambda-url.ap-southeast-2.on.aws/",
        }
        return c, paths

    def test_resume_reads_only_the_three_unverified_paths_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c, paths = self.resume_fixture(root)
            digest = "D" * 64
            with self.assertRaises(ValueError):
                publish.smoke_api_resume(c, {}, root, digest, None)
            confirm = publish.prepare_amendment(root, digest)
            with self.assertRaises(ValueError):
                publish.load_amendment(root, digest, "0" * 64)
            amendment = publish.load_amendment(root, digest, confirm)
            self.assertEqual(amendment["remaining_paths"], paths[2:])
            self.assertFalse(publish.api_accepted(root, amendment))
            bodies = [
                {"count": 168},
                {
                    "total": 14,
                    "metadata": {
                        "export_sha256": "d3226cf36a89bf6d543d4eb2f075da3c912a2f0ea8e12db48cd4fcb0dbc8b803"
                    },
                },
                {"count": 3},
            ]
            seen = []

            def get(path):
                seen.append(path)
                return Mock(
                    status_code=200, json=Mock(return_value=bodies[len(seen) - 1])
                )

            result = publish.smoke_api_resume(c, {}, root, digest, amendment, get)
            self.assertEqual(seen, paths[2:])
            self.assertEqual(result["status"], "PASS_REMAINING_THREE_SIGNED_READS")
            self.assertTrue(publish.api_accepted(root, amendment))
            self.assertFalse(publish.api_accepted(root, None))
            with self.assertRaises(ValueError):
                publish.smoke_api_resume(c, {}, root, digest, amendment, get)
            self.assertEqual(len(seen), 3)

    def test_resume_refuses_any_other_first_attempt_or_changed_api(self):
        for statuses in ((200, 403), (200, 200, 500), (200, 200, 200, 403)):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                self.resume_fixture(root, statuses)
                with self.assertRaises(ValueError):
                    publish.prepare_amendment(root, "D" * 64)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c, _ = self.resume_fixture(root)
            amendment = publish.load_amendment(
                root, "D" * 64, publish.prepare_amendment(root, "D" * 64)
            )
            c["lambda"].get_function_configuration.return_value["CodeSha256"] = "X"
            get = Mock()
            with self.assertRaises(ValueError):
                publish.smoke_api_resume(c, {}, root, "D" * 64, amendment, get)
            get.assert_not_called()

    def test_amendment_overrides_only_publish_and_cloud_helper_is_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.resume_fixture(root)
            publish.prepare_amendment(root, "D" * 64)
            a = json.loads((root / publish.AMENDMENT).read_text())
            self.assertEqual(
                list(a["website_source_sha256_overrides"]), ["historical/publish.py"]
            )
            self.assertEqual(a["remaining_signed_gets"], 3)
        self.assertEqual(
            hashlib.sha256(Path(cloud.__file__).read_bytes()).hexdigest(),
            "c6c762faea90cbcfa314ead5731cfb32e5a588f04f62f90f9e6dc7587c068828",
            "the run-2 plan pins cloud.py",
        )


if __name__ == "__main__":
    unittest.main()
