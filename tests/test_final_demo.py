"""Offline orchestration, packaging and Lambda tests; synthetic values only."""

import copy
import io
import json
import os
import sys
import tempfile
import unittest
import zipfile
from contextlib import redirect_stdout
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from clean_api.data import EVIDENCE_SHA256, STATIONS, Dataset
from final_demo import glue_job, lambda_api
from final_demo.contract import (
    SOURCES,
    digest,
    encode,
    new_plan,
    validate_computed,
    validate_plan,
)
from final_demo.control import Receipt, absent, empty
from final_demo.prepare import job_definition, policy, prepare


def reference():
    dimensions = [
        {
            "location_id": i,
            "name": f"SYNTHETIC station {n}",
            "lat": 21.0,
            "lon": 105.0,
            "station_type": "reference" if n < 2 else "low_cost",
        }
        for n, i in enumerate(STATIONS)
    ]
    facts = [
        {
            "location_id": i,
            "hour_start": (
                datetime(2026, 11, 7, 2, tzinfo=UTC) + timedelta(hours=h)
            ).isoformat(),
            "pm25_avg": float(h),
            "aqi": h * 2,
        }
        for i in STATIONS
        for h in range(24)
    ]
    return {
        "facts": facts,
        "dimensions": dimensions,
        "evidence_sha256": EVIDENCE_SHA256,
    }


def export_value():
    r = reference()
    r["provenance"] = {
        "execution": "AWS_GLUE",
        "run": "20261114_aabbccdd",
        "database": "airlake_final_20261114_aabbccdd",
        "source_snapshots": SOURCES,
        "output_snapshots": {
            "hourly": [123],
            "dimensions": [456],
            "breakpoints": [789],
        },
    }
    return r


class ContractTests(unittest.TestCase):
    def test_plan_rejects_account_region_scope_sources_and_cost_escalation(self):
        for key, value in [
            ("account", "000000000000"),
            ("region", "us-east-1"),
            ("prefix", "integration-real/"),
            ("database", "airlake_gold"),
            ("workers", 10),
            ("timeout_minutes", 100),
            ("max_retries", 1),
            ("sources", {}),
        ]:
            p = new_plan("20261114_aabbccdd")
            p[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_plan(p)

    def test_computed_comparison_rejects_wrong_means_or_orphans(self):
        r = reference()
        validate_computed(r["facts"], r["dimensions"], r)
        for key, value in [
            ("pm25_avg", 999.0),
            ("pm25_avg", float("nan")),
            ("location_id", 999),
            ("aqi", 501),
        ]:
            facts = copy.deepcopy(r["facts"])
            facts[0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_computed(facts, r["dimensions"], r)

    def test_custom_runtime_config_has_no_automatic_dependency_resolution(self):
        p = new_plan("20261114_aabbccdd")
        job = job_definition(p, "a" * 64)
        a = job["NonOverridableArguments"]
        self.assertEqual(
            ("5.0", 2, "G.1X", 10, 0),
            (
                job["GlueVersion"],
                job["NumberOfWorkers"],
                job["WorkerType"],
                job["Timeout"],
                job["MaxRetries"],
            ),
        )
        self.assertEqual("true", a["--user-jars-first"])
        self.assertNotIn("--datalake-formats", a)
        self.assertNotIn("--packages", a)
        self.assertIn("format-version=2", a["--conf"])
        self.assertIn("timeZone=UTC", a["--conf"])

    def test_iam_only_writes_isolated_scope_api_reads_one_object(self):
        p = new_plan("20261114_aabbccdd")
        pol = policy(p)
        for s in pol["Statement"]:
            if any(x in s["Action"] for x in ["s3:PutObject", "s3:DeleteObject"]):
                self.assertIn(p["prefix"], json.dumps(s["Resource"]))
                self.assertNotIn("integration-real", json.dumps(s["Resource"]))
                self.assertNotIn("artifacts/", json.dumps(s["Resource"]))
        api = policy(p, True)
        s3 = [s for s in api["Statement"] if "s3:GetObject" in s["Action"]]
        self.assertEqual(1, len(s3))
        self.assertTrue(s3[0]["Resource"].endswith(p["prefix"] + "exports/gold.json"))
        self.assertFalse(
            any("athena:StartQueryExecution" in s["Action"] for s in api["Statement"])
        )

    def test_absence_and_inventory_fail_closed(self):
        from botocore.exceptions import ClientError

        absent(
            Mock(
                side_effect=ClientError(
                    {"Error": {"Code": "EntityNotFoundException"}}, "GetDatabase"
                )
            ),
            {"EntityNotFoundException"},
        )
        with self.assertRaises(ClientError):
            absent(
                Mock(
                    side_effect=ClientError(
                        {"Error": {"Code": "AccessDeniedException"}}, "GetDatabase"
                    )
                ),
                {"EntityNotFoundException"},
            )
        with self.assertRaises(ValueError):
            absent(Mock(return_value={}), {"EntityNotFoundException"})
        s3 = Mock()
        s3.get_paginator.return_value.paginate.return_value = [
            {},
            {"Contents": [{"Key": "existing"}]},
        ]
        with self.assertRaises(ValueError):
            empty(s3, new_plan("20261114_aabbccdd"))

    def test_ambiguous_write_receipt_is_retained_and_never_retried(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "receipt.json"
            r = Receipt(p, "a" * 64)
            call = Mock(side_effect=TimeoutError())
            with self.assertRaises(TimeoutError):
                r.write("test-write", call)
            self.assertEqual(
                "UNCERTAIN", json.loads(p.read_text())["operations"][0]["status"]
            )
            call.assert_called_once()
            with self.assertRaises(ValueError):
                Receipt(p, "a" * 64)

    def test_prepare_private_packet_includes_unmodified_production_code(self):
        r = reference()
        data = Dataset(r["facts"], r["dimensions"], evidence_sha256=EVIDENCE_SHA256)
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            e = root / "e.json"
            e.write_text(
                json.dumps(
                    {"hourly_rows": r["facts"], "dimension_rows": r["dimensions"]}
                )
            )
            j = root / "j.jar"
            j.write_bytes(b"synthetic-dependency")
            with (
                patch("final_demo.prepare.load_evidence", return_value=data),
                patch(
                    "final_demo.prepare.JARS",
                    {
                        "runtime.jar": digest(j.read_bytes()),
                        "aws-bundle.jar": digest(j.read_bytes()),
                    },
                ),
            ):
                p, sha = prepare(root / "packet", e, j, j, run="20261114_aabbccdd")
            self.assertEqual(sha, digest((root / "packet/plan.json").read_bytes()))
            self.assertEqual(0o700, (root / "packet").stat().st_mode & 0o777)
            with zipfile.ZipFile(root / "packet/payload.zip") as z:
                for name, expected in p["production_sha256"].items():
                    self.assertEqual(expected, digest(z.read(name)))


class JobOrchestrationTests(unittest.TestCase):
    def environment(self, bad_candidate=False):
        p = new_plan("20261114_aabbccdd")
        p["production_sha256"] = {
            "scripts/gold_batch.py": digest(b"gold"),
            "scripts/silver_batch.py": digest(b"silver"),
        }
        gold = Mock()
        gold.__file__ = "gold.py"
        gold.__loader__ = Mock()
        gold.__loader__.get_data.return_value = b"gold"
        gold.run_gold_batch.return_value = {
            "hourly_sha256": "same",
            "dimension_sha256": "same",
        }
        silver = Mock()
        silver.__file__ = "silver.py"
        silver.__loader__ = Mock()
        silver.__loader__.get_data.return_value = b"silver"
        silver.assert_silver_key_integrity = Mock(return_value=(0, 0))
        spark = Mock()
        spark.version = "3.5.4"
        spark.conf.get.return_value = "UTC"
        spark._jvm.org.apache.iceberg.IcebergBuild.version.return_value = "1.12.0"
        spark.read.format.return_value.option.return_value.load.return_value.cache.return_value.limit.return_value.count.return_value = 559
        spark.table.return_value.select.return_value.distinct.return_value.collect.return_value = [
            [x] for x in STATIONS
        ]
        spark.table.return_value.count.return_value = 7
        spark.sql.return_value.collect.return_value = [
            {"key": "format-version", "value": "2"}
        ]
        gold.build_hourly_aqi_source.return_value = (Mock(), {})
        r = reference()
        facts = copy.deepcopy(r["facts"])
        if bad_candidate:
            facts[0]["pm25_avg"] = 999.0
        return (
            p,
            gold,
            silver,
            spark,
            r,
            [facts, r["dimensions"], r["facts"], r["dimensions"]],
        )

    def test_job_reuses_gold_preflight_and_merges_and_checks_unchanged_rerun(self):
        p, g, s, spark, r, rows = self.environment()
        with (
            patch.dict(
                "sys.modules", {"scripts.gold_batch": g, "scripts.silver_batch": s}
            ),
            patch("final_demo.glue_job.records", side_effect=rows),
            patch(
                "final_demo.glue_job.snapshots",
                return_value={"hourly": [1], "dimensions": [2], "breakpoints": [3]},
            ),
        ):
            _, e = glue_job.execute(spark, p, r)
        self.assertEqual("PASS", e["status"])
        self.assertEqual(2, g.run_gold_batch.call_count)
        self.assertEqual("final_demo_silver", g.SILVER_TABLE)
        self.assertIn(p["database"], g.HOURLY_TABLE)

    def test_bad_candidate_rejects_before_any_production_gold_write(self):
        p, g, s, spark, r, rows = self.environment(True)
        with (
            patch.dict(
                "sys.modules", {"scripts.gold_batch": g, "scripts.silver_batch": s}
            ),
            patch("final_demo.glue_job.records", side_effect=rows),
            self.assertRaises(ValueError),
        ):
            glue_job.execute(spark, p, r)
        g.run_gold_batch.assert_not_called()

    def test_duplicate_silver_and_wrong_runtime_reject_before_writes(self):
        for mode in ("duplicate", "runtime", "utc"):
            p, g, s, spark, r, _rows = self.environment()
            if mode == "duplicate":
                s.assert_silver_key_integrity.return_value = (0, 1)
            elif mode == "runtime":
                spark._jvm.org.apache.iceberg.IcebergBuild.version.return_value = (
                    "1.7.1"
                )
            else:
                spark.conf.get.return_value = "Asia/Bangkok"
            with (
                patch.dict(
                    "sys.modules", {"scripts.gold_batch": g, "scripts.silver_batch": s}
                ),
                self.assertRaises(ValueError),
            ):
                glue_job.execute(spark, p, r)
            g.run_gold_batch.assert_not_called()


class LambdaTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.dict(os.environ, {"AIRLAKE_FINAL_RUN": "20261114_aabbccdd"})
        )
        self.enterContext(patch.object(lambda_api, "_cached", None))
        self.value = export_value()
        self.raw = encode(self.value)
        os.environ["AIRLAKE_EXPORT_SHA256"] = digest(self.raw)

    def event(self, path="/api/v1/gold/hourly", query="limit=100", method="GET"):
        return {
            "rawPath": path,
            "rawQueryString": query,
            "requestContext": {
                "http": {"method": method},
                "authorizer": {"iam": {"accountId": "701364614078"}},
            },
        }

    def data(self):
        return lambda_api.CloudDataset(self.value, digest(self.raw))

    def test_unauthenticated_and_other_account_rejected_before_s3(self):
        for event in ({}, self.event()):
            if event:
                event["requestContext"]["authorizer"]["iam"]["accountId"] = (
                    "000000000000"
                )
            with patch("final_demo.lambda_api.load_dataset") as load:
                self.assertEqual(403, lambda_api.handler(event, None)["statusCode"])
                load.assert_not_called()

    def test_json_csv_health_stations_and_filter_share_existing_api_logic(self):
        with patch("final_demo.lambda_api.load_dataset", return_value=self.data()):
            result = lambda_api.handler(self.event(), None)
            self.assertEqual(72, json.loads(result["body"])["total"])
            result = lambda_api.handler(
                self.event(query="location_id=4946812&format=csv"), None
            )
            self.assertEqual(
                "text/csv; charset=utf-8", result["headers"]["content-type"]
            )
            result = lambda_api.handler(self.event("/api/v1/health", ""), None)
            self.assertEqual("not_checked", json.loads(result["body"])["athena_health"])
            result = lambda_api.handler(self.event("/api/v1/stations", ""), None)
            self.assertEqual(3, json.loads(result["body"])["count"])
            self.assertEqual(
                "airlake_final_20261114_aabbccdd",
                json.loads(result["body"])["metadata"]["provenance"]["database"],
            )

    def test_invalid_input_writes_and_unknown_routes_reject(self):
        with patch("final_demo.lambda_api.load_dataset", return_value=self.data()):
            self.assertEqual(
                400,
                lambda_api.handler(self.event(query="limit=99999"), None)["statusCode"],
            )
            self.assertEqual(
                405, lambda_api.handler(self.event(method="POST"), None)["statusCode"]
            )
            self.assertEqual(
                404, lambda_api.handler(self.event("/missing"), None)["statusCode"]
            )

    def test_s3_export_hash_rejects_corruption_and_reads_exact_object(self):
        s3 = Mock()
        s3.get_object.return_value = {"Body": io.BytesIO(self.raw)}
        self.assertEqual(72, len(lambda_api.load_dataset(s3).rows))
        self.assertTrue(
            s3.get_object.call_args.kwargs["Key"].endswith("/exports/gold.json")
        )
        lambda_api._cached = None
        s3.get_object.return_value = {"Body": io.BytesIO(b"corrupt")}
        with self.assertRaises(ValueError):
            lambda_api.load_dataset(s3)

    def test_corrupt_export_is_503_without_sdk_error_details(self):
        with patch(
            "final_demo.lambda_api.load_dataset",
            side_effect=ValueError("SENSITIVE_SENTINEL"),
        ):
            response = lambda_api.handler(self.event(), None)
        self.assertEqual(503, response["statusCode"])
        self.assertNotIn("SENSITIVE_SENTINEL", response["body"])

    def test_export_wrong_source_or_snapshot_cannot_claim_cloud_provenance(self):
        for kind in ("source", "snapshot"):
            v = export_value()
            if kind == "source":
                v["provenance"]["source_snapshots"] = {}
            else:
                v["provenance"]["output_snapshots"]["hourly"] = []
            with self.assertRaises(ValueError):
                lambda_api.CloudDataset(v, "a" * 64)


class HTTPRunnerTests(unittest.TestCase):
    def test_signed_smoke_makes_eight_bounded_gets_and_preserves_values(self):
        from urllib.parse import parse_qs, urlsplit

        from final_demo import http_smoke

        p = new_plan("20261114_aabbccdd")
        r = reference()
        r["facts"] = [
            {
                "location_id": location,
                "hour_start": (
                    datetime(2026, 11, 7, 2, tzinfo=UTC) + timedelta(hours=h)
                ).isoformat(),
                "pm25_avg": float(h),
                "aqi": h,
            }
            for location, count in zip(STATIONS, (25, 23, 24), strict=True)
            for h in range(count)
        ]
        data = Dataset(r["facts"], r["dimensions"], evidence_sha256=EVIDENCE_SHA256)
        calls = []

        def get(url, **kwargs):
            calls.append(url)
            response = Mock()
            if len(calls) == 1:
                response.status_code = 403
                return response
            response.status_code = 200
            u = urlsplit(url)
            if u.path.endswith("/health"):
                value = {"backend": "s3_verified_aws_glue_export"}
            elif u.path.endswith("/stations"):
                value = {"data": list(data.stations), "count": 3}
            else:
                value, fmt = data.page(parse_qs(u.query))
                value["metadata"]["provenance"]["database"] = p["database"]
                if fmt == "csv":
                    import csv

                    buffer = io.StringIO()
                    writer = csv.DictWriter(buffer, fieldnames=list(data.rows[0]))
                    writer.writeheader()
                    writer.writerows(data.rows)
                    response.text = buffer.getvalue()
            response.json.return_value = value
            return response

        session = Mock()
        session.client.return_value.get_caller_identity.return_value = {
            "Account": "701364614078"
        }
        session.client.return_value.get_function_url_config.return_value = {
            "AuthType": "AWS_IAM",
            "FunctionUrl": "https://synthetic-test.lambda-url.ap-southeast-2.on.aws/",
        }
        http = Mock()
        http.get.side_effect = get
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            raw = encode(p)
            (folder / "plan.json").write_bytes(raw)
            (folder / "reference.json").write_bytes(encode(r))
            with (
                patch.object(
                    sys,
                    "argv",
                    [
                        "http_smoke",
                        "--packet",
                        td,
                        "--execute",
                        "--confirm-plan-sha256",
                        digest(raw),
                    ],
                ),
                patch("boto3.Session", return_value=session),
                patch("requests.Session", return_value=http),
                patch("botocore.auth.SigV4Auth"),
                redirect_stdout(io.StringIO()),
            ):
                http_smoke.main()
            self.assertEqual(
                "PASS", json.loads((folder / "http-result.json").read_text())["status"]
            )
        self.assertEqual(8, len(calls))
        self.assertTrue(all("/api/v1/" in path for path in calls))


if __name__ == "__main__":
    unittest.main()
