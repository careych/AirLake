"""Offline configuration and source-selection guards; no Spark or AWS execution."""

import ast
import csv
import gzip
import hashlib
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

REPOSITORY = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(
        "scripts." + name, REPOSITORY / "scripts" / (name + ".py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


config = load("pipeline_config")
batch = load("real_batch")


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.cloud = config.CatalogConfig(
            mode="glue",
            catalog="cloudreal",
            warehouse="s3://fixture-bucket/integration-real/fixture/",
            account="701364614078",
            bronze_namespace="airlake_it2_fixture_bronze",
            silver_namespace="airlake_it2_fixture_silver",
            gold_namespace="airlake_it2_fixture_gold",
        )

    def test_defaults_preserve_local_identifiers(self):
        c = config.CatalogConfig()
        self.assertEqual("local.bronze.aq_measurements", c.tables()["BRONZE_TABLE"])
        self.assertEqual("local.gold.pm25_aqi_hourly", c.tables()["HOURLY_TABLE"])
        self.assertEqual(
            "./warehouse", c.settings()["spark.sql.catalog.local.warehouse"]
        )
        self.assertEqual("hadoop", c.settings()["spark.sql.catalog.local.type"])

    def test_custom_identifiers_bind_all_five_tables(self):
        c = replace(
            config.CatalogConfig(),
            catalog="fixture",
            bronze_namespace="b",
            silver_namespace="s",
            gold_namespace="g",
            hourly_table="hours",
        )
        namespace = dict.fromkeys(c.tables())
        c.bind(namespace)
        self.assertEqual("fixture.g.hours", namespace["HOURLY_TABLE"])
        self.assertEqual("fixture.s.aqi_breakpoints", namespace["BREAKPOINT_TABLE"])
        self.assertEqual(5, len(namespace))

    def test_sql_injection_identifiers_are_rejected(self):
        for field in (
            "catalog",
            "bronze_namespace",
            "silver_namespace",
            "gold_namespace",
            "hourly_table",
        ):
            with self.subTest(field=field), self.assertRaises(ValueError):
                replace(config.CatalogConfig(), **{field: "x; DROP TABLE y"}).validate()

    def test_remote_hadoop_warehouse_is_rejected(self):
        for path in (
            "s3://bucket/path",
            "hdfs://remote/warehouse",
            "file://remote/path",
            " ",
        ):
            with self.subTest(path=path), self.assertRaises(ValueError):
                replace(config.CatalogConfig(), warehouse=path).validate()

    def test_table_identifier_collisions_are_rejected(self):
        for change in (
            {"dimension_table": "pm25_aqi_hourly"},
            {"breakpoint_table": "aq_measurements"},
            {"bronze_namespace": "silver"},
        ):
            with (
                self.subTest(change=change),
                self.assertRaisesRegex(ValueError, "distinct"),
            ):
                replace(config.CatalogConfig(), **change).validate()

    def test_reserved_catalog_names_are_rejected(self):
        for name in ("spark_catalog", "global_temp"):
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(ValueError, "reserved"),
            ):
                replace(config.CatalogConfig(), catalog=name).validate()

    def test_cloud_configuration_is_exact_and_does_not_use_hadoop(self):
        settings = self.cloud.settings()
        self.assertEqual(
            "org.apache.iceberg.aws.glue.GlueCatalog",
            settings["spark.sql.catalog.cloudreal.catalog-impl"],
        )
        self.assertEqual(
            "org.apache.iceberg.aws.s3.S3FileIO",
            settings["spark.sql.catalog.cloudreal.io-impl"],
        )
        self.assertNotIn("spark.sql.catalog.cloudreal.type", settings)
        self.assertEqual("", settings["spark.jars.packages"])

    def test_cloud_scope_region_and_account_fail_closed(self):
        for change in (
            {"region": "us-east-1"},
            {"account": "wrong"},
            {"warehouse": "s3://bucket/warehouse/"},
            {"warehouse": "s3://bucket/integration-real/../warehouse"},
            {"gold_namespace": "airlake_gold"},
            {"gold_namespace": self.cloud.silver_namespace},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                replace(self.cloud, **change).validate()

    def test_cloud_requires_opt_in_and_matching_account_before_spark(self):
        for c in (
            self.cloud,
            replace(self.cloud, allow_cloud_write=True),
            replace(self.cloud, allow_cloud_write=True, confirm_account="000000000000"),
        ):
            with self.assertRaisesRegex(ValueError, "Cloud execution requires"):
                c.create_spark("must-not-start")

    def test_static_credentials_and_redirects_reject_before_dependency_loading(self):
        c = replace(
            self.cloud, allow_cloud_write=True, confirm_account=self.cloud.account
        )
        for name in (
            "AWS_ACCESS_KEY_ID",
            "AWS_ENDPOINT_URL_S3",
            "PYSPARK_GATEWAY_PORT",
            "SPARK_CONF_DIR",
            "SPARK_REMOTE",
            "MASTER",
            "HADOOP_CLASSPATH",
        ):
            with (
                self.subTest(name=name),
                patch.dict("os.environ", {name: "fixture"}),
                self.assertRaisesRegex(ValueError, "Unsafe inherited"),
            ):
                c.create_spark("must-not-start")

    def test_missing_cloud_jars_fail_before_spark_or_aws(self):
        c = replace(
            self.cloud, allow_cloud_write=True, confirm_account=self.cloud.account
        )
        with (
            patch.dict("os.environ", {}, clear=True),
            self.assertRaisesRegex(ValueError, "verified Iceberg"),
        ):
            c.create_spark("must-not-start")

    def test_cli_plan_without_execution_gate(self):
        args = config.parser("fixture").parse_args(["--plan"])
        self.assertTrue(args.plan)
        self.assertEqual(config.CatalogConfig(), config.from_args(args))

    def test_gold_business_functions_are_identical_to_verified_baseline(self):
        original = subprocess.check_output(
            [
                "git",
                "-C",
                str(REPOSITORY),
                "show",
                "ba031316a1678c222da58e40c160e8e6e7f0f992:scripts/gold_batch.py",
            ],
            text=True,
        )
        before = {
            n.name: n
            for n in ast.parse(original).body
            if isinstance(n, ast.FunctionDef)
        }
        after = {
            n.name: n
            for n in ast.parse((REPOSITORY / "scripts/gold_batch.py").read_text()).body
            if isinstance(n, ast.FunctionDef)
        }
        for name in before.keys() - {
            "main",
            "create_spark_session",
            "ensure_breakpoint_table",
            "create_gold_tables",
        }:
            with self.subTest(function=name):
                self.assertEqual(ast.dump(before[name]), ast.dump(after[name]))

    def test_historical_gold_record_is_unchanged(self):
        name = "tests/evidence/gold_dimension_name_repair_2026-10-25.json"
        original = subprocess.check_output(
            [
                "git",
                "-C",
                str(REPOSITORY),
                "show",
                "ba031316a1678c222da58e40c160e8e6e7f0f992:" + name,
            ]
        )
        self.assertEqual(original, (REPOSITORY / name).read_bytes())

    def test_frozen_real_data_and_separately_verified_wiring_match(self):
        evidence = json.loads(
            (REPOSITORY / "tests/evidence/real_openaq_2026-11-06.json").read_text()
        )
        self.assertEqual("PASS_REAL_KAFKA_BRONZE_SILVER_GOLD", evidence["status"])
        wiring = json.loads(
            (REPOSITORY / "tests/evidence/cloud_wiring_readiness.json").read_text()
        )
        self.assertEqual("PASS", wiring["offline_integration"]["status"])
        self.assertEqual(
            wiring["retained_acceptance_sha256"],
            hashlib.sha256(
                (REPOSITORY / "tests/evidence/real_openaq_2026-11-06.json").read_bytes()
            ).hexdigest(),
        )
        for name, digest in evidence["source_sha256"].items():
            with self.subTest(path=name):
                if name in wiring["current_source_sha256"]:
                    self.assertEqual(digest, wiring["previous_source_sha256"][name])
                    digest = wiring["current_source_sha256"][name]
                self.assertEqual(
                    digest, hashlib.sha256((REPOSITORY / name).read_bytes()).hexdigest()
                )


class SelectionTests(unittest.TestCase):
    def test_optimized_python_cannot_bypass_acceptance_assertions(self):
        result = subprocess.run(
            [sys.executable, "-B", "-O", str(REPOSITORY / "scripts/real_batch.py")],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(0, result.returncode)
        self.assertIn("requires Python assertions enabled", result.stderr)

    def row(self, **updates):
        row = dict(
            zip(
                batch.FIELDS,
                [
                    "4946812",
                    "13502151",
                    "Fixture station",
                    "2025-07-03T22:40:00+07:00",
                    "21.0",
                    "105.8",
                    "pm25",
                    "µg/m³",
                    "22.28",
                ],
            )
        )
        row.update(updates)
        return row

    def test_original_measurement_time_and_zero_are_accepted(self):
        self.assertEqual(
            "2025-07-03T15:40:00+00:00",
            batch.valid_record(self.row(value="0")).isoformat(),
        )

    def test_invalid_measurements_and_metadata_are_rejected(self):
        for updates in (
            {"value": "nan"},
            {"value": "inf"},
            {"value": "-inf"},
            {"value": "-1"},
            {"location": ""},
            {"lat": "inf"},
            {"lon": "181"},
            {"units": "mg/m3"},
            {"datetime": "2025-07-03T22:40:00"},
            {"sensors_id": "wrong"},
        ):
            with (
                self.subTest(updates=updates),
                self.assertRaises((ValueError, TypeError)),
            ):
                batch.valid_record(self.row(**updates))

    def test_selection_is_deterministic_bounded_and_preserves_raw_fields(self):
        with tempfile.TemporaryDirectory(prefix="airlake-selection-unit-") as directory:
            root = Path(directory)
            (root / "4946812").mkdir()
            path = root / "4946812/source.csv.gz"
            with gzip.open(path, "wt", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=batch.FIELDS)
                writer.writeheader()
                writer.writerows(
                    [
                        self.row(parameter="pm10"),
                        self.row(value="nan"),
                        self.row(),
                        self.row(datetime="2025-07-03T22:45:00+07:00"),
                    ]
                )
            one = batch.select_records(root, 1)
            two = batch.select_records(root, 1)
            self.assertEqual(one, two)
            self.assertEqual([self.row()], one[0])
            self.assertEqual(3, one[1]["scanned_rows"])
            self.assertEqual(1, one[1]["excluded_non_pm25"])
            self.assertEqual(1, sum(one[1]["rejected_pm25_by_reason"].values()))
            self.assertEqual(3, one[1]["row_provenance"][0]["data_row"])
            json.dumps(one[1])

    def test_no_source_and_invalid_caps_fail(self):
        with tempfile.TemporaryDirectory(prefix="airlake-selection-unit-") as directory:
            for cap in (0, 1001, -1, 1):
                with self.subTest(cap=cap), self.assertRaises(ValueError):
                    batch.select_records(Path(directory), cap)


if __name__ == "__main__":
    unittest.main()
