"""Offline safety checks of the real runner with inert AWS and Spark adapters."""

import argparse
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from zipfile import ZipFile

REPOSITORY = Path(__file__).resolve().parents[1]
EXPECTED_ROWS = [(1, "alpha", 10.0), (2, "beta", 0.0), (3, "gamma", 25.0)]


class AWSIcebergSmokeTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch("socket.socket", side_effect=AssertionError("Offline test"))
        )
        self.enterContext(
            patch("subprocess.run", side_effect=AssertionError("No child commands"))
        )
        self.enterContext(
            patch("subprocess.Popen", side_effect=AssertionError("No Spark child"))
        )
        self.enterContext(patch.dict("sys.modules", {"pyspark": None}))
        self.enterContext(patch.dict(os.environ, {}, clear=True))
        spec = importlib.util.spec_from_file_location(
            "aws_smoke_subject", REPOSITORY / "scripts/aws_iceberg_smoke.py"
        )
        self.subject = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.subject)
        self.root = Path(
            self.enterContext(
                tempfile.TemporaryDirectory(prefix="airlake-aws-smoke-unit-")
            )
        ).resolve()
        if self.root.resolve().is_relative_to(REPOSITORY):
            raise RuntimeError("Unit fixtures require an external temporary directory")
        self.output = io.StringIO()
        self.errors = io.StringIO()
        self.enterContext(contextlib.redirect_stdout(self.output))
        self.enterContext(contextlib.redirect_stderr(self.errors))
        self.target = self.subject.plan("20261025_" + "a" * 32)
        self.report = {
            "core": {"ready": True},
            "aws_bundle": {"ready": True, "login_provider_present": True},
            "pyspark": "3.5.9",
            "python_supported": True,
            "java_home_available": True,
            "aws_cli_available": True,
        }
        self.args = argparse.Namespace(
            runtime_jar=self.root / "core.jar",
            aws_bundle_jar=self.root / "aws.jar",
            aws_bundle_sha256="b" * 64,
            spark_profile="airlake",
        )
        self.responses = {
            ("sts", "get-caller-identity"): {"Account": self.subject.ACCOUNT},
            ("s3api", "head-bucket"): {},
            ("s3api", "get-bucket-location"): {
                "LocationConstraint": self.subject.REGION
            },
            ("s3api", "get-bucket-versioning"): {},
            ("s3api", "list-objects-v2"): {
                "Name": self.subject.BUCKET,
                "Prefix": self.target["prefix"],
                "KeyCount": 0,
                "IsTruncated": False,
            },
            ("glue", "get-database"): None,
            ("glue", "get-table"): {
                "Table": {
                    "Parameters": {
                        "metadata_location": self.target["table_location"]
                        + "/metadata/fixture.json"
                    }
                }
            },
            ("athena", "get-work-group"): {
                "WorkGroup": {
                    "State": "ENABLED",
                    "Configuration": {
                        "EngineVersion": {
                            "EffectiveEngineVersion": "Athena engine version 3"
                        },
                        "EnforceWorkGroupConfiguration": True,
                        "ResultConfiguration": {"OutputLocation": self.subject.RESULTS},
                        "BytesScannedCutoffPerQuery": 10737418240,
                    },
                }
            },
        }
        self.aws = Mock()
        self.aws.call.side_effect = lambda service, operation, *args: self.responses[
            (service, operation)
        ]

    def jar(self, classes):
        path = self.root / "fixture.jar"
        with ZipFile(path, "w") as handle:
            for name in classes:
                handle.writestr(
                    name.replace(".", "/") + ".class", b"inert; never loaded"
                )
        return path, hashlib.sha256(path.read_bytes()).hexdigest()

    def fake_spark(self):
        spark = Mock()
        frame = spark.table.return_value
        frame.schema.fields = [
            SimpleNamespace(
                name=n, dataType=SimpleNamespace(simpleString=lambda t=t: t)
            )
            for n, t in (("id", "int"), ("label", "string"), ("pm25", "double"))
        ]
        frame.orderBy.return_value.collect.return_value = EXPECTED_ROWS
        frame.select.return_value.collect.side_effect = [
            [SimpleNamespace(snapshot_id=123, operation="append")],
            [
                SimpleNamespace(
                    file_path=self.target["table_location"] + "/data/fixture.parquet"
                )
            ],
        ]
        iceberg = spark.sparkContext._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable.return_value
        iceberg.location.return_value = self.target["table_location"]
        iceberg.operations.return_value.current.return_value.formatVersion.return_value = 2
        iceberg.properties.return_value.get.return_value = "parquet"
        return spark

    def test_import_does_not_import_spark_or_launch_commands(self):
        self.assertTrue(callable(self.subject.main))

    def test_default_is_offline_dry_run_with_exact_rows(self):
        with patch.object(self.subject, "dependencies", return_value=self.report):
            self.assertEqual(0, self.subject.main([]))
        output = json.loads(self.output.getvalue())
        self.assertEqual("dry_run_no_aws_no_spark", output["mode"])
        self.assertEqual(
            [list(r) for r in EXPECTED_ROWS], output["target"]["expected_rows"]
        )
        self.assertEqual(3, output["target"]["expected_count"])
        self.assertEqual(35.0, output["target"]["expected_sum"])

    def test_plan_file_dry_run_preserves_exact_reviewed_resource_names(self):
        path = self.root / "approved-plan.json"
        path.write_text(json.dumps(self.target))
        with patch.object(self.subject, "dependencies", return_value=self.report):
            self.assertEqual(0, self.subject.main(["--plan-file", str(path)]))
        result = json.loads(self.output.getvalue())
        self.assertEqual(self.target, result["target"])
        self.assertIn("--plan-file", result["future_execution_command"])

    def test_plan_file_cannot_select_production_namespace(self):
        path = self.root / "invalid-plan.json"
        path.write_text(json.dumps(self.target | {"database": "airlake_gold"}))
        with patch.object(self.subject, "execute") as execution:
            self.assertEqual(
                2,
                self.subject.main(
                    [
                        "--execute",
                        "--confirm-account",
                        self.subject.ACCOUNT,
                        "--plan-file",
                        str(path),
                    ]
                ),
            )
        execution.assert_not_called()
        self.assertIn("unsafe_target", self.errors.getvalue())

    def test_execute_receives_only_the_exact_approved_plan(self):
        path = self.root / "approved-plan.json"
        path.write_text(json.dumps(self.target))
        with (
            patch.object(self.subject, "dependencies", return_value=self.report),
            patch.object(self.subject, "execute", return_value=0) as execution,
        ):
            self.assertEqual(
                0,
                self.subject.main(
                    [
                        "--execute",
                        "--confirm-account",
                        self.subject.ACCOUNT,
                        "--plan-file",
                        str(path),
                    ]
                ),
            )
        self.assertEqual(self.target, execution.call_args.args[1])

    def test_dry_run_reports_missing_dependencies_without_aws(self):
        self.report["aws_bundle"] = {"ready": False, "problem": "missing_jar"}
        with patch.object(self.subject, "dependencies", return_value=self.report):
            self.assertEqual(0, self.subject.main([]))
        self.assertIn("missing_jar", self.output.getvalue())

    def test_real_dependency_inspection_in_dry_run_is_offline(self):
        self.assertEqual(
            0,
            self.subject.main(
                [
                    "--runtime-jar",
                    str(self.root / "absent-core.jar"),
                    "--aws-bundle-jar",
                    str(self.root / "absent-aws.jar"),
                ]
            ),
        )
        report = json.loads(self.output.getvalue())["dependencies"]
        self.assertEqual("missing_jar", report["core"]["problem"])
        self.assertEqual("missing_jar", report["aws_bundle"]["problem"])

    def test_confirmation_alone_is_still_dry_run(self):
        with patch.object(self.subject, "dependencies", return_value=self.report):
            self.assertEqual(
                0, self.subject.main(["--confirm-account", self.subject.ACCOUNT])
            )
        self.assertEqual(
            "dry_run_no_aws_no_spark", json.loads(self.output.getvalue())["mode"]
        )

    def test_execute_requires_exact_confirmation_before_dependency_checks(self):
        with patch.object(self.subject, "dependencies") as inspect:
            for arguments in (
                ["--execute"],
                ["--execute", "--confirm-account", "000000000000"],
            ):
                self.assertEqual(2, self.subject.main(arguments))
            inspect.assert_not_called()

    def test_wrong_region_is_rejected_before_aws(self):
        self.assertEqual(2, self.subject.main(["--region", "us-east-1"]))
        self.assertIn("wrong_region", self.errors.getvalue())

    def test_generated_targets_are_unique_and_fixed_to_smoke_scope(self):
        first, second = self.subject.plan(), self.subject.plan()
        self.assertNotEqual(first["run_id"], second["run_id"])
        self.assertTrue(first["database"].startswith("airlake_smoke_"))
        self.assertTrue(
            first["warehouse"].startswith(
                "s3://airlake-701364614078-lake/integration-smoke/"
            )
        )

    def test_no_production_namespace_or_alternate_target_is_valid(self):
        for database in (
            "airlake_bronze",
            "airlake_silver",
            "airlake_gold",
            "local.gold",
        ):
            with (
                self.subTest(database=database),
                self.assertRaises(self.subject.SmokeError),
            ):
                self.subject.validate_plan(self.target | {"database": database})
        for field in (
            "prefix",
            "warehouse",
            "table_location",
            "table",
            "account",
            "region",
        ):
            with self.subTest(field=field), self.assertRaises(self.subject.SmokeError):
                self.subject.validate_plan(self.target | {field: "unapproved"})

    def test_run_id_rejects_path_and_sql_injection(self):
        for run in (
            "../warehouse",
            "x'; DROP DATABASE airlake_gold",
            "",
            "20261025_../",
        ):
            with self.subTest(run=run), self.assertRaises(self.subject.SmokeError):
                self.subject.plan(run)

    def test_unsafe_spark_cloud_and_credential_environment_is_rejected(self):
        for key in (
            "SPARK_CONF_DIR",
            "SPARK_REMOTE",
            "PYSPARK_SUBMIT_ARGS",
            "PYSPARK_GATEWAY_PORT",
            "HADOOP_CONF_DIR",
            "CLASSPATH",
            "JAVA_TOOL_OPTIONS",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_ENDPOINT_URL",
            "AWS_CONFIG_FILE",
            "HTTPS_PROXY",
        ):
            with self.subTest(key=key), self.assertRaises(self.subject.SmokeError):
                self.subject.validate_environment({key: "fixture"})

    def test_wrong_inherited_region_or_profile_is_rejected(self):
        for env in (
            {"AWS_REGION": "us-east-1"},
            {"AWS_DEFAULT_REGION": "us-east-1"},
            {"AWS_PROFILE": "other"},
        ):
            with self.subTest(env=env), self.assertRaises(self.subject.SmokeError):
                self.subject.validate_environment(env)

    def test_missing_jar_and_untrusted_hash_fail_closed(self):
        result = self.subject.inspect_jar(self.root / "absent.jar", (), "a" * 64)
        self.assertEqual("missing_jar", result["problem"])
        path, digest = self.jar(self.subject.AWS_CLASSES)
        for checksum in ("", "a" * 64):
            self.assertFalse(
                self.subject.inspect_jar(path, self.subject.AWS_CLASSES, checksum)[
                    "ready"
                ]
            )
        self.assertTrue(
            self.subject.inspect_jar(path, self.subject.AWS_CLASSES, digest)["ready"]
        )

    def test_correct_filename_and_checksum_do_not_replace_class_checks(self):
        path, digest = self.jar(())
        renamed = path.with_name("iceberg-aws-bundle-1.12.0.jar")
        path.rename(renamed)
        result = self.subject.inspect_jar(renamed, self.subject.AWS_CLASSES, digest)
        self.assertFalse(result["ready"])
        self.assertEqual(list(self.subject.AWS_CLASSES), result["missing_classes"])

    def test_invalid_zip_is_rejected(self):
        path = self.root / "broken.jar"
        path.write_bytes(b"not a jar")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        self.assertEqual(
            "invalid_jar", self.subject.inspect_jar(path, (), digest)["problem"]
        )

    def test_direct_login_requires_signin_provider_in_verified_bundle(self):
        self.report["aws_bundle"]["login_provider_present"] = False
        with self.assertRaisesRegex(self.subject.SmokeError, "signin_module_missing"):
            self.subject.require_dependencies(self.report, "airlake")
        self.subject.require_dependencies(self.report, "airlake-spark")

    def test_missing_dependencies_prevent_child_and_aws_launch(self):
        self.report["core"]["ready"] = False
        with patch.object(self.subject, "dependencies", return_value=self.report):
            self.assertEqual(
                2,
                self.subject.main(
                    ["--execute", "--confirm-account", self.subject.ACCOUNT]
                ),
            )
        self.assertIn("local_dependencies_not_ready", self.errors.getvalue())

    def test_profiles_allow_only_login_and_exact_reviewed_process(self):
        directory = self.root / ".aws"
        directory.mkdir()
        config = directory / "config"
        config.write_text(
            "[profile airlake]\nregion = ap-southeast-2\nlogin_session = fixture-session\n"
            "[profile airlake-spark]\nregion = ap-southeast-2\n"
            "credential_process = /fixture/aws configure export-credentials --profile airlake --format process\n"
        )
        with patch.object(self.subject.Path, "home", return_value=self.root):
            self.subject.validate_profiles("airlake-spark", "/fixture/aws")
            config.write_text(
                config.read_text().replace("/fixture/aws", "/unapproved/tool")
            )
            with self.assertRaisesRegex(self.subject.SmokeError, "requires_review"):
                self.subject.validate_profiles("airlake-spark", "/fixture/aws")

    def test_static_profile_credentials_are_rejected_without_printing(self):
        directory = self.root / ".aws"
        directory.mkdir()
        (directory / "config").write_text(
            "[profile airlake]\nregion = ap-southeast-2\nlogin_session = fixture\n"
        )
        (directory / "credentials").write_text(
            "[airlake]\naws_access_key_id=FAKE_PRIVATE_VALUE\n"
        )
        with (
            patch.object(self.subject.Path, "home", return_value=self.root),
            self.assertRaisesRegex(self.subject.SmokeError, "not_allowed"),
        ):
            self.subject.validate_profiles("airlake", "/fixture/aws")
        self.assertNotIn(
            "FAKE_PRIVATE_VALUE", self.output.getvalue() + self.errors.getvalue()
        )

    def test_cli_history_must_not_capture_credential_process_responses(self):
        directory = self.root / ".aws"
        directory.mkdir()
        (directory / "config").write_text(
            "[default]\ncli_history=enabled\n[profile airlake]\n"
            "region=ap-southeast-2\nlogin_session=fixture\n"
        )
        with (
            patch.object(self.subject.Path, "home", return_value=self.root),
            self.assertRaisesRegex(self.subject.SmokeError, "aws_cli_history_enabled"),
        ):
            self.subject.validate_profiles("airlake", "/fixture/aws")

    def test_preflight_accepts_only_fully_proven_unused_scope(self):
        self.subject.preflight(self.aws, self.target)
        self.assertEqual(7, self.aws.call.call_count)

    def test_wrong_aws_account_stops_inventory(self):
        self.responses[("sts", "get-caller-identity")] = {"Account": "000000000000"}
        with self.assertRaisesRegex(self.subject.SmokeError, "wrong_account"):
            self.subject.preflight(self.aws, self.target)
        self.assertEqual(1, self.aws.call.call_count)

    def test_bucket_in_wrong_region_is_rejected(self):
        self.responses[("s3api", "get-bucket-location")] = {
            "LocationConstraint": "us-east-1"
        }
        with self.assertRaisesRegex(self.subject.SmokeError, "wrong_bucket_region"):
            self.subject.preflight(self.aws, self.target)

    def test_versioned_or_suspended_bucket_needs_separate_review(self):
        for status in ("Enabled", "Suspended"):
            self.responses[("s3api", "get-bucket-versioning")] = {"Status": status}
            with (
                self.subTest(status=status),
                self.assertRaisesRegex(self.subject.SmokeError, "requires_review"),
            ):
                self.subject.preflight(self.aws, self.target)

    def test_existing_namespace_prevents_spark_writes(self):
        self.responses[("glue", "get-database")] = {
            "Database": {"Name": self.target["database"]}
        }
        spark = Mock()
        with self.assertRaisesRegex(self.subject.SmokeError, "namespace_exists"):
            self.subject.preflight(self.aws, self.target)
            self.subject.run_probe(spark, self.aws, self.target, Mock())
        spark.sql.assert_not_called()

    def test_nonempty_truncated_or_uncertain_prefix_is_rejected(self):
        valid = self.responses[("s3api", "list-objects-v2")]
        invalid = [
            valid | {"KeyCount": 1},
            valid | {"IsTruncated": True},
            {},
            valid | {"Contents": [{"Key": "fixture"}]},
            valid | {"Prefix": "integration-smoke/other/"},
            valid | {"Name": "other-bucket"},
            valid | {"KeyCount": False},
        ]
        for listing in invalid:
            self.responses[("s3api", "list-objects-v2")] = listing
            with (
                self.subTest(listing=listing),
                self.assertRaisesRegex(
                    self.subject.SmokeError, "prefix_not_proven_empty"
                ),
            ):
                self.subject.assert_unused(self.aws, self.target)

    def test_access_denied_is_not_namespace_absence_and_never_prints_raw_error(self):
        private = "FAKE_PRIVATE_TOKEN_DO_NOT_LOG"
        result = SimpleNamespace(
            returncode=255,
            stdout="",
            stderr=(
                "An error occurred (AccessDeniedException) when calling the GetDatabase operation: "
                + private
            ),
        )
        with (
            patch.object(self.subject.subprocess, "run", return_value=result),
            self.assertRaisesRegex(
                self.subject.SmokeError, "aws_inventory_failed"
            ) as caught,
        ):
            self.subject.AWSCLI("/fixture/aws").call("glue", "get-database")
        self.assertNotIn(
            private,
            str(caught.exception) + self.output.getvalue() + self.errors.getvalue(),
        )

    def test_only_glue_get_database_entity_not_found_proves_absence(self):
        result = SimpleNamespace(
            returncode=255,
            stdout="",
            stderr=(
                "An error occurred (EntityNotFoundException) when calling the GetDatabase operation: absent"
            ),
        )
        with patch.object(self.subject.subprocess, "run", return_value=result):
            self.assertIsNone(
                self.subject.AWSCLI("/fixture/aws").call("glue", "get-database")
            )
            with self.assertRaises(self.subject.SmokeError):
                self.subject.AWSCLI("/fixture/aws").call("glue", "get-table")
        result.stderr = result.stderr.replace("GetDatabase", "CreateOAuth2Token")
        with (
            patch.object(self.subject.subprocess, "run", return_value=result),
            self.assertRaises(self.subject.SmokeError),
        ):
            self.subject.AWSCLI("/fixture/aws").call("glue", "get-database")

    def test_malformed_cli_json_fails_closed(self):
        with (
            patch.object(
                self.subject.subprocess,
                "run",
                return_value=SimpleNamespace(
                    returncode=0, stdout="not-json", stderr=""
                ),
            ),
            self.assertRaisesRegex(self.subject.SmokeError, "invalid_json"),
        ):
            self.subject.AWSCLI("/fixture/aws").call("sts", "get-caller-identity")

    def test_cli_adapter_cannot_mutate_or_execute_athena_sql(self):
        for service, operation in (
            ("athena", "start-query-execution"),
            ("s3api", "put-object"),
            ("glue", "create-table"),
            ("ec2", "run-instances"),
        ):
            with (
                self.subTest(operation=operation),
                self.assertRaises(self.subject.SmokeError),
            ):
                self.subject.AWSCLI("/fixture/aws").call(service, operation)

    def test_changed_enforced_workgroup_configuration_is_rejected(self):
        configuration = self.responses[("athena", "get-work-group")]["WorkGroup"][
            "Configuration"
        ]
        configuration["ResultConfiguration"]["OutputLocation"] = "s3://unapproved/"
        with self.assertRaisesRegex(self.subject.SmokeError, "configuration_changed"):
            self.subject.preflight(self.aws, self.target)

    def test_private_settings_never_resolve_local_production_paths(self):
        settings = self.subject.spark_settings(
            self.root, self.target, (self.args.runtime_jar, self.args.aws_bundle_jar)
        )
        self.assertEqual("", settings["spark.jars.packages"])
        self.assertEqual("1", settings["spark.task.maxFailures"])
        self.assertEqual(
            "0", settings["spark.sql.catalog.cloudsmoke.s3.retry.num-retries"]
        )
        self.assertEqual(
            self.target["warehouse"], settings["spark.sql.catalog.cloudsmoke.warehouse"]
        )
        for key in ("spark.sql.catalog.local.warehouse", "spark.sql.warehouse.dir"):
            self.assertTrue(settings[key].startswith(self.root.as_uri()))
            self.assertNotIn(REPOSITORY.as_uri(), settings[key])
        env = self.subject.child_environment(self.root, "airlake-spark")
        self.assertEqual("airlake-spark", env["AWS_PROFILE"])
        self.assertEqual("1", env["AWS_MAX_ATTEMPTS"])
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", env)

    def test_macos_injected_locale_survives_worker_environment_validation(self):
        with patch.dict(os.environ, {"__CF_USER_TEXT_ENCODING": "fixture-locale"}):
            env = self.subject.child_environment(self.root, "airlake-spark")
        self.assertEqual("fixture-locale", env["__CF_USER_TEXT_ENCODING"])
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(
                env, self.subject.child_environment(self.root, "airlake-spark")
            )
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", env)
        self.assertEqual("--master local[1] pyspark-shell", env["PYSPARK_SUBMIT_ARGS"])

    def test_success_validates_actual_rows_schema_and_snapshot_without_athena(self):
        spark = self.fake_spark()
        journal = self.subject.Journal(self.root, self.target, self.report)
        self.subject.run_probe(spark, self.aws, self.target, journal)
        data = json.loads(journal.path.read_text())
        self.assertEqual("spark_glue_passed_athena_not_executed", data["status"])
        self.assertEqual([123], data["observations"]["snapshot_ids"])
        self.assertEqual(3, data["observations"]["count"])
        self.assertEqual(35.0, data["observations"]["pm25_sum"])
        spark.createDataFrame.assert_called_once_with(
            EXPECTED_ROWS, "id INT, label STRING, pm25 DOUBLE"
        )
        spark.createDataFrame.return_value.coalesce.return_value.writeTo.return_value.append.assert_called_once()
        self.assertEqual([], data["athena_queries"])
        self.assertNotIn(
            "IF NOT EXISTS", " ".join(c.args[0] for c in spark.sql.call_args_list)
        )

    def test_wrong_rows_reject_instead_of_trusting_metadata_counts(self):
        spark = self.fake_spark()
        spark.table.return_value.orderBy.return_value.collect.return_value = (
            self.subject.ROWS[:2]
        )
        journal = self.subject.Journal(self.root, self.target, self.report)
        with self.assertRaisesRegex(self.subject.SmokeError, "row_mismatch"):
            self.subject.run_probe(spark, self.aws, self.target, journal)
        self.assertEqual("failed_or_uncertain", journal.data["status"])

    def test_partial_append_failure_preserves_resource_plan_and_attempt_without_cleanup(
        self,
    ):
        spark = self.fake_spark()
        spark.createDataFrame.return_value.coalesce.return_value.writeTo.return_value.append.side_effect = RuntimeError(
            "FAKE_PRIVATE_TOKEN"
        )
        journal = self.subject.Journal(self.root, self.target, self.report)
        with self.assertRaises(RuntimeError):
            self.subject.run_probe(spark, self.aws, self.target, journal)
        data = json.loads(journal.path.read_text())
        self.assertEqual(self.target, data["target"])
        self.assertEqual(
            {"operation": "append_three_rows", "status": "failed_or_uncertain"},
            data["operations"][-1],
        )
        self.assertEqual(2, spark.sql.call_count)
        self.assertNotIn("DROP", " ".join(c.args[0] for c in spark.sql.call_args_list))
        self.assertNotIn("FAKE_PRIVATE_TOKEN", journal.path.read_text())
        self.assertNotIn(str(self.root), journal.path.read_text())

    def test_target_race_is_rechecked_before_first_spark_write(self):
        self.responses[("glue", "get-database")] = {"Database": {}}
        spark = self.fake_spark()
        journal = self.subject.Journal(self.root, self.target, self.report)
        with self.assertRaisesRegex(self.subject.SmokeError, "namespace_exists"):
            self.subject.run_probe(spark, self.aws, self.target, journal)
        spark.sql.assert_not_called()

    def test_wrong_schema_snapshot_format_and_file_scope_are_rejected(self):
        cases = ("schema", "snapshot", "file", "metadata", "format")
        for case in cases:
            with self.subTest(case=case):
                spark = self.fake_spark()
                if case == "schema":
                    spark.table.return_value.schema.fields[0].name = "wrong"
                elif case == "snapshot":
                    spark.table.return_value.select.return_value.collect.side_effect = [
                        [],
                        [],
                    ]
                elif case == "file":
                    spark.table.return_value.select.return_value.collect.side_effect = [
                        [SimpleNamespace(snapshot_id=123, operation="append")],
                        [SimpleNamespace(file_path="s3://unapproved/data.parquet")],
                    ]
                elif case == "metadata":
                    self.responses[("glue", "get-table")] = {"Table": {}}
                else:
                    iceberg = spark.sparkContext._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable.return_value
                    iceberg.operations.return_value.current.return_value.formatVersion.return_value = 3
                journal = self.subject.Journal(self.root, self.target, self.report)
                with self.assertRaises(self.subject.SmokeError):
                    self.subject.run_probe(spark, self.aws, self.target, journal)
                self.assertEqual("failed_or_uncertain", journal.data["status"])
                self.responses[("glue", "get-table")] = {
                    "Table": {
                        "Parameters": {
                            "metadata_location": self.target["table_location"]
                            + "/metadata/fixture.json"
                        }
                    }
                }

    def test_incompatible_loaded_runtime_or_java_identity_cannot_reach_write_phase(
        self,
    ):
        for case in ("sdk", "iceberg", "java_account", "class_origin"):
            with self.subTest(case=case):
                spark = Mock()
                spark.sparkContext.master = "local[1]"
                settings = self.subject.spark_settings(
                    self.root,
                    self.target,
                    (self.args.runtime_jar, self.args.aws_bundle_jar),
                )
                spark.sparkContext.getConf.return_value.get.side_effect = (
                    settings.__getitem__
                )
                jvm = spark.sparkContext._jvm
                properties = {
                    "java.specification.version": "17",
                    "java.io.tmpdir": str(self.root / "tmp"),
                    "derby.system.home": str(self.root / "derby"),
                    "aws.maxAttempts": "1",
                }
                jvm.java.lang.System.getProperty.side_effect = properties.__getitem__
                jvm.org.apache.iceberg.IcebergBuild.version.return_value = "1.12.0"
                jvm.software.amazon.awssdk.core.util.VersionInfo.SDK_VERSION = "2.54.17"
                jvm.scala.util.Properties.versionNumberString.return_value = "2.12.18"
                loader = jvm.java.lang.Thread.currentThread.return_value.getContextClassLoader.return_value

                def load(name, case=case):
                    klass = Mock()
                    location = klass.getProtectionDomain.return_value.getCodeSource.return_value.getLocation.return_value
                    location.getProtocol.return_value = "file"
                    jar = (
                        self.args.runtime_jar
                        if name in self.subject.CORE_CLASSES
                        else self.args.aws_bundle_jar
                    )
                    location.toURI.return_value.getPath.return_value = str(jar)
                    if case == "class_origin":
                        location.toURI.return_value.getPath.return_value = (
                            "/unapproved/other.jar"
                        )
                    return klass

                loader.loadClass.side_effect = load
                client = jvm.software.amazon.awssdk.services.sts.StsClient.builder.return_value.region.return_value.credentialsProvider.return_value.build.return_value
                client.getCallerIdentity.return_value.account.return_value = (
                    self.subject.ACCOUNT
                )
                if case == "sdk":
                    jvm.software.amazon.awssdk.core.util.VersionInfo.SDK_VERSION = (
                        "wrong"
                    )
                elif case == "iceberg":
                    jvm.org.apache.iceberg.IcebergBuild.version.return_value = "wrong"
                elif case == "java_account":
                    client.getCallerIdentity.return_value.account.return_value = (
                        "000000000000"
                    )
                session = Mock()
                session.getActiveSession.return_value = None
                builder = session.builder
                builder.master.return_value = builder
                builder.appName.return_value = builder
                builder.config.return_value = builder
                builder.getOrCreate.return_value = spark
                journal = self.subject.Journal(self.root, self.target, self.report)
                with (
                    patch.dict(
                        "sys.modules",
                        {
                            "pyspark": SimpleNamespace(
                                SparkContext=SimpleNamespace(_active_spark_context=None)
                            ),
                            "pyspark.sql": SimpleNamespace(SparkSession=session),
                        },
                    ),
                    patch.object(self.subject, "run_probe") as write_phase,
                    self.assertRaisesRegex(
                        self.subject.SmokeError,
                        {
                            "sdk": "loaded_runtime_version_mismatch",
                            "iceberg": "loaded_runtime_version_mismatch",
                            "java_account": "wrong_java_account",
                            "class_origin": "unexpected_loaded_jar",
                        }[case],
                    ),
                ):
                    self.subject.worker(
                        self.root, self.target, self.args, journal, self.aws
                    )
                write_phase.assert_not_called()
                spark.stop.assert_called_once()

    def test_known_spark_module_defaults_preserve_exact_probe_options(self):
        settings = {
            "spark.driver.extraJavaOptions": "-Djava.io.tmpdir=/fixture/tmp -Dderby.system.home=/fixture/derby -Daws.maxAttempts=1"
        }
        spark = Mock()
        defaults = "-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED"
        spark.sparkContext._jvm.org.apache.spark.launcher.JavaModuleOptions.defaultModuleOptions.return_value = defaults
        spark.sparkContext.getConf.return_value.get.return_value = (
            defaults + " " + settings["spark.driver.extraJavaOptions"]
        )
        properties = {
            "java.io.tmpdir": "/fixture/tmp",
            "derby.system.home": "/fixture/derby",
            "aws.maxAttempts": "1",
        }
        spark.sparkContext._jvm.java.lang.System.getProperty.side_effect = (
            properties.__getitem__
        )
        self.subject.verify_spark_settings(spark, settings)

        spark.sparkContext.getConf.return_value.get.return_value = (
            "-Djava.net.preferIPv6Addresses=false "
            + defaults
            + " "
            + settings["spark.driver.extraJavaOptions"]
        )
        self.subject.verify_spark_settings(spark, settings)

    def test_arbitrary_java_prefix_or_changed_effective_properties_are_rejected(self):
        settings = {"spark.driver.extraJavaOptions": "-Daws.maxAttempts=1"}
        spark = Mock()
        spark.sparkContext._jvm.org.apache.spark.launcher.JavaModuleOptions.defaultModuleOptions.return_value = "-XX:+IgnoreUnrecognizedVMOptions"
        spark.sparkContext.getConf.return_value.get.return_value = (
            "-Dunexpected=true -Daws.maxAttempts=1"
        )
        with self.assertRaisesRegex(
            self.subject.SmokeError, "spark_configuration_mismatch"
        ):
            self.subject.verify_spark_settings(spark, settings)
        spark.sparkContext.getConf.return_value.get.return_value = "-Daws.maxAttempts=1"
        spark.sparkContext._jvm.java.lang.System.getProperty.return_value = "3"
        with self.assertRaisesRegex(
            self.subject.SmokeError, "effective_jvm_property_mismatch"
        ):
            self.subject.verify_spark_settings(spark, settings)

    def test_worker_cannot_be_invoked_without_opt_in_or_from_repository(self):
        self.assertEqual(2, self.subject.main(["--worker-root", str(REPOSITORY)]))
        self.assertEqual(
            2,
            self.subject.main(
                [
                    "--worker-root",
                    str(REPOSITORY),
                    "--execute",
                    "--confirm-account",
                    self.subject.ACCOUNT,
                ]
            ),
        )

    def test_parent_launch_is_fresh_private_and_retains_failure_manifest(self):
        child = Mock()
        child.wait.return_value = 1
        captured = {}

        def launch(command, **kwargs):
            captured.update(kwargs)
            self.assertIn("--execute", command)
            self.assertIn(self.subject.ACCOUNT, command)
            self.assertTrue(kwargs["start_new_session"])
            self.assertFalse(kwargs["cwd"].is_relative_to(REPOSITORY))
            self.assertEqual(subprocess.DEVNULL, kwargs["stderr"])
            return child

        with (
            patch.object(self.subject, "validate_profiles"),
            patch.object(self.subject.shutil, "which", return_value="/fixture/aws"),
            patch.object(
                self.subject.tempfile, "gettempdir", return_value=str(self.root)
            ),
            patch.object(self.subject.subprocess, "Popen", side_effect=launch),
        ):
            self.assertEqual(
                1, self.subject.execute(self.args, self.target, self.report)
            )
        data = json.loads((captured["cwd"] / "manifest.json").read_text())
        self.assertEqual("failed_or_uncertain", data["status"])
        child.wait.assert_called_once_with(timeout=300)

    def test_parent_deadline_stops_only_its_child_group_and_keeps_manifest(self):
        child = Mock(pid=4242)
        child.wait.side_effect = [subprocess.TimeoutExpired("fixture-child", 300), 0, 0]
        with (
            patch.object(self.subject, "validate_profiles"),
            patch.object(self.subject.shutil, "which", return_value="/fixture/aws"),
            patch.object(
                self.subject.tempfile, "gettempdir", return_value=str(self.root)
            ),
            patch.object(self.subject.subprocess, "Popen", return_value=child),
            patch.object(self.subject.os, "killpg") as kill,
        ):
            self.assertEqual(
                1, self.subject.execute(self.args, self.target, self.report)
            )
        self.assertEqual([4242, 4242], [call.args[0] for call in kill.call_args_list])
        manifest = next(self.root.glob("airlake-aws-smoke-*/manifest.json"))
        self.assertEqual(
            "failed_or_uncertain", json.loads(manifest.read_text())["status"]
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
