"""Synthetic offline orchestration evidence; no real AWS, upstream or Spark calls."""

import copy
import io
import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

from live_data import operations, refresh
from live_data.contract import DATABASE, NAME, PREFIX, encode, sha
from live_data.export import load as load_export
from live_data.store import pack, unpack

NOW = datetime(2026, 11, 14, 12, tzinfo=UTC)
WINDOW = "b" * 64
PRODUCTION = {"scripts/gold_batch.py": "c" * 64, "scripts/silver_batch.py": "d" * 64}


def exported(digest):
    return {
        "facts": [
            {
                "location_id": 4946812,
                "hour_start": "2026-11-14T11:00:00Z",
                "pm25_avg": 10.0,
                "aqi": 20,
            }
        ],
        "dimensions": [{"location_id": 4946812, "name": "SYNTHETIC station"}],
        "provenance": {
            "database": refresh.version_database(digest),
            "execution": "AWS_GLUE",
            "input_manifest_sha256": digest,
            "computed_at": "2026-11-14T12:00:00Z",
            "aqi_method": "simplified_hourly_mean_mvp; NOT official VN_AQI",
            "snapshots": {
                k: [i + 1]
                for i, k in enumerate(
                    ("bronze", "silver", "breakpoints", "dimensions", "hourly")
                )
            },
        },
    }


class Cloud:
    """Minimal conditional-state emulator; API calls remain test doubles."""

    def __init__(self):
        self.c = {
            n: Mock()
            for n in ("s3", "dynamodb", "lambda", "scheduler", "glue", "athena")
        }
        self.items = {
            ("RUN", "LEASE"): {
                "pk": "RUN",
                "sk": "LEASE",
                "attempts": 4,
                "active": False,
                "lease_until": 0,
            },
            ("EXPORT", "CURRENT"): {
                "pk": "EXPORT",
                "sk": "CURRENT",
                "key": PREFIX + "exports/" + "a" * 64 + "/gold.json",
                "sha256": "a" * 64,
                "athena_query_id": "old-verified",
            },
        }
        self.old = copy.deepcopy(self.items[("EXPORT", "CURRENT")])
        self.c["dynamodb"].get_item.side_effect = self.get
        self.c["dynamodb"].put_item.side_effect = self.put
        self.c["dynamodb"].update_item.side_effect = self.update
        self.c["lambda"].invoke.side_effect = self.poll
        self.controller = refresh.Controller(
            self.c, WINDOW, NOW + timedelta(minutes=45), PRODUCTION
        )
        self.raw_key = PREFIX + "bronze/" + "f" * 64 + ".json"
        record = {"datetime": "2026-11-14T11:00:00Z"}
        self.raw = encode(
            {
                "record": record,
                "record_sha256": sha(encode(record)),
                "observed_at": record["datetime"],
                "ingested_at": "2026-11-14T12:00:00Z",
            }
        )
        self.manifest = {
            "schema_version": 1,
            "objects": [{"key": self.raw_key, "sha256": sha(self.raw)}],
            "production_sha256": PRODUCTION,
        }
        self.digest = sha(encode(self.manifest))
        self.value = exported(self.digest)
        self.proof = {
            "status": "PASS",
            "source_count": 1,
            "silver": {"output_count": 1},
            "gold": {
                "hourly_output_count": 1,
                "dimension_output_count": 1,
                "orphan_location_count": 0,
                "duplicate_hourly_key_count": 0,
                "duplicate_dimension_key_count": 0,
                "invalid_pm25_count": 0,
            },
            "snapshots_before": self.value["provenance"]["snapshots"],
            "snapshots_after": self.value["provenance"]["snapshots"],
            "export_sha256": sha(encode(self.value)),
        }
        self.c["s3"].get_paginator.return_value.paginate.side_effect = lambda **_: [
            {"Contents": [{"Key": self.raw_key}]}
        ]
        self.c["s3"].get_object.side_effect = self.read
        self.c["glue"].get_job_runs.return_value = {"JobRuns": []}
        self.c["glue"].get_database.side_effect = ClientError(
            {"Error": {"Code": "EntityNotFoundException"}}, "GetDatabase"
        )
        self.c["glue"].start_job_run.return_value = {"JobRunId": "synthetic-job"}
        self.c["glue"].get_job_run.return_value = {
            "JobRun": {
                "JobRunState": "SUCCEEDED",
                "Arguments": {
                    "--manifest_sha256": self.digest,
                    "--refresh_database": refresh.version_database(self.digest),
                },
            }
        }
        self.c["scheduler"].get_schedule.return_value = {
            "Name": NAME + "-5m",
            "State": "DISABLED",
            "GroupName": "default",
            "ScheduleExpression": "rate(5 minutes)",
            "ScheduleExpressionTimezone": "UTC",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "Target": {},
            "StartDate": NOW,
            "EndDate": NOW + timedelta(minutes=45),
        }
        self.c["athena"].get_work_group.return_value = {
            "WorkGroup": {
                "State": "ENABLED",
                "Configuration": {
                    "EnforceWorkGroupConfiguration": True,
                    "ResultConfiguration": {
                        "OutputLocation": "s3://" + refresh.RESULT_BUCKET + "/"
                    },
                    "EngineVersion": {
                        "EffectiveEngineVersion": "Athena engine version 3"
                    },
                },
            }
        }
        self.c["athena"].start_query_execution.return_value = {
            "QueryExecutionId": "synthetic-query"
        }
        self.c["athena"].get_query_execution.side_effect = lambda **_: {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Query": refresh.query_sql(self.value, self.digest),
                "WorkGroup": "airlake",
                "QueryExecutionContext": {
                    "Database": refresh.version_database(self.digest)
                },
                "Statistics": {"DataScannedInBytes": 10},
                "ResultConfiguration": {
                    "OutputLocation": "s3://"
                    + refresh.RESULT_BUCKET
                    + "/synthetic-query.csv"
                },
            }
        }
        columns = [
            ("location_id", "integer"),
            ("hour_start", "varchar"),
            ("pm25_avg", "double"),
            ("aqi", "integer"),
            ("name", "varchar"),
        ]

        def page(rows):
            return {
                "ResultSet": {
                    "ResultSetMetadata": {
                        "ColumnInfo": [{"Name": n, "Type": t} for n, t in columns]
                    },
                    "Rows": [{"Data": [{"VarCharValue": x} for x in r]} for r in rows],
                }
            }

        self.pages = [
            page([[n for n, _ in columns]]),
            page(
                [["4946812", "2026-11-14T11:00:00Z", "10.0", "20", "SYNTHETIC station"]]
            ),
        ]
        self.c["athena"].get_paginator.return_value.paginate.side_effect = lambda **_: (
            copy.deepcopy(self.pages)
        )

    def get(self, **kwargs):
        key = unpack(kwargs["Key"])
        item = self.items.get((key["pk"], key["sk"]))
        return {"Item": pack(item)} if item else {}

    def put(self, **kwargs):
        row = unpack(kwargs["Item"])
        if row["pk"] == "EXPORT" and kwargs.get("ConditionExpression"):
            expected = unpack(kwargs["ExpressionAttributeValues"])
            old = self.items[("EXPORT", "CURRENT")]
            if old["key"] != expected[":key"] or old["sha256"] != expected[":sha"]:
                raise ClientError(
                    {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
                )
        self.items[(row["pk"], row["sk"])] = copy.deepcopy(row)
        return {}

    def update(self, **kwargs):
        key = unpack(kwargs["Key"])
        row = self.items.setdefault((key["pk"], key["sk"]), dict(key))
        vals = unpack(kwargs["ExpressionAttributeValues"])
        if ":yes" in vals:
            if row.get("active"):
                raise ClientError(
                    {"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem"
                )
            row.update(active=True, owner_id=vals[":owner"])
        else:
            row["active"] = False
        return {}

    def read(self, **kwargs):
        key = kwargs["Key"]
        data = (
            self.raw
            if key == self.raw_key
            else encode(self.proof)
            if "/evidence/" in key
            else encode(self.value)
        )
        return {"Body": io.BytesIO(data)}

    def poll(self, **kwargs):
        self.items[("RUN", "LEASE")]["attempts"] += 1
        return {
            "Payload": io.BytesIO(
                encode(
                    {
                        "status": "PASS",
                        "accepted_new": 0,
                        "replayed": 1,
                        "rejected": 0,
                        "polled_at": NOW.isoformat(),
                    }
                )
            )
        }


class RefreshTests(unittest.TestCase):
    def test_two_scheduled_polls_one_glue_one_query_cas_promotion_and_predecessor(self):
        cloud = Cloud()
        self.assertEqual(cloud.controller.tick(NOW)["phase"], "GLUE_RUNNING")
        self.assertEqual(cloud.items[("EXPORT", "CURRENT")], cloud.old)
        self.assertEqual(
            cloud.controller.tick(NOW + timedelta(minutes=5))["phase"], "QUERY_RUNNING"
        )
        self.assertEqual(cloud.items[("EXPORT", "CURRENT")], cloud.old)
        self.assertEqual(
            cloud.controller.tick(NOW + timedelta(minutes=10))["phase"], "COMPLETE"
        )
        pointer = cloud.items[("EXPORT", "CURRENT")]
        self.assertEqual(pointer["previous"], cloud.old)
        self.assertEqual(pointer["snapshots"], cloud.value["provenance"]["snapshots"])
        self.assertEqual(cloud.c["lambda"].invoke.call_count, 2)
        cloud.c["glue"].start_job_run.assert_called_once()
        cloud.c["athena"].start_query_execution.assert_called_once()
        cloud.controller.tick(NOW + timedelta(minutes=15))
        self.assertEqual(cloud.c["glue"].start_job_run.call_count, 1)
        self.assertEqual(cloud.c["athena"].start_query_execution.call_count, 1)
        self.assertFalse(cloud.items[("OPS", "LOCK")]["active"])
        adapter = Mock(s3=cloud.c["s3"])
        adapter.read.return_value = pointer
        self.assertEqual(len(load_export(adapter)[0]), 1)

    def test_no_new_data_does_not_start_glue_or_query(self):
        cloud = Cloud()
        cloud.items[("EXPORT", "CURRENT")]["key"] = (
            PREFIX + "exports/" + cloud.digest + "/gold.json"
        )
        self.assertEqual(cloud.controller.tick(NOW)["phase"], "NO_NEW_DATA")
        cloud.c["glue"].start_job_run.assert_not_called()
        cloud.c["athena"].start_query_execution.assert_not_called()

    def test_empty_raw_skips(self):
        cloud = Cloud()
        cloud.c["s3"].get_paginator.return_value.paginate.return_value = []
        cloud.c["s3"].get_paginator.return_value.paginate.side_effect = None
        self.assertEqual(cloud.controller.tick(NOW)["phase"], "NO_NEW_DATA")
        cloud.c["glue"].start_job_run.assert_not_called()

    def test_ambiguous_start_never_retried_and_previous_export_retained(self):
        cloud = Cloud()
        cloud.c["glue"].start_job_run.side_effect = TimeoutError(
            "synthetic uncertain response"
        )
        with self.assertRaises(RuntimeError):
            cloud.controller.tick(NOW)
        self.assertEqual(cloud.items[("OPS", WINDOW)]["phase"], "STARTING_GLUE")
        self.assertEqual(cloud.items[("EXPORT", "CURRENT")], cloud.old)
        with self.assertRaises(RuntimeError):
            cloud.controller.tick(NOW)
        self.assertEqual(cloud.c["glue"].start_job_run.call_count, 1)

    def test_old_watermark_stops_instead_of_silently_skipping_gap(self):
        cloud = Cloud()
        sensor = next(iter(refresh.PILOTS.values()))
        cloud.items[("CURSOR", str(sensor))] = {
            "value": {"watermark": "2026-11-10T00:00:00Z"}
        }
        with self.assertRaises(RuntimeError):
            cloud.controller.tick(NOW)
        cloud.c["lambda"].invoke.assert_not_called()

    def test_failed_glue_never_queries_or_promotes(self):
        cloud = Cloud()
        cloud.controller.tick(NOW)
        cloud.c["glue"].get_job_run.return_value["JobRun"]["JobRunState"] = "FAILED"
        with self.assertRaises(RuntimeError):
            cloud.controller.tick(NOW + timedelta(minutes=5))
        cloud.c["athena"].start_query_execution.assert_not_called()
        self.assertEqual(cloud.items[("EXPORT", "CURRENT")], cloud.old)

    def test_glue_success_without_matching_count_proof_never_queries(self):
        cloud = Cloud()
        cloud.controller.tick(NOW)
        cloud.proof["source_count"] = 999
        with self.assertRaises(RuntimeError):
            cloud.controller.tick(NOW + timedelta(minutes=5))
        cloud.c["athena"].start_query_execution.assert_not_called()

    def test_query_inflight_preserves_pointer(self):
        cloud = Cloud()
        cloud.controller.tick(NOW)
        cloud.controller.tick(NOW + timedelta(minutes=5))
        cloud.c["athena"].get_query_execution.side_effect = None
        cloud.c["athena"].get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "RUNNING"}}
        }
        self.assertEqual(
            cloud.controller.tick(NOW + timedelta(minutes=10))["phase"], "QUERY_RUNNING"
        )
        self.assertEqual(cloud.items[("EXPORT", "CURRENT")], cloud.old)

    def test_mismatched_athena_rows_stop_before_promotion(self):
        cloud = Cloud()
        cloud.controller.tick(NOW)
        cloud.controller.tick(NOW + timedelta(minutes=5))
        cloud.pages[1]["ResultSet"]["Rows"][0]["Data"][2]["VarCharValue"] = "999"
        with self.assertRaises(RuntimeError):
            cloud.controller.tick(NOW + timedelta(minutes=10))
        self.assertEqual(cloud.items[("EXPORT", "CURRENT")], cloud.old)

    def test_pointer_changed_concurrently_cas_fails_without_overwrite(self):
        cloud = Cloud()
        cloud.controller.tick(NOW)
        cloud.controller.tick(NOW + timedelta(minutes=5))
        cloud.items[("EXPORT", "CURRENT")]["sha256"] = "concurrent-change"
        with self.assertRaises(RuntimeError):
            cloud.controller.tick(NOW + timedelta(minutes=10))
        self.assertEqual(
            cloud.items[("EXPORT", "CURRENT")]["sha256"], "concurrent-change"
        )

    def test_deadline_stops_without_poll_or_start(self):
        cloud = Cloud()
        self.assertEqual(
            cloud.controller.tick(NOW + timedelta(hours=1))["phase"], "STOP_DEADLINE"
        )
        cloud.c["lambda"].invoke.assert_not_called()
        cloud.c["glue"].start_job_run.assert_not_called()

    def test_active_poller_and_existing_job_fail_closed(self):
        for active_poller in (True, False):
            with self.subTest(active_poller=active_poller):
                cloud = Cloud()
                if active_poller:
                    cloud.items[("RUN", "LEASE")]["active"] = True
                else:
                    cloud.c["glue"].get_job_runs.return_value = {
                        "JobRuns": [{"JobRunState": "RUNNING"}]
                    }
                with self.assertRaises(RuntimeError):
                    cloud.controller.tick(NOW)
                cloud.c["glue"].start_job_run.assert_not_called()

    def test_failed_stop_does_not_skip_both_concurrency_stops(self):
        cloud = Cloud()
        cloud.c["scheduler"].update_schedule.side_effect = TimeoutError()
        with self.assertRaises(RuntimeError):
            refresh.disable(cloud.c)
        self.assertEqual(cloud.c["lambda"].put_function_concurrency.call_count, 2)

    def test_budget_and_near_deadline_do_not_start_work(self):
        cloud = Cloud()
        cloud.items[("OPS", WINDOW)] = {"ticks": 12}
        with self.assertRaises(RuntimeError):
            cloud.controller.tick(NOW)
        cloud.c["lambda"].invoke.assert_not_called()
        cloud = Cloud()
        self.assertEqual(
            cloud.controller.tick(NOW + timedelta(minutes=35))["phase"],
            "DEFERRED_NEAR_DEADLINE",
        )
        cloud.c["glue"].start_job_run.assert_not_called()

    def test_unknown_write_phase_blocks_new_calls(self):
        for phase in ("STARTING_GLUE", "STARTING_QUERY", "PROMOTING"):
            with self.subTest(phase=phase):
                cloud = Cloud()
                cloud.items[("OPS", WINDOW)] = {"phase": phase}
                with self.assertRaises(RuntimeError):
                    cloud.controller.tick(NOW)
                cloud.c["lambda"].invoke.assert_not_called()
                cloud.c["glue"].start_job_run.assert_not_called()

    def test_invalid_export_never_passes(self):
        for kind in (
            "nan",
            "orphan",
            "duplicate",
            "wrong_db",
            "wrong_snapshot",
            "non_utc_grain",
        ):
            with self.subTest(kind=kind):
                cloud = Cloud()
                v = cloud.value
                if kind == "nan":
                    v["facts"][0]["pm25_avg"] = float("inf")
                if kind == "orphan":
                    v["facts"][0]["location_id"] = 1
                if kind == "duplicate":
                    v["facts"].append(dict(v["facts"][0]))
                if kind == "wrong_db":
                    v["provenance"]["database"] = DATABASE
                if kind == "wrong_snapshot":
                    v["provenance"]["snapshots"]["hourly"] = []
                if kind == "non_utc_grain":
                    v["facts"][0]["hour_start"] = "2026-11-14T11:05:00Z"
                with self.assertRaises((ValueError, TypeError)):
                    refresh.validate_export(json.dumps(v).encode(), cloud.digest)


class OperationsTests(unittest.TestCase):
    def test_write_guard_before_aws_clients(self):
        with (
            patch("live_data.operations.load", return_value=({}, "hash")),
            patch("live_data.operations.clients") as clients,
            patch("sys.argv", ["operations", "enable", "--packet", "/tmp/test"]),
        ):
            with self.assertRaises(SystemExit):
                operations.main()
            clients.assert_not_called()

    def test_offline_packet_rejects_wrong_rollback_zip(self):
        with tempfile.TemporaryDirectory() as root:
            p = Path(root)
            (p / "inventory.json").write_text(
                json.dumps(
                    {
                        "account": "701364614078",
                        "schedule": {"Name": NAME + "-5m"},
                        "functions": {"api": {"CodeSha256": "wrong"}},
                    }
                )
            )
            (p / "old.zip").write_bytes(b"test")
            with self.assertRaises(ValueError):
                operations.prepare(p / "packet", p / "inventory.json", p / "old.zip")
            self.assertFalse((p / "packet").exists())

    def test_policy_has_no_secrets_telegram_or_unscoped_catalog_mutation(self):
        p = operations.policies()
        text = json.dumps(p)
        for forbidden in (
            "secretsmanager:",
            "sns:",
            "AirLake_Subscriptions",
            "AirLake_Bot",
            "PowerUserAccess",
        ):
            self.assertNotIn(forbidden, text)
        for role in p.values():
            for s in role["Statement"]:
                self.assertNotEqual(s["Resource"], "*")
        self.assertIn(DATABASE + "_r*", json.dumps(p["glue"]))

    def test_job_update_uses_existing_job_and_pinned_jars(self):
        p = {
            "baseline": {
                "job": {
                    "Role": "test-role",
                    "Command": {"Name": "glueetl", "ScriptLocation": "old"},
                    "MaxRetries": 3,
                    "NonOverridableArguments": {
                        "--manifest_key": "old",
                        "--manifest_sha256": "old",
                        "--extra-jars": "verified-runtime,verified-aws",
                    },
                    "Timeout": 10,
                    "ExecutionProperty": {"MaxConcurrentRuns": 1},
                }
            }
        }
        job = operations.desired_job(p, WINDOW)
        self.assertEqual(
            job["NonOverridableArguments"]["--extra-jars"],
            "verified-runtime,verified-aws",
        )
        self.assertNotIn("--manifest_key", job["NonOverridableArguments"])
        self.assertEqual(job["MaxRetries"], 0)
        self.assertIn("/operations/" + WINDOW, job["Command"]["ScriptLocation"])

    def test_namespace_allowlist_rejects_other_resources(self):
        self.assertTrue(refresh.allowed_database(DATABASE))
        self.assertTrue(refresh.allowed_database(refresh.version_database(WINDOW)))
        for value in ("airlake_gold", DATABASE + "_rx", "evil; DROP DATABASE x"):
            self.assertFalse(refresh.allowed_database(value))
        with self.assertRaises(ValueError):
            refresh.version_database("bad")

    def test_query_is_pinned_and_bounded(self):
        value = exported(WINDOW)
        sql = refresh.query_sql(value, WINDOW)
        self.assertIn("FOR VERSION AS OF 5", sql)
        self.assertIn("FOR VERSION AS OF 4", sql)
        self.assertIn("LIMIT 1001", sql)
        self.assertIn("AT TIME ZONE", sql)
        self.assertNotIn(DATABASE + '".', sql)


class ControlTests(unittest.TestCase):
    def test_enable_preserves_environment_and_sets_one_bounded_async_schedule(self):
        cloud = Cloud()
        cloud.c["lambda"].get_function_configuration.return_value = {
            "RevisionId": "revision",
            "Environment": {
                "Variables": {
                    "KEEP": "test-only",
                    "AIRLAKE_LIVE_RUN": "20261114_9e2c740b",
                }
            },
        }
        with (
            tempfile.TemporaryDirectory() as root,
            patch("live_data.operations.preflight"),
        ):
            result = operations.enable(cloud.c, Path(root), {"minutes": 45}, WINDOW)
            self.assertEqual(result["status"], "ENABLED_BOUNDED_NOT_ACCEPTED")
        for call in cloud.c["lambda"].update_function_configuration.call_args_list:
            self.assertEqual(
                call.kwargs["Environment"]["Variables"]["KEEP"], "test-only"
            )
            self.assertEqual(call.kwargs["RevisionId"], "revision")
        args = cloud.c["scheduler"].update_schedule.call_args.kwargs
        self.assertEqual(args["State"], "ENABLED")
        self.assertEqual((args["EndDate"] - args["StartDate"]).total_seconds(), 2700)
        self.assertEqual(cloud.c["lambda"].put_function_concurrency.call_count, 2)
        self.assertTrue(
            all(
                x.kwargs["ReservedConcurrentExecutions"] == 1
                for x in cloud.c["lambda"].put_function_concurrency.call_args_list
            )
        )

    def test_status_command_is_read_only_and_parses(self):
        with (
            patch("live_data.operations.load", return_value=({}, WINDOW)),
            patch("live_data.operations.clients") as c,
            patch("live_data.operations.identity"),
            patch(
                "live_data.operations_evidence.status",
                return_value={"mode": "READ_ONLY"},
            ) as status,
            patch("sys.argv", ["operations", "status", "--packet", "/tmp/unused"]),
        ):
            operations.main()
            status.assert_called_once_with(c.return_value, WINDOW)

    def test_fresh_input_boundary_records_measurement_watermark(self):
        cloud = Cloud()
        cloud.controller.tick(NOW)
        self.assertEqual(
            cloud.items[("OPS", WINDOW)]["input_watermark"], "2026-11-14T11:00:00+00:00"
        )
        manifest = cloud.c["s3"].put_object.call_args.kwargs
        self.assertEqual(manifest["IfNoneMatch"], "*")
        self.assertEqual(json.loads(manifest["Body"]), cloud.manifest)

    def test_api_rejects_pointer_snapshot_mismatch(self):
        cloud = Cloud()
        for minute in (0, 5, 10):
            cloud.controller.tick(NOW + timedelta(minutes=minute))
        pointer = cloud.items[("EXPORT", "CURRENT")]
        pointer["snapshots"]["hourly"] = [999]
        adapter = Mock(s3=cloud.c["s3"])
        adapter.read.return_value = pointer
        with self.assertRaises(RuntimeError):
            load_export(adapter)

    def test_http_budget_three_reads_and_csv_matches_promoted_json(self):
        from live_data.operations_evidence import smoke

        cloud = Cloud()
        for minute in (0, 5, 10):
            cloud.controller.tick(NOW + timedelta(minutes=minute))
        pointer = cloud.items[("EXPORT", "CURRENT")]
        cloud.c["lambda"].get_function_url_config.return_value = {
            "AuthType": "AWS_IAM",
            "FunctionUrl": "https://synthetic.lambda-url.ap-southeast-2.on.aws/",
        }
        latest = {
            "count": 3,
            "generated_at": NOW.isoformat(),
            "metadata": {"poll_interval_is_not_source_freshness": True},
            "data": [
                {
                    "location_id": loc,
                    "freshness": {"status": "no_data"},
                    "poll": {"status": "test-only"},
                }
                for loc in refresh.PILOTS
            ],
        }
        row = cloud.value["facts"][0]
        current = {
            "data": [row],
            "count": 1,
            "total": 1,
            "next_offset": None,
            "metadata": {
                "export_sha256": pointer["sha256"],
                "athena_query_id": pointer["athena_query_id"],
            },
        }
        responses = [
            Mock(status_code=200),
            Mock(status_code=200),
            Mock(status_code=200),
        ]
        responses[0].json.return_value = latest
        responses[1].json.return_value = current
        responses[
            2
        ].text = (
            "location_id,hour_start,pm25_avg,aqi\n4946812,2026-11-14T11:00:00Z,10,20\n"
        )
        get = Mock(side_effect=responses)
        with tempfile.TemporaryDirectory() as root:
            self.assertEqual(
                smoke(cloud.c, Path(root), WINDOW, get)["status"],
                "PASS_THREE_API_READS",
            )
            self.assertTrue((Path(root) / "latest-api.html").exists())
            with self.assertRaises(ValueError):
                smoke(cloud.c, Path(root), WINDOW, get)
        self.assertEqual(get.call_count, 3)

    def test_http_failure_is_not_retried(self):
        from live_data.operations_evidence import smoke

        cloud = Cloud()
        cloud.c["lambda"].get_function_url_config.return_value = {
            "AuthType": "AWS_IAM",
            "FunctionUrl": "https://synthetic.lambda-url.ap-southeast-2.on.aws/",
        }
        get = Mock(return_value=Mock(status_code=503))
        with tempfile.TemporaryDirectory() as root, self.assertRaises(ValueError):
            smoke(cloud.c, Path(root), WINDOW, get)
        get.assert_called_once()


class InventoryTests(unittest.TestCase):
    def test_inventory_excludes_function_environment_values(self):
        cloud = Cloud()
        c = cloud.c
        c["sts"] = Mock()
        c["sts"].get_caller_identity.return_value = {"Account": "701364614078"}
        c["lambda"].get_function_configuration.side_effect = lambda **kw: {
            "FunctionName": kw["FunctionName"],
            "CodeSha256": "code-hash",
            "Role": "role",
            "Timeout": 180,
            "State": "Active",
            "LastUpdateStatus": "Successful",
            "RevisionId": "revision",
            "Environment": {"Variables": {"DO_NOT_PUBLISH": "private-test-only"}},
        }
        c["lambda"].get_function_concurrency.return_value = {
            "ReservedConcurrentExecutions": 0
        }
        c["glue"].get_job.return_value = {
            "Job": {"Role": "role", "Command": {"Name": "glueetl"}, "CreatedOn": NOW}
        }
        result = operations.inventory(c)
        text = json.dumps(result)
        self.assertNotIn("DO_NOT_PUBLISH", text)
        self.assertNotIn("private-test-only", text)
        self.assertNotIn("CreatedOn", result["job"])
        self.assertEqual(result["run"]["attempts"]["N"], "4")


class CSVVerificationTests(unittest.TestCase):
    def test_duplicate_csv_keys_cannot_substitute_for_missing_rows(self):
        from live_data.operations_evidence import smoke

        cloud = Cloud()
        cloud.c["lambda"].get_function_url_config.return_value = {
            "AuthType": "AWS_IAM",
            "FunctionUrl": "https://synthetic.lambda-url.ap-southeast-2.on.aws/",
        }
        pointer = cloud.items[("EXPORT", "CURRENT")]
        latest = {
            "count": 3,
            "generated_at": NOW.isoformat(),
            "metadata": {"poll_interval_is_not_source_freshness": True},
            "data": [
                {
                    "location_id": loc,
                    "freshness": {"status": "no_data"},
                    "poll": {"status": "test-only"},
                }
                for loc in refresh.PILOTS
            ],
        }
        one = cloud.value["facts"][0]
        two = {**one, "hour_start": "2026-11-14T10:00:00Z"}
        current = {
            "data": [one, two],
            "count": 2,
            "total": 2,
            "next_offset": None,
            "metadata": {
                "export_sha256": pointer["sha256"],
                "athena_query_id": pointer["athena_query_id"],
            },
        }
        responses = [
            Mock(status_code=200),
            Mock(status_code=200),
            Mock(status_code=200),
        ]
        responses[0].json.return_value = latest
        responses[1].json.return_value = current
        responses[
            2
        ].text = "location_id,hour_start,pm25_avg,aqi\n4946812,2026-11-14T11:00:00Z,10,20\n4946812,2026-11-14T11:00:00Z,10,20\n"
        with tempfile.TemporaryDirectory() as root, self.assertRaises(ValueError):
            smoke(cloud.c, Path(root), WINDOW, Mock(side_effect=responses))
