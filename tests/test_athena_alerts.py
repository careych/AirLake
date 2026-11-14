"""Offline Athena/alert integration; AWS and Telegram are service doubles."""

import copy
import json
import socket
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

from alert_fakes import (
    ACCOUNT,
    REGION,
    RULE,
    SNS,
    TOPIC,
    gold_fixture,
    record,
    setup_app,
)

from alerts.athena import COLUMNS, AthenaConfig, AthenaGoldSource, AthenaQueryExecutor
from alerts.delivery import DeliveryError, TelegramSender
from alerts.gold import InvalidAlert, SNSPublisher, evaluate, publish_batch
from bot_lambda.src.lambda_function import (
    Application,
    lambda_handler,
    test_recipient_policy,
)


def configuration(**changes):
    row = gold_fixture()
    catalog, database, hourly = row["source_table"].split(".")
    return replace(
        AthenaConfig(
            account=ACCOUNT,
            region=REGION,
            workgroup="fixture",
            catalog=catalog,
            database=database,
            hourly_table=hourly,
            dimension_table="dim_location",
            allowed_tables=(
                (catalog, database, hourly),
                (catalog, database, "dim_location"),
            ),
            hourly_snapshot=row["source_snapshot_id"],
            dimension_snapshot="4323462633891640540",
            start="2025-07-03T15:00:00Z",
            end="2025-07-03T16:00:00Z",
            location_ids=(row["location_id"],),
            results_bucket="fixture-results",
        ),
        **changes,
    )


def page(rows=None, *, header=True, token=None):
    if rows is None:
        row = gold_fixture()
        rows = [
            [
                str(row["location_id"]),
                row["name"],
                "2025-07-03 15:00:00.000000 UTC",
                str(row["pm25_avg"]),
                str(row["aqi"]),
            ]
        ]
    values = ([[n for n, _ in COLUMNS]] if header else []) + rows
    result = {
        "ResultSet": {
            "ResultSetMetadata": {
                "ColumnInfo": [{"Name": n, "Type": t} for n, t in COLUMNS]
            },
            "Rows": [
                {"Data": [{} if v is None else {"VarCharValue": v} for v in row]}
                for row in values
            ],
        }
    }
    if token is not None:
        result["NextToken"] = token
    return result


class AthenaReaderTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(
                socket,
                "socket",
                side_effect=AssertionError("No network in offline tests"),
            )
        )
        self.row, self.clock, _, _, self.telegram, self.ledger, self.app = setup_app()
        self.client, self.identity = Mock(), Mock()
        for client in (self.client, self.identity):
            client.meta = SimpleNamespace(region_name=REGION)
        self.identity.get_caller_identity.return_value = {"Account": ACCOUNT}
        self.client.get_work_group.return_value = {
            "WorkGroup": {
                "State": "ENABLED",
                "Configuration": {
                    "EnforceWorkGroupConfiguration": True,
                    "EngineVersion": {
                        "EffectiveEngineVersion": "Athena engine version 3"
                    },
                    "ResultConfiguration": {"OutputLocation": "s3://fixture-results/"},
                },
            }
        }
        self.client.start_query_execution.return_value = {
            "QueryExecutionId": "fixture-query-id"
        }
        self.client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        self.client.get_query_results.return_value = page()

    def executor(self, config=None, **options):
        selected = config or configuration()
        execution = self.client.get_query_execution.return_value["QueryExecution"]
        execution.setdefault("Query", selected.sql())
        execution.setdefault("WorkGroup", selected.workgroup)
        execution.setdefault(
            "QueryExecutionContext",
            {"Catalog": selected.catalog, "Database": selected.database},
        )
        execution.setdefault(
            "ResultConfiguration",
            {
                "OutputLocation": f"s3://{selected.results_bucket}/{selected.results_prefix}fixture-query-id.csv"
            },
        )
        return AthenaQueryExecutor(
            self.client,
            self.identity,
            config or configuration(),
            stations=self.app.stations,
            rule=RULE,
            clock=self.clock,
            execute_queries=options.pop("execute_queries", True),
            sleep=lambda _: None,
            **options,
        )

    def test_preparation_and_default_opt_out_make_no_service_calls(self):
        executor = self.executor(execute_queries=False)
        sql = executor.config.sql()
        self.assertIn("FOR VERSION AS OF " + self.row["source_snapshot_id"], sql)
        self.assertIn("FOR VERSION AS OF 4323462633891640540", sql)
        self.assertIn("LIMIT 1001", sql)
        self.assertIn("TIMESTAMP '2025-07-03 15:00:00 UTC'", sql)
        self.assertIn("LEFT JOIN", sql)
        with self.assertRaisesRegex(InvalidAlert, "disabled"):
            executor.read_pinned_hourly_rows()
        self.assertEqual([], self.client.mock_calls)
        self.assertEqual([], self.identity.mock_calls)

    def test_live_schema_rows_reach_evaluator_and_mocked_delivery(self):
        source = AthenaGoldSource(self.executor())
        sns = SNS()
        publisher = SNSPublisher(
            sns,
            TOPIC,
            self.ledger,
            account=ACCOUNT,
            region=REGION,
            stations=self.app.stations,
            clock=self.clock,
        )
        self.assertEqual(
            ["published"],
            publish_batch(source, RULE, self.app.stations, self.clock(), publisher),
        )
        payload = json.loads(sns.messages[0]["Message"])
        event = {"Records": [record(payload)]}
        self.assertEqual(
            {"batchItemFailures": []}, lambda_handler(event, None, app=self.app)
        )
        self.assertEqual(
            {"batchItemFailures": []}, lambda_handler(event, None, app=self.app)
        )
        self.assertEqual(1, len(self.telegram.sent))
        self.assertEqual(
            self.row["source_snapshot_id"], payload["observation"]["source_snapshot_id"]
        )
        kwargs = self.client.start_query_execution.call_args.kwargs
        self.assertEqual(
            {"ExpectedBucketOwner": ACCOUNT}, kwargs["ResultConfiguration"]
        )
        self.assertFalse(
            kwargs["ResultReuseConfiguration"]["ResultReuseByAgeConfiguration"][
                "Enabled"
            ]
        )

    def test_default_catalog_case_is_normalized_but_other_catalog_is_rejected(self):
        base_config = configuration()
        config = replace(
            base_config,
            catalog="AwsDataCatalog",
            allowed_tables=(
                ("AwsDataCatalog", base_config.database, base_config.hourly_table),
                ("AwsDataCatalog", base_config.database, base_config.dimension_table),
            ),
        )
        self.executor(config)  # Populate metadata for Athena's default catalog.
        execution = self.client.get_query_execution.return_value["QueryExecution"]
        context = execution["QueryExecutionContext"]
        context["Catalog"] = "awsdatacatalog"
        rows = self.executor(config).read_pinned_hourly_rows()
        self.assertEqual(1, len(rows))

        context["Catalog"] = "other_catalog"
        with self.assertRaisesRegex(InvalidAlert, "context"):
            self.executor(config).read_pinned_hourly_rows()

        context["Catalog"] = "AwsDataCatalog"
        context["Database"] = "other_database"
        with self.assertRaisesRegex(InvalidAlert, "context"):
            self.executor(config).read_pinned_hourly_rows()

    def test_identifier_allowlist_and_snapshot_injection_reject_offline(self):
        for change in (
            {"database": "gold;DROP"},
            {"catalog": "other"},
            {"hourly_table": "unapproved"},
            {"hourly_snapshot": "1 OR true"},
            {"dimension_snapshot": "0"},
            {"hourly_snapshot": str(2**63)},
            {"hourly_table": "dim_location"},
        ):
            with self.subTest(change=change), self.assertRaises(InvalidAlert):
                configuration(**change).sql()
        self.assertEqual([], self.client.mock_calls)

    def test_window_and_station_scope_are_bounded(self):
        for change in (
            {"start": "2025-07-01T15:00:00Z"},
            {"end": "2025-07-03T15:00:00Z"},
            {"start": "2025-07-03T15:01:00Z"},
            {"location_ids": ()},
            {"location_ids": (True,)},
            {"location_ids": (1, 1)},
            {"max_rows": 1001},
            {"timeout_seconds": 0},
            {"historical_test_mode": "yes"},
        ):
            with self.subTest(change=change), self.assertRaises(InvalidAlert):
                configuration(**change).validate()

    def test_wrong_account_and_region_fail_before_submission(self):
        self.identity.get_caller_identity.return_value = {"Account": "111111111111"}
        with self.assertRaisesRegex(InvalidAlert, "account"):
            self.executor().read_pinned_hourly_rows()
        self.identity.get_caller_identity.return_value = {"Account": ACCOUNT}
        self.client.meta.region_name = "us-east-1"
        with self.assertRaisesRegex(InvalidAlert, "region"):
            self.executor().read_pinned_hourly_rows()
        self.client.start_query_execution.assert_not_called()

    def test_workgroup_engine_enforcement_and_output_fail_closed(self):
        baseline = copy.deepcopy(self.client.get_work_group.return_value)
        for change in ("engine", "enforce", "output", "state"):
            response = copy.deepcopy(baseline)
            group = response["WorkGroup"]
            settings = group["Configuration"]
            if change == "engine":
                settings["EngineVersion"]["EffectiveEngineVersion"] = (
                    "Athena engine version 2"
                )
            if change == "enforce":
                settings["EnforceWorkGroupConfiguration"] = False
            if change == "output":
                settings["ResultConfiguration"]["OutputLocation"] = "s3://unapproved/"
            if change == "state":
                group["State"] = "DISABLED"
            self.client.get_work_group.return_value = response
            with self.subTest(change=change), self.assertRaises(InvalidAlert):
                self.executor().read_pinned_hourly_rows()
        self.client.start_query_execution.assert_not_called()

    def test_historical_data_cannot_be_current_by_default(self):
        self.clock.now = datetime(2026, 11, 7, tzinfo=UTC)
        with self.assertRaisesRegex(InvalidAlert, "stale"):
            self.executor().read_pinned_hourly_rows()
        self.client.start_query_execution.assert_not_called()
        executor = self.executor(
            configuration(historical_test_mode=True),
            test_now=datetime(2025, 7, 3, 16, 30, tzinfo=UTC),
        )
        rows = executor.read_pinned_hourly_rows()
        self.assertEqual(1, len(rows))
        # The real consumer/evaluator clock is NOT moved backwards by reader test mode.
        with self.assertRaises(InvalidAlert):
            evaluate(
                SimpleNamespace(read=lambda: rows),
                RULE,
                self.app.stations,
                self.clock(),
            )

    def test_historical_clock_requires_both_explicit_controls(self):
        with self.assertRaises(InvalidAlert):
            self.executor(test_now=self.clock())
        with self.assertRaises(InvalidAlert):
            self.executor(configuration(historical_test_mode=True))

    def test_unclosed_hour_fails_before_query(self):
        self.clock.now = datetime(2025, 7, 3, 15, 50, tzinfo=UTC)
        with self.assertRaises(InvalidAlert):
            self.executor().read_pinned_hourly_rows()
        self.client.start_query_execution.assert_not_called()

    def test_failed_cancelled_unknown_queries_retain_query_id(self):
        for state in ("FAILED", "CANCELLED", "UNKNOWN"):
            self.client.get_query_execution.return_value = {
                "QueryExecution": {
                    "Status": {"State": state, "StateChangeReason": "SECRET"}
                }
            }
            executor = self.executor()
            with self.subTest(state=state), self.assertRaises(InvalidAlert) as error:
                executor.read_pinned_hourly_rows()
            self.assertNotIn("SECRET", str(error.exception))
            self.assertEqual("fixture-query-id", executor.query_id)

    def test_poll_timeout_does_not_resubmit_or_cancel(self):
        executor = self.executor(monotonic=Mock(side_effect=[0, 61]))
        with self.assertRaisesRegex(InvalidAlert, "timed out"):
            executor.read_pinned_hourly_rows()
        with self.assertRaisesRegex(InvalidAlert, "no implicit retry"):
            executor.read_pinned_hourly_rows()
        self.client.start_query_execution.assert_called_once()
        self.client.stop_query_execution.assert_not_called()

    def test_ambiguous_submission_is_sanitized_and_not_retried(self):
        self.client.start_query_execution.side_effect = TimeoutError("SECRET URL TOKEN")
        executor = self.executor()
        with self.assertRaises(InvalidAlert) as error:
            executor.read_pinned_hourly_rows()
        self.assertNotIn("SECRET", str(error.exception))
        self.assertEqual("submission-uncertain", executor.query_id)
        with self.assertRaises(InvalidAlert):
            executor.read_pinned_hourly_rows()
        self.client.start_query_execution.assert_called_once()

    def test_result_schema_mismatch(self):
        response = page()
        response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"][2]["Type"] = (
            "timestamp"
        )
        self.client.get_query_results.return_value = response
        with self.assertRaisesRegex(InvalidAlert, "schema"):
            self.executor().read_pinned_hourly_rows()

    def test_empty_results_and_missing_header_reject(self):
        for response in (page([]), page(header=False)):
            self.client.get_query_results.return_value = response
            with self.assertRaises(InvalidAlert):
                self.executor().read_pinned_hourly_rows()

    def test_results_paginate_without_losing_first_row_on_later_page(self):
        response = page()
        raw = response["ResultSet"]["Rows"][1]["Data"]
        second = [v["VarCharValue"] for v in raw]
        second[2] = "2025-07-03 16:00:00 UTC"
        self.client.get_query_results.side_effect = [
            page(token="next"),
            page([second], header=False),
        ]
        self.clock.now += timedelta(hours=1)
        rows = self.executor(
            configuration(end="2025-07-03T17:00:00Z")
        ).read_pinned_hourly_rows()
        self.assertEqual(2, len(rows))
        self.assertEqual(
            "next", self.client.get_query_results.call_args.kwargs["NextToken"]
        )

    def test_repeated_pagination_token_rejects(self):
        self.client.get_query_results.side_effect = [
            page(token="repeat"),
            page([], header=False, token="repeat"),
        ]
        with self.assertRaisesRegex(InvalidAlert, "pagination"):
            self.executor().read_pinned_hourly_rows()

    def test_duplicate_join_keys_and_result_overflow_reject(self):
        for config in (configuration(), configuration(max_rows=1)):
            response = page()
            response["ResultSet"]["Rows"].append(
                copy.deepcopy(response["ResultSet"]["Rows"][1])
            )
            self.client.get_query_results.return_value = response
            with self.assertRaises(InvalidAlert):
                self.executor(config).read_pinned_hourly_rows()

    def test_invalid_null_nonfinite_identity_timestamp_and_out_of_scope_results(self):
        original = [v["VarCharValue"] for v in page()["ResultSet"]["Rows"][1]["Data"]]
        for index, value in (
            (0, "4946813"),
            (0, "true"),
            (1, None),
            (1, "invented station"),
            (2, "2025-07-03 15:00:00"),
            (2, "2025-07-03 14:00:00 UTC"),
            (2, "2025-07-03 15:01:00 UTC"),
            (3, "NaN"),
            (3, "Infinity"),
            (3, "-1"),
            (4, "501"),
            (4, "44.0"),
        ):
            row = list(original)
            row[index] = value
            self.client.get_query_results.return_value = page([row])
            with (
                self.subTest(index=index, value=value),
                self.assertRaises(InvalidAlert),
            ):
                self.executor().read_pinned_hourly_rows()

    def test_access_denied_never_becomes_empty_success(self):
        self.client.get_query_results.side_effect = RuntimeError("AccessDenied SECRET")
        with self.assertRaises(InvalidAlert) as error:
            self.executor().read_pinned_hourly_rows()
        self.assertNotIn("SECRET", str(error.exception))

    def test_distinct_result_rows_over_limit_reject(self):
        response = page()
        raw = response["ResultSet"]["Rows"][1]["Data"]
        second = [v["VarCharValue"] for v in raw]
        second[2] = "2025-07-03 16:00:00 UTC"
        self.client.get_query_results.return_value = page(
            [[v["VarCharValue"] for v in raw], second]
        )
        self.clock.now += timedelta(hours=1)
        with self.assertRaisesRegex(InvalidAlert, "row limit"):
            self.executor(
                configuration(end="2025-07-03T17:00:00Z", max_rows=1)
            ).read_pinned_hourly_rows()

    def test_sdk_factory_configures_one_attempt_and_blocks_endpoint_overrides(self):
        import sys

        from alerts.athena import sdk_executor

        session = Mock()
        boto = SimpleNamespace(Session=Mock(return_value=session))
        settings = SimpleNamespace(Config=Mock(side_effect=lambda **kw: kw))
        with patch.dict(sys.modules, {"boto3": boto, "botocore.config": settings}):
            with (
                patch.dict(
                    "os.environ", {"AWS_ENDPOINT_URL": "https://unapproved.invalid"}
                ),
                self.assertRaises(InvalidAlert),
            ):
                sdk_executor(
                    configuration(),
                    stations=self.app.stations,
                    rule=RULE,
                    clock=self.clock,
                )
            boto.Session.assert_not_called()
            sdk_executor(
                configuration(), stations=self.app.stations, rule=RULE, clock=self.clock
            )
            config = settings.Config.call_args.kwargs
            self.assertEqual(1, config["retries"]["total_max_attempts"])
            self.assertTrue(config["ignore_configured_endpoint_urls"])
            self.assertEqual(2, session.client.call_count)
            session.client.return_value.start_query_execution.assert_not_called()


class RecipientIsolationTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(socket, "socket", side_effect=AssertionError("Offline only"))
        )
        (
            self.row,
            self.clock,
            self.table,
            self.subs,
            self.telegram,
            self.ledger,
            self.app,
        ) = setup_app()
        from alerts.gold import Alert, Observation

        self.payload = Alert(Observation.from_row(self.row), RULE).payload()

    def guarded(self, **overrides):
        kwargs = {
            "stations": self.app.stations,
            "rules": [RULE],
            "clock": self.clock,
            "queue_arn": self.app.queue_arn,
            "topic_arn": TOPIC,
            "account": ACCOUNT,
            "region": REGION,
            "partial_batch": True,
            "webhook_secret": "fixture-webhook-secret",
            "test_mode": True,
            "test_recipient": "101",
        }
        kwargs.update(overrides)
        return Application(self.app.subscriptions, self.telegram, self.ledger, **kwargs)

    def test_test_mode_requires_exactly_one_canonical_recipient(self):
        for mode, recipient in (
            (True, None),
            (True, ""),
            (True, "101,102"),
            (False, "101"),
            ("1", "101"),
        ):
            with self.subTest(mode=mode), self.assertRaises(InvalidAlert):
                test_recipient_policy(mode, recipient)
        self.assertEqual("101", test_recipient_policy(True, "101"))
        self.assertIsNone(test_recipient_policy(False, None))

    def test_only_approved_eligible_recipient_receives_retry_and_duplicate(self):
        self.subs.pages = [[{"chat_id": "101"}, {"chat_id": "102"}]]
        app = self.guarded()
        event = {"Records": [record(self.payload)]}
        self.telegram.fail_for.add("101")
        self.assertTrue(lambda_handler(event, None, app=app)["batchItemFailures"])
        self.telegram.fail_for.clear()
        self.assertEqual([], lambda_handler(event, None, app=app)["batchItemFailures"])
        self.assertEqual([], lambda_handler(event, None, app=app)["batchItemFailures"])
        self.assertEqual(["101", "101"], [x[0] for x in self.telegram.attempts])
        self.assertEqual(1, len(self.telegram.sent))

    def test_guard_never_creates_eligibility_or_subscriptions(self):
        self.subs.pages = [[{"chat_id": "102"}]]
        self.assertEqual(
            [],
            lambda_handler(
                {"Records": [record(self.payload)]}, None, app=self.guarded()
            )["batchItemFailures"],
        )
        self.assertEqual([], self.telegram.sent)
        self.assertEqual([], self.subs.writes)

    def test_other_webhook_recipient_cannot_send_or_change_subscriptions(self):
        event = {
            "headers": {"X-Telegram-Bot-Api-Secret-Token": "fixture-webhook-secret"},
            "body": json.dumps(
                {
                    "update_id": 123,
                    "message": {"chat": {"id": 102}, "text": "/subscribe"},
                }
            ),
        }
        self.assertEqual(
            403, lambda_handler(event, None, app=self.guarded())["statusCode"]
        )
        self.assertEqual([], self.subs.writes)
        self.assertEqual([], self.telegram.sent)
        self.assertEqual({}, self.table.items)

    def test_approved_webhook_commands_and_duplicates_preserved(self):
        app = self.guarded()
        for i, command in enumerate(("/subscribe", "/unsubscribe", "/help")):
            event = {
                "headers": {
                    "X-Telegram-Bot-Api-Secret-Token": "fixture-webhook-secret"
                },
                "body": json.dumps(
                    {
                        "update_id": 123 + i,
                        "message": {"chat": {"id": 101}, "text": command},
                    }
                ),
            }
            self.assertEqual(200, lambda_handler(event, None, app=app)["statusCode"])
            self.assertEqual(200, lambda_handler(event, None, app=app)["statusCode"])
        self.assertEqual(3, len(self.telegram.sent))

    def test_http_timeout_and_5xx_are_sanitized_retryable_failures(self):
        for exception in (
            TimeoutError("PRIVATE"),
            __import__("urllib.error").error.HTTPError(
                "https://private.invalid", 503, "PRIVATE", {}, None
            ),
        ):
            sender = TelegramSender("fixture-token", opener=Mock(side_effect=exception))
            with (
                self.subTest(exception=type(exception)),
                self.assertRaises(DeliveryError) as error,
            ):
                sender.send("101", "test")
            self.assertNotIn("PRIVATE", str(error.exception))


class HistoricalRuntimeClockTests(unittest.TestCase):
    def test_fixed_reference_is_explicit_and_does_not_rewind_ledger(self):
        from alert_fakes import LedgerTable

        from alerts.delivery import DynamoLedger
        from bot_lambda.src.lambda_function import observation_clock

        live_now = datetime(2026, 11, 7, tzinfo=UTC)
        wall_clock = lambda: live_now
        with self.assertRaises(InvalidAlert):
            observation_clock(False, "2025-07-03T16:30:00Z", wall_clock)
        clock = observation_clock(True, "2025-07-03T16:30:00Z", wall_clock)
        self.assertEqual(2025, clock().year)
        table = LedgerTable()
        ledger = DynamoLedger(
            table, wall_clock, lease_seconds=180, retention_seconds=30 * 86400
        )
        ledger.claim("fixture-history-test")
        item = table.items["fixture-history-test"]
        self.assertEqual(int(live_now.timestamp()) + 180, item["lease_until"])
        self.assertEqual(int(live_now.timestamp()) + 30 * 86400, item["expires_at"])
        self.assertIs(wall_clock, observation_clock(False, None, wall_clock))
