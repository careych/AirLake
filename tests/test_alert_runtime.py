"""Executable MVP orchestration uses no AWS requests or Telegram sockets."""

import copy
import io
import json
import socket
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from alert_fakes import RULE, SNS, record, setup_app
from test_athena_alerts import page

from alerts.athena import AthenaQueryExecutor
from alerts.delivery import DynamoSubscriptions
from alerts.gold import InvalidAlert, SNSPublisher
from alerts.runtime import HistoricalSNS, execute, load_plan, main, validate_plan
from bot_lambda.src.lambda_function import application_from_environment, lambda_handler

ROOT = Path(__file__).resolve().parents[1]


class AlertRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(socket, "socket", side_effect=AssertionError("Offline only"))
        )
        self.plan, _ = load_plan(ROOT / "docs/plans/athena-alert-staging.json")
        self.config, _, _, _ = validate_plan(self.plan)
        (
            self.row,
            self.clock,
            self.table,
            self.subs,
            self.telegram,
            self.ledger,
            self.app,
        ) = setup_app()
        self.app.topic_arn = f"arn:aws:sns:{self.plan['region']}:{self.plan['account']}:{self.plan['names']['sns_topic']}"
        self.app.queue_arn = f"arn:aws:sqs:{self.plan['region']}:{self.plan['account']}:{self.plan['names']['queue']}"
        self.app.test_recipient = "101"
        self.app.historical_demo = True
        self.sns = SNS()
        self.transport = HistoricalSNS(self.sns)
        self.publisher = SNSPublisher(
            self.transport,
            self.app.topic_arn,
            self.ledger,
            account=self.plan["account"],
            region=self.plan["region"],
            stations=self.app.stations,
            clock=self.clock,
        )
        self.client, self.sts = Mock(), Mock()
        for client in (self.client, self.sts):
            client.meta = SimpleNamespace(region_name=self.plan["region"])
        self.sts.get_caller_identity.return_value = {"Account": self.plan["account"]}
        self.client.get_work_group.return_value = {
            "WorkGroup": {
                "State": "ENABLED",
                "Configuration": {
                    "EnforceWorkGroupConfiguration": True,
                    "EngineVersion": {
                        "EffectiveEngineVersion": "Athena engine version 3"
                    },
                    "ResultConfiguration": {
                        "OutputLocation": "s3://"
                        + self.config.results_bucket
                        + "/"
                        + self.config.results_prefix
                    },
                },
            }
        }
        self.client.start_query_execution.return_value = {
            "QueryExecutionId": "fixture-query"
        }
        self.execution = {
            "Query": self.config.sql(),
            "WorkGroup": self.config.workgroup,
            "QueryExecutionContext": {
                "Catalog": self.config.catalog,
                "Database": self.config.database,
            },
            "ResultConfiguration": {
                "OutputLocation": "s3://"
                + self.config.results_bucket
                + "/"
                + self.config.results_prefix
                + "fixture-query.csv"
            },
            "Status": {"State": "SUCCEEDED"},
        }
        self.client.get_query_execution.return_value = {
            "QueryExecution": self.execution
        }
        self.client.get_query_results.return_value = page()
        self.persisted = []

    def executor(self):
        return AthenaQueryExecutor(
            self.client,
            self.sts,
            self.config,
            stations=self.app.stations,
            rule=RULE,
            clock=self.clock,
            execute_queries=True,
            test_now=self.clock(),
        )

    def run_once(self, executor=None):
        result = {}
        execute(
            self.plan,
            executor or self.executor(),
            self.publisher,
            result,
            lambda r: self.persisted.append(copy.deepcopy(r)),
        )
        return result

    def envelope(self):
        payload = json.loads(self.sns.messages[-1]["Message"])
        item = record(payload)
        item["eventSourceARN"] = self.app.queue_arn
        wrapper = json.loads(item["body"])
        wrapper["TopicArn"] = self.app.topic_arn
        item["body"] = json.dumps(wrapper)
        return {"Records": [item]}

    def test_full_callable_path_labels_historical_delivery_and_evidence(self):
        result = self.run_once()
        self.assertEqual("published", result["publication"])
        self.assertEqual("fixture-query", result["query_id"])
        self.assertIn("HISTORICAL", self.sns.messages[0]["Subject"])
        self.assertEqual(
            [], lambda_handler(self.envelope(), None, app=self.app)["batchItemFailures"]
        )
        self.assertIn("HISTORICAL DEMONSTRATION", self.telegram.sent[0][1])
        self.assertIn("NOT A CURRENT", self.telegram.sent[0][1])
        self.assertNotIn("101", json.dumps(result))

    def test_unchanged_event_deduplicates_publication_and_consumer(self):
        self.run_once()
        self.assertEqual("duplicate", self.run_once()["publication"])
        self.assertEqual(1, len(self.sns.messages))
        for _ in range(2):
            self.assertEqual(
                [],
                lambda_handler(self.envelope(), None, app=self.app)[
                    "batchItemFailures"
                ],
            )
        self.assertEqual(1, len(self.telegram.sent))

    def test_query_failure_and_wrong_snapshot_query_never_publish(self):
        for value in ("FAILED", "SUCCEEDED"):
            self.execution["Status"]["State"] = value
            if value == "SUCCEEDED":
                self.execution["Query"] = self.config.sql().replace(
                    self.config.hourly_snapshot, "123"
                )
            with self.subTest(state=value), self.assertRaises(InvalidAlert):
                self.run_once()
        self.assertEqual([], self.sns.messages)
        self.assertTrue(self.persisted[-1]["status"].startswith("FAILED"))

    def test_executor_plan_snapshot_mismatch_rejects_before_query(self):
        from dataclasses import replace

        executor = self.executor()
        executor.config = replace(self.config, hourly_snapshot="123")
        with self.assertRaises(InvalidAlert):
            self.run_once(executor)
        self.client.start_query_execution.assert_not_called()

    def test_mismatched_values_or_identity_do_not_publish(self):
        for index, value in ((0, "4946813"), (1, "invented"), (3, "22.0"), (4, "45")):
            response = page()
            response["ResultSet"]["Rows"][1]["Data"][index]["VarCharValue"] = value
            self.client.get_query_results.return_value = response
            with self.subTest(index=index), self.assertRaises(InvalidAlert):
                self.run_once()
        self.assertEqual([], self.sns.messages)

    def test_sns_uncertainty_retains_event_and_stops_without_retry(self):
        self.sns.fail = True
        with self.assertRaises(InvalidAlert):
            self.run_once()
        self.assertEqual(1, len(self.sns.messages))
        self.assertEqual("before_publish", self.persisted[-1]["phase"])
        self.assertIsNotNone(self.persisted[-1]["event_id"])

    def test_telegram_failure_partial_batch_retry_and_redrive_expectation(self):
        self.run_once()
        event = self.envelope()
        self.telegram.fail_for.add("101")
        for _ in range(self.plan["sqs"]["RedrivePolicy"]["maxReceiveCount"]):
            self.assertEqual(
                [{"itemIdentifier": "sqs-1"}],
                lambda_handler(event, None, app=self.app)["batchItemFailures"],
            )
        self.assertEqual([], self.telegram.sent)
        # This tests retry responses, not the real SQS service's DLQ transfer.
        self.assertEqual(5, self.plan["sqs"]["RedrivePolicy"]["maxReceiveCount"])
        self.telegram.fail_for.clear()
        self.assertEqual(
            [], lambda_handler(event, None, app=self.app)["batchItemFailures"]
        )
        self.assertEqual(1, len(self.telegram.sent))

    def test_recipient_lookup_uses_get_item_and_never_scan(self):
        table = Mock()
        table.get_item.return_value = {"Item": {"chat_id": "101"}}
        self.app.subscriptions = DynamoSubscriptions(
            table, self.app.stations, only_recipient="101"
        )
        self.run_once()
        lambda_handler(self.envelope(), None, app=self.app)
        table.scan.assert_not_called()
        table.get_item.assert_called_once_with(
            Key={"chat_id": "101"}, ConsistentRead=True
        )
        self.assertEqual(["101"], [x[0] for x in self.telegram.sent])

    def test_wrong_recipient_returned_from_table_rejects_before_send(self):
        table = Mock()
        table.get_item.return_value = {"Item": {"chat_id": "102"}}
        self.app.subscriptions = DynamoSubscriptions(
            table, self.app.stations, only_recipient="101"
        )
        self.run_once()
        self.assertTrue(
            lambda_handler(self.envelope(), None, app=self.app)["batchItemFailures"]
        )
        self.assertEqual([], self.telegram.sent)

    def test_shared_resource_identifier_rejected_offline(self):
        for key, value in [
            ("sns_topic", "AirLake_Alerts"),
            ("queue", "AirLake_Bot_Queue"),
            ("subscriptions", "AirLake_Subscriptions"),
            ("lambda", "AirLake_Bot_API"),
        ]:
            plan = copy.deepcopy(self.plan)
            plan["names"][key] = value
            with self.subTest(key=key), self.assertRaises(InvalidAlert):
                validate_plan(plan)

    def test_default_entrypoint_has_no_sdk_or_network(self):
        with (
            patch(
                "alerts.runtime.construct",
                side_effect=AssertionError("Must stay offline"),
            ),
            redirect_stdout(io.StringIO()) as output,
        ):
            self.assertEqual(0, main([]))
        self.assertIn("NO AWS REQUESTS", output.getvalue())

    def test_execution_confirmation_and_plan_hash_required(self):
        with (
            redirect_stdout(io.StringIO()),
            patch("sys.stderr", io.StringIO()),
            self.assertRaises(SystemExit),
        ):
            main(["--execute", "--confirm-account", "wrong"])

    def test_staging_factory_rejects_shared_names_before_clients(self):
        import sys

        env = {
            "AIRLAKE_ALERT_DELIVERY_ENABLED": "1",
            "AIRLAKE_ACCOUNT_ID": self.plan["account"],
            "AWS_REGION": self.plan["region"],
            "AIRLAKE_ALERT_STATIONS": json.dumps(self.plan["stations"]),
            "AIRLAKE_ALERT_RULES": json.dumps([self.plan["historical_test"]["rule"]]),
            "AIRLAKE_ALERT_TOPIC_ARN": self.app.topic_arn,
            "AIRLAKE_ALERT_QUEUE_ARN": self.app.queue_arn,
            "AIRLAKE_SUBSCRIPTION_TABLE": "AirLake_Subscriptions",
            "AIRLAKE_DELIVERY_TABLE": self.plan["names"]["delivery_ledger"],
            "AIRLAKE_PARTIAL_BATCH_FAILURES": "1",
            "AIRLAKE_ALERT_TEST_MODE": "1",
            "AIRLAKE_ALERT_TEST_RECIPIENT": "101",
            "AIRLAKE_ALERT_TEST_BOT_ID": "900001",
            "AIRLAKE_ALERT_STAGING_NAME": self.plan["names"]["lambda"],
        }
        session = Mock()
        with (
            patch.dict("os.environ", env, clear=True),
            patch.dict(
                sys.modules,
                {
                    "boto3": SimpleNamespace(Session=session),
                    "botocore.config": SimpleNamespace(Config=Mock()),
                },
            ),
            self.assertRaisesRegex(InvalidAlert, "isolated staging resources"),
        ):
            application_from_environment(
                SimpleNamespace(
                    invoked_function_arn=f"arn:aws:lambda:{self.plan['region']}:{self.plan['account']}:function:{self.plan['names']['lambda']}"
                )
            )
        session.assert_not_called()


class SecretFactoryTests(unittest.TestCase):
    def setUp(self):
        self.plan, _ = load_plan(ROOT / "docs/plans/athena-alert-staging.json")
        p = self.plan
        n = p["names"]
        account, region = p["account"], p["region"]
        self.env = {
            "AIRLAKE_ALERT_DELIVERY_ENABLED": "1",
            "AWS_REGION": region,
            "AIRLAKE_ACCOUNT_ID": account,
            "AIRLAKE_ALERT_STATIONS": json.dumps(p["stations"]),
            "AIRLAKE_ALERT_RULES": json.dumps([p["historical_test"]["rule"]]),
            "AIRLAKE_ALERT_TOPIC_ARN": f"arn:aws:sns:{region}:{account}:{n['sns_topic']}",
            "AIRLAKE_ALERT_QUEUE_ARN": f"arn:aws:sqs:{region}:{account}:{n['queue']}",
            "AIRLAKE_SUBSCRIPTION_TABLE": n["subscriptions"],
            "AIRLAKE_DELIVERY_TABLE": n["delivery_ledger"],
            "AIRLAKE_DELIVERY_LEASE_SECONDS": "180",
            "AIRLAKE_DEDUPE_RETENTION_SECONDS": "5184000",
            "AIRLAKE_PARTIAL_BATCH_FAILURES": "1",
            "AIRLAKE_ALERT_TEST_MODE": "1",
            "AIRLAKE_ALERT_TEST_RECIPIENT": "101",
            "AIRLAKE_ALERT_TEST_BOT_ID": "900001",
            "AIRLAKE_ALERT_TEST_REFERENCE_TIME": p["historical_test"]["reference_time"],
            "AIRLAKE_ALERT_STAGING_NAME": n["lambda"],
            "TELEGRAM_TOKEN_SECRET_ARN": f"arn:aws:secretsmanager:{region}:{account}:secret:airlake/alert-staging-v1/telegram-Ab1234",
        }
        self.context = SimpleNamespace(
            invoked_function_arn=f"arn:aws:lambda:{region}:{account}:function:{n['lambda']}:staging"
        )
        self.session = Mock()
        self.session.client.return_value.get_secret_value.return_value = {
            "SecretString": "900001:fixture-only-token"
        }
        import sys

        self.enterContext(
            patch.dict(
                sys.modules,
                {
                    "boto3": SimpleNamespace(Session=Mock(return_value=self.session)),
                    "botocore.config": SimpleNamespace(Config=Mock()),
                },
            )
        )
        self.enterContext(patch.dict("os.environ", self.env, clear=True))
        self.enterContext(
            patch.object(socket, "socket", side_effect=AssertionError("Offline only"))
        )

    def test_factory_fetches_only_referenced_secret_and_targets_single_subscription(
        self,
    ):
        app = application_from_environment(self.context)
        self.assertEqual("101", app.subscriptions.only_recipient)
        self.assertTrue(app.historical_demo)
        self.assertEqual(2025, app.clock().year)
        self.assertGreater(app.ledger.clock().year, 2025)
        self.session.client.assert_called_once()
        self.assertEqual("secretsmanager", self.session.client.call_args.args[0])
        self.session.client.return_value.get_secret_value.assert_called_once_with(
            SecretId=self.env["TELEGRAM_TOKEN_SECRET_ARN"]
        )

    def test_secret_failure_is_sanitized_and_no_dynamo_created(self):
        self.session.client.return_value.get_secret_value.side_effect = RuntimeError(
            "PRIVATE_SECRET"
        )
        with self.assertRaises(InvalidAlert) as error:
            application_from_environment(self.context)
        self.assertNotIn("PRIVATE_SECRET", str(error.exception))
        self.session.resource.assert_not_called()

    def test_raw_token_rejected_in_test_mode_before_secret_read(self):
        with (
            patch.dict("os.environ", {"TELEGRAM_TOKEN": "fixture-only-token"}),
            self.assertRaises(InvalidAlert),
        ):
            application_from_environment(self.context)
        self.session.client.assert_not_called()

    def test_staging_function_cannot_disable_mode_and_remove_marker_environment(self):
        env = dict(
            self.env, AIRLAKE_ALERT_TEST_MODE="0", TELEGRAM_TOKEN="fixture-only-token"
        )
        for name in (
            "AIRLAKE_ALERT_TEST_RECIPIENT",
            "AIRLAKE_ALERT_TEST_REFERENCE_TIME",
            "AIRLAKE_ALERT_TEST_BOT_ID",
            "AIRLAKE_ALERT_STAGING_NAME",
            "TELEGRAM_TOKEN_SECRET_ARN",
        ):
            env.pop(name)
        env.update(
            AIRLAKE_ALERT_TOPIC_ARN="arn:aws:sns:ap-southeast-2:701364614078:AirLake_Alerts",
            AIRLAKE_ALERT_QUEUE_ARN="arn:aws:sqs:ap-southeast-2:701364614078:AirLake_Bot_Queue",
            AIRLAKE_SUBSCRIPTION_TABLE="AirLake_Subscriptions",
            AIRLAKE_DELIVERY_TABLE="ProductionDeliveryLedger",
        )
        with patch.dict("os.environ", env, clear=True), self.assertRaises(InvalidAlert):
            application_from_environment(self.context)
        self.session.client.assert_not_called()
        self.session.resource.assert_not_called()

    def test_staging_resource_cannot_disable_mode_from_another_function(self):
        env = dict(self.env, AIRLAKE_ALERT_TEST_MODE="0")
        for name in (
            "AIRLAKE_ALERT_TEST_RECIPIENT",
            "AIRLAKE_ALERT_TEST_REFERENCE_TIME",
            "AIRLAKE_ALERT_TEST_BOT_ID",
            "AIRLAKE_ALERT_STAGING_NAME",
        ):
            env.pop(name)
        context = SimpleNamespace(
            invoked_function_arn="arn:aws:lambda:ap-southeast-2:701364614078:function:OtherBot"
        )
        with patch.dict("os.environ", env, clear=True), self.assertRaises(InvalidAlert):
            application_from_environment(context)
        self.session.client.assert_not_called()

    def test_staging_requires_partial_batches_and_no_webhook_secret(self):
        for change in (
            {"AIRLAKE_PARTIAL_BATCH_FAILURES": "0"},
            {"TELEGRAM_WEBHOOK_SECRET": "fixture-webhook-secret"},
        ):
            with (
                self.subTest(change=change),
                patch.dict("os.environ", change),
                self.assertRaises(InvalidAlert),
            ):
                application_from_environment(self.context)
        self.session.client.assert_not_called()

    def test_staging_rejects_wrong_account_or_region_before_clients(self):
        for account, region in (
            ("000000000000", "ap-southeast-2"),
            ("701364614078", "us-east-1"),
        ):
            env = dict(self.env, AIRLAKE_ACCOUNT_ID=account, AWS_REGION=region)
            context = SimpleNamespace(
                invoked_function_arn=f"arn:aws:lambda:{region}:{account}:function:airlake-alert-staging-v1"
            )
            with (
                self.subTest(account=account, region=region),
                patch.dict("os.environ", env, clear=True),
                self.assertRaises(InvalidAlert),
            ):
                application_from_environment(context)
        self.session.client.assert_not_called()

    def test_staging_requires_owner_approved_positive_bot_identity(self):
        for value in (None, "", "0", "-900001", "0900001", "900001,900002"):
            env = dict(self.env)
            if value is None:
                env.pop("AIRLAKE_ALERT_TEST_BOT_ID")
            else:
                env["AIRLAKE_ALERT_TEST_BOT_ID"] = value
            with (
                self.subTest(value=value),
                patch.dict("os.environ", env, clear=True),
                self.assertRaises(InvalidAlert),
            ):
                application_from_environment(self.context)
        self.session.client.assert_not_called()

    def test_wrong_bot_token_is_rejected_without_dynamo_or_sensitive_error(self):
        token = "900002:fixture-only-token"
        self.session.client.return_value.get_secret_value.return_value = {
            "SecretString": token
        }
        with self.assertRaises(InvalidAlert) as error:
            application_from_environment(self.context)
        self.assertNotIn(token, str(error.exception))
        self.session.resource.assert_not_called()

    def test_secret_is_plaintext_not_json_and_rejects_malformed_values(self):
        for token in (
            '{"token":"900001:fixture-only-token"}',
            {"token": "fixture-only-token"},
            "900001:fixture-only-token\n",
            "900001:",
            "900001:fixture:extra",
        ):
            self.session.client.return_value.get_secret_value.return_value = {
                "SecretString": token
            }
            with self.subTest(token=token), self.assertRaises(InvalidAlert):
                application_from_environment(self.context)
        self.session.resource.assert_not_called()

    def test_every_shared_destination_is_rejected_before_secret_lookup(self):
        for name, value in (
            (
                "AIRLAKE_ALERT_TOPIC_ARN",
                "arn:aws:sns:ap-southeast-2:701364614078:AirLake_Alerts",
            ),
            (
                "AIRLAKE_ALERT_QUEUE_ARN",
                "arn:aws:sqs:ap-southeast-2:701364614078:AirLake_Bot_Queue",
            ),
            ("AIRLAKE_SUBSCRIPTION_TABLE", "AirLake_Subscriptions"),
        ):
            with (
                self.subTest(name=name),
                patch.dict("os.environ", {name: value}),
                self.assertRaises(InvalidAlert),
            ):
                application_from_environment(self.context)
        self.session.client.assert_not_called()


class StagingPlanBoundaryTests(unittest.TestCase):
    def test_roles_and_secret_name_cannot_be_retargeted(self):
        from alerts.runtime import validate_plan

        original, _ = load_plan(ROOT / "docs/plans/athena-alert-staging.json")
        for kind, name in (
            ("names", "consumer_role"),
            ("names", "reader_role"),
            ("secrets", "name"),
        ):
            plan = json.loads(json.dumps(original))
            plan[kind][name] = "ProductionResource"
            with self.subTest(name=name), self.assertRaises(InvalidAlert):
                validate_plan(plan)
