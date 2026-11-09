"""Offline bot, lease and transport checks adapted to the teammate code."""

import base64
import json
import socket
import unittest
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from unittest.mock import MagicMock, Mock, patch

from alert_fakes import RULE, record, setup_app

from alerts.delivery import DeliveryError, DynamoSubscriptions, TelegramSender
from alerts.gold import Alert, InvalidAlert, Observation
from bot_lambda.src.create_table import create_subscription_table, table_request
from bot_lambda.src.lambda_function import application_from_environment, lambda_handler


class AlertDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(
                socket,
                "socket",
                side_effect=AssertionError("Offline tests forbid network"),
            )
        )
        (
            self.row,
            self.clock,
            self.table,
            self.subs,
            self.sender,
            self.ledger,
            self.app,
        ) = setup_app()
        self.payload = Alert(Observation.from_row(self.row), RULE).payload()

    def call(self, records):
        return lambda_handler({"Records": records}, None, app=self.app)

    def test_raw_and_sns_wrapped_records_deliver_once(self):
        result = self.call(
            [record(self.payload), record(self.payload, raw=True, message_id="sqs-raw")]
        )
        self.assertEqual({"batchItemFailures": []}, result)
        self.assertEqual(1, len(self.sender.sent))
        self.assertEqual("101", self.sender.sent[0][0])

    def test_partial_batch_reports_only_failed_message_id(self):
        invalid = record(self.payload, message_id="invalid")
        invalid["body"] = "not JSON"
        self.assertEqual(
            {"batchItemFailures": [{"itemIdentifier": "invalid"}]},
            self.call([record(self.payload), invalid]),
        )
        self.assertEqual(1, len(self.sender.sent))

    def test_without_partial_batch_configuration_failures_raise(self):
        self.app.partial_batch = False
        with self.assertRaises(DeliveryError):
            self.call([dict(record(self.payload), body="invalid")])

    def test_unidentified_or_duplicate_sqs_record_ids_fail_the_batch(self):
        for rows in ([{"body": "{}"}], [record(self.payload), record(self.payload)]):
            with self.assertRaises(InvalidAlert):
                self.call(rows)
        self.assertEqual([], self.sender.sent)

    def test_wrong_queue_topic_and_unapproved_rule_reject(self):
        wrong_queue = dict(record(self.payload), eventSourceARN="untrusted")
        wrapped = json.loads(record(self.payload)["body"])
        wrapped["TopicArn"] = "untrusted"
        wrong_topic = dict(record(self.payload), body=json.dumps(wrapped))
        changed = Alert(
            Observation.from_row(self.row), replace(RULE, minimum_aqi=1)
        ).payload()
        for item in (wrong_queue, wrong_topic, record(changed)):
            with self.subTest(item=item):
                self.assertTrue(self.call([item])["batchItemFailures"])
        self.assertEqual([], self.sender.sent)

    def test_telegram_failure_retries_failed_recipient_without_duplicate_fanout(self):
        self.subs.pages = [[{"chat_id": "101"}, {"chat_id": "102"}]]
        self.sender.fail_for.add("102")
        self.assertEqual(
            [{"itemIdentifier": "sqs-1"}],
            self.call([record(self.payload)])["batchItemFailures"],
        )
        self.assertEqual(["101"], [x[0] for x in self.sender.sent])
        self.sender.fail_for.clear()
        self.assertEqual([], self.call([record(self.payload)])["batchItemFailures"])
        self.assertEqual(["101", "102"], [x[0] for x in self.sender.sent])
        self.assertEqual(["101", "102", "102"], [x[0] for x in self.sender.attempts])

    def test_subscription_pagination_and_eligibility(self):
        self.subs.pages = [
            [{"chat_id": "101", "station_ids": ["4946813"]}],
            [
                {
                    "chat_id": "102",
                    "station_ids": ["4946812"],
                    "minimum_aqi": Decimal(44),
                }
            ],
            [
                {"chat_id": "103", "enabled": False},
                {"chat_id": "104", "minimum_aqi": Decimal(100)},
            ],
        ]
        self.assertEqual([], self.call([record(self.payload)])["batchItemFailures"])
        self.assertEqual(["102"], [x[0] for x in self.sender.sent])
        self.assertEqual(3, len(self.subs.scans))
        self.assertEqual({"chat_id": "1"}, self.subs.scans[1]["ExclusiveStartKey"])

    def test_legacy_chat_only_subscription_preserves_known_station_opt_in(self):
        self.subs.pages = [[{"chat_id": "101"}]]
        self.assertEqual([], self.call([record(self.payload)])["batchItemFailures"])
        self.assertEqual(1, len(self.sender.sent))

    def test_unsubscribed_station_never_delivers(self):
        self.subs.pages = [[{"chat_id": "101", "station_ids": ["4946813"]}]]
        self.assertEqual([], self.call([record(self.payload)])["batchItemFailures"])
        self.assertEqual([], self.sender.sent)

    def test_pagination_failure_is_before_any_send(self):
        self.subs.pages = [[{"chat_id": "101"}], [{"chat_id": "102"}]]
        self.app.subscriptions = DynamoSubscriptions(
            self.subs, self.app.stations, max_pages=1
        )
        self.assertTrue(self.call([record(self.payload)])["batchItemFailures"])
        self.assertEqual([], self.sender.sent)

    def test_fanout_limit_is_before_any_send(self):
        self.subs.pages = [[{"chat_id": "101"}, {"chat_id": "102"}]]
        self.app.subscriptions = DynamoSubscriptions(
            self.subs, self.app.stations, max_recipients=1
        )
        self.assertTrue(self.call([record(self.payload)])["batchItemFailures"])
        self.assertEqual([], self.sender.sent)

    def test_inflight_duplicate_preserves_message_for_retry(self):
        key = "telegram#" + self.payload["event_id"] + "#101"
        claim = self.ledger.claim(key)
        self.assertEqual("acquired", claim.status)
        self.assertTrue(self.call([record(self.payload)])["batchItemFailures"])
        self.assertEqual([], self.sender.sent)
        self.clock.now += timedelta(seconds=61)
        self.assertEqual([], self.call([record(self.payload)])["batchItemFailures"])
        self.assertEqual(1, len(self.sender.sent))
        with self.assertRaises(DeliveryError):
            self.ledger.complete(key, claim.owner)

    def test_uncertain_state_write_is_not_silently_acknowledged(self):
        self.table.fail_complete = True
        self.assertTrue(self.call([record(self.payload)])["batchItemFailures"])
        self.assertEqual(1, len(self.sender.sent))
        self.assertTrue(self.call([record(self.payload)])["batchItemFailures"])
        self.assertEqual(1, len(self.sender.sent))
        self.clock.now += timedelta(seconds=61)
        self.assertEqual([], self.call([record(self.payload)])["batchItemFailures"])
        # The documented send-before-SENT crash window is at-least-once.
        self.assertEqual(2, len(self.sender.sent))

    def test_low_lambda_budget_leaves_message_retryable(self):
        context = Mock()
        context.get_remaining_time_in_millis.return_value = 1000
        self.assertTrue(
            lambda_handler({"Records": [record(self.payload)]}, context, app=self.app)[
                "batchItemFailures"
            ]
        )
        self.assertEqual([], self.sender.sent)

    def test_durable_ledger_uses_atomic_conditions_and_consistent_read(self):
        first = self.ledger.claim("fixture-key")
        self.assertEqual("busy", self.ledger.claim("fixture-key").status)
        self.ledger.retry("fixture-key", first.owner)
        second = self.ledger.claim("fixture-key")
        self.assertNotEqual(first.owner, second.owner)
        self.ledger.complete("fixture-key", second.owner)
        self.clock.now += timedelta(seconds=120)
        self.assertEqual("sent", self.ledger.claim("fixture-key").status)
        condition = self.table.calls[0][1]["ConditionExpression"]
        self.assertIn("attribute_not_exists(delivery_id)", condition)
        self.assertIn("lease_until <= :now", condition)
        update = next(k for action, k in self.table.calls if action == "update")
        self.assertIn("#o = :owner", update["ConditionExpression"])

    def test_ledger_errors_fail_closed(self):
        self.app.ledger = Mock()
        self.app.ledger.claim.side_effect = RuntimeError("Synthetic DynamoDB outage")
        self.assertTrue(self.call([record(self.payload)])["batchItemFailures"])
        self.assertEqual([], self.sender.sent)

    def test_unknown_station_stale_and_tampered_event_are_not_sent(self):
        for changed in (
            dict(self.row, location_id=999),
            dict(self.row, hour_start="2025-07-01T15:00:00Z"),
        ):
            payload = Alert(Observation.from_row(changed), RULE).payload()
            self.assertTrue(self.call([record(payload)])["batchItemFailures"])
        self.assertEqual([], self.sender.sent)

    def webhook(
        self,
        text="/subscribe",
        *,
        update_id=1,
        secret="fixture-webhook-secret",
        recipient=201,
    ):
        return {
            "headers": {"X-Telegram-Bot-Api-Secret-Token": secret},
            "body": json.dumps(
                {
                    "update_id": update_id,
                    "message": {"chat": {"id": recipient}, "text": text},
                }
            ),
        }

    def test_existing_subscribe_command_and_duplicate_update(self):
        event = self.webhook()
        self.assertEqual(200, lambda_handler(event, None, app=self.app)["statusCode"])
        self.assertEqual(200, lambda_handler(event, None, app=self.app)["statusCode"])
        self.assertEqual(1, len(self.sender.sent))
        self.assertEqual("201", self.subs.pages[0][-1]["chat_id"])
        self.assertEqual(["4946812", "4946813"], self.subs.pages[0][-1]["station_ids"])

    def test_scoped_subscription_unsubscribe_and_help(self):
        self.assertEqual(
            200,
            lambda_handler(self.webhook("/subscribe 4946812"), None, app=self.app)[
                "statusCode"
            ],
        )
        self.assertEqual(["4946812"], self.subs.pages[0][-1]["station_ids"])
        self.assertEqual(
            200,
            lambda_handler(self.webhook("/help", update_id=2), None, app=self.app)[
                "statusCode"
            ],
        )
        self.assertEqual(
            200,
            lambda_handler(
                self.webhook("/unsubscribe", update_id=3), None, app=self.app
            )["statusCode"],
        )
        self.assertFalse(any(x["chat_id"] == "201" for x in self.subs.pages[0]))

    def test_invalid_webhook_secret_or_update_never_touches_subscriptions(self):
        cases = [
            (self.webhook(secret="wrong"), 403),
            (self.webhook("/subscribe 999"), 400),
            ({"headers": {}, "body": "bad"}, 403),
        ]
        for event, status in cases:
            self.assertEqual(
                status, lambda_handler(event, None, app=self.app)["statusCode"]
            )
        self.assertEqual([], self.subs.writes)
        self.assertEqual([], self.sender.sent)

    def test_webhook_base64_and_retry_after_send_failure(self):
        event = self.webhook()
        event["body"] = base64.b64encode(event["body"].encode()).decode()
        event["isBase64Encoded"] = True
        self.sender.fail_for.add("201")
        self.assertEqual(503, lambda_handler(event, None, app=self.app)["statusCode"])
        self.sender.fail_for.clear()
        self.assertEqual(200, lambda_handler(event, None, app=self.app)["statusCode"])
        self.assertEqual(1, sum(x["chat_id"] == "201" for x in self.subs.pages[0]))
        self.assertEqual(1, len(self.sender.sent))

    def test_missing_webhook_configuration_fails_closed(self):
        self.app.webhook_secret = None
        self.assertEqual(
            503, lambda_handler(self.webhook(), None, app=self.app)["statusCode"]
        )

    def test_runtime_is_disabled_without_explicit_opt_in(self):
        with patch.dict("os.environ", {}, clear=True), self.assertRaises(InvalidAlert):
            application_from_environment()

    def test_wrong_runtime_context_rejects_before_sdk_construction(self):
        with (
            patch.dict(
                "os.environ",
                {
                    "AIRLAKE_ALERT_DELIVERY_ENABLED": "1",
                    "AIRLAKE_ACCOUNT_ID": "000000000000",
                    "AWS_REGION": "ap-southeast-2",
                },
                clear=True,
            ),
            self.assertRaises(InvalidAlert),
        ):
            application_from_environment(
                Mock(
                    invoked_function_arn="arn:aws:lambda:us-east-1:111111111111:function:wrong"
                )
            )

    def test_unicode_and_duplicate_webhook_secret_headers_reject(self):
        event = self.webhook(secret="invalid-unicode-☃")
        self.assertEqual(403, lambda_handler(event, None, app=self.app)["statusCode"])
        event = self.webhook()
        event["headers"]["x-telegram-bot-api-secret-token"] = "fixture-webhook-secret"
        self.assertEqual(403, lambda_handler(event, None, app=self.app)["statusCode"])
        self.assertEqual([], self.sender.sent)

    def test_subscription_table_helper_preserves_chat_key_and_requires_confirmation(
        self,
    ):
        self.assertEqual(
            "chat_id",
            table_request("fixture-subscriptions")["KeySchema"][0]["AttributeName"],
        )
        self.assertEqual(
            "delivery_id",
            table_request("fixture-ledger", delivery_ledger=True)["KeySchema"][0][
                "AttributeName"
            ],
        )
        client = Mock()
        with self.assertRaises(ValueError):
            create_subscription_table(client, "fixture-subscriptions")
        client.create_table.assert_not_called()
        create_subscription_table(client, "fixture-subscriptions", confirmed=True)
        client.create_table.assert_called_once()

    def test_telegram_transport_enforces_timeout_status_and_sanitized_errors(self):
        opener = MagicMock()
        response = opener.return_value.__enter__.return_value
        response.status = 200
        response.read.return_value = b'{"ok":true}'
        token = "fixture-token"
        sender = TelegramSender(token, opener=opener)
        sender.send("101", "fixture text")
        self.assertEqual(10, opener.call_args.kwargs["timeout"])
        for status, data in (
            (500, b'{"ok":false}'),
            (200, b'{"ok":false}'),
            (200, b"not-json"),
        ):
            response.status, response.read.return_value = status, data
            with self.assertRaises(DeliveryError) as error:
                sender.send("101", "fixture text")
            self.assertNotIn(token, str(error.exception))
        opener.side_effect = TimeoutError("fixture transient timeout")
        count = opener.call_count
        with self.assertRaises(DeliveryError):
            sender.send("101", "fixture text")
        self.assertEqual(count + 1, opener.call_count)


if __name__ == "__main__":
    unittest.main()
