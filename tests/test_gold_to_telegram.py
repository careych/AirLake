"""Offline vertical slice: retained Gold → SNS → SQS/Lambda → fake Telegram."""

import json
import socket
import unittest
from unittest.mock import patch

from alert_fakes import ACCOUNT, REGION, RULE, SNS, TOPIC, Source, record, setup_app

from alerts.gold import SNSPublisher, publish_batch
from bot_lambda.src.lambda_function import lambda_handler


class GoldToTelegramTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(
                socket, "socket", side_effect=AssertionError("E2E uses mocks only")
            )
        )
        self.row, self.clock, _, _, self.telegram, ledger, self.app = setup_app()
        self.sns = SNS()
        self.publisher = SNSPublisher(
            self.sns,
            TOPIC,
            ledger,
            account=ACCOUNT,
            region=REGION,
            stations=self.app.stations,
            clock=self.clock,
        )

    def replay(self, row, message_id="fixture-e2e", raw=False):
        result = publish_batch(
            Source([row]), RULE, self.app.stations, self.clock(), self.publisher
        )
        if result == ["published"]:
            payload = json.loads(self.sns.messages[-1]["Message"])
            response = lambda_handler(
                {"Records": [record(payload, message_id=message_id, raw=raw)]},
                None,
                app=self.app,
            )
            self.assertEqual([], response["batchItemFailures"])
        return result

    def test_actual_retained_hour_reaches_only_eligible_telegram_adapter(self):
        self.assertEqual(["published"], self.replay(self.row))
        self.assertEqual(1, len(self.sns.messages))
        self.assertEqual(1, len(self.telegram.sent))
        recipient, text = self.telegram.sent[0]
        self.assertEqual("101", recipient)
        self.assertIn("21.7875", text)
        self.assertIn("hourly MVP AQI: 44", text)
        self.assertIn("2025-07-03T15:00:00Z", text)
        self.assertIn(RULE.version, text)
        self.assertIn(self.row["source_snapshot_id"], text)

    def test_rerun_is_suppressed_then_synthetic_correction_is_new_revision(self):
        self.replay(self.row)
        self.assertEqual(["duplicate"], self.replay(self.row, "rerun"))
        self.assertEqual(
            ["duplicate"],
            self.replay(dict(self.row, source_snapshot_id="12345"), "snapshot-only"),
        )
        # Synthetic correction, not a claim of an observed cloud correction.
        corrected = dict(self.row, pm25_avg=22.5, aqi=45, source_snapshot_id="12346")
        self.assertEqual(["published"], self.replay(corrected, "correction", raw=True))
        self.assertEqual(2, len(self.telegram.sent))
        self.assertNotEqual(self.telegram.sent[0][1], self.telegram.sent[1][1])
        self.assertEqual(
            ["duplicate"], self.replay(self.row, "reversion-to-seen-content")
        )
        self.assertEqual(2, len(self.telegram.sent))

    def test_correction_below_test_rule_has_no_automatic_retraction(self):
        self.replay(self.row)
        self.assertEqual([], self.replay(dict(self.row, aqi=10, pm25_avg=5.0), "below"))
        self.assertEqual(1, len(self.telegram.sent))

    def test_recipient_retry_is_independent_of_publisher_deduplication(self):
        publish_batch(
            Source([self.row]), RULE, self.app.stations, self.clock(), self.publisher
        )
        payload = json.loads(self.sns.messages[-1]["Message"])
        event = {"Records": [record(payload)]}
        self.telegram.fail_for.add("101")
        self.assertTrue(lambda_handler(event, None, app=self.app)["batchItemFailures"])
        self.assertEqual(
            ["duplicate"],
            publish_batch(
                Source([self.row]),
                RULE,
                self.app.stations,
                self.clock(),
                self.publisher,
            ),
        )
        self.telegram.fail_for.clear()
        self.assertEqual(
            [], lambda_handler(event, None, app=self.app)["batchItemFailures"]
        )
        self.assertEqual(1, len(self.telegram.sent))


if __name__ == "__main__":
    unittest.main()
