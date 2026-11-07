"""Pure Gold evaluation/SNS tests with retained, explicitly offline Gold rows."""

import copy
import socket
import unittest
from dataclasses import replace
from datetime import timedelta
from unittest.mock import Mock, patch

from alert_fakes import ACCOUNT, REGION, RULE, SNS, TOPIC, Source, setup_app

from alerts.athena import AthenaGoldSource
from alerts.gold import (
    Alert,
    InvalidAlert,
    Observation,
    SNSPublisher,
    canonical,
    evaluate,
    publish_batch,
    strict_json,
)


class GoldAlertTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(
                socket,
                "socket",
                side_effect=AssertionError("No network in offline tests"),
            )
        )
        self.row, self.clock, self.table, _, _, self.ledger, self.app = setup_app()
        self.source = Source([self.row])

    def test_evaluates_retained_gold_hour_without_recalculating_aqi(self):
        (alert,) = evaluate(self.source, RULE, self.app.stations, self.clock())
        self.assertEqual(44, alert.observation.aqi)
        self.assertEqual(21.7875, alert.observation.pm25_avg)
        self.assertIn("not official VN_AQI", alert.text())
        self.assertEqual(
            alert,
            Alert.parse(
                strict_json(canonical(alert.payload())), self.app.stations, self.clock()
            ),
        )

    def test_below_threshold_produces_no_publication(self):
        client = SNS()
        publisher = SNSPublisher(
            client,
            TOPIC,
            self.ledger,
            account=ACCOUNT,
            region=REGION,
            stations=self.app.stations,
            clock=self.clock,
        )
        self.assertEqual(
            [],
            publish_batch(
                self.source,
                replace(RULE, minimum_aqi=100),
                self.app.stations,
                self.clock(),
                publisher,
            ),
        )
        self.assertEqual([], client.messages)

    def test_no_implicit_health_threshold_or_age_policy(self):
        with self.assertRaises(TypeError):
            type(RULE)("not-configured", "v1")
        for changes in (
            {"minimum_aqi": True},
            {"maximum_age_seconds": 0},
            {"version": ""},
        ):
            with self.subTest(changes=changes), self.assertRaises(InvalidAlert):
                replace(RULE, **changes).validate()

    def test_bad_measurements_and_provenance_reject_before_publish(self):
        client = Mock()
        changes = [
            {"pm25_avg": v}
            for v in (None, float("nan"), float("inf"), -float("inf"), -1, True)
        ]
        changes += [{"aqi": v} for v in (-1, 501, None, True, 44.5)]
        changes += [
            {"units": "mg/m3"},
            {"aqi_method": "official_vn_aqi"},
            {"location_id": 999},
            {"name": "invented station"},
            {"source_snapshot_id": ""},
            {"source_table": "table; DROP"},
            {"hour_start": "2025-07-03T15:01:00Z"},
            {"hour_start": "2025-07-03T15:00:00"},
            {"hour_start": "2025-07-03T22:00:00+07:00"},
        ]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(InvalidAlert):
                publish_batch(
                    Source([dict(self.row, **change)]),
                    RULE,
                    self.app.stations,
                    self.clock(),
                    client,
                )
        client.publish.assert_not_called()

    def test_missing_stale_future_and_open_hours_reject(self):
        for rows in (
            [],
            [{}],
            [dict(self.row, hour_start="2025-07-02T15:00:00Z")],
            [dict(self.row, hour_start="2025-07-03T16:00:00Z")],
        ):
            with self.subTest(rows=rows), self.assertRaises(InvalidAlert):
                evaluate(Source(rows), RULE, self.app.stations, self.clock())

    def test_whole_batch_preflight_prevents_partial_publication(self):
        client = Mock()
        with self.assertRaises(InvalidAlert):
            publish_batch(
                Source([self.row, dict(self.row, pm25_avg=-1)]),
                RULE,
                self.app.stations,
                self.clock(),
                client,
            )
        client.publish.assert_not_called()

    def test_duplicate_gold_rows_deduplicate_but_conflicts_reject(self):
        self.assertEqual(
            1,
            len(
                evaluate(
                    Source([self.row, self.row]), RULE, self.app.stations, self.clock()
                )
            ),
        )
        with self.assertRaises(InvalidAlert):
            evaluate(
                Source([self.row, dict(self.row, aqi=45)]),
                RULE,
                self.app.stations,
                self.clock(),
            )

    def test_revision_identity_and_snapshot_only_rerun(self):
        alert = Alert(Observation.from_row(self.row), RULE)
        same = Alert(
            Observation.from_row(dict(self.row, source_snapshot_id="12345")), RULE
        )
        changed = Alert(
            Observation.from_row(dict(self.row, aqi=45, pm25_avg=22.5)), RULE
        )
        self.assertEqual(alert.event_id, same.event_id)
        self.assertNotEqual(alert.event_id, changed.event_id)
        self.assertNotEqual(
            alert.event_id,
            Alert(alert.observation, replace(RULE, version="test-v2")).event_id,
        )

    def test_mixed_snapshot_and_oversized_reads_reject(self):
        for rows in (
            [self.row, dict(self.row, source_snapshot_id="12345")],
            [self.row] * 1001,
        ):
            with self.assertRaises(InvalidAlert):
                evaluate(Source(rows), RULE, self.app.stations, self.clock())

    def test_event_tampering_and_nonstandard_json_reject(self):
        payload = Alert(Observation.from_row(self.row), RULE).payload()
        payload["event_id"] = "forged"
        with self.assertRaises(InvalidAlert):
            Alert.parse(payload, self.app.stations, self.clock())
        for text in ('{"aqi":NaN}', '{"aqi":1,"aqi":2}', "null" * 100000):
            with self.assertRaises(InvalidAlert):
                strict_json(text)

    def test_sns_publication_deduplicates_reruns(self):
        client = SNS()
        publisher = SNSPublisher(
            client,
            TOPIC,
            self.ledger,
            account=ACCOUNT,
            region=REGION,
            stations=self.app.stations,
            clock=self.clock,
        )
        self.assertEqual(
            ["published"],
            publish_batch(
                self.source, RULE, self.app.stations, self.clock(), publisher
            ),
        )
        self.assertEqual(
            ["duplicate"],
            publish_batch(
                self.source, RULE, self.app.stations, self.clock(), publisher
            ),
        )
        self.assertEqual(1, len(client.messages))
        self.assertEqual(ACCOUNT, client.messages[0]["TopicArn"].split(":")[4])
        self.assertEqual(
            "String", client.messages[0]["MessageAttributes"]["event_id"]["DataType"]
        )

    def test_sns_failure_is_one_attempt_and_explicit_retry_can_succeed(self):
        client = SNS()
        client.fail = True
        publisher = SNSPublisher(
            client,
            TOPIC,
            self.ledger,
            account=ACCOUNT,
            region=REGION,
            stations=self.app.stations,
            clock=self.clock,
        )
        with self.assertRaises(RuntimeError):
            publish_batch(self.source, RULE, self.app.stations, self.clock(), publisher)
        self.assertEqual(1, len(client.messages))
        client.fail = False
        self.assertEqual(
            ["published"],
            publish_batch(
                self.source, RULE, self.app.stations, self.clock(), publisher
            ),
        )

    def test_athena_boundary_never_fabricates_a_live_result(self):
        with self.assertRaises(InvalidAlert):
            AthenaGoldSource().read()
        executor = Mock()
        executor.read_pinned_hourly_rows.return_value = [copy.deepcopy(self.row)]
        self.assertEqual([self.row], AthenaGoldSource(executor).read())
        executor.read_pinned_hourly_rows.assert_called_once_with()

    def test_wrong_account_and_region_reject_without_sns_calls(self):
        for account, region in (("111111111111", REGION), (ACCOUNT, "us-east-1")):
            with self.assertRaises(InvalidAlert):
                SNSPublisher(
                    Mock(),
                    TOPIC,
                    self.ledger,
                    account=account,
                    region=region,
                    stations=self.app.stations,
                    clock=self.clock,
                )

    def test_gold_fact_fields_match_retained_schema(self):
        import json

        from alert_fakes import ROOT

        evidence = json.loads(
            (ROOT / "tests/evidence/real_openaq_2026-11-06.json").read_text()
        )
        self.assertEqual(
            {"location_id", "hour_start", "pm25_avg", "aqi"},
            set(evidence["hourly_rows"][0]),
        )
        self.assertEqual(
            "struct<location_id:int,hour_start:timestamp,pm25_avg:double,aqi:int>",
            evidence["schemas"]["HOURLY_TABLE"],
        )

    def test_fifo_publish_is_rejected_before_any_client_call(self):
        client = Mock()
        with self.assertRaises(InvalidAlert):
            SNSPublisher(
                client,
                TOPIC + ".fifo",
                self.ledger,
                account=ACCOUNT,
                region=REGION,
                stations=self.app.stations,
                clock=self.clock,
            )
        client.publish.assert_not_called()

    def test_direct_publisher_cannot_hide_invalid_measurement_by_serializing(self):
        client = Mock()
        publisher = SNSPublisher(
            client,
            TOPIC,
            self.ledger,
            account=ACCOUNT,
            region=REGION,
            stations=self.app.stations,
            clock=self.clock,
        )
        for value in (True, "21.7875", None, float("nan")):
            with self.assertRaises(InvalidAlert):
                publisher.publish(
                    Alert(Observation.from_row(dict(self.row, pm25_avg=value)), RULE)
                )
        client.publish.assert_not_called()

    def test_negative_zero_is_not_a_correction(self):
        positive = Alert(
            Observation.from_row(dict(self.row, pm25_avg=0.0, aqi=0)), RULE
        )
        negative = Alert(
            Observation.from_row(dict(self.row, pm25_avg=-0.0, aqi=0)), RULE
        )
        self.assertEqual(positive.event_id, negative.event_id)

    def test_expired_retry_rejects_before_publication(self):
        self.clock.now += timedelta(hours=3)
        client = Mock()
        with self.assertRaises(InvalidAlert):
            publish_batch(self.source, RULE, self.app.stations, self.clock(), client)
        client.publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
