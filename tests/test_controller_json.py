"""Actual handler boundary regression; clients and tick are offline doubles."""

import io
import json
import unittest
from contextlib import redirect_stdout
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import Mock, patch

from live_data.contract import RUN
from live_data.json_types import normalize


class JSONTests(unittest.TestCase):
    def test_nested_integrals_fractions_and_null_preserve_precision(self):
        value = {
            "source_count": Decimal(120),
            "snapshot": Decimal(9007199254740993),
            "nested": [
                Decimal("0.100000000000000000000001"),
                None,
                True,
                {"n": Decimal("-2.0")},
            ],
            "zero": Decimal(0),
        }
        actual = json.loads(json.dumps(normalize(value), allow_nan=False))
        self.assertEqual(actual["source_count"], 120)
        self.assertEqual(actual["snapshot"], 9007199254740993)
        self.assertEqual(
            actual["nested"], ["0.100000000000000000000001", None, True, {"n": -2}]
        )
        self.assertEqual(actual["zero"], 0)
        self.assertIsInstance(value["source_count"], Decimal)

    def test_timestamps_keep_instant_and_fractional_precision(self):
        dt = datetime(
            2026, 11, 14, 18, 8, 11, 576507, tzinfo=timezone(timedelta(hours=7))
        )
        self.assertEqual(normalize(dt), "2026-11-14T11:08:11.576507Z")
        self.assertEqual(normalize(date(2026, 11, 14)), "2026-11-14")
        self.assertEqual(
            normalize("2026-11-14T11:08:11.576507+00:00"),
            "2026-11-14T11:08:11.576507+00:00",
        )
        with self.assertRaises(ValueError):
            normalize(datetime(2026, 11, 14, tzinfo=None))  # noqa: DTZ001 -- rejection fixture

    def test_nonfinite_and_unsupported_values_rejected(self):
        for x in (
            Decimal("NaN"),
            Decimal("Infinity"),
            Decimal("-Infinity"),
            float("nan"),
            float("inf"),
        ):
            with self.subTest(value=str(x)), self.assertRaises(ValueError):
                normalize(x)
        with self.assertRaises(TypeError):
            normalize(b"private")
        with self.assertRaises(TypeError):
            normalize({1: "bad key"})

    def test_fractional_processing_timestamp_accepts_utc_without_relaxing_hour_keys(
        self,
    ):
        from live_data import refresh
        from live_data.contract import encode

        digest = "b" * 64
        value = {
            "facts": [
                {
                    "location_id": 4946812,
                    "hour_start": "2026-11-14T10:00:00Z",
                    "pm25_avg": 10.0,
                    "aqi": 20,
                }
            ],
            "dimensions": [{"location_id": 4946812, "name": "SYNTHETIC station"}],
            "provenance": {
                "database": refresh.version_database(digest),
                "execution": "AWS_GLUE",
                "input_manifest_sha256": digest,
                "computed_at": "2026-11-14T11:08:11.576507+00:00",
                "aqi_method": "simplified_hourly_mean_mvp; NOT official VN_AQI",
                "snapshots": {
                    k: [i + 1]
                    for i, k in enumerate(
                        ("bronze", "silver", "breakpoints", "dimensions", "hourly")
                    )
                },
            },
        }
        self.assertEqual(len(refresh.validate_export(encode(value), digest)[1]), 1)
        value["facts"][0]["hour_start"] = "2026-11-14T10:00:00.001Z"
        with self.assertRaises(ValueError):
            refresh.validate_export(encode(value), digest)

    def test_processing_timestamp_rejects_naive_non_utc_and_nonstrings(self):
        from live_data import refresh
        from live_data.contract import encode

        digest = "b" * 64
        for timestamp in (
            "2026-11-14T11:00:00",
            "2026-11-14T18:00:00+07:00",
            None,
            123,
        ):
            value = {
                "provenance": {
                    "database": refresh.version_database(digest),
                    "execution": "AWS_GLUE",
                    "input_manifest_sha256": digest,
                    "computed_at": timestamp,
                    "aqi_method": "simplified_hourly_mean_mvp; NOT official VN_AQI",
                }
            }
            with (
                self.subTest(timestamp=timestamp),
                self.assertRaises((ValueError, TypeError)),
            ):
                refresh.validate_export(encode(value), digest)

    def test_real_handler_logs_and_returns_serializable_dynamodb_values(self):
        from live_data import refresh

        tick = {
            "phase": "QUERY_RUNNING",
            "ticks": Decimal(3),
            "source_count": Decimal(120),
            "query_id": "test-only",
            "details": {
                "ratio": Decimal("0.123456789012345678901"),
                "at": datetime(2026, 11, 14, tzinfo=UTC),
                "optional": None,
            },
        }
        stream = io.StringIO()
        with (
            patch.dict(
                "os.environ",
                {
                    "AIRLAKE_LIVE_RUN": RUN,
                    "AIRLAKE_OPS_WINDOW": "a" * 64,
                    "AIRLAKE_OPS_DEADLINE": "2026-11-14T12:00:00Z",
                    "AIRLAKE_PRODUCTION_SHA256": "{}",
                },
            ),
            patch("boto3.Session", return_value=Mock()),
            patch.object(refresh, "Controller") as controller,
            redirect_stdout(stream),
        ):
            controller.return_value.tick.return_value = tick
            result = refresh.handler({}, None)
        self.assertEqual(json.loads(json.dumps(result))["source_count"], 120)
        report = json.loads(stream.getvalue())
        self.assertEqual(report["source_count"], 120)
        self.assertEqual(report["details"]["ratio"], "0.123456789012345678901")
        self.assertEqual(report["details"]["at"], "2026-11-14T00:00:00Z")
        self.assertIsNone(report["details"]["optional"])
        self.assertEqual(report["RefreshComplete"], 0)
