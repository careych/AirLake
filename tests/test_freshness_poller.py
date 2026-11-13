"""Unit tests for AirLake Freshness Poller and CloudWatch metric publisher.

Covers:
1. Sensor age calculation and status classification (TƯƠI, GIÁN ĐOẠN, CHẾT).
2. CloudWatch MetricData structure and dimension formatting.
3. Resilience against network errors, timeouts, and HTTP 429 responses.
4. Mock mode generation and AWS Lambda handler execution.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime, timedelta
import importlib.util
import io
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

REPOSITORY = Path(__file__).resolve().parents[1]


class TestFreshnessPoller(unittest.TestCase):
    """Unit test suite for freshness_poller.py with fully mocked network and AWS calls."""

    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location(
            "freshness_poller", REPOSITORY / "freshness_poller.py"
        )
        cls.poller = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.poller)

        # Ensure mock requests exists if requests is not installed in current environment
        if cls.poller.requests is None:
            cls.poller.requests = MagicMock()
            cls.poller.RequestException = Exception

    def setUp(self):
        # Prevent any real socket connections during unit testing
        self.enterContext(
            patch("socket.socket", side_effect=AssertionError("No socket calls in unit tests"))
        )
        # Suppress stdout/stderr to keep test output clean
        self.enterContext(contextlib.redirect_stdout(io.StringIO()))
        self.enterContext(contextlib.redirect_stderr(io.StringIO()))

    def test_calculate_age_and_freshness_status(self):
        """Test age_hours calculation and classification into TƯƠI, GIÁN ĐOẠN, and CHẾT."""
        now = datetime.now(UTC)
        test_cases = [
            # (minutes_ago, expected_status)
            (30, "TƯƠI"),        # 0.5 hours (<= 1.0 hour)
            (55, "TƯƠI"),        # ~0.9 hours (<= 1.0 hour)
            (120, "GIÁN ĐOẠN"),  # 2.0 hours (<= 24.0 hours)
            (720, "GIÁN ĐOẠN"),  # 12.0 hours (<= 24.0 hours)
            (1500, "CHẾT"),      # 25.0 hours (> 24.0 hours)
            (4320, "CHẾT"),      # 72.0 hours (> 24.0 hours)
        ]

        for minutes_ago, expected_status in test_cases:
            with self.subTest(minutes_ago=minutes_ago, expected_status=expected_status):
                dt = now - timedelta(minutes=minutes_ago)
                mock_response = MagicMock()
                mock_response.json.return_value = {
                    "results": [
                        {
                            "sensorsId": 13502151,
                            "parameter": {"name": "pm25"},
                            "value": 35.5,
                            "unit": "µg/m³",
                            "datetime": {"utc": dt.isoformat().replace("+00:00", "Z")},
                        }
                    ]
                }
                mock_response.raise_for_status = MagicMock()

                with patch.object(self.poller.requests, "get", return_value=mock_response):
                    res = self.poller.fetch_station_freshness(
                        location_id=4946812,
                        api_key="test_key",
                        target_sensor_id=13502151,
                    )

                self.assertIsNotNone(res)
                self.assertEqual(res["location_id"], 4946812)
                self.assertEqual(res["station_name"], "Nhân Chính")
                self.assertEqual(res["sensor_id"], 13502151)
                self.assertEqual(res["status"], expected_status)
                self.assertAlmostEqual(res["age_hours"], minutes_ago / 60.0, delta=0.05)

    def test_cloudwatch_metric_format(self):
        """Test formatting of CloudWatch MetricData and dimensional attributes."""
        mock_metrics = [
            {
                "location_id": 4946812,
                "station_name": "Nhân Chính",
                "sensor_id": 13502151,
                "parameter": "pm25",
                "value": 28.5,
                "unit": "µg/m³",
                "timestamp_utc": datetime.now(UTC).isoformat(),
                "age_hours": 0.45,
                "status": "TƯƠI",
            },
            {
                "location_id": 6123215,
                "station_name": "OceanPark",
                "sensor_id": 14581375,
                "parameter": "pm25",
                "value": 55.0,
                "unit": "µg/m³",
                "timestamp_utc": datetime.now(UTC).isoformat(),
                "age_hours": 3.2,
                "status": "GIÁN ĐOẠN",
            },
        ]

        # 1. Test Dry Run mode (no AWS call)
        dry_result = self.poller.publish_metrics_to_cloudwatch(
            mock_metrics, region_name="ap-southeast-2", dry_run=True
        )
        self.assertTrue(dry_result)

        # 2. Test live publish with mocked CloudWatch client
        mock_cw = MagicMock()
        mock_boto3 = MagicMock()
        mock_boto3.client.return_value = mock_cw
        with patch.object(self.poller, "boto3", mock_boto3):
            published = self.poller.publish_metrics_to_cloudwatch(
                mock_metrics, region_name="ap-southeast-2", dry_run=False
            )
            self.assertTrue(published)
            mock_cw.put_metric_data.assert_called_once()
            call_kwargs = mock_cw.put_metric_data.call_args[1]

            self.assertEqual(call_kwargs["Namespace"], "AirLake")
            metric_data = call_kwargs["MetricData"]
            self.assertEqual(len(metric_data), 2)

            first_metric = metric_data[0]
            self.assertEqual(first_metric["MetricName"], "SensorAgeHours")
            self.assertEqual(first_metric["Value"], 0.45)
            self.assertEqual(first_metric["Unit"], "None")

            dimensions = {d["Name"]: d["Value"] for d in first_metric["Dimensions"]}
            self.assertEqual(dimensions["location_id"], "4946812")
            self.assertEqual(dimensions["sensors_id"], "13502151")
            self.assertEqual(dimensions["parameter"], "pm25")

    def test_network_and_api_error_handling(self):
        """Test poller resilience when encountering network failures, timeouts, and 429 rate limits."""
        # 1. Simulated connection error / timeout
        with patch.object(
            self.poller.requests,
            "get",
            side_effect=self.poller.RequestException("Connection timed out"),
        ):
            res = self.poller.fetch_station_freshness(
                location_id=4946812, api_key="dummy_key"
            )
            self.assertIsNone(res, "Should gracefully return None on network timeout")

        # 2. Simulated HTTP 429 Too Many Requests error
        mock_429 = MagicMock()
        mock_429.raise_for_status.side_effect = self.poller.RequestException("429 Client Error: Too Many Requests")
        with patch.object(self.poller.requests, "get", return_value=mock_429):
            res = self.poller.fetch_station_freshness(
                location_id=4946813, api_key="dummy_key"
            )
            self.assertIsNone(res, "Should gracefully return None on 429 rate limit")

        # 3. Malformed payload missing PM2.5 sensor
        mock_empty = MagicMock()
        mock_empty.json.return_value = {"results": [{"parameter": "co", "value": 0.5}]}
        mock_empty.raise_for_status = MagicMock()
        with patch.object(self.poller.requests, "get", return_value=mock_empty):
            res = self.poller.fetch_station_freshness(
                location_id=4946812, api_key="dummy_key"
            )
            self.assertIsNone(res, "Should return None when target PM2.5 sensor is missing")

    def test_mock_mode_and_lambda_handler(self):
        """Test mock data generator and AWS Lambda entry point."""
        # 1. Test mock data generation
        mock_data = self.poller.generate_mock_freshness_data()
        self.assertEqual(len(mock_data), 3)

        station_ids = [item["location_id"] for item in mock_data]
        self.assertIn(4946812, station_ids)
        self.assertIn(4946813, station_ids)
        self.assertIn(6123215, station_ids)

        for s in mock_data:
            self.assertIn("status", s)
            self.assertIn(s["status"], ("TƯƠI", "GIÁN ĐOẠN", "CHẾT"))
            self.assertGreaterEqual(s["age_hours"], 0.0)

        # 2. Test run_poll with mock=True
        poll_results = self.poller.run_poll(mock=True)
        self.assertEqual(len(poll_results), 3)

        # 3. Test lambda_handler execution
        event = {"source": "aws.events", "detail-type": "Scheduled Event"}
        context = SimpleNamespace(function_name="airlake-freshness-poller")

        with patch.object(self.poller, "run_poll", return_value=mock_data) as mock_run:
            response = self.poller.lambda_handler(event, context)
            mock_run.assert_called_once_with(region_name=self.poller.DEFAULT_AWS_REGION, dry_run=False)

            self.assertEqual(response["statusCode"], 200)
            body = response["body"]
            self.assertEqual(body["count"], 3)
            self.assertEqual(len(body["stations"]), 3)


if __name__ == "__main__":
    unittest.main()
