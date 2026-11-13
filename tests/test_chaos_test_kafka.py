"""Unit test suite for Kafka 3-Broker KRaft Chaos & Fault-Tolerance experiment.

Covers:
1. Verification of high-reliability producer configuration (acks=all, idempotence).
2. Verification of Bronze 9-field JSON measurement payload schema.
3. Verification of Data Loss Rate calculation and report assertions.
4. Simulated broker failover with buffer error retries and self-healing.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, patch

REPOSITORY = Path(__file__).resolve().parents[1]


class TestKafkaChaos(unittest.TestCase):
    """Test suite for scripts/chaos_test_kafka.py with fully mocked Kafka components."""

    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location(
            "chaos_test_kafka", REPOSITORY / "scripts" / "chaos_test_kafka.py"
        )
        cls.chaos = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.chaos)

    def setUp(self):
        # Prevent any accidental network socket connections during unit testing
        self.enterContext(
            patch("socket.socket", side_effect=AssertionError("No socket calls allowed in unit tests"))
        )
        # Suppress standard output/error to keep test runs clean
        self.enterContext(contextlib.redirect_stdout(io.StringIO()))
        self.enterContext(contextlib.redirect_stderr(io.StringIO()))

    def test_producer_configuration_enforces_zero_data_loss(self):
        """Verify that Kafka Producer configuration strictly enforces Zero Data Loss."""
        conf = self.chaos.build_producer_config("10.0.0.11:9092,10.0.1.12:9092")

        self.assertEqual(conf["bootstrap.servers"], "10.0.0.11:9092,10.0.1.12:9092")
        self.assertEqual(conf["acks"], "all", "acks must be 'all' to ensure multi-broker in-sync replication")
        self.assertTrue(conf["enable.idempotence"], "Idempotence must be enabled to prevent duplicate messages")
        self.assertGreaterEqual(conf["retries"], 5, "Retries must be configured to ride through leader election")
        self.assertGreaterEqual(conf["delivery.timeout.ms"], 10000, "Delivery timeout must accommodate failover latency")

    def test_measurement_payload_matches_schema(self):
        """Verify generated measurement conforms to the required 9-field Bronze schema."""
        for seq in range(6):
            key, payload_str = self.chaos.generate_measurement_payload(seq)
            data = json.loads(payload_str)

            self.assertIn(":", key)
            self.assertIn("location_id", data)
            self.assertIn("sensors_id", data)
            self.assertIn("location", data)
            self.assertIn("datetime", data)
            self.assertIn("parameter", data)
            self.assertEqual(data["parameter"], "pm25")
            self.assertIn("value", data)
            self.assertEqual(data["units"], "µg/m³")
            self.assertEqual(data["seq_num"], str(seq))

    def test_data_loss_calculation_and_report_verdicts(self):
        """Verify accurate loss rate computation and reporting verdicts."""
        # 1. Zero data loss case (Successful Chaos Test)
        report_ok = self.chaos.generate_chaos_report(
            submitted=1000,
            delivered=1000,
            failed=0,
            disruption_time_s=5.2,
            election_latency_ms=1150.0,
            bootstrap_servers="localhost:19092",
            topic="aq_measurements",
        )
        self.assertIn("0.00%", report_ok)
        self.assertIn("🟢 ĐẠT (Zero Data Loss)", report_ok)
        self.assertIn("🟢 ĐẠT TIÊU CHÍ NFR-03", report_ok)

        # 2. Defective data loss case (Failing Chaos Test)
        report_fail = self.chaos.generate_chaos_report(
            submitted=1000,
            delivered=985,
            failed=15,
            disruption_time_s=15.0,
            election_latency_ms=5000.0,
            bootstrap_servers="localhost:19092",
            topic="aq_measurements",
        )
        self.assertIn("1.50%", report_fail)
        self.assertIn("🔴 THẤT BẠI", report_fail)
        self.assertIn("🔴 CHƯA ĐẠT NFR-03", report_fail)

    def test_run_chaos_test_with_mock_producer_and_failover_retry(self):
        """Verify execution flow with temporary BufferError simulation during broker outage."""
        mock_producer = MagicMock()
        produce_attempts = 0

        def simulated_produce(topic, key, value, callback):
            nonlocal produce_attempts
            produce_attempts += 1
            # Simulate a temporary buffer full / failover on attempt #4
            if produce_attempts == 4:
                raise BufferError("Simulated buffer full during Kafka leader re-election")
            # Successful ack callback
            callback(None, MagicMock())

        mock_producer.produce.side_effect = simulated_produce
        mock_producer.flush.return_value = 0

        disruption_called = False

        def mock_disruption():
            nonlocal disruption_called
            disruption_called = True

        with TemporaryDirectory() as tmp_dir:
            out_report = Path(tmp_dir) / "test_report.md"
            res = self.chaos.run_chaos_test(
                bootstrap_servers="localhost:19092",
                topic="aq_measurements",
                total_messages=10,
                rate_per_sec=0,
                disruption_callback=mock_disruption,
                output_file=str(out_report),
                producer_instance=mock_producer,
            )

            self.assertTrue(disruption_called, "Disruption callback must be triggered")
            self.assertEqual(res["submitted"], 10)
            self.assertEqual(res["delivered"], 10)
            self.assertEqual(res["failed"], 0)
            self.assertEqual(res["loss_rate"], 0.0)
            self.assertTrue(out_report.exists(), "Output markdown report must be written")
            self.assertIn("🟢 ĐẠT TIÊU CHÍ NFR-03", out_report.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
