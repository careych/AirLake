"""Unit test suite for Kafka 3-Broker KRaft Chaos & Fault-Tolerance experiment.

Covers:
1. Verification of high-reliability producer configuration (acks=all, idempotence).
2. Verification of Bronze 9-field JSON measurement payload schema.
3. Verification of Data Loss Rate calculation and report assertions.
4. Simulated broker failover with buffer error retries and self-healing.
5. Verification of Fail-safe guard blocking accidental production topic contamination.
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

    def test_default_topic_is_isolated(self):
        """Verify default topic is isolated and does not target production."""
        self.assertEqual(self.chaos.DEFAULT_TOPIC, "aq_measurements_chaos_test")
        self.assertIn("aq_measurements", self.chaos.PRODUCTION_TOPICS)

    def test_failsafe_guard_blocks_production_topic(self):
        """Verify fail-safe guard prevents running chaos test on production topic without explicit permission."""
        with self.assertRaises(ValueError) as cm:
            self.chaos.validate_topic_safety("aq_measurements", allow_production_topic=False)
        self.assertIn("Không được phép chạy kịch bản kiểm thử", str(cm.exception))

        # Explicit allowance must pass
        self.chaos.validate_topic_safety("aq_measurements", allow_production_topic=True)
        # Test topics must always pass
        self.chaos.validate_topic_safety("aq_measurements_chaos_test", allow_production_topic=False)

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
            self.assertIn(data["location_id"], (4946812, 4946813, 6123215))
            self.assertIn("pm25", data["parameter"])
            self.assertGreaterEqual(data["value"], 0.0)
            self.assertEqual(data["unit"], "ug/m3")
            self.assertEqual(data["test_marker"], "chaos-experiment-nfr03")
            self.assertEqual(data["test_seq"], seq)

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
            topic="aq_measurements_chaos_test",
        )
        self.assertIn("0.00%", report_ok)
        self.assertIn("ĐẠT (PASSED)", report_ok)
        self.assertIn("aq_measurements_chaos_test", report_ok)

        # 2. Defective data loss case (Failing Chaos Test)
        report_fail = self.chaos.generate_chaos_report(
            submitted=1000,
            delivered=985,
            failed=15,
            disruption_time_s=15.0,
            election_latency_ms=5000.0,
            bootstrap_servers="localhost:19092",
            topic="aq_measurements_chaos_test",
        )
        self.assertIn("1.50%", report_fail)
        self.assertIn("KHÔNG ĐẠT (FAILED)", report_fail)

    def test_run_chaos_test_with_mock_producer_and_failover_retry(self):
        """Verify execution flow with temporary BufferError simulation during broker outage."""
        mock_producer = MagicMock()
        produce_attempts = 0

        def simulated_produce(topic, key, value, on_delivery):
            nonlocal produce_attempts
            produce_attempts += 1
            # Simulate a temporary buffer full / failover on attempt #4
            if produce_attempts == 4:
                raise BufferError("Simulated buffer full during Kafka leader re-election")
            # Successful ack callback
            on_delivery(None, MagicMock())

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
                topic="aq_measurements_chaos_test",
                total_messages=10,
                rate_per_sec=1000,
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
            self.assertIn("ĐẠT (PASSED)", out_report.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
