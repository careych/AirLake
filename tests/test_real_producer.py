"""Exercise producer control flow with inert Kafka clients and temporary CSVs."""

import contextlib
import csv
import gzip
import importlib.util
import io
import os
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

REPOSITORY = Path(__file__).resolve().parents[1]


class FixtureKafkaError(Exception):
    pass


class ProducerTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch(
                "socket.socket", side_effect=AssertionError("No sockets in unit tests")
            )
        )
        kafka = ModuleType("confluent_kafka")
        admin = ModuleType("confluent_kafka.admin")
        kafka.KafkaException = FixtureKafkaError
        kafka.Producer = Mock(name="inert Producer")
        admin.AdminClient = Mock(name="inert AdminClient")
        spec = importlib.util.spec_from_file_location(
            "producer_subject", REPOSITORY / "real_producer.py"
        )
        self.subject = importlib.util.module_from_spec(spec)
        with patch.dict(
            "sys.modules", {"confluent_kafka": kafka, "confluent_kafka.admin": admin}
        ):
            spec.loader.exec_module(self.subject)
        self.client = kafka.Producer.return_value
        self.client.flush.return_value = 0
        self.admin = admin.AdminClient.return_value
        self.admin.list_topics.return_value.topics = {
            "aq_measurements_test1": SimpleNamespace(error=None)
        }
        self.client.produce.side_effect = lambda *args, **kwargs: kwargs["callback"](
            None, None
        )
        if Path(tempfile.gettempdir()).resolve().is_relative_to(REPOSITORY):
            raise RuntimeError("Unit fixtures must be outside the repository")
        temporary = self.enterContext(
            tempfile.TemporaryDirectory(prefix="airlake-producer-unit-")
        )
        self.source = Path(temporary) / "fixture.csv.gz"
        with gzip.open(self.source, "wt", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(
                stream, fieldnames=["location_id", "sensors_id", "value"]
            )
            writer.writeheader()
            writer.writerows(
                {"location_id": "4946812", "sensors_id": "13502151", "value": str(i)}
                for i in range(3)
            )
        self.enterContext(
            patch.object(self.subject.glob, "glob", return_value=[str(self.source)])
        )
        self.enterContext(contextlib.redirect_stdout(io.StringIO()))
        self.enterContext(contextlib.redirect_stderr(io.StringIO()))

    def run_main(self, argv=()):
        with patch("sys.argv", ["real_producer.py", *argv]):
            self.subject.main()

    def test_defaults_do_not_inherit_remote_broker_environment(self):
        with patch.dict(
            os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "fixture.invalid:9092"}
        ):
            args = self.subject.parse_args([])
        self.assertEqual("localhost:19092", args.bootstrap_servers)
        self.assertEqual("aq_measurements_test1", args.topic)
        self.assertEqual(2000, args.max_rows)

    def test_explicit_cli_configuration_is_used(self):
        args = self.subject.parse_args(
            [
                "--bootstrap-servers",
                "localhost:29092",
                "--topic",
                "fixture",
                "--max-rows",
                "2",
            ]
        )
        self.assertEqual(
            ("localhost:29092", "fixture", 2),
            (args.bootstrap_servers, args.topic, args.max_rows),
        )

    def test_invalid_arguments_reject_before_clients_exist(self):
        for argv in (
            ["nan"],
            ["inf"],
            ["-inf"],
            ["-1"],
            ["--max-rows", "-1"],
            ["--topic", " "],
            ["--bootstrap-servers", ""],
        ):
            with self.subTest(argv=argv), self.assertRaises(SystemExit):
                self.run_main(argv)
        self.subject.AdminClient.assert_not_called()
        self.subject.Producer.assert_not_called()

    def test_no_source_files_prevents_client_creation(self):
        self.subject.glob.glob.return_value = []
        with self.assertRaisesRegex(SystemExit, "No CSV.GZ files"):
            self.run_main()
        self.subject.AdminClient.assert_not_called()

    def test_missing_topic_prevents_producer_creation(self):
        self.admin.list_topics.return_value.topics = {}
        with self.assertRaisesRegex(SystemExit, "does not exist"):
            self.run_main()
        self.subject.Producer.assert_not_called()

    def test_topic_metadata_error_prevents_producer_creation(self):
        self.admin.list_topics.return_value.topics[
            "aq_measurements_test1"
        ].error = "fixture unavailable"
        with self.assertRaisesRegex(SystemExit, "Topic metadata is unavailable"):
            self.run_main()
        self.subject.Producer.assert_not_called()

    def test_broker_error_prevents_producer_creation(self):
        self.admin.list_topics.side_effect = FixtureKafkaError("fixture unavailable")
        with self.assertRaisesRegex(SystemExit, "Cannot connect"):
            self.run_main()
        self.subject.Producer.assert_not_called()

    def test_row_cap_counts_only_submitted_rows(self):
        self.run_main(["--max-rows", "2"])
        self.assertEqual(2, self.client.produce.call_count)
        self.client.flush.assert_called_once_with(30)

    def test_explicit_zero_limit_sends_all_fixture_rows(self):
        self.run_main(["--max-rows", "0"])
        self.assertEqual(3, self.client.produce.call_count)

    def test_full_queue_retries_without_losing_a_row(self):
        attempts = 0

        def produce(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise BufferError("fixture full queue")
            kwargs["callback"](None, None)

        self.client.produce.side_effect = produce
        self.run_main(["--max-rows", "2"])
        self.assertEqual(3, attempts)
        self.client.poll.assert_any_call(1)

    def test_delivery_failure_is_not_reported_as_success(self):
        self.client.produce.side_effect = lambda *args, **kwargs: kwargs["callback"](
            "fixture failure", None
        )
        with self.assertRaisesRegex(SystemExit, "not fully successful"):
            self.run_main(["--max-rows", "1"])

    def test_queued_messages_after_flush_are_a_failure(self):
        self.client.flush.return_value = 1
        with self.assertRaisesRegex(SystemExit, "not fully successful"):
            self.run_main(["--max-rows", "1"])

    def test_kafka_exception_flushes_before_propagating(self):
        self.client.produce.side_effect = FixtureKafkaError("fixture produce error")
        with self.assertRaises(FixtureKafkaError):
            self.run_main()
        self.client.flush.assert_called_once_with(30)


if __name__ == "__main__":
    unittest.main()
