"""Deterministic API/cursor/delivery tests; no sockets or Kafka broker."""

import copy
import json
import socket
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from ingestion.openaq import (
    API,
    FIELDS,
    PILOTS,
    State,
    bronze_record,
    ingest,
    key_from_environment,
)

NOW = datetime(2026, 11, 8, 2, tzinfo=UTC)


def metadata(location, sensor):
    return {
        "id": location,
        "name": "Fixture station " + str(location),
        "isMobile": False,
        "coordinates": {"latitude": 21.0, "longitude": 105.0},
        "sensors": [{"id": sensor, "parameter": {"name": "pm25", "units": "µg/m³"}}],
    }


def measurement(value=10.0, when=NOW):
    return {
        "value": value,
        "parameter": {"name": "pm25", "units": "µg/m³"},
        "flagInfo": {"hasFlags": False},
        "period": {"label": "raw", "datetimeTo": {"utc": when.isoformat()}},
    }


class FakeAPI:
    def __init__(self, rows=None):
        self.rows = rows or {}
        self.calls = []

    def metadata(self, location, sensor):
        return metadata(location, sensor)

    def get(self, path, params):
        self.calls.append((path, params))
        sensor = int(path.split("/")[2])
        rows = self.rows.get(sensor, [])
        page = params["page"]
        return {"results": copy.deepcopy(rows[(page - 1) * 100 : page * 100])}


class IncrementalTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(socket, "socket", side_effect=AssertionError("Mocks only"))
        )
        self.root = Path(
            self.enterContext(tempfile.TemporaryDirectory(prefix="airlake-live-"))
        )
        self.state = State(self.root, "airlake_live_fixture01", "localhost:19092")
        self.addCleanup(self.state.close)
        self.sensor = next(iter(PILOTS.values()))
        self.api = FakeAPI({self.sensor: [measurement()]})
        self.sent = []

    def run_batch(self, **kw):
        return ingest(
            self.api,
            self.state,
            self.sent.append,
            start=NOW - timedelta(hours=3),
            end=NOW,
            max_records=9,
            **kw,
        )

    def test_exact_bronze_schema_real_producer_key_fields_and_repeat_suppression(self):
        first = self.run_batch()
        second = self.run_batch()
        self.assertEqual(1, first["published"])
        self.assertEqual(0, second["published"])
        self.assertEqual(set(FIELDS), set(self.sent[0]))
        self.assertTrue(all(isinstance(v, str) for v in self.sent[0].values()))
        self.assertEqual("2026-11-08T02:00:00Z", self.sent[0]["datetime"])
        provenance = json.loads(
            self.state.db.execute("SELECT provenance FROM outbox").fetchone()[0]
        )
        self.assertIn("/sensors/", provenance["endpoint"])

    def test_missing_metadata_or_nonfinite_units_ids_flags_reject_before_publish(self):
        loc, sensor = next(iter(PILOTS.items()))
        meta = metadata(loc, sensor)
        for update in (
            {"value": float("nan")},
            {"value": float("inf")},
            {"value": -1},
            {"value": None},
            {"sensorsId": 99},
            {"parameter": {"name": "pm25", "units": "ppm"}},
            {"flagInfo": {"hasFlags": True}},
        ):
            item = measurement()
            item.update(update)
            with self.subTest(update=update), self.assertRaises(ValueError):
                bronze_record(item, meta, sensor, NOW - timedelta(hours=1), NOW)
        self.assertEqual([], self.sent)

    def test_zero_accepted_and_fractional_or_naive_or_outside_timestamp_rejected(self):
        loc, sensor = next(iter(PILOTS.items()))
        meta = metadata(loc, sensor)
        self.assertEqual(
            "0",
            bronze_record(measurement(0), meta, sensor, NOW - timedelta(hours=1), NOW)[
                "value"
            ],
        )
        for when in [
            NOW + timedelta(microseconds=1),
            NOW + timedelta(seconds=1),
            NOW.replace(tzinfo=None),
        ]:
            with self.subTest(when=when), self.assertRaises(ValueError):
                bronze_record(
                    measurement(when=when), meta, sensor, NOW - timedelta(hours=1), NOW
                )

    def test_partial_delivery_preserves_pending_and_cursor_then_resumes(self):
        self.api.rows[self.sensor] = [
            measurement(i, NOW - timedelta(minutes=i)) for i in range(3)
        ]

        def fail_second(row):
            if self.sent:
                raise TimeoutError("Synthetic delivery uncertainty")
            self.sent.append(row)

        with self.assertRaises(TimeoutError):
            ingest(
                self.api,
                self.state,
                fail_second,
                start=NOW - timedelta(hours=3),
                end=NOW,
                max_records=9,
            )
        self.assertEqual(
            1,
            self.state.db.execute(
                "SELECT count(*) FROM outbox WHERE sent=1"
            ).fetchone()[0],
        )
        self.assertEqual(2, self.run_batch()["published"])
        self.assertEqual(3, len(self.sent))
        self.assertEqual(0, self.run_batch()["published"])

    def test_whole_page_is_validated_before_any_delivery(self):
        self.api.rows[self.sensor] = [measurement(), measurement(-1)]
        with self.assertRaises(ValueError):
            self.run_batch()
        self.assertEqual([], self.sent)
        self.assertEqual(
            0, self.state.db.execute("SELECT count(*) FROM outbox").fetchone()[0]
        )

    def test_corrected_value_is_a_new_event_and_zero_response_is_valid(self):
        self.run_batch()
        self.api.rows[self.sensor] = [measurement(12)]
        self.assertEqual(1, self.run_batch()["published"])
        self.api.rows = {}
        self.assertEqual(0, self.run_batch()["published"])

    def test_record_budget_and_fixed_page_size_persist_pending(self):
        self.api.rows[self.sensor] = [
            measurement(i, NOW - timedelta(seconds=i)) for i in range(100)
        ]
        result = self.run_batch()
        self.assertLessEqual(result["published"], 9)
        self.assertGreater(result["outbox_pending_total"], 0)
        self.assertTrue(all(p["limit"] == 100 for _, p in self.api.calls))
        self.assertEqual(
            1,
            self.state.db.execute(
                "SELECT page FROM cursor WHERE sensor=?", (self.sensor,)
            ).fetchone()[0],
        )

    def test_state_cannot_bind_to_another_topic_or_enter_checkout(self):
        with self.assertRaises(ValueError):
            State(self.root, "other_topic", "localhost:19092")
        with self.assertRaises(ValueError):
            State(
                Path(__file__).resolve().parents[1] / "airlake-live-unsafe",
                "x",
                "localhost:19092",
            )

    def test_api_retries_transient_and_rate_limit_but_not_authentication(self):
        session = Mock()
        session.get.side_effect = [
            Mock(status_code=429, headers={"Retry-After": "1"}),
            Mock(status_code=200, json=lambda: {"results": []}),
        ]
        sleep = Mock()
        self.assertEqual(
            [], API("fixture-key", session, sleep).get("/fixture")["results"]
        )
        sleep.assert_called_once_with(1)
        session.get.side_effect = None
        session.get.return_value = Mock(status_code=401)
        with self.assertRaises(RuntimeError):
            API("fixture-key", session).get("/fixture")

    def test_second_writer_is_rejected_and_binding_is_checked_after_close(self):
        with self.assertRaises(ValueError):
            State(self.root, "airlake_live_fixture01", "localhost:19092")
        self.state.close()
        with self.assertRaises(ValueError):
            State(self.root, "different", "localhost:19092")
        self.state = State(self.root, "airlake_live_fixture01", "localhost:19092")
        self.addCleanup(self.state.close)

    def test_unavailable_pilot_is_reported_without_inventing_metadata(self):
        from ingestion.openaq import UnavailableStation

        original = self.api.metadata

        def metadata_or_missing(location, sensor):
            if sensor == self.sensor:
                raise UnavailableStation("synthetic 404")
            return original(location, sensor)

        self.api.metadata = metadata_or_missing
        other = list(PILOTS.values())[1]
        self.api.rows[other] = [measurement()]
        report = self.run_batch()
        self.assertEqual("SOURCE_UNAVAILABLE_HTTP_404", report["stations"][0]["status"])
        self.assertEqual(1, report["published"])
        self.assertEqual(
            0,
            self.state.db.execute(
                "SELECT count(*) FROM cursor WHERE sensor=?", (self.sensor,)
            ).fetchone()[0],
        )

    def test_catchup_window_remains_bounded_and_cannot_rewind(self):
        self.run_batch()
        ingest(
            self.api,
            self.state,
            self.sent.append,
            start=NOW + timedelta(days=2),
            end=NOW + timedelta(days=3),
            max_records=9,
        )
        start, end, _ = self.state.db.execute(
            "SELECT start,end,page FROM cursor WHERE sensor=?", (self.sensor,)
        ).fetchone()
        self.assertLessEqual(
            datetime.fromisoformat(end) - datetime.fromisoformat(start),
            timedelta(days=1),
        )
        with self.assertRaises(ValueError):
            self.state.window(self.sensor, NOW - timedelta(hours=3), NOW)

    def test_no_key_is_never_replaced_with_mock_data(self):
        with patch.dict("os.environ", {}, clear=True), self.assertRaises(ValueError):
            key_from_environment()

    def test_metadata_requires_correct_station_sensor_and_coordinates(self):
        loc, sensor = next(iter(PILOTS.items()))
        session = Mock()
        row = metadata(loc, sensor)
        row["coordinates"]["latitude"] = float("inf")
        session.get.return_value = Mock(
            status_code=200, json=lambda: {"results": [row]}
        )
        with self.assertRaises(ValueError):
            API("fixture-key", session).metadata(loc, sensor)
