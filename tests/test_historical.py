"""Synthetic logic fixtures only; real September acceptance is recorded separately."""

import copy
import csv
import gzip
import io
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from historical.analytics import summarize
from historical.api import handle, load
from historical.data import save_scan, scan
from ingestion.openaq import FIELDS
from live_data.contract import sha

START = "2026-08-31T17:00:00Z"
END = "2026-09-30T17:00:00Z"


def synthetic_facts(days=30, station=6123215):
    t = datetime.fromisoformat(START)
    return [
        {
            "location_id": station,
            "hour_start": (t + timedelta(hours=i)).isoformat().replace("+00:00", "Z"),
            "pm25_avg": float(10 + i % 24),
            "aqi": 20,
        }
        for i in range(days * 24)
    ]


class AnalyticsTests(unittest.TestCase):
    def test_thirty_genuine_shape_days_support_all_descriptive_analytics(self):
        s = summarize(synthetic_facts(), START, END, [6123215])["stations"]["6123215"]
        self.assertEqual(
            (s["hour_count"], s["complete_days"], len(s["heatmap"])), (720, 30, 720)
        )
        self.assertEqual(s["coverage_percent"], 100)
        self.assertEqual(len(s["daily"]), 30)
        self.assertEqual(s["monthly_extremes"]["status"], "OBSERVED_COMPLETE_DAYS_ONLY")
        self.assertEqual(s["weekday_weekend"]["status"], "DESCRIPTIVE_ONLY")
        self.assertEqual(s["mean_24h_descriptive"]["hour_count"], 24)
        self.assertTrue(all(r["days"] == 30 for r in s["hour_of_day"]))

    def test_sparse_hours_never_become_monthly_or_weekly_claims(self):
        rows = synthetic_facts()[:4]
        s = summarize(rows, START, END, [6123215])["stations"]["6123215"]
        self.assertEqual(s["monthly_extremes"]["status"], "INSUFFICIENT_MONTH_COVERAGE")
        self.assertEqual(s["weekday_weekend"]["status"], "INSUFFICIENT_COMPLETE_DAYS")
        self.assertTrue(all(r["pm25_avg"] is None for r in s["hour_of_day"]))
        self.assertIsNone(s["mean_24h_descriptive"])
        self.assertEqual(s["daily"][0]["hours_present"], 4)
        self.assertFalse(s["daily"][0]["complete_24_hours"])
        self.assertEqual(len(s["missing_hours_utc"]), 716)

    def test_gaps_are_not_interpolated_and_daily_mean_uses_available_hours(self):
        rows = synthetic_facts(1)
        del rows[5]
        s = summarize(rows, START, "2026-09-01T17:00:00Z", [6123215])["stations"][
            "6123215"
        ]
        self.assertEqual(s["hour_count"], 23)
        self.assertEqual(s["missing_hours_utc"], ["2026-08-31T22:00:00Z"])
        self.assertAlmostEqual(
            s["daily"][0]["pm25_avg"], sum(r["pm25_avg"] for r in rows) / 23
        )
        self.assertIsNone(s["mean_24h_descriptive"])

    def test_utc_to_vietnam_calendar_boundary_and_exclusive_end(self):
        rows = synthetic_facts(2)
        s = summarize(rows, START, "2026-09-01T17:00:00Z", [6123215])["stations"][
            "6123215"
        ]
        self.assertEqual(s["hour_count"], 24)
        self.assertEqual(s["daily"][0]["date_local"], "2026-09-01")
        self.assertEqual(s["heatmap"][0]["hour_local"], 0)

    def test_duplicates_nonfinite_and_negative_values_fail(self):
        for value in (float("nan"), float("inf"), -1):
            rows = synthetic_facts(1)
            rows[0]["pm25_avg"] = value
            with self.assertRaises(ValueError):
                summarize(rows, START, END, [6123215])
        rows = synthetic_facts(1)
        rows.append(copy.deepcopy(rows[0]))
        with self.assertRaises(ValueError):
            summarize(rows, START, END, [6123215])

    def test_zero_is_real_and_no_data_is_not_healthy(self):
        rows = synthetic_facts(1)
        rows[0]["pm25_avg"] = 0
        s = summarize(rows, START, END, [6123215, 4946812])["stations"]
        self.assertEqual(s["6123215"]["extremes"]["lowest_hour"]["pm25_avg"], 0)
        self.assertEqual(s["4946812"]["status"], "NO_DATA")
        self.assertIsNone(s["4946812"]["extremes"]["highest_hour"])


class ArchiveTests(unittest.TestCase):
    def file(self, root, rows):
        p = root / "6123215/year=2026/month=09/location-6123215-20260901.csv.gz"
        p.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(p, "wt", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(rows)
        return p

    def row(self, **kwargs):
        return (
            dict(
                zip(
                    FIELDS,
                    [
                        "6123215",
                        "14581375",
                        "SYNTHETIC fixture",
                        "2026-09-01T01:00:00+07:00",
                        "20.9933",
                        "105.9441",
                        "pm25",
                        "µg/m³",
                        "10",
                    ],
                    strict=True,
                )
            )
            | kwargs
        )

    def scan(self, root, meta=None):
        return scan(
            root,
            START,
            "2026-09-01T17:00:00Z",
            locations=(6123215,),
            station_metadata=meta,
        )

    def test_offsets_dedup_and_source_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.file(root, [self.row(), self.row()])
            a, rs = self.scan(root)
            self.assertEqual(a["accepted_unique_records"], 1)
            self.assertEqual(a["stations"]["6123215"]["duplicate_identities"], 1)
            self.assertEqual(
                a["hourly_reference"][0]["hour_start"], "2026-08-31T18:00:00Z"
            )
            self.assertEqual(
                rs[0]["source"]["source_datetime"], "2026-09-01T01:00:00+07:00"
            )
            self.assertEqual(rs[0]["source"]["line"], 2)

    def test_conflicting_duplicate_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.file(root, [self.row(), self.row(value="11")])
            with self.assertRaises(ValueError):
                self.scan(root)

    def test_serialization_noise_is_only_normalized_to_verified_coordinates(self):
        meta = [
            {"id": 6123215, "coordinates": {"latitude": 20.9933, "longitude": 105.9441}}
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.file(root, [self.row(lon="105.94410000000002")])
            a, rs = self.scan(root, meta)
            self.assertEqual(rs[0]["record"]["lon"], "105.9441")
            self.assertEqual(
                rs[0]["source"]["coordinate_serialization_original"]["lon"],
                "105.94410000000002",
            )
            self.assertEqual(
                a["stations"]["6123215"]["coordinate_serialization_normalized"], 1
            )
            self.file(root, [self.row(lon="105.9451")])
            with self.assertRaises(ValueError):
                self.scan(root, meta)

    def test_unsupported_sensor_units_and_naive_time_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for row in (
                self.row(sensors_id="1"),
                self.row(units="mg/m³"),
                self.row(datetime="2026-09-01T01:00:00"),
                self.row(value="NaN"),
            ):
                self.file(root, [row])
                with self.assertRaises(ValueError):
                    self.scan(root)

    def test_save_is_private_and_never_overwrites_source_or_prior_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            f = self.file(root, [self.row()])
            before = f.read_bytes()
            a, rs = self.scan(root)
            out = root / "new"
            result = save_scan(out, a, rs)
            self.assertEqual(
                result["records_sha256"], sha((out / "records.jsonl").read_bytes())
            )
            self.assertEqual(f.read_bytes(), before)
            with self.assertRaises(ValueError):
                save_scan(out, a, rs)


class HistoryAPITests(unittest.TestCase):
    def fixture(self):
        facts = synthetic_facts(1)
        analysis = summarize(facts, START, "2026-09-01T17:00:00Z", [6123215])
        snaps = {
            k: [i + 1]
            for i, k in enumerate(
                ("bronze", "silver", "breakpoints", "dimensions", "hourly", "daily")
            )
        }
        ex = {
            "facts": facts,
            "daily": analysis["stations"]["6123215"]["daily"],
            "dimensions": [
                {"location_id": loc, "name": "SYNTHETIC station"}
                for loc in (4946812, 4946813, 6123215)
            ],
            "provenance": {
                "execution": "AWS_GLUE",
                "snapshots": snaps,
                "window_start_utc": START,
                "window_end_utc": "2026-09-01T17:00:00Z",
            },
        }
        raw = json.dumps(ex).encode()
        proof = json.dumps(
            {
                "export_sha256": sha(raw),
                "hourly_all_rows_match": "PASS",
                "daily_all_rows_match": "PASS",
                "snapshots": snaps,
            }
        ).encode()
        store = Mock()
        store.s3.get_object.side_effect = lambda **kw: {
            "Body": io.BytesIO(proof if kw["Key"].endswith("acceptance.json") else raw)
        }
        env = {
            "AIRLAKE_HISTORY_EXPORT_KEY": "historical-demo/20261114_7a6e91c2/outputs/export.json",
            "AIRLAKE_HISTORY_EXPORT_SHA256": sha(raw),
            "AIRLAKE_HISTORY_ACCEPTANCE_SHA256": sha(proof),
        }
        return store, env, ex

    def test_hourly_daily_json_csv_filters_and_pagination(self):
        store, env, _ = self.fixture()
        out = json.loads(
            handle({"rawQueryString": "station_id=6123215&limit=10"}, store, env)[
                "body"
            ]
        )
        self.assertEqual((out["count"], out["total"]), (10, 24))
        self.assertIn("offset=10", out["next"])
        out = handle({"rawQueryString": "granularity=daily&format=csv"}, store, env)
        rows = list(csv.DictReader(io.StringIO(out["body"])))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["hours_present"], "24")

    def test_bad_parameters_are_rejected_before_s3(self):
        store, env, _ = self.fixture()
        for query in (
            "station_id=0",
            "sensor_id=0",
            "limit=1001",
            "offset=3001",
            "station_id=6123215&sensor_id=13502151",
            "format=sql",
            "limit=2&limit=3",
            "database=production",
            "start=2026-09-01T00:00:00+07:00",
        ):
            with self.subTest(query=query), self.assertRaises(ValueError):
                handle({"rawQueryString": query}, store, env)
        store.s3.get_object.assert_not_called()

    def test_tampered_artifact_missing_acceptance_or_unsafe_key_rejected(self):
        for key, value in (
            ("AIRLAKE_HISTORY_EXPORT_KEY", "production/export.json"),
            ("AIRLAKE_HISTORY_EXPORT_SHA256", "a" * 64),
            ("AIRLAKE_HISTORY_ACCEPTANCE_SHA256", "b" * 64),
        ):
            store, env, _ = self.fixture()
            env[key] = value
            with self.assertRaises(RuntimeError):
                load(store, env)

    def test_private_lambda_guard_and_legacy_route_preserved(self):
        from live_data.api import handler

        store = Mock()
        event = {
            "rawPath": "/api/v1/history",
            "requestContext": {"http": {"method": "GET"}},
        }
        self.assertEqual(handler(event, None, store=store)["statusCode"], 403)
        store.s3.get_object.assert_not_called()
        with patch(
            "live_data.api.historical_handler", return_value={"statusCode": 200}
        ) as legacy:
            self.assertEqual(
                handler({"rawPath": "/api/v1/health"}, None)["statusCode"], 200
            )
            legacy.assert_called_once()
