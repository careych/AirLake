"""Synthetic fixtures for the live layers; no network or AWS."""

import unittest
from datetime import UTC, datetime, timedelta

from nowcast import openmeteo, project, sensor_community, validate

NOW = datetime(2026, 11, 15, 1, 20, tzinfo=UTC)
RUN = int(datetime(2026, 11, 14, 12, tzinfo=UTC).timestamp())


def model_body(
    start=datetime(2026, 11, 14, tzinfo=UTC), hours=144, value=lambda i: 20.0 + i % 24
):
    return {
        "latitude": 21.0,
        "longitude": 105.9,
        "hourly_units": {"pm2_5": "μg/m³"},
        "hourly": {
            "time": [
                (start + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M")
                for i in range(hours)
            ],
            "pm2_5": [value(i) if i < hours - 11 else None for i in range(hours)],
        },
    }


META = {
    "last_run_initialisation_time": RUN,
    "last_run_availability_time": RUN + 36000,
    "update_interval_seconds": 43200,
}


class OpenMeteoTests(unittest.TestCase):
    def test_current_is_latest_past_hour_and_forecast_is_strictly_future(self):
        m = openmeteo.normalize(META, model_body(), NOW)
        self.assertEqual(m["current"]["valid_at"], "2026-11-15T01:00:00Z")
        self.assertEqual(m["current"]["kind"], "model_estimate")
        self.assertEqual(m["forecast"][0]["valid_at"], "2026-11-15T02:00:00Z")
        self.assertEqual(m["forecast"][0]["hours_ahead"], 1)
        self.assertEqual(m["forecast"][0]["lead_hours_from_run"], 14)
        self.assertEqual(m["model_run_at"], "2026-11-14T12:00:00Z")
        self.assertTrue(all(d["hours"] >= 20 for d in m["daily"]))
        w = m["relative_windows_24h"]
        self.assertTrue(w["distinct"])
        self.assertLess(w["lower"]["mean"], w["higher"]["mean"])
        flat = openmeteo.normalize(META, model_body(value=lambda i: 30.0), NOW)
        self.assertFalse(flat["relative_windows_24h"]["distinct"])
        self.assertIsNone(flat["relative_windows_24h"]["lower"])

    def test_rejects_wrong_cell_unit_gaps_and_bad_values(self):
        for change in (
            lambda b: b.update(latitude=10.8),
            lambda b: b["hourly_units"].update(pm2_5="ppm"),
            lambda b: b["hourly"]["time"].pop(30),
            lambda b: b["hourly"]["pm2_5"].__setitem__(5, -1.0),
            lambda b: b["hourly"]["pm2_5"].__setitem__(5, float("nan")),
        ):
            body = model_body()
            change(body)
            with self.assertRaises(ValueError):
                openmeteo.normalize(META, body, NOW)
        with self.assertRaises(ValueError):
            openmeteo.normalize(
                {**META, "update_interval_seconds": 3600}, model_body(), NOW
            )

    def test_needs_a_full_next_day(self):
        with self.assertRaises(ValueError):
            openmeteo.normalize(META, model_body(hours=40), NOW)


class SensorCommunityTests(unittest.TestCase):
    def record(self, sid, ts, p2, indoor=0):
        return {
            "sensor": {"id": sid},
            "location": {"indoor": indoor},
            "timestamp": ts,
            "sensordatavalues": [
                {"value_type": "P1", "value": "9"},
                {"value_type": "P2", "value": p2},
            ],
        }

    def test_latest_ignores_indoor_and_other_sensors(self):
        rows = [
            self.record(99385, "2026-11-15 01:10:00", "40.1"),
            self.record(99385, "2026-11-15 01:12:30", "41.5"),
            self.record(99385, "2026-11-15 01:13:00", "90", indoor=1),
            self.record(1, "2026-11-15 01:14:00", "80"),
        ]
        self.assertEqual(
            sensor_community.latest(rows, 99385),
            {"observed_at": "2026-11-15T01:12:30Z", "pm25": 41.5},
        )
        self.assertIsNone(sensor_community.latest([], 99385))

    def test_archive_hours_need_enough_readings_and_are_not_filled(self):
        head = "sensor_id;sensor_type;location;lat;lon;timestamp;P1;durP1;ratioP1;P2;durP2;ratioP2\n"
        full = "".join(
            f"99385;SDS011;1;21.07;105.824;2026-09-15T00:{i * 4:02d}:00;9;;;{10 + i};;\n"
            for i in range(15)
        )
        sparse = "".join(
            f"99385;SDS011;1;21.07;105.824;2026-09-15T01:{i * 6:02d}:00;9;;;50;;\n"
            for i in range(5)
        )
        rows = sensor_community.hourly(head + full + sparse, 99385)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["observed_hour_utc"], "2026-09-15T00:00:00Z")
        self.assertEqual((rows[0]["pm25"], rows[0]["readings"]), (17.0, 15))
        with self.assertRaises(ValueError):
            sensor_community.hourly(
                head + "99385;SDS011;1;21;105;2026-09-15T02:00:00;9;;;-3;;\n", 99385
            )


class ProjectionTests(unittest.TestCase):
    def test_layers_stay_separate_and_freshness_uses_observation_time(self):
        model = openmeteo.normalize(META, model_body(), NOW)
        value = project.build(
            NOW,
            {"observed_at": "2026-11-15T00:00:00Z", "pm25": 40.5},
            None,
            model,
            {"status": "SYNTHETIC"},
        )
        dragon, sc = value["observations"]
        self.assertEqual(
            (dragon["kind"], dragon["freshness"]["status"]), ("observed", "recent")
        )
        self.assertEqual(sc["freshness"]["status"], "no_data")
        self.assertIsNone(sc["pm25"])
        self.assertEqual(value["model"]["kind"], "model")
        self.assertNotIn("pm25", value["model"])

    def test_openaq_latest_picks_pm25_sensor(self):
        body = {
            "results": [
                {
                    "sensorsId": 14581374,
                    "value": 30,
                    "datetime": {"utc": "2026-11-15T00:00:00Z"},
                },
                {
                    "sensorsId": 14581375,
                    "value": 40.5,
                    "datetime": {"utc": "2026-11-15T00:00:00Z"},
                },
            ]
        }
        self.assertEqual(project.openaq_latest(body)["pm25"], 40.5)


class ValidationTests(unittest.TestCase):
    def test_bias_mae_and_ratio_split_use_edge_mean_alignment(self):
        start = datetime(2026, 8, 31, 17, tzinfo=UTC)
        series = [
            ((start + timedelta(hours=i)).isoformat(), 30.0) for i in range(30 * 24 + 1)
        ]
        obs = [
            {
                "observed_hour_utc": (start + timedelta(hours=i)).isoformat(),
                "pm25": 20.0,
            }
            for i in range(30 * 24)
        ]
        r = validate.compare(series, obs)
        self.assertEqual(
            (r["hourly"]["n"], r["hourly"]["mae"], r["hourly"]["bias"]),
            (720, 10.0, 10.0),
        )
        self.assertEqual(r["daily_complete"]["n"], 30)
        self.assertAlmostEqual(r["calibration_test"]["ratio"], 2 / 3)
        self.assertAlmostEqual(r["calibration_test"]["test_calibrated"]["mae"], 0.0)

    def test_missing_model_edge_drops_the_hour(self):
        start = datetime(2026, 9, 1, tzinfo=UTC)
        r = validate.compare(
            [(start.isoformat(), 1.0)],
            [{"observed_hour_utc": start.isoformat(), "pm25": 1.0}],
        )
        self.assertEqual(r["hourly"]["n"], 0)


if __name__ == "__main__":
    unittest.main()
