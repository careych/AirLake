import json
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from nowcast import model_areas, refresh
from nowcast.http import UpstreamError
from tests.test_nowcast import FakeS3

NOW = datetime(2026, 11, 15, 9, 20, tzinfo=UTC)
RUN = datetime(2026, 11, 15, 0, tzinfo=UTC)
META = {
    "last_run_initialisation_time": int(RUN.timestamp()),
    "last_run_availability_time": int((RUN + timedelta(hours=8)).timestamp()),
    "update_interval_seconds": 43200,
}


def body(area, base):
    start = datetime(2026, 11, 14, 0, tzinfo=UTC)
    times = [
        (start + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M") for i in range(144)
    ]
    return {
        "latitude": round(area["lat"], 1),
        "longitude": round(area["lon"], 1),
        "hourly_units": {"pm2_5": "μg/m³"},
        "hourly": {"time": times, "pm2_5": [base + i % 7 for i in range(144)]},
    }


def bodies():
    # value depends only on the native cell, as CAMS does
    return [
        body(
            a,
            10
            * (
                1
                + sorted(
                    {model_areas.cell(x["lat"], x["lon"])[0] for x in model_areas.AREAS}
                ).index(model_areas.cell(a["lat"], a["lon"])[0])
            ),
        )
        for a in model_areas.AREAS
    ]


class ModelAreaTests(unittest.TestCase):
    def test_area_list_matches_the_site(self):
        site = Path(__file__).resolve().parents[1] / "citizen/web/areas.json"
        self.assertEqual(
            json.loads(site.read_text()),
            {**json.loads(site.read_text()), "areas": model_areas.AREAS},
        )
        self.assertEqual(len(model_areas.AREAS), 126)

    def test_groups_follow_native_cells_and_share_one_value(self):
        v = model_areas.build(META, bodies(), NOW)
        self.assertEqual(sum(len(g["areas"]) for g in v["groups"]), 126)
        self.assertEqual(
            v["group_count"],
            len({model_areas.cell(a["lat"], a["lon"])[0] for a in model_areas.AREAS}),
        )
        for g in v["groups"]:
            self.assertTrue(g["cell"]["verified"])
            self.assertEqual(g["current"]["valid_at"], "2026-11-15T09:00:00Z")
            self.assertEqual(len(g["forecast"]), model_areas.FORECAST_HOURS)
        self.assertIn("no interpolation", v["grouping"])

    def test_cell_bounds(self):
        self.assertEqual(
            model_areas.cell(21.03, 105.82), ((21.2, 106.0), [21.0, 105.8, 21.4, 106.2])
        )
        self.assertEqual(model_areas.cell(21.03, 105.79)[0], (21.2, 105.6))

    def test_refresh_keeps_publishing_when_the_area_model_fails(self):
        model = {
            "model_run_at": "2026-11-15T00:00:00Z",
            "model_available_at": "2026-11-15T08:00:00Z",
        }

        def failing(*a, **k):
            raise UpstreamError("HTTP 429")

        s3 = FakeS3()
        out = refresh.run_areas(
            refresh.Store(s3),
            failing,
            {"model_areas": {"model_run_at": "2026-11-14T12:00:00Z"}},
            model,
            NOW,
        )
        self.assertEqual(
            (out["fetch_status"], out["groups"], out["last_good_model_run_at"]),
            ("unavailable", [], "2026-11-14T12:00:00Z"),
        )
        ok = refresh.run_areas(
            refresh.Store(s3), lambda *a, **k: bodies(), None, model, NOW
        )
        self.assertEqual(ok["fetch_status"], "ok")
        self.assertEqual(len(s3.puts), 1)  # raw run stored once
        again = refresh.run_areas(
            refresh.Store(s3), failing, None, model, NOW + timedelta(minutes=15)
        )
        self.assertEqual(again["fetch_status"], "ok")  # reused from S3, no new call


if __name__ == "__main__":
    unittest.main()
