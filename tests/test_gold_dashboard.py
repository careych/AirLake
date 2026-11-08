"""Dashboard schema/provenance/freshness checks against retained Gold fixture."""

import json
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from dashboard.assets import pack
from dashboard.install import install
from dashboard.prepare import prepare


class GoldDashboardTests(unittest.TestCase):
    def setUp(self):
        base = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.root = base / "runtime"
        self.manifest = prepare(self.root)
        self.db = sqlite3.connect(self.root / "gold.sqlite")
        self.addCleanup(self.db.close)

    def test_actual_retained_gold_schema_and_counts(self):
        self.assertEqual(
            23, self.db.execute("SELECT count(*) FROM gold_hourly").fetchone()[0]
        )
        self.assertEqual(
            ["location_id", "hour_start", "pm25_avg", "aqi"],
            [r[1] for r in self.db.execute("PRAGMA table_info(pm25_aqi_hourly)")],
        )
        self.assertFalse(self.manifest["synthetic_measurements"])
        self.assertEqual("UTC", self.manifest["timezone"])

    def test_latest_is_per_station_and_historical_data_is_stale(self):
        row = self.db.execute(
            "SELECT hour_start_utc,pm25_avg,aqi,age_hours,freshness FROM gold_latest"
        ).fetchone()
        self.assertEqual("2025-07-04 13:00:00", row[0])
        self.assertEqual(49, row[2])
        self.assertGreater(row[3], 24)
        self.assertEqual("STALE (>24h)", row[4])
        self.db.execute(
            "INSERT INTO dim_location VALUES(99,'Synthetic test-only second station',21,105,'reference')"
        )
        self.db.execute(
            "INSERT INTO pm25_aqi_hourly VALUES(99,'2025-07-03 12:00:00',10,20)"
        )
        self.assertEqual(
            2, self.db.execute("SELECT count(*) FROM gold_latest").fetchone()[0]
        )

    def test_fixture_preparation_refuses_overwrite_and_checkout_output(self):
        with self.assertRaises(FileExistsError):
            prepare(self.root)
        with self.assertRaises(ValueError):
            prepare(Path(__file__).resolve().parents[1] / "private-demo")

    def test_assets_are_importable_yaml_without_secrets_or_runtime_data(self):
        bundle = self.root / "dashboard.zip"
        count = pack(bundle)
        self.assertEqual(10, count)
        with zipfile.ZipFile(bundle) as archive:
            text = "\n".join(archive.read(n).decode() for n in archive.namelist())
            self.assertIn("HISTORICAL", text)
            self.assertNotIn("dist_bar", text)
            private = json.loads((self.root / "private.json").read_text())
            self.assertNotIn(private["SUPERSET_SECRET_KEY"], text)
            self.assertNotIn(private["ADMIN_PASSWORD"], text)
        with self.assertRaises(FileExistsError):
            pack(bundle)

    def test_installer_never_targets_remote_or_recreates_an_installed_dashboard(self):
        with patch(
            "dashboard.install.requests.Session",
            side_effect=AssertionError("No network"),
        ):
            with self.assertRaises(ValueError):
                install(self.root, "https://production.invalid")
            (self.root / "dashboard-acceptance.json").write_text("{}")
            with self.assertRaises(ValueError):
                install(self.root)

    def test_athena_sql_is_read_only_pinned_and_explicitly_bounded(self):
        sql = (
            Path(__file__).resolve().parents[1] / "dashboard/sql/athena_gold.sql"
        ).read_text()
        self.assertEqual(2, sql.count("FOR VERSION AS OF"))
        self.assertIn("2025-07-04 14:00:00 UTC", sql)
        self.assertNotIn("CREATE TABLE", sql)
        self.assertNotIn("INSERT INTO", sql)
        self.assertIn("NOT a live", sql)
