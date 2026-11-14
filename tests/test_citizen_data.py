"""Synthetic unit fixtures, never published as real observations."""

import copy
import json
import unittest
from datetime import UTC, datetime

from citizen.data import download, project
from ingestion.openaq import retry_delay


def fixtures():
    ids = [4946812, 4946813, 6123215]
    export = {
        "dimensions": [
            {"location_id": loc, "name": "SYNTHETIC " + str(loc)} for loc in ids
        ],
        "facts": [
            {
                "location_id": loc,
                "hour_start": "2026-11-14T10:00:00Z",
                "pm25_avg": 0.0,
                "aqi": 0,
            }
            for loc in ids
        ],
        "provenance": {"computed_at": "2026-11-14T11:00:00.123Z"},
    }
    inv = {
        "latest": {
            str(loc): {
                "observed_at": "2026-11-14T10:00:00Z",
                "ingested_at": "2026-11-14T10:05:00Z",
                "pm25": 0.0,
                "units": "µg/m³",
                "cadence_seconds": 3600,
                "DO_NOT_PUBLISH": "SYNTHETIC private marker",
            }
            for loc in ids
        }
    }
    cov = {
        "license_41": {
            "id": 41,
            "redistributionAllowed": True,
            "modificationAllowed": True,
            "sourceUrl": "https://creativecommons.org/licenses/by/4.0/",
        },
        "locations": [
            {
                "id": loc,
                "isMobile": False,
                "coordinates": {"latitude": 21.0, "longitude": 105.8},
                "provider": {"name": "SYNTHETIC provider"},
                "licenses": [
                    {
                        "id": 41,
                        "dateFrom": "2023-07-15",
                        "dateTo": None,
                        "attribution": {"name": "SYNTHETIC author"},
                    }
                ]
                if loc == 6123215
                else None,
            }
            for loc in ids
        ],
    }
    return export, inv, cov, {str(loc): 1 for loc in ids}


class CitizenTests(unittest.TestCase):
    def data(self, mode):
        ex, inv, cov, counts = fixtures()
        return project(ex, inv, cov, mode, counts)

    def test_public_allowlist_excludes_uncleared_stations_and_private_fields(self):
        data = self.data("public")
        self.assertEqual([r["id"] for r in data["stations"]], [6123215])
        text = json.dumps(data)
        for secret in (
            "4946812",
            "4946813",
            "DO_NOT_PUBLISH",
            "database",
            "query_id",
            "snapshot",
        ):
            self.assertNotIn(secret, text)
        self.assertEqual(data["stations"][0]["latest"]["pm25"], 0)
        self.assertEqual(len(data["hourly"]), 1)

    def test_private_preview_downloads_still_restricted(self):
        data = self.data("instructor")
        self.assertEqual(len(data["stations"]), 3)
        for loc in (4946812, 4946813):
            with self.assertRaises(ValueError):
                download(data, loc)
        result = json.loads(download(data, 6123215))
        self.assertEqual(len(result["data"]), 1)
        self.assertIn("SYNTHETIC author", result["source"])
        self.assertIn(
            "https://creativecommons.org/licenses/by/4.0/",
            download(data, 6123215, fmt="csv"),
        )

    def test_license_missing_or_redistribution_forbidden_fails_closed(self):
        for key in ("redistributionAllowed", "modificationAllowed"):
            ex, inv, cov, counts = fixtures()
            cov["license_41"][key] = False
            with self.assertRaises(ValueError):
                project(ex, inv, cov, "public", counts)
        ex, inv, cov, counts = fixtures()
        cov["locations"][-1]["licenses"] = None
        with self.assertRaises(ValueError):
            project(ex, inv, cov, "public", counts)

    def test_bad_readings_and_duplicate_hour_keys_rejected(self):
        for value in (-1, float("inf"), float("nan")):
            ex, inv, cov, counts = fixtures()
            ex["facts"][-1]["pm25_avg"] = value
            with self.assertRaises(ValueError):
                project(ex, inv, cov, "public", counts)
        ex, inv, cov, counts = fixtures()
        ex["facts"].append(copy.deepcopy(ex["facts"][-1]))
        with self.assertRaises(ValueError):
            project(ex, inv, cov, "public", counts)

    def test_time_filter_boundaries_empty_and_no_fabricated_rows(self):
        data = self.data("public")
        self.assertEqual(
            len(
                json.loads(
                    download(
                        data, 6123215, "2026-11-14T10:00:00Z", "2026-11-14T11:00:00Z"
                    )
                )["data"]
            ),
            1,
        )
        self.assertEqual(
            len(
                json.loads(
                    download(
                        data, 6123215, "2026-11-14T09:00:00Z", "2026-11-14T10:00:00Z"
                    )
                )["data"]
            ),
            0,
        )
        with self.assertRaises(ValueError):
            download(data, 6123215, "2026-11-14T11:00:00Z", "2026-11-14T10:00:00Z")
        self.assertFalse(data["automatic_refresh"])
        self.assertFalse(data["seven_day_completeness_verified"])
        self.assertFalse(data["telegram"]["delivery_verified_in_this_release"])

    def test_retry_after_seconds_dates_case_and_bounds(self):
        now = datetime(2026, 11, 14, 10, tzinfo=UTC)
        self.assertEqual(retry_delay({"retry-after": "3"}, 0, now), 3)
        self.assertEqual(
            retry_delay({"Retry-After": "Sat, 14 Nov 2026 10:00:20 GMT"}, 0, now), 20
        )
        self.assertEqual(retry_delay({}, 1, now), 2)
        self.assertEqual(
            retry_delay({"Retry-After": "Sat, 14 Nov 2026 09:00:20 GMT"}, 0, now), 1
        )
        for value in ("31", "bad", "9999999999999999999"):
            with self.assertRaises(RuntimeError):
                retry_delay({"Retry-After": value}, 0, now)
