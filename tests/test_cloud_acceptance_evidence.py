"""Check published cloud observations offline; never execute AWS or Spark."""

import hashlib
import json
import math
import re
import unittest
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

REPOSITORY = Path(__file__).resolve().parents[1]
EVIDENCE = REPOSITORY / "tests/evidence/real_openaq_cloud_2026-11-07.json"


class CloudAcceptanceEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = EVIDENCE.read_text()
        cls.record = json.loads(cls.text)
        cls.plan = json.loads((REPOSITORY / cls.record["plan"]["path"]).read_text())
        cls.local = json.loads(
            (REPOSITORY / cls.record["local_reference"]["path"]).read_text()
        )

    def test_executed_sources_and_frozen_plan_match(self):
        record = self.record
        self.assertEqual("PASS_BOUNDED_REAL_OPENAQ_CLOUD_ACCEPTANCE", record["status"])
        for name, expected in record["source_sha256"].items():
            with self.subTest(path=name):
                path = (REPOSITORY / name).resolve()
                self.assertTrue(path.is_relative_to(REPOSITORY))
                self.assertEqual(
                    expected, hashlib.sha256(path.read_bytes()).hexdigest()
                )
        for entry in (record["plan"], record["local_reference"]):
            self.assertEqual(
                entry["sha256"],
                hashlib.sha256((REPOSITORY / entry["path"]).read_bytes()).hexdigest(),
            )

    def test_source_provenance_counts_and_schemas_match_local_acceptance(self):
        source = self.record["source_selection"]
        for key in ("source_files", "selected_rows", "selected_sha256"):
            self.assertEqual(source[key], self.plan["source_selection"][key])
            self.assertEqual(source[key], self.local["selection"][key])
        self.assertEqual(200, source["selected_rows"])
        self.assertEqual(self.local["schemas"], self.record["schemas"])
        self.assertEqual(
            {
                "BRONZE_TABLE": 200,
                "SILVER_TABLE": 200,
                "BREAKPOINT_TABLE": 7,
                "DIMENSION_TABLE": 1,
                "HOURLY_TABLE": 23,
            },
            self.record["initial_counts"],
        )

    def test_unchanged_rerun_preserves_recorded_rows_snapshots_and_metadata(self):
        for key, state in self.record["table_states"].items():
            with self.subTest(table=key):
                before = state["before_unchanged_rerun"]
                self.assertEqual(before, state["after_unchanged_rerun"])
                self.assertEqual(
                    self.record["initial_counts"][key], before["row_count"]
                )
                self.assertRegex(before["rows_sha256"], r"^[a-f0-9]{64}$")
                self.assertEqual(1, len(before["snapshot_ids"]))
                self.assertTrue(
                    before["metadata_location"].startswith(self.plan["run_prefix"])
                )
        self.assertTrue(
            self.record["rerun_checks"][
                "unchanged_rerun_all_rows_snapshots_metadata_identical"
            ]
        )
        self.assertTrue(
            self.record["local_preservation"][
                "checkpoint_files_byte_identical_on_rerun"
            ]
        )

    def test_two_athena_results_reconcile_actual_cloud_values(self):
        queries = self.record["athena_queries"]
        self.assertEqual(2, len(queries))
        self.assertEqual(2, len({q["query_id"] for q in queries}))
        for query in queries:
            self.assertEqual("SUCCEEDED", query["state"])
            self.assertEqual("airlake", query["workgroup"])
            self.assertEqual("Athena engine version 3", query["engine"])
            self.assertEqual(self.plan["athena"][query["label"] + "_sql"], query["sql"])
        counts = dict(zip(queries[0]["headers"], queries[0]["rows"][0]))
        for key, expected in {
            "bronze_count": 200,
            "silver_count": 200,
            "dimension_count": 1,
            "hourly_count": 23,
            "duplicate_hourly_keys": 0,
            "orphan_count": 0,
        }.items():
            self.assertEqual(expected, int(counts[key]))
        self.assertTrue(
            math.isclose(float(counts["pm25_sum"]), 5746.71, rel_tol=0, abs_tol=1e-9)
        )
        hourly = queries[1]
        self.assertEqual(
            "timestamp with time zone", hourly["column_types"]["hour_start"]
        )
        self.assertEqual(
            "timestamptz", hourly["validation"]["actual_iceberg_hour_start_type"]
        )
        self.assertEqual(23, len(hourly["rows"]))
        for row, expected in zip(hourly["rows"], self.local["hourly_rows"]):
            location, name, hour, average, aqi = row
            self.assertEqual(expected["location_id"], int(location))
            self.assertEqual(self.local["dimension_rows"][0]["name"], name)
            self.assertTrue(hour.endswith(" UTC"))
            self.assertEqual(
                datetime.fromisoformat(expected["hour_start"]),
                datetime.fromisoformat(hour.removesuffix(" UTC")).replace(tzinfo=UTC),
            )
            self.assertEqual(expected["aqi"], int(aqi))
            self.assertTrue(
                math.isclose(
                    float(average), expected["pm25_avg"], rel_tol=0, abs_tol=1e-9
                )
            )

    def test_cleanup_inventory_is_explicit_scoped_and_unexecuted(self):
        inventory = self.record["cleanup_inventory"]
        resources = self.record["resources"]
        self.assertFalse(inventory["cleanup_executed"])
        self.assertTrue(inventory["approval_required"])
        self.assertEqual(self.plan["glue_databases"], resources["databases"])
        self.assertEqual(self.plan["tables"], resources["tables"])
        self.assertEqual(3, len(inventory["glue"]))
        self.assertEqual(5, sum(len(tables) for tables in inventory["glue"].values()))
        objects = inventory["lake_objects"]
        self.assertEqual(25, len(objects))
        self.assertEqual(25, len({o["Key"] for o in objects}))
        self.assertEqual(resources["total_bytes"], sum(o["Size"] for o in objects))
        prefix = urlparse(self.plan["run_prefix"]).path.lstrip("/")
        self.assertTrue(all(o["Key"].startswith(prefix) for o in objects))
        results = inventory["athena_result_objects"]
        self.assertEqual(4, len(results))
        for query in self.record["athena_queries"]:
            self.assertEqual(2, len(query["result_objects"]))
            for obj in query["result_objects"]:
                self.assertIn(obj, results)
                self.assertEqual("airlake-701364614078-athena-results", obj["bucket"])
                self.assertTrue(obj["key"].startswith(query["query_id"]))

    def test_publication_record_excludes_credentials_and_personal_paths(self):
        for pattern in (
            r"/Users/|/home/|/private/tmp/|/private/var/",
            r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b",
            r"github_pat_|ghp_",
            r"\b[0-9]{8,12}:[A-Za-z0-9_-]{30,}\b",
            r"-----BEGIN .*PRIVATE KEY-----",
        ):
            self.assertIsNone(re.search(pattern, self.text), pattern)


if __name__ == "__main__":
    unittest.main()
