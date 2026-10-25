"""Validate the retained acceptance record without executing pipeline code."""

import hashlib
import json
import re
import unittest
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
EVIDENCE = REPOSITORY / "tests/evidence/gold_dimension_name_repair_2026-10-25.json"


class AcceptanceEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = EVIDENCE.read_text(encoding="utf-8")
        cls.data = json.loads(cls.text)

    def test_recorded_counts_and_method_results_agree(self):
        self.assertEqual(
            {
                "passed": 48,
                "failed": 0,
                "errors": 0,
                "skipped": 0,
                "exit": 0,
                "seconds": 130.311,
            },
            self.data["checks"]["integration"],
        )
        results = self.data["integration_method_results"]
        self.assertEqual(48, len(results))
        self.assertEqual(48, len({record["test"] for record in results}))
        self.assertTrue(all(record["status"] == "PASS" for record in results))
        self.assertEqual(9, self.data["checks"]["pure"]["passed"])

    def test_verified_gold_sources_still_match_the_record(self):
        for name, expected in self.data["source_sha256"].items():
            with self.subTest(path=name):
                self.assertFalse(Path(name).is_absolute())
                path = (REPOSITORY / name).resolve()
                self.assertTrue(path.is_relative_to(REPOSITORY))
                self.assertEqual(
                    expected, hashlib.sha256(path.read_bytes()).hexdigest()
                )

    def test_state_references_match_canonical_hashes(self):
        for key, state in self.data["states"].items():
            canonical = json.dumps(
                state, sort_keys=True, separators=(",", ":")
            ).encode()
            self.assertEqual(key, hashlib.sha256(canonical).hexdigest())

    def test_rejections_retain_identical_recorded_rows_and_snapshots(self):
        records = self.data["rejection_comparisons"]
        self.assertEqual(85, len(records))
        for record in records:
            with self.subTest(test=record["test"], case=record["case_index_in_method"]):
                self.assertEqual("RuntimeError", record["exception_type"])
                self.assertIn(record["before_state"], self.data["states"])
                self.assertEqual(record["before_state"], record["after_state"])

    def test_repairs_change_only_null_name_then_preserve_all_state(self):
        records = self.data["successful_repairs"]
        self.assertEqual(4, len(records))
        for record in records:
            with self.subTest(test=record["test"]):
                before = self.data["states"][record["before_state"]]
                repaired = self.data["states"][record["repaired_state"]]
                self.assertEqual(record["repaired_state"], record["rerun_state"])
                dimension = next(
                    table for table in before if ".gold.locations_" in table
                )
                expected = [json.loads(row) for row in before[dimension][0]]
                for row in expected:
                    if row["location_id"] == record["location_id"]:
                        self.assertIsNone(row.get("name"))
                        row["name"] = f"Fixture station {record['location_id']}"
                actual = [json.loads(row) for row in repaired[dimension][0]]
                self.assertEqual(expected, actual)
                self.assertEqual(
                    set(before[dimension][1])
                    | set(record["added_dimension_snapshot_ids"]),
                    set(repaired[dimension][1]),
                )
                self.assertEqual(1, len(record["added_dimension_snapshot_ids"]))
                for table in before.keys() - {dimension}:
                    self.assertEqual(before[table], repaired[table])

    def test_publication_record_has_no_personal_paths_or_token_patterns(self):
        patterns = (
            r"/Users/",
            r"/home/",
            r"[A-Z]:\\Users\\",
            r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b",
            r"github_pat_",
            r"ghp_",
            r"\b[0-9]{8,12}:[A-Za-z0-9_-]{30,}\b",
            r"-----BEGIN .*PRIVATE KEY-----",
        )
        for pattern in patterns:
            with self.subTest(pattern=pattern):
                self.assertIsNone(re.search(pattern, self.text))


if __name__ == "__main__":
    unittest.main()
