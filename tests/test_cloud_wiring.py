"""Offline checkpoint binding and accepted Kafka dependency contracts."""

import contextlib
import json
import socket
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from scripts.bronze_local import prepare_cloud_checkpoint
from scripts.pipeline_config import KAFKA_JARS, CatalogConfig

REPOSITORY = Path(__file__).resolve().parents[1]
RUN = "20261106_33333333333333333333333333333333"


class CloudCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(
                socket,
                "socket",
                side_effect=AssertionError("No network in checkpoint unit tests"),
            )
        )
        parent = Path(
            self.enterContext(
                tempfile.TemporaryDirectory(prefix="airlake-checkpoint-unit-")
            )
        ).resolve()
        self.root = parent / ("airlake-it2-cloud-" + RUN)
        self.root.mkdir()
        self.enterContext(contextlib.chdir(self.root))
        self.path = self.root / "bronze-checkpoint"
        self.marker = self.root / "bronze-checkpoint.binding.json"
        self.config = CatalogConfig(
            mode="glue",
            catalog="cloudreal",
            warehouse=f"s3://fixture/integration-real/{RUN}/warehouse/",
            account="701364614078",
            allow_cloud_write=True,
            confirm_account="701364614078",
            bronze_namespace="airlake_it2_fixture_bronze",
            silver_namespace="airlake_it2_fixture_silver",
            gold_namespace="airlake_it2_fixture_gold",
        )

    def prepare(self, path=None, config=None, topic=None, broker="localhost:19092"):
        return prepare_cloud_checkpoint(
            config or self.config,
            str(path or self.path),
            topic or "airlake_it2_" + RUN,
            broker,
        )

    def test_first_use_and_compatible_rerun_preserve_state(self):
        self.assertEqual(str(self.path), self.prepare())
        before = self.marker.read_bytes()
        (self.path / "metadata").write_text("opaque fixture checkpoint metadata")
        self.assertEqual(str(self.path), self.prepare())
        self.assertEqual(before, self.marker.read_bytes())
        self.assertEqual(
            "opaque fixture checkpoint metadata", (self.path / "metadata").read_text()
        )

    def test_outside_remote_and_parent_escape_paths_are_rejected(self):
        for path in (
            self.root.parent / "outside",
            self.root / "../outside",
            "s3://fixture/checkpoint",
        ):
            with self.subTest(path=str(path)), self.assertRaises(ValueError):
                self.prepare(path=path)
        self.assertFalse(self.marker.exists())

    def test_checkpoint_symlink_escape_is_rejected(self):
        outside = self.root.parent / "outside"
        outside.mkdir()
        self.path.symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "working directory"):
            self.prepare()
        self.assertEqual([], list(outside.iterdir()))

    def test_marker_and_nested_symlinks_are_rejected(self):
        outside = self.root.parent / "outside"
        outside.mkdir()
        self.marker.symlink_to(outside / "binding.json")
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            self.prepare()
        self.marker.unlink()
        self.prepare()
        (self.path / "offsets").symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.prepare()

    def test_unknown_checkpoint_is_not_adopted(self):
        self.path.mkdir()
        (self.path / "offsets").write_text("existing offsets")
        with self.assertRaisesRegex(ValueError, "no verified binding"):
            self.prepare()
        self.assertEqual("existing offsets", (self.path / "offsets").read_text())

    def test_changed_query_target_or_broker_rejects_without_rewriting(self):
        self.prepare()
        before = self.marker.read_bytes()
        for kwargs in (
            {"config": replace(self.config, bronze_table="other")},
            {"broker": "localhost:29092"},
            {"topic": "airlake_it2_wrong"},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.prepare(**kwargs)
            self.assertEqual(before, self.marker.read_bytes())

    def test_missing_bound_checkpoint_does_not_reset_offsets(self):
        self.prepare()
        self.path.rmdir()
        with self.assertRaisesRegex(ValueError, "offset reset"):
            self.prepare()
        self.assertFalse(self.path.exists())

    def test_malformed_binding_is_not_overwritten(self):
        self.prepare()
        self.marker.write_text("invalid json")
        with self.assertRaisesRegex(ValueError, "incompatible"):
            self.prepare()
        self.assertEqual("invalid json", self.marker.read_text())

    def test_confirmation_and_workdir_are_required(self):
        with self.assertRaisesRegex(ValueError, "confirmation"):
            self.prepare(config=replace(self.config, allow_cloud_write=False))
        with (
            contextlib.chdir(self.root.parent),
            self.assertRaisesRegex(ValueError, "working directory"),
        ):
            self.prepare()

    def test_kafka_checksums_match_actual_accepted_local_set(self):
        evidence = json.loads(
            (REPOSITORY / "tests/evidence/real_openaq_2026-11-06.json").read_text()
        )
        self.assertEqual(5, len(KAFKA_JARS))
        for name, (digest, _) in KAFKA_JARS.items():
            self.assertEqual(evidence["jars_sha256"][name], digest)


if __name__ == "__main__":
    unittest.main()
