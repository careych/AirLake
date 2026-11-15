"""Live update release tooling; no AWS."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import release_live_update as rel


class LiveUpdateTests(unittest.TestCase):
    def inputs(self, root):
        out = {}
        for k in (
            "export",
            "audit",
            "metadata",
            "latest",
            "source_metadata",
            "evidence",
            "acceptance",
            "credentials",
        ):
            (root / k).write_text("{}")
            out[k] = str(root / k)
        return out

    def test_rollback_copy_must_be_the_deployed_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "old.zip").write_bytes(b"not the deployed code")
            with self.assertRaises(ValueError):
                rel.prepare(root / "p", self.inputs(root), None, root / "old.zip")

    def test_plan_detects_drift(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old.zip"
            old.write_bytes(b"x")
            originals = (
                rel.DEPLOYED_REFRESH_CODE,
                rel.DEPLOYED_BOT_CODE,
                rel.PREVIOUS_BOT_ZIP,
            )
            rel.DEPLOYED_REFRESH_CODE = rel.DEPLOYED_BOT_CODE = rel.code_sha(b"x")
            rel.PREVIOUS_BOT_ZIP = str(old)
            try:
                digest = rel.prepare(
                    root / "p", self.inputs(root), "https://t.me/AirLakeHanoiBot", old
                )
            finally:
                (
                    rel.DEPLOYED_REFRESH_CODE,
                    rel.DEPLOYED_BOT_CODE,
                    rel.PREVIOUS_BOT_ZIP,
                ) = originals
            plan = rel.load(root / "p", digest)
            self.assertEqual(plan["bot_url"], "https://t.me/AirLakeHanoiBot")
            (root / "latest").write_text('{"changed": 1}')
            with self.assertRaises(ValueError):
                rel.load(root / "p", digest)


if __name__ == "__main__":
    unittest.main()
