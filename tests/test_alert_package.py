"""Offline packaging guards use tiny synthetic wheels, never SDK downloads."""

import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from scripts.build_alert_lambda import build, dependency_closure, wheel_files


class AlertPackageTests(unittest.TestCase):
    def wheel(self, entries):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            for name, data in entries.items():
                archive.writestr(name, data)
        data = buffer.getvalue()
        directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        path = directory / "fixture-1-py3-none-any.whl"
        path.write_bytes(data)
        package = {
            "wheels": [
                {
                    "url": "https://example.invalid/" + path.name,
                    "hash": "sha256:" + hashlib.sha256(data).hexdigest(),
                }
            ]
        }
        return package, directory

    def test_wrong_hash_and_archive_traversal_reject(self):
        package, directory = self.wheel({"module.py": "pass"})
        package["wheels"][0]["hash"] = "sha256:wrong"
        with self.assertRaises(ValueError):
            wheel_files(package, directory)
        for path in (
            "../escape.py",
            "/absolute.py",
            "folder\\escape.py",
            "module.so",
            "pkg.data/data/required.dat",
        ):
            package, directory = self.wheel({path: "pass"})
            with self.subTest(path=path), self.assertRaises(ValueError):
                wheel_files(package, directory)

    def test_optional_cli_is_excluded_but_runtime_and_license_are_retained(self):
        package, directory = self.wheel(
            {
                "module.py": "pass",
                "fixture.data/scripts/cli.py": "pass",
                "fixture.dist-info/licenses/LICENSE.txt": "fixture license",
            }
        )
        files = wheel_files(package, directory)
        self.assertEqual(
            {"module.py", "fixture.dist-info/licenses/LICENSE.txt"}, set(files)
        )

    def test_dependency_closure_deduplicates_shared_packages(self):
        packages = {
            "boto3": {"dependencies": [{"name": "first"}, {"name": "second"}]},
            "first": {"dependencies": [{"name": "second"}]},
            "second": {},
        }
        self.assertEqual(["boto3", "first", "second"], dependency_closure(packages))

    def test_build_is_repeatable_scoped_and_refuses_overwrite(self):
        package, directory = self.wheel({"boto3/__init__.py": '__version__="fixture"'})
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        (root / "alerts").mkdir()
        (root / "bot_lambda/src").mkdir(parents=True)
        (root / "alerts/__init__.py").write_text("")
        (root / "bot_lambda/src/lambda_function.py").write_text(
            "def lambda_handler(e,c): return {}"
        )
        wheel = package["wheels"][0]
        (root / "uv.lock").write_text(
            '[[package]]\nname="boto3"\nversion="fixture"\nwheels=[{url="'
            + wheel["url"]
            + '", hash="'
            + wheel["hash"]
            + '"}]\n'
        )
        outside = Path(self.enterContext(tempfile.TemporaryDirectory()))
        first = build(directory, outside / "first", root=root)
        second = build(directory, outside / "second", root=root)
        self.assertEqual(first, second)
        self.assertEqual("NOT DEPLOYED", first["deployment_status"])
        self.assertEqual("fixture", first["dependencies"]["boto3"])
        self.assertEqual(
            first,
            json.loads((outside / "first/alert-lambda-manifest.json").read_text()),
        )
        with self.assertRaises(ValueError):
            build(directory, outside / "first", root=root)
        with self.assertRaises(ValueError):
            build(directory, root / "output", root=root)
