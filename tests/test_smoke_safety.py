"""Check smoke import, opt-in, and child isolation without starting Spark."""

import contextlib
import importlib.util
import io
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPOSITORY = Path(__file__).resolve().parents[1]


class SmokeSafetyTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch(
                "socket.socket", side_effect=AssertionError("No sockets in unit tests")
            )
        )
        self.enterContext(patch.dict("sys.modules", {"pyspark": None}))
        spec = importlib.util.spec_from_file_location(
            "smoke_subject", REPOSITORY / "scripts/iceberg_smoke_test.py"
        )
        self.subject = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.subject)
        if Path(tempfile.gettempdir()).resolve().is_relative_to(REPOSITORY):
            raise RuntimeError("Unit fixtures must be outside the repository")
        self.root = Path(
            self.enterContext(tempfile.TemporaryDirectory(prefix="airlake-smoke-unit-"))
        )
        self.jar = self.root / self.subject.JAR_NAME
        self.jar.write_bytes(b"inert fixture; never loaded into a JVM")
        self.enterContext(
            patch.dict(
                os.environ,
                {self.subject.JAR_VARIABLE: str(self.jar), "JAVA_HOME": "fixture-jdk"},
            )
        )
        self.enterContext(contextlib.redirect_stderr(io.StringIO()))
        self.enterContext(contextlib.redirect_stdout(io.StringIO()))

    def test_import_has_no_spark_dependency_or_side_effects(self):
        self.assertTrue(callable(self.subject.main))

    def test_missing_opt_in_never_launches_a_process(self):
        with patch.object(self.subject.subprocess, "run") as runner:
            with self.assertRaises(SystemExit):
                self.subject.main([])
            runner.assert_not_called()

    def test_missing_runtime_jar_never_launches_a_process(self):
        with (
            patch.dict(os.environ, {self.subject.JAR_VARIABLE: ""}),
            patch.object(self.subject.subprocess, "run") as runner,
        ):
            with self.assertRaises(SystemExit):
                self.subject.main(["--run"])
            runner.assert_not_called()

    def test_repository_worker_path_rejects_before_spark_import(self):
        with self.assertRaisesRegex(RuntimeError, "external temporary"):
            self.subject._worker(REPOSITORY, self.jar)

    def test_repository_temp_base_rejects_before_creating_any_directory(self):
        with (
            patch.object(
                self.subject.tempfile,
                "gettempdir",
                return_value=str(REPOSITORY / "warehouse"),
            ),
            patch.object(self.subject.tempfile, "TemporaryDirectory") as temporary,
        ):
            with self.assertRaisesRegex(RuntimeError, "base directory must be outside"):
                self.subject.main(["--run"])
            temporary.assert_not_called()

    def test_child_has_private_cwd_home_config_and_no_inherited_credentials(self):
        inherited = {
            key: "fixture-only"
            for key in (
                "AWS_SECRET_ACCESS_KEY",
                "AWS_PROFILE",
                "HADOOP_CONF_DIR",
                "SPARK_MASTER",
                "SPARK_CONF_DIR",
                "JAVA_TOOL_OPTIONS",
                "PYSPARK_GATEWAY_PORT",
            )
        }

        def launch(command, *, cwd, env, check):
            self.assertFalse(cwd.is_relative_to(REPOSITORY))
            self.assertTrue(cwd.name.startswith("airlake-iceberg-smoke-"))
            self.assertTrue(cwd.is_dir())
            self.assertEqual(str(cwd), command[-1])
            self.assertEqual(str(cwd / "home"), env["HOME"])
            self.assertEqual(str(cwd / "conf"), env["SPARK_CONF_DIR"])
            self.assertEqual(
                "--master local[1] pyspark-shell", env["PYSPARK_SUBMIT_ARGS"]
            )
            self.assertFalse(check)
            for key in inherited.keys() - {"SPARK_CONF_DIR"}:
                self.assertNotIn(key, env)
            return SimpleNamespace(returncode=0)

        with (
            patch.dict(os.environ, inherited),
            patch.object(self.subject.subprocess, "run", side_effect=launch) as runner,
        ):
            self.assertEqual(0, self.subject.main(["--run"]))
            runner.assert_called_once()

    def test_child_failure_exit_code_is_preserved(self):
        with patch.object(
            self.subject.subprocess, "run", return_value=SimpleNamespace(returncode=7)
        ):
            self.assertEqual(7, self.subject.main(["--run"]))


if __name__ == "__main__":
    unittest.main()
