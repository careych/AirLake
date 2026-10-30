"""Deliberate Iceberg write/read check in a disposable external warehouse."""

import argparse
import os
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
JAR_VARIABLE = "AIRLAKE_ICEBERG_RUNTIME_JAR"
JAR_NAME = "iceberg-spark-runtime-3.5_2.12-1.12.0.jar"


def _worker(root: Path, jar: Path) -> int:
    if (
        root.is_relative_to(REPOSITORY)
        or Path.cwd().resolve() != root
        or not root.name.startswith("airlake-iceberg-smoke-")
    ):
        raise RuntimeError("Smoke worker requires its external temporary directory")

    from pyspark import SparkContext, __version__
    from pyspark.sql import SparkSession

    if __version__ != "3.5.9":
        raise RuntimeError("The smoke check requires PySpark 3.5.9")
    if (
        SparkContext._active_spark_context is not None
        or SparkSession.getActiveSession()
    ):
        raise RuntimeError("Refusing to reuse a Spark context or session")

    catalog = "smoke_" + uuid.uuid4().hex
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("AirLake-Isolated-Iceberg-Smoke")
        .config("spark.jars", str(jar))
        .config("spark.jars.packages", "")
        .config("spark.jars.ivy", str(root / "ivy"))
        .config("spark.ui.enabled", "false")
        .config("spark.eventLog.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", (root / "spark-warehouse").as_uri())
        .config("spark.local.dir", str(root / "spill"))
        .config("spark.hadoop.hadoop.tmp.dir", str(root / "hadoop"))
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", (root / "warehouse").as_uri())
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", (root / "fallback").as_uri())
        .getOrCreate()
    )
    try:
        for key, expected in (
            (f"spark.sql.catalog.{catalog}.warehouse", (root / "warehouse").as_uri()),
            ("spark.sql.catalog.local.warehouse", (root / "fallback").as_uri()),
            ("spark.sql.catalogImplementation", "in-memory"),
        ):
            if spark.conf.get(key) != expected:
                raise RuntimeError(f"Unexpected smoke isolation setting: {key}")
        if spark.sparkContext.master != "local[1]":
            raise RuntimeError("Smoke check requires a local Spark master")
        print(f"Temporary smoke warehouse: {(root / 'warehouse').as_uri()}", flush=True)
        if (
            spark.sparkContext._jvm.java.lang.System.getProperty(
                "java.specification.version"
            )
            != "17"
        ):
            raise RuntimeError("The smoke check requires Java 17")
        spark.sparkContext.setLogLevel("ERROR")
        spark.sql(f"CREATE NAMESPACE {catalog}.smoke")
        table = f"{catalog}.smoke.dependency_check"
        spark.createDataFrame([("fixture",)], "id STRING").writeTo(table).using(
            "iceberg"
        ).create()
        rows = spark.table(table).collect()
        if len(rows) != 1 or rows[0].id != "fixture":
            raise RuntimeError("Iceberg smoke readback did not match the fixture")
        print("PASS: one fixture row written/read in a disposable Iceberg warehouse.")
        return 0
    finally:
        spark.stop()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run",
        action="store_true",
        help="Create and read a disposable table outside the repository, then remove it.",
    )
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if not args.run:
        parser.error("Pass --run to opt into the isolated Iceberg write/read check")
    jar = Path(os.environ.get(JAR_VARIABLE, "")).expanduser().resolve()
    if not jar.is_file() or jar.name != JAR_NAME:
        parser.error(f"{JAR_VARIABLE} must name a preinstalled {JAR_NAME}")
    if not os.environ.get("JAVA_HOME"):
        parser.error("Set JAVA_HOME to a preinstalled Java 17 JDK")
    if args.worker is not None:
        return _worker(args.worker.resolve(), jar)

    if Path(tempfile.gettempdir()).resolve().is_relative_to(REPOSITORY):
        raise RuntimeError("Temporary base directory must be outside the repository")
    with tempfile.TemporaryDirectory(prefix="airlake-iceberg-smoke-") as temporary:
        root = Path(temporary).resolve()
        if root.is_relative_to(REPOSITORY):
            raise RuntimeError("Temporary directory must be outside the repository")
        (root / "home").mkdir()
        (root / "conf").mkdir()
        env = {
            key: os.environ[key]
            for key in (
                "PATH",
                "JAVA_HOME",
                "LANG",
                "LC_ALL",
                "LC_CTYPE",
                "TZ",
                "SYSTEMROOT",
                "WINDIR",
            )
            if key in os.environ
        }
        env.update(
            {
                JAR_VARIABLE: str(jar),
                "HOME": str(root / "home"),
                "SPARK_CONF_DIR": str(root / "conf"),
                "SPARK_LOCAL_IP": "127.0.0.1",
                "SPARK_LOCAL_HOSTNAME": "localhost",
                "SPARK_LOCAL_DIRS": str(root / "spill"),
                "SPARK_LOG_DIR": str(root / "logs"),
                "TMPDIR": str(root),
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYSPARK_PYTHON": sys.executable,
                "PYSPARK_DRIVER_PYTHON": sys.executable,
                "PYSPARK_SUBMIT_ARGS": "--master local[1] pyspark-shell",
            }
        )
        print(
            "Smoke resources are temporary and removed after the child exits.",
            flush=True,
        )
        return subprocess.run(
            [
                sys.executable,
                "-B",
                str(Path(__file__).resolve()),
                "--run",
                "--worker",
                str(root),
            ],
            cwd=root,
            env=env,
            check=False,
        ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
