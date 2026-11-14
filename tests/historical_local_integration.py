"""Explicit local-only archive acceptance, fresh process and temporary Iceberg catalog."""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def worker(work, source, jar):
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    from final_demo.prepare import JARS
    from historical.glue_job import execute

    work = Path(work).resolve()
    if (
        Path.cwd() != work
        or work.is_relative_to(ROOT)
        or not work.name.startswith("airlake-history-local-")
    ):
        raise ValueError("Isolated cwd required")
    if (
        SparkContext._active_spark_context is not None
        or SparkSession.getActiveSession() is not None
    ):
        raise ValueError("Fresh Spark required")
    if hashlib.sha256(Path(jar).read_bytes()).hexdigest() != JARS["runtime.jar"]:
        raise ValueError("Runtime hash mismatch")
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("AirLake-Historical-Isolated")
        .config("spark.jars", jar)
        .config("spark.jars.packages", "")
        .config("spark.ui.enabled", "false")
        .config("spark.eventLog.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.warehouse.dir", str(work / "session-warehouse"))
        .config("spark.local.dir", str(work / "spill"))
        .config("spark.hadoop.hadoop.tmp.dir", str(work / "hadoop"))
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.defaultCatalog", "history")
        .config("spark.sql.catalog.history", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.history.type", "hadoop")
        .config("spark.sql.catalog.history.warehouse", (work / "warehouse").as_uri())
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config(
            "spark.sql.catalog.local.warehouse", (work / "fallback-warehouse").as_uri()
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        audit = json.loads((source / "audit.json").read_text())
        raw = (source / "records.jsonl").read_bytes()
        if hashlib.sha256(raw).hexdigest() != audit["records_sha256"]:
            raise ValueError("Frozen local input changed")
        export, evidence = execute(
            spark,
            [json.loads(line) for line in raw.splitlines()],
            audit,
            "airlake_history_20261114_7a6e91c2",
            execution="LOCAL_ISOLATED_SPARK",
        )
        (work / "export.json").write_text(
            json.dumps(export, sort_keys=True, allow_nan=False)
        )
        (work / "evidence.json").write_text(
            json.dumps(evidence, sort_keys=True, allow_nan=False)
        )
        print(
            json.dumps(
                {
                    "status": "PASS_LOCAL_REAL_HISTORICAL_TRANSFORM_NOT_AWS",
                    "source_records": len(raw.splitlines()),
                    "hourly_facts": len(export["facts"]),
                    "daily_rows": len(export["daily"]),
                    "dimensions": len(export["dimensions"]),
                    "all_six_snapshots_unchanged": evidence["snapshots_before"]
                    == evidence["snapshots_after"],
                }
            ),
            flush=True,
        )
    finally:
        spark.stop()


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--source", type=Path, required=True)
    a.add_argument("--runtime-jar", type=Path, required=True)
    a.add_argument("--execute-local", action="store_true")
    a.add_argument("--worker-root", type=Path)
    x = a.parse_args()
    if not x.execute_local:
        a.exit(1, "Explicit --execute-local required; no Spark started\n")
    if x.worker_root:
        worker(x.worker_root, x.source, x.runtime_jar)
        return
    if any(k.startswith(("SPARK_", "PYSPARK_", "AWS_", "HADOOP_")) for k in os.environ):
        raise ValueError("Use the documented clean child environment")
    work = Path(tempfile.mkdtemp(prefix="airlake-history-local-", dir="/private/tmp"))
    work.chmod(0o700)
    env = {
        k: os.environ[k]
        for k in ("PATH", "JAVA_HOME", "HOME", "TMPDIR")
        if k in os.environ
    }
    env.update(
        PYTHONPATH=str(ROOT),
        SPARK_LOCAL_IP="127.0.0.1",
        PYSPARK_PYTHON=sys.executable,
        PYSPARK_DRIVER_PYTHON=sys.executable,
        TZ="UTC",
        SPARK_CONF_DIR=str(work / "conf"),
        HADOOP_CONF_DIR=str(work / "hadoop-conf"),
    )
    (work / "conf").mkdir()
    (work / "hadoop-conf").mkdir()
    print("LOCAL_EVIDENCE_DIRECTORY=" + str(work), flush=True)
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            str(Path(__file__).resolve()),
            "--source",
            str(x.source.resolve()),
            "--runtime-jar",
            str(x.runtime_jar.resolve()),
            "--execute-local",
            "--worker-root",
            str(work),
        ],
        cwd=work,
        env=env,
        check=False,
    )
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
