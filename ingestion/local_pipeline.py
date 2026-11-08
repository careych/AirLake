"""Run unchanged Bronze/Silver/Gold against one retained isolated live-ingest topic."""

import argparse
import fcntl
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--child", action="store_true")
    a = p.parse_args()
    if a.root.is_symlink():
        raise ValueError("Unsafe ingestion root symlink")
    root = a.root.resolve()
    if (
        root.is_symlink()
        or not root.name.startswith("airlake-live-")
        or any((v / ".git").exists() for v in (root, *root.parents))
    ):
        raise ValueError("External ingestion root required")
    binding = json.loads((root / "topic-binding.json").read_text())
    if binding["broker"] not in ("localhost:19092", "127.0.0.1:19092") or not binding[
        "topic"
    ].startswith("airlake_live_"):
        raise ValueError("Unsafe Kafka scope")
    if not a.execute:
        print(
            json.dumps(
                {
                    "mode": "PLAN ONLY",
                    "topic": binding["topic"],
                    "catalog": "live",
                    "cloud": False,
                }
            )
        )
        return
    lake = root / "lakehouse"
    if lake.is_symlink():
        raise ValueError("Unsafe lakehouse path")
    lake.mkdir(exist_ok=True)
    if not a.child:
        lock = (root / "ingestion.lock").open("a+")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock.close()
            raise ValueError(
                "Stop ingestion before starting the bounded pipeline"
            ) from None
        env = {
            k: v
            for k, v in os.environ.items()
            if k
            in (
                "PATH",
                "JAVA_HOME",
                "LANG",
                "LC_ALL",
                "TMPDIR",
                "USER",
                "LOGNAME",
                "SYSTEMROOT",
            )
        }
        env.update(
            PYTHONPATH=str(ROOT),
            PYTHONDONTWRITEBYTECODE="1",
            PYSPARK_PYTHON=sys.executable,
            SPARK_LOCAL_IP="127.0.0.1",
            AWS_EC2_METADATA_DISABLED="true",
            TZ="UTC",
            AIRLAKE_LIVE_CHILD_ROOT=str(lake),
        )
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "ingestion.local_pipeline",
                "--root",
                str(root),
                "--execute",
                "--child",
            ],
            cwd=lake,
            env=env,
            check=False,
        )
        raise SystemExit(result.returncode)
    from scripts import bronze_local, gold_batch, silver_batch
    from scripts.pipeline_config import KAFKA_JARS, CatalogConfig
    from scripts.real_batch import table_state

    cache = Path.home() / ".ivy2/jars"
    jars = {
        cache
        / "org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.12.0.jar": "8dd8c90e54ed1a20f2461309d047a255ee56382eb7c0a46162b27a4936fd86dc"
    }
    jars.update({cache / k: v[0] for k, v in KAFKA_JARS.items()})
    for path, digest in jars.items():
        if (
            not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != digest
        ):
            raise ValueError("Missing/unverified cached JAR; no automatic downloads")
    config = CatalogConfig(catalog="live", warehouse=(lake / "warehouse").as_uri())
    identity = dict(
        binding,
        bronze_source_sha256=hashlib.sha256(
            (ROOT / "scripts/bronze_local.py").read_bytes()
        ).hexdigest(),
        tables=config.tables(),
    )
    marker = lake / "pipeline-binding.json"
    if marker.exists():
        if json.loads(marker.read_text()) != identity:
            raise ValueError("Existing checkpoint/query binding differs; do not reset")
    elif any(lake.iterdir()):
        raise ValueError("Unowned existing runtime files")
    else:
        marker.write_text(json.dumps(identity, sort_keys=True))
    if any(path.is_symlink() for path in lake.rglob("*")):
        raise ValueError("Runtime symlinks are unsafe")
    settings = {
        "spark.master": "local[1]",
        "spark.jars": ",".join(map(str, jars)),
        "spark.jars.packages": "",
        "spark.jars.ivy": str(lake / "ivy"),
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": (lake / "fallback").as_uri(),
        "spark.sql.catalogImplementation": "in-memory",
        "spark.sql.warehouse.dir": str(lake / "spark-warehouse"),
        "spark.local.dir": str(lake / "spill"),
        "spark.hadoop.hadoop.tmp.dir": str(lake / "hadoop"),
        "spark.hadoop.fs.defaultFS": "file:///",
        "spark.sql.shuffle.partitions": "1",
        "spark.ui.enabled": "false",
        "spark.driver.host": "127.0.0.1",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.driver.extraJavaOptions": f"-Dderby.system.home={lake / 'derby'} -Djava.io.tmpdir={lake / 'tmp'} -Duser.timezone=UTC",
    }
    for name in ["tmp", "spill", "hadoop"]:
        (lake / name).mkdir(exist_ok=True)
    spark = config.create_spark("AirLake-Live-Ingestion-Isolated", extra=settings)
    tables = config.tables()
    config.bind(silver_batch.__dict__)
    config.bind(gold_batch.__dict__)
    try:
        report = {
            "topic": binding["topic"],
            "bronze": bronze_local.run_bronze(
                spark,
                table=tables["BRONZE_TABLE"],
                bootstrap_servers=binding["broker"],
                topic=binding["topic"],
                checkpoint=str(lake / "checkpoint"),
                available_now=True,
            ),
            "silver": silver_batch.run_silver_batch(spark),
            "gold": gold_batch.run_gold_batch(spark),
        }
        before = table_state(spark, tables)
        report["bronze_rerun"] = bronze_local.run_bronze(
            spark,
            table=tables["BRONZE_TABLE"],
            bootstrap_servers=binding["broker"],
            topic=binding["topic"],
            checkpoint=str(lake / "checkpoint"),
            available_now=True,
        )
        silver_batch.run_silver_batch(spark)
        gold_batch.run_gold_batch(spark)
        after = table_state(spark, tables)
        if before != after or report["bronze_rerun"]["input_rows"] != 0:
            raise RuntimeError("Unchanged rerun changed rows/snapshots")
        report.update(
            unchanged_rerun=True,
            snapshot_ids={k: v["snapshots"] for k, v in after.items()},
            counts={k: spark.table(v).count() for k, v in tables.items()},
            hourly_rows=[
                json.loads(v)
                for v in spark.table(tables["HOURLY_TABLE"])
                .orderBy("location_id", "hour_start")
                .toJSON()
                .collect()
            ],
        )
        import collections
        import math
        import sqlite3

        state = sqlite3.connect(f"file:{root / 'cursor.sqlite'}?mode=ro", uri=True)
        source = [
            json.loads(v[0])
            for v in state.execute("SELECT record FROM outbox WHERE sent=1")
        ]
        state.close()
        expected = collections.defaultdict(list)
        from ingestion.openaq import stamp, utc

        for row in source:
            expected[
                (
                    int(row["location_id"]),
                    stamp(utc(row["datetime"]).replace(minute=0, second=0)),
                )
            ].append(float(row["value"]))
        for row in report["hourly_rows"]:
            key = (row["location_id"], stamp(utc(row["hour_start"])))
            values = expected.pop(key)
            assert math.isclose(
                row["pm25_avg"],
                math.fsum(values) / len(values),
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
        assert not expected
        report["independent_utc_mean_check"] = "PASS"
        (root / "pipeline-report.json").write_text(
            json.dumps(report, indent=2, default=str, ensure_ascii=False) + "\n"
        )
        print(
            "PIPELINE_ACCEPTANCE", json.dumps(report, default=str, ensure_ascii=False)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
