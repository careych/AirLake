"""Bounded historical replay through the existing producer and real pipeline.

Default preparation is offline. --execute-local uses one new local Kafka topic
and a retained temporary warehouse/checkpoint; cloud execution is separate.
"""

import argparse
import collections
import csv
import gzip
import hashlib
import json
import math
import os
import re
import signal
import subprocess
import sys
import tempfile
import uuid
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
FIELDS = [
    "location_id",
    "sensors_id",
    "location",
    "datetime",
    "lat",
    "lon",
    "parameter",
    "units",
    "value",
]
SENSOR = "13502151"


def valid_record(row):
    if any(not isinstance(row.get(k), str) or not row[k].strip() for k in FIELDS):
        raise ValueError("missing_required_field")
    if row["location_id"] != "4946812" or row["sensors_id"] != SENSOR:
        raise ValueError("unsupported_station_or_sensor")
    if row["parameter"] != "pm25" or row["units"] != "µg/m³":
        raise ValueError("incompatible_parameter_or_units")
    value, lat, lon = (float(row[k]) for k in ("value", "lat", "lon"))
    if not all(math.isfinite(v) for v in (value, lat, lon)) or value < 0:
        raise ValueError("invalid_numeric_value")
    if not -90 <= lat <= 90 or not -180 <= lon <= 180:
        raise ValueError("invalid_coordinates")
    if not re.fullmatch(
        r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:Z|[+-]\d\d:\d\d)", row["datetime"]
    ):
        raise ValueError("incompatible_timestamp")
    when = datetime.fromisoformat(row["datetime"])
    if when.utcoffset() is None:
        raise ValueError("missing_timezone")
    return when.astimezone(UTC)


def select_records(source_root, limit):
    if not 1 <= limit <= 1000:
        raise ValueError("Row cap must be between 1 and 1000")
    rows, provenance, sources = [], [], []
    rejected = collections.Counter()
    scanned = non_pm25 = 0
    metadata = None
    keys = {}
    for path in sorted((source_root / "4946812").rglob("*.csv.gz")):
        relative = (
            str(path.relative_to(REPOSITORY))
            if path.is_relative_to(REPOSITORY)
            else path.name
        )
        sources.append(
            {"path": relative, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        )
        with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames != FIELDS:
                raise ValueError(
                    "Source columns differ from the existing Bronze contract"
                )
            for number, row in enumerate(reader, 1):
                scanned += 1
                if row.get("parameter") != "pm25":
                    non_pm25 += 1
                    continue
                try:
                    valid_record(row)
                    station = tuple(row[k] for k in ("location", "lat", "lon"))
                    if metadata is not None and station != metadata:
                        raise ValueError("conflicting_source_metadata")
                    key = tuple(
                        row[k] for k in ("location_id", "sensors_id", "datetime")
                    )
                    if key in keys and keys[key] != row:
                        raise ValueError("conflicting_measurement_key")
                except (ValueError, TypeError, OverflowError) as exc:
                    rejected[
                        str(exc)
                        if re.fullmatch(r"[a-z_]+", str(exc))
                        else "invalid_source_record"
                    ] += 1
                    continue
                metadata = station
                keys[key] = row
                rows.append(row)
                provenance.append({"source": relative, "data_row": number})
                if len(rows) == limit:
                    break
        if len(rows) == limit:
            break
    if not rows:
        raise ValueError("No compatible real PM2.5 measurements")
    payload = "\n".join(
        json.dumps(row, sort_keys=True, ensure_ascii=False) for row in rows
    )
    return rows, {
        "rule": "Lexicographic station-4946812 file path, physical CSV data-row order; first compatible PM2.5 rows, no synthesis",
        "limit": limit,
        "source_files": sources,
        "scanned_rows": scanned,
        "excluded_non_pm25": non_pm25,
        "rejected_pm25_by_reason": dict(rejected),
        "selected_rows": len(rows),
        "selected_sha256": hashlib.sha256(payload.encode()).hexdigest(),
        "row_provenance": provenance,
    }


def save(root, report):
    path = root / "manifest.json"
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    temporary.replace(path)


def table_state(spark, tables):
    result = {}
    for name in tables.values():
        frame = spark.table(name)
        result[name] = {
            "rows": sorted(frame.toJSON().collect()),
            "snapshots": sorted(
                r.snapshot_id
                for r in spark.table(name + ".snapshots")
                .select("snapshot_id")
                .collect()
            ),
        }
    return result


def execute_local(root, rows, report):
    # This function is reached only inside the fresh, external worker process.
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic

    sys.path.insert(0, str(REPOSITORY))
    from pyspark.sql import functions as F

    from scripts import bronze_local, gold_batch, silver_batch
    from scripts.pipeline_config import CatalogConfig

    topic = report["topic"]
    broker = "localhost:19092"
    admin = AdminClient({"bootstrap.servers": broker})
    if topic in admin.list_topics(timeout=10).topics:
        raise ValueError("Isolated topic already exists; refusing replay")
    report["operations"].append("create_new_local_topic_attempted")
    save(root, report)
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[
        topic
    ].result(20)
    replay = root / "replay"
    fixture = replay / "data/raw/4946812/year=2025/month=07/selected.csv.gz"
    fixture.parent.mkdir(parents=True)
    with gzip.open(fixture, "wt", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    produced = subprocess.run(
        [
            sys.executable,
            "-B",
            str(REPOSITORY / "real_producer.py"),
            "--topic",
            topic,
            "--max-rows",
            str(len(rows)),
        ],
        cwd=replay,
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    (root / "producer.log").write_text(produced.stdout + produced.stderr)
    if produced.returncode:
        raise RuntimeError("Existing Kafka producer failed; inspect producer.log")
    report["producer"] = {
        key: int(re.search(label + r": (\d+)", produced.stdout)[1])
        for key, label in (
            ("submitted", "Rows submitted"),
            ("delivered", "Delivery successes"),
            ("failed", "Delivery failures"),
            ("queued", "Messages still queued after flush"),
        )
    }
    assert report["producer"] == {
        "submitted": len(rows),
        "delivered": len(rows),
        "failed": 0,
        "queued": 0,
    }
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": topic + "_audit",
            "enable.auto.commit": False,
        }
    )
    try:
        partition = TopicPartition(topic, 0)
        low, high = consumer.get_watermark_offsets(partition, timeout=10)
        assert (low, high) == (0, len(rows))
        consumer.assign([TopicPartition(topic, 0, 0)])
        messages = []
        while len(messages) < len(rows):
            msg = consumer.poll(20)
            if msg is None or msg.error():
                raise RuntimeError("Kafka audit did not read the expected records")
            messages.append(msg)
        assert [json.loads(msg.value()) for msg in messages] == rows
        assert all(
            msg.key().decode() == row["location_id"] + ":" + row["sensors_id"]
            for msg, row in zip(messages, rows)
        )
        report["kafka"] = {
            "partition": 0,
            "low_offset": low,
            "high_offset": high,
            "verified_messages": len(messages),
        }
    finally:
        consumer.close()
    save(root, report)

    config = CatalogConfig(
        catalog="it2_" + report["run_id"], warehouse=(root / "warehouse").as_uri()
    )
    tables = config.tables()
    config.bind(silver_batch.__dict__)
    config.bind(gold_batch.__dict__)
    cache = Path.home() / ".ivy2/jars"
    names = [
        "org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.12.0.jar",
        "org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.9.jar",
        "org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.9.jar",
        "org.apache.kafka_kafka-clients-3.4.1.jar",
        "org.apache.commons_commons-pool2-2.11.1.jar",
        "com.google.code.findbugs_jsr305-3.0.0.jar",
    ]
    jars = [cache / name for name in names]
    if any(not p.is_file() for p in jars):
        raise RuntimeError("Required cached JAR missing; no downloads are enabled")
    if (
        hashlib.sha256(jars[0].read_bytes()).hexdigest()
        != "8dd8c90e54ed1a20f2461309d047a255ee56382eb7c0a46162b27a4936fd86dc"
    ):
        raise RuntimeError("Iceberg runtime checksum mismatch")
    report["jars_sha256"] = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in jars
    }
    settings = {
        "spark.master": "local[1]",
        "spark.jars": ",".join(map(str, jars)),
        "spark.jars.packages": "",
        "spark.jars.ivy": str(root / "ivy"),
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": (root / "fallback").as_uri(),
        "spark.sql.catalogImplementation": "in-memory",
        "spark.sql.warehouse.dir": str(root / "spark-warehouse"),
        "spark.local.dir": str(root / "spill"),
        "spark.hadoop.hadoop.tmp.dir": str(root / "hadoop"),
        "spark.hadoop.fs.defaultFS": "file:///",
        "spark.sql.shuffle.partitions": "1",
        "spark.sql.streaming.forceDeleteTempCheckpointLocation": "false",
        "spark.ui.enabled": "false",
        "spark.driver.host": "127.0.0.1",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.driver.extraJavaOptions": f"-Dderby.system.home={root / 'derby'} -Djava.io.tmpdir={root / 'tmp'} -Duser.timezone=UTC",
    }
    spark = config.create_spark("AirLake-Real-OpenAQ-Isolated", extra=settings)
    try:
        if not spark.sparkContext._jvm.java.lang.System.getProperty(
            "java.version"
        ).startswith("17."):
            raise RuntimeError("Java 17 required")
        report["tables"] = tables
        report["bronze"] = bronze_local.run_bronze(
            spark,
            table=tables["BRONZE_TABLE"],
            topic=topic,
            checkpoint=str(root / "checkpoint"),
            available_now=True,
        )
        assert report["bronze"] == {"input_rows": len(rows), "output_rows": len(rows)}
        actual = spark.table(tables["BRONZE_TABLE"]).collect()
        assert collections.Counter(
            tuple(r[k] for k in FIELDS) for r in actual
        ) == collections.Counter(tuple(r[k] for k in FIELDS) for r in rows)
        actual_times = collections.Counter(
            (r.datetime, r.epoch_ms)
            for r in spark.table(tables["BRONZE_TABLE"])
            .select("datetime", F.unix_millis("kafka_ts").alias("epoch_ms"))
            .collect()
        )
        assert actual_times == collections.Counter(
            (row["datetime"], msg.timestamp()[1]) for row, msg in zip(rows, messages)
        )
        report["silver"] = silver_batch.run_silver_batch(spark)
        unique = {
            tuple(row[k] for k in ("location_id", "sensors_id", "datetime")): row
            for row in rows
        }
        expected_silver = {
            (
                int(r["location_id"]),
                int(r["sensors_id"]),
                "pm25",
                float(r["value"]),
                r["units"],
                valid_record(r),
            )
            for r in unique.values()
        }
        actual_silver = {
            (
                r.location_id,
                r.sensors_id,
                r.parameter,
                r.value,
                r.units,
                r.measured_at.replace(tzinfo=UTC),
            )
            for r in spark.table(tables["SILVER_TABLE"]).collect()
        }
        assert actual_silver == expected_silver
        report["gold"] = gold_batch.run_gold_batch(spark)
        hours = collections.defaultdict(list)
        for r in unique.values():
            hours[valid_record(r).replace(minute=0, second=0)].append(float(r["value"]))
        hourly = spark.table(tables["HOURLY_TABLE"]).orderBy("hour_start").collect()
        assert len(hourly) == len(hours)
        for row in hourly:
            values = hours[row.hour_start.replace(tzinfo=UTC)]
            assert math.isclose(
                row.pm25_avg,
                math.fsum(values) / len(values),
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
            lo, hi, c_lo, c_hi = next(
                b for b in gold_batch.BREAKPOINTS if c_lo_hi_match(b, row.pm25_avg)
            )
            expected_aqi = (
                int(
                    Decimal(
                        str((hi - lo) / (c_hi - c_lo) * (row.pm25_avg - c_lo) + lo)
                    ).quantize(Decimal(1), rounding=ROUND_HALF_UP)
                )
                if row.pm25_avg < 500
                else 500
            )
            assert row.aqi == expected_aqi
        dim = spark.table(tables["DIMENSION_TABLE"]).collect()
        assert len(dim) == 1 and (
            dim[0].location_id,
            dim[0].name,
            dim[0].lat,
            dim[0].lon,
            dim[0].station_type,
        ) == (
            4946812,
            rows[0]["location"],
            float(rows[0]["lat"]),
            float(rows[0]["lon"]),
            "reference",
        )
        assert [
            (f.name, f.dataType.simpleString())
            for f in spark.table(tables["HOURLY_TABLE"]).schema.fields
        ] == gold_batch.HOURLY_SCHEMA
        before = table_state(spark, tables)
        report["bronze_rerun"] = bronze_local.run_bronze(
            spark,
            table=tables["BRONZE_TABLE"],
            topic=topic,
            checkpoint=str(root / "checkpoint"),
            available_now=True,
        )
        report["silver_rerun"] = silver_batch.run_silver_batch(spark)
        report["gold_rerun"] = gold_batch.run_gold_batch(spark)
        after = table_state(spark, tables)
        assert before == after
        assert report["bronze_rerun"]["input_rows"] == 0
        report["unchanged_rerun"] = {
            "rows_and_snapshots_equal": True,
            "snapshot_ids": {k: v["snapshots"] for k, v in after.items()},
        }
        report["hourly_rows"] = [
            json.loads(s)
            for s in spark.table(tables["HOURLY_TABLE"])
            .orderBy("hour_start")
            .toJSON()
            .collect()
        ]
        report["dimension_rows"] = [
            json.loads(s)
            for s in spark.table(tables["DIMENSION_TABLE"]).toJSON().collect()
        ]
        report["schemas"] = {
            k: spark.table(v).schema.simpleString() for k, v in tables.items()
        }
        report["status"] = "PASS_REAL_KAFKA_BRONZE_SILVER_GOLD"
        save(root, report)
    finally:
        spark.stop()


def c_lo_hi_match(breakpoint, value):
    return (
        breakpoint[2] <= value < breakpoint[3] or value >= 500 and breakpoint[3] == 500
    )


def main(argv=None):
    if not __debug__:
        raise ValueError("Acceptance execution requires Python assertions enabled")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--execute-local", action="store_true")
    parser.add_argument("--worker-root", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.worker_root:
        root = args.worker_root.resolve()
        if (
            not args.execute_local
            or Path.cwd().resolve() != root
            or root.is_relative_to(REPOSITORY)
            or not root.name.startswith("airlake-real-batch-")
        ):
            raise ValueError(
                "Worker requires the explicit launcher and external working directory"
            )
        report = json.loads((root / "manifest.json").read_text())
        rows = json.loads((root / "selected-rows.json").read_text())
        if (
            not re.fullmatch(r"\d{8}_[a-f0-9]{32}", report["run_id"])
            or report["topic"] != "airlake_it2_" + report["run_id"]
        ):
            raise ValueError("Worker topic must be the exact isolated run identifier")
        expected_rows, expected_selection = select_records(
            REPOSITORY / "data/raw", report["selection"]["limit"]
        )
        if rows != expected_rows or report["selection"] != expected_selection:
            raise ValueError("Frozen replay differs from original source measurements")
        try:
            execute_local(root, rows, report)
        except BaseException as exc:
            report["status"] = "FAILED_OR_UNCERTAIN"
            report["failure_type"] = type(exc).__name__
            save(root, report)
            raise
        return 0
    rows, selection = select_records(REPOSITORY / "data/raw", args.limit)
    base = Path(tempfile.gettempdir()).resolve()
    if base.is_relative_to(REPOSITORY):
        raise ValueError("Output directory must be outside repository")
    root = Path(tempfile.mkdtemp(prefix="airlake-real-batch-", dir=base)).resolve()
    run_id = datetime.now(UTC).strftime("%Y%m%d") + "_" + uuid.uuid4().hex
    report = {
        "run_id": run_id,
        "topic": "airlake_it2_" + run_id,
        "selection": selection,
        "status": "PREPARED_OFFLINE",
        "operations": [],
        "automatic_cleanup": False,
        "source_sha256": {
            name: hashlib.sha256((REPOSITORY / name).read_bytes()).hexdigest()
            for name in (
                "real_producer.py",
                "scripts/real_batch.py",
                "scripts/pipeline_config.py",
                "scripts/bronze_local.py",
                "scripts/silver_batch.py",
                "scripts/gold_batch.py",
            )
        },
    }
    (root / "selected-rows.json").write_text(
        json.dumps(rows, ensure_ascii=False) + "\n"
    )
    save(root, report)
    print("RETAINED_RUN_ROOT=" + str(root), flush=True)
    print(
        json.dumps(
            {
                "topic": report["topic"],
                "selection": {
                    k: v for k, v in selection.items() if k != "row_provenance"
                },
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    if not args.execute_local:
        return 0
    for directory in ("conf", "tmp", "logs", "spill", "hadoop"):
        (root / directory).mkdir()
    for name in ("aws-config", "aws-credentials"):
        (root / name).touch()
    env = {
        k: os.environ[k]
        for k in (
            "PATH",
            "HOME",
            "JAVA_HOME",
            "LANG",
            "LC_ALL",
            "LC_CTYPE",
            "__CF_USER_TEXT_ENCODING",
        )
        if k in os.environ
    }
    env.update(
        {
            "SPARK_CONF_DIR": str(root / "conf"),
            "SPARK_LOCAL_IP": "127.0.0.1",
            "TZ": "UTC",
            "TMPDIR": str(root / "tmp"),
            "PYSPARK_PYTHON": sys.executable,
            "PYSPARK_DRIVER_PYTHON": sys.executable,
            "PYTHONDONTWRITEBYTECODE": "1",
            "AWS_CONFIG_FILE": str(root / "aws-config"),
            "AWS_SHARED_CREDENTIALS_FILE": str(root / "aws-credentials"),
            "AWS_EC2_METADATA_DISABLED": "true",
        }
    )
    with (root / "pipeline.log").open("w") as log:
        child = subprocess.Popen(
            [
                sys.executable,
                "-B",
                str(Path(__file__).resolve()),
                "--execute-local",
                "--worker-root",
                str(root),
            ],
            cwd=root,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            code = child.wait(timeout=600)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
            report = json.loads((root / "manifest.json").read_text())
            report["status"] = "FAILED_OR_UNCERTAIN"
            report["failure_type"] = "parent_deadline_or_interruption"
            save(root, report)
            code = 1
    result = json.loads((root / "manifest.json").read_text())
    print("RESULT=" + result["status"])
    return code


if __name__ == "__main__":
    raise SystemExit(main())
