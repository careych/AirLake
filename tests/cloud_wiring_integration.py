"""Opt-in local Kafka/checkpoint tests with the exact proposed cloud classpath.

No producer or AWS API calls. The Java identity boundary is mocked explicitly;
all Spark SQL targets a disposable Hadoop catalog, never the Glue catalog.
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

REPOSITORY = Path(__file__).resolve().parents[1]
OPT_IN = "AIRLAKE_RUN_CLOUD_WIRING_TESTS"
RUN = "20261106_f3b3b3e0677d4a6790e4a54faf68e4aa"


def worker(root):
    root = Path(root).resolve()
    if (
        Path.cwd().resolve() != root
        or root.is_relative_to(REPOSITORY)
        or root.name != "airlake-it2-cloud-" + RUN
    ):
        raise ValueError("Offline worker requires its dedicated external directory")
    sys.path.insert(0, str(REPOSITORY))
    from confluent_kafka import Consumer, TopicPartition
    from pyspark.sql import functions as F

    from scripts import bronze_local as bronze
    from scripts import pipeline_config as pipeline

    plan = json.loads(
        (REPOSITORY / "docs/plans/real_openaq_2026-11-06.json").read_text()
    )
    topic = plan["source_kafka_topic"]
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:19092",
            "group.id": "airlake_wiring_readonly",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(topic=topic, timeout=10)
        if len(metadata.topics[topic].partitions) != 1:
            raise ValueError("Expected one accepted source partition")
        if consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10) != (
            0,
            200,
        ):
            raise ValueError("Accepted topic must still contain offsets [0,200)")
    finally:
        consumer.close()

    config = pipeline.CatalogConfig(
        mode="glue",
        catalog="cloudreal",
        warehouse=f"s3://unused-offline-fixture/integration-real/{RUN}/warehouse/",
        account="701364614078",
        allow_cloud_write=True,
        confirm_account="701364614078",
        bronze_namespace="airlake_it2_offline_bronze",
        silver_namespace="airlake_it2_offline_silver",
        gold_namespace="airlake_it2_offline_gold",
    )
    jars = pipeline.verified_cloud_jars()
    extra = {
        "spark.sql.catalog.probe": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.probe.type": "hadoop",
        "spark.sql.catalog.probe.warehouse": (root / "probe-warehouse").as_uri(),
        "spark.driver.host": "127.0.0.1",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": "1",
    }
    # Empty private AWS config/credentials files make accidental credential
    # resolution fail locally as well. Never substitute synthetic credentials.
    with patch.object(pipeline, "verify_java_identity") as identity:
        spark = config.create_spark("AirLake-Offline-Cloud-Wiring", extra=extra)
        identity.assert_called_once_with(spark, config)
    report = {
        "aws_identity": "MOCKED for offline startup only",
        "aws_api_calls": 0,
        "cloud_ddl": 0,
        "producer_calls": 0,
        "topic": topic,
        "explicit_jars": [p.name for p in jars],
        "class_origins": {},
    }
    checkpoint = root / "bronze-checkpoint"
    checkpoint_config = replace(config, catalog="probe")
    sink = checkpoint_config.tables()["BRONZE_TABLE"]

    class WiringTests(unittest.TestCase):
        def test_a_exact_classpath_and_runtime(self):
            jvm = spark.sparkContext._jvm
            self.assertEqual("3.5.9", spark.version)
            self.assertTrue(
                jvm.scala.util.Properties.versionNumberString().startswith("2.12.")
            )
            self.assertTrue(
                jvm.java.lang.System.getProperty("java.version").startswith("17.")
            )
            self.assertEqual(
                "airlake-spark", jvm.java.lang.System.getProperty("aws.profile")
            )
            self.assertEqual(
                "", spark.sparkContext.getConf().get("spark.jars.packages")
            )
            loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
            names = [
                "org.apache.spark.sql.kafka010.KafkaSourceProvider",
                "org.apache.spark.kafka010.KafkaTokenUtil",
                "org.apache.kafka.clients.consumer.KafkaConsumer",
                "org.apache.commons.pool2.impl.GenericObjectPool",
                "javax.annotation.Nullable",
                "org.apache.iceberg.aws.glue.GlueCatalog",
                "org.apache.iceberg.aws.s3.S3FileIO",
                "software.amazon.awssdk.services.s3.S3Client",
                "software.amazon.awssdk.services.glue.GlueClient",
                "software.amazon.awssdk.services.sts.StsClient",
                "software.amazon.awssdk.auth.credentials.ProcessCredentialsProvider",
                "software.amazon.awssdk.http.apache5.Apache5HttpClient",
            ]
            for name in names:
                cls = loader.loadClass(name)
                location = (
                    cls.getProtectionDomain().getCodeSource().getLocation().toString()
                )
                self.assertTrue(location.endswith(".jar"))
                report["class_origins"][name] = location
            self.assertEqual(
                6, len(jars)
            )  # JSR-305 is already bundled, with identical bytes.

        def test_b_real_kafka_read_and_local_checkpoint_restart(self):
            bronze.prepare_cloud_checkpoint(
                checkpoint_config,
                str(checkpoint),
                "airlake_it2_" + RUN,
                "localhost:19092",
            )
            first = bronze.run_bronze(
                spark,
                table=sink,
                topic=topic,
                checkpoint=str(checkpoint),
                available_now=True,
            )
            self.assertEqual({"input_rows": 200, "output_rows": 200}, first)
            frame = spark.table(sink)
            self.assertEqual(
                200,
                frame.filter(
                    (F.col("location_id") == "4946812")
                    & (F.col("sensors_id") == "13502151")
                    & (F.col("parameter") == "pm25")
                ).count(),
            )
            before = (
                sorted(frame.toJSON().collect()),
                sorted(
                    r.snapshot_id
                    for r in spark.table(sink + ".snapshots")
                    .select("snapshot_id")
                    .collect()
                ),
            )
            marker = (root / "bronze-checkpoint.binding.json").read_bytes()
            bronze.prepare_cloud_checkpoint(
                checkpoint_config,
                str(checkpoint),
                "airlake_it2_" + RUN,
                "localhost:19092",
            )
            second = bronze.run_bronze(
                spark,
                table=sink,
                topic=topic,
                checkpoint=str(checkpoint),
                available_now=True,
            )
            self.assertEqual({"input_rows": 0, "output_rows": 200}, second)
            after = (
                sorted(spark.table(sink).toJSON().collect()),
                sorted(
                    r.snapshot_id
                    for r in spark.table(sink + ".snapshots")
                    .select("snapshot_id")
                    .collect()
                ),
            )
            self.assertEqual(before, after)
            self.assertEqual(
                marker, (root / "bronze-checkpoint.binding.json").read_bytes()
            )
            report["first_query"] = first
            report["resumed_query"] = second
            report["retained_local_snapshots"] = after[1]
            report["checkpoint_metadata_present"] = (checkpoint / "metadata").is_file()
            self.assertTrue(report["checkpoint_metadata_present"])

        def test_c_incompatible_binding_keeps_checkpoint_files(self):
            before = {
                str(p.relative_to(checkpoint)): p.read_bytes()
                for p in checkpoint.rglob("*")
                if p.is_file()
            }
            with self.assertRaisesRegex(ValueError, "incompatible"):
                bronze.prepare_cloud_checkpoint(
                    replace(checkpoint_config, bronze_table="different"),
                    str(checkpoint),
                    "airlake_it2_" + RUN,
                    "localhost:19092",
                )
            self.assertEqual(
                before,
                {
                    str(p.relative_to(checkpoint)): p.read_bytes()
                    for p in checkpoint.rglob("*")
                    if p.is_file()
                },
            )

    try:
        result = unittest.TextTestRunner(verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(WiringTests)
        )
        report.update(
            {
                "tests_run": result.testsRun,
                "failures": len(result.failures),
                "errors": len(result.errors),
                "skipped": len(result.skipped),
                "status": "PASS"
                if result.wasSuccessful() and not result.skipped
                else "FAIL",
            }
        )
        (root / "offline-result.json").write_text(json.dumps(report, indent=2) + "\n")
        return 0 if report["status"] == "PASS" else 1
    finally:
        spark.stop()


def main():
    if os.environ.get(OPT_IN) != "1":
        raise SystemExit("Offline Spark tests require " + OPT_IN + "=1")
    if len(sys.argv) == 3 and sys.argv[1] == "--worker":
        return worker(sys.argv[2])
    if len(sys.argv) != 1:
        raise SystemExit("Unexpected test arguments")
    parent = Path(tempfile.mkdtemp(prefix="airlake-wiring-runtime-")).resolve()
    if parent.is_relative_to(REPOSITORY):
        raise ValueError("Test resources must be outside repository")
    root = parent / ("airlake-it2-cloud-" + RUN)
    root.mkdir()
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
            "AIRLAKE_ICEBERG_RUNTIME_JAR",
            "AIRLAKE_ICEBERG_AWS_BUNDLE_JAR",
        )
        if k in os.environ
    }
    env.update(
        {
            OPT_IN: "1",
            "AWS_CONFIG_FILE": str(root / "aws-config"),
            "AWS_SHARED_CREDENTIALS_FILE": str(root / "aws-credentials"),
            "AWS_EC2_METADATA_DISABLED": "true",
            "SPARK_LOCAL_IP": "127.0.0.1",
            "PYSPARK_PYTHON": sys.executable,
            "PYSPARK_DRIVER_PYTHON": sys.executable,
            "PYTHONDONTWRITEBYTECODE": "1",
            "TZ": "UTC",
        }
    )
    print("RETAINED_OFFLINE_TEST_ROOT=" + str(root), flush=True)
    with (root / "offline.log").open("w") as log:
        return subprocess.run(
            [
                sys.executable,
                "-B",
                str(Path(__file__).resolve()),
                "--worker",
                str(root),
            ],
            cwd=root,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
            timeout=240,
        ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
