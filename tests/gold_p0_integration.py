"""Opt-in P0 tests against real Gold transformations and disposable Iceberg tables.

Not a default unittest-discovery module. Launching without explicit opt-in refuses
before importing PySpark. The launcher creates a fresh child process and temp CWD;
only the child imports production code or starts Spark. No Maven downloads.
See tests/README.md for prerequisites, policy coverage, and the execution command.
"""

import importlib.util
import json
import math
import os
import subprocess
import sys
import tempfile
import unittest
import uuid
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

REPOSITORY = Path(__file__).resolve().parents[1]
OPT_IN = "AIRLAKE_RUN_GOLD_P0_INTEGRATION"
JAR_VARIABLE = "AIRLAKE_ICEBERG_RUNTIME_JAR"
STATION = 4946812
OTHER_STATION = 4946813
HOUR = datetime(2026, 9, 30, 17, tzinfo=UTC)
PAST_HOUR = HOUR - timedelta(hours=1)
SILVER_SCHEMA = (
    "location_id INT, sensors_id INT, parameter STRING, value DOUBLE, "
    "units STRING, measured_at TIMESTAMP, ingested_at TIMESTAMP"
)
BRONZE_SCHEMA = (
    "location_id STRING, sensors_id STRING, location STRING, datetime STRING, "
    "lat STRING, lon STRING, parameter STRING, units STRING, value STRING, "
    "kafka_ts TIMESTAMP"
)
DIMENSION_SCHEMA = (
    "location_id INT, name STRING, lat DOUBLE, lon DOUBLE, station_type STRING"
)
HOURLY_SCHEMA = "location_id INT, hour_start TIMESTAMP, pm25_avg DOUBLE, aqi INT"


def _silver_rows(values, station=STATION):
    return [
        (station, 13502151, "pm25", value, "µg/m³", HOUR + timedelta(minutes=i), HOUR)
        for i, value in enumerate(values)
    ]


def _bronze_rows(stations=(STATION,)):
    return [
        (
            str(station),
            "13502151",
            f"Fixture station {station}",
            "2026-10-01T00:00:00+07:00",
            "21.0",
            "105.8",
            "pm25",
            "µg/m³",
            "20",
            HOUR,
        )
        for station in stations
    ]


def _worker(root, jar):
    """Called only by the gated launcher, inside its newly created child."""
    root = Path(root).resolve()
    if Path.cwd().resolve() != root or root.is_relative_to(REPOSITORY):
        raise RuntimeError("Integration worker requires an external temporary CWD")
    if not root.name.startswith("airlake-gold-p0-"):
        raise RuntimeError("Unexpected integration temporary directory")

    # These imports are intentionally unreachable during ordinary discovery.
    from pyspark import SparkContext
    from pyspark import __version__ as spark_version
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    if spark_version != "3.5.9":
        raise RuntimeError("These fixtures require the project's PySpark 3.5.9")
    if SparkContext._active_spark_context is not None:
        raise RuntimeError("Refusing to reuse an active Spark context")
    if SparkSession.getActiveSession() is not None:
        raise RuntimeError("Refusing to reuse an active Spark session")

    spec = importlib.util.spec_from_file_location(
        "airlake_gold_p0_subject", REPOSITORY / "scripts" / "gold_batch.py"
    )
    gold = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gold)
    catalog = "p0_" + uuid.uuid4().hex
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("AirLake-P0-Isolated-Regression")
        .config("spark.jars", str(jar))
        .config("spark.jars.packages", "")
        .config("spark.jars.ivy", str(root / "ivy"))
        .config("spark.ui.enabled", "false")
        .config("spark.eventLog.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.defaultCatalog", catalog)
        .config("spark.sql.warehouse.dir", str(root / "spark-warehouse"))
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
        # Production namespace DDL still names local.silver/local.gold explicitly.
        # Bind that fallback catalog to another disposable directory as well.
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config(
            "spark.sql.catalog.local.warehouse", (root / "fallback-warehouse").as_uri()
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print(
        "P0_ISOLATION="
        + json.dumps(
            {
                "pid": os.getpid(),
                "cwd": str(Path.cwd().resolve()),
                "catalog": catalog,
                "master": spark.sparkContext.master,
                "configuration": {
                    key: spark.conf.get(key)
                    for key in (
                        f"spark.sql.catalog.{catalog}.warehouse",
                        "spark.sql.catalog.local.warehouse",
                        "spark.sql.warehouse.dir",
                        "spark.sql.catalogImplementation",
                        "spark.sql.session.timeZone",
                        "spark.hadoop.fs.defaultFS",
                        "spark.jars",
                        "spark.jars.packages",
                    )
                },
            }
        ),
        flush=True,
    )

    class GoldP0IntegrationTests(unittest.TestCase):
        def setUp(self):
            self.stack = ExitStack()
            self.addCleanup(self.stack.close)
            self.spark = spark
            suffix = uuid.uuid4().hex
            identifiers = {
                "BRONZE_TABLE": f"{catalog}.bronze.measurements_{suffix}",
                "SILVER_TABLE": f"{catalog}.silver.measurements_{suffix}",
                "BREAKPOINT_TABLE": f"{catalog}.silver.breakpoints_{suffix}",
                "DIMENSION_TABLE": f"{catalog}.gold.locations_{suffix}",
                "HOURLY_TABLE": f"{catalog}.gold.hourly_{suffix}",
            }
            self.stack.enter_context(patch.multiple(gold, **identifiers))
            self.stack.enter_context(
                patch.object(
                    gold,
                    "create_spark_session",
                    side_effect=AssertionError(
                        "Production session factory is forbidden"
                    ),
                )
            )
            for namespace in ("bronze", "silver", "gold"):
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
            self._create(gold.BRONZE_TABLE, _bronze_rows(), BRONZE_SCHEMA)
            self._create(gold.SILVER_TABLE, _silver_rows([20.0]), SILVER_SCHEMA)
            gold.ensure_breakpoint_table(spark)
            gold.create_gold_tables(spark)

        def _check_table(self, table):
            self.assertTrue(table.startswith(catalog + "."))
            self.assertEqual(
                (root / "warehouse").as_uri(),
                spark.conf.get(f"spark.sql.catalog.{catalog}.warehouse"),
            )

        def _create(self, table, rows, schema):
            self._check_table(table)
            spark.createDataFrame(rows, schema).writeTo(table).using("iceberg").create()

        def _replace(self, table, rows, schema):
            self._check_table(table)
            spark.createDataFrame(rows, schema).writeTo(table).overwrite(F.lit(True))

        def _gold_state(self):
            state = {}
            for table in (
                gold.DIMENSION_TABLE,
                gold.HOURLY_TABLE,
                gold.BREAKPOINT_TABLE,
            ):
                self._check_table(table)
                if not spark.catalog.tableExists(table):
                    state[table] = None
                    continue
                # Read ACTUAL isolated rows and snapshots, not metadata row counts.
                state[table] = (
                    sorted(spark.table(table).toJSON().collect()),
                    sorted(
                        row.snapshot_id
                        for row in spark.table(table + ".snapshots").collect()
                    ),
                )
            return state

        def _assert_rejected_before_gold_writes(self, message):
            before = self._gold_state()
            error = None
            with patch.object(spark, "sql", wraps=spark.sql) as sql:
                try:
                    gold.run_gold_batch(spark)
                except RuntimeError as exc:
                    error = exc
                after = self._gold_state()
                print(
                    "P0_WRITE_SAFETY="
                    + json.dumps(
                        {
                            "test": self.id(),
                            "before": before,
                            "after": after,
                            "error": None if error is None else str(error),
                        }
                    ),
                    flush=True,
                )
                with self.subTest(check="domain rejection"):
                    self.assertIsNotNone(error, "Invalid input returned Gold output")
                    self.assertRegex(str(error), message)
                with self.subTest(check="no Gold MERGE statement"):
                    merges = [
                        call.args[0]
                        for call in sql.call_args_list
                        if call.args
                        and call.args[0].lstrip().upper().startswith("MERGE INTO")
                    ]
                    self.assertEqual([], merges)
                with self.subTest(check="no initialization or write SQL"):
                    writes = [
                        call.args[0]
                        for call in sql.call_args_list
                        if call.args
                        and call.args[0]
                        .lstrip()
                        .upper()
                        .startswith(
                            (
                                "CREATE ",
                                "MERGE ",
                                "INSERT ",
                                "UPDATE ",
                                "DELETE ",
                                "ALTER ",
                                "DROP ",
                            )
                        )
                    ]
                    self.assertEqual([], writes)
                with self.subTest(check="no Gold row or snapshot changes"):
                    self.assertEqual(before, after)

        def _seed_null_name(self, station=STATION):
            rows = [(station, None, 21.25, 105.9, "reference")]
            if station != STATION:
                rows.append((STATION, "Retained station", 21.0, 105.8, "reference"))
            self._replace(gold.DIMENSION_TABLE, rows, DIMENSION_SCHEMA)
            self._replace(gold.HOURLY_TABLE, [(STATION, HOUR, 20.0, 40)], HOURLY_SCHEMA)

        def _reject_repair_metadata(self, metadata, station=STATION):
            self._seed_null_name(station)
            rows = ([] if station == STATION else _bronze_rows()) + metadata
            self._replace(gold.BRONZE_TABLE, rows, BRONZE_SCHEMA)
            with self.assertRaisesRegex(
                RuntimeError, "(?i)metadata|conflicting|mapping"
            ):
                gold.build_dimension_source(
                    spark,
                    spark.table(gold.SILVER_TABLE),
                    spark.table(gold.DIMENSION_TABLE),
                ).collect()
            self._assert_rejected_before_gold_writes("(?i)metadata|conflicting|mapping")

        def _assert_name_repair_and_stable_rerun(self, station):
            before = self._gold_state()
            metrics = gold.run_gold_batch(spark)
            repaired = self._gold_state()
            expected = [json.loads(row) for row in before[gold.DIMENSION_TABLE][0]]
            for row in expected:
                if row["location_id"] == station:
                    self.assertIsNone(row.get("name"))
                    row["name"] = f"Fixture station {station}"
            actual = [json.loads(row) for row in repaired[gold.DIMENSION_TABLE][0]]
            self.assertEqual(
                sorted(expected, key=lambda row: row["location_id"]),
                sorted(actual, key=lambda row: row["location_id"]),
            )
            self.assertEqual(0, metrics["dimension_new_count"])
            self.assertEqual(1, metrics["dimension_changed_count"])
            self.assertEqual(0, metrics["hourly_new_count"])
            self.assertEqual(0, metrics["hourly_changed_count"])
            old_snapshots = set(before[gold.DIMENSION_TABLE][1])
            new_snapshots = set(repaired[gold.DIMENSION_TABLE][1])
            self.assertTrue(old_snapshots < new_snapshots)
            self.assertEqual(1, len(new_snapshots - old_snapshots))
            for table in (gold.HOURLY_TABLE, gold.BREAKPOINT_TABLE):
                self.assertEqual(before[table], repaired[table])
            rerun_metrics = gold.run_gold_batch(spark)
            rerun = self._gold_state()
            self.assertEqual(repaired, rerun)
            for key in (
                "dimension_new_count",
                "dimension_changed_count",
                "hourly_new_count",
                "hourly_changed_count",
            ):
                self.assertEqual(0, rerun_metrics[key])
            print(
                "P0_NAME_REPAIR="
                + json.dumps(
                    {
                        "test": self.id(),
                        "location_id": station,
                        "before": before,
                        "repaired": repaired,
                        "rerun": rerun,
                        "repair_metrics": metrics,
                        "rerun_metrics": rerun_metrics,
                    }
                ),
                flush=True,
            )

        def _numeric_case(self, values, overflow=False):
            self._replace(gold.SILVER_TABLE, _silver_rows(values), SILVER_SCHEMA)
            silver = spark.table(gold.SILVER_TABLE).coalesce(1)
            if overflow:
                self.assertTrue(all(math.isfinite(value) for value in values))
                actual_average = silver.agg(F.avg("value")).first()[0]
                self.assertFalse(
                    math.isfinite(actual_average), "Overflow fixture did not overflow"
                )
            with (
                self.subTest(stage="real transformation rejects"),
                self.assertRaisesRegex(RuntimeError, "(?i)finite|invalid|PM2.5"),
            ):
                output, _ = gold.build_hourly_aqi_source(
                    spark, silver, spark.table(gold.BREAKPOINT_TABLE)
                )
                output.collect()
            with self.subTest(stage="real batch rejects before writes"):
                self._assert_rejected_before_gold_writes("(?i)finite|invalid|PM2.5")

        def test_finite_positive_measurements_are_accepted(self):
            self._replace(gold.SILVER_TABLE, _silver_rows([10.0, 30.0]), SILVER_SCHEMA)
            rows, _ = gold.build_hourly_aqi_source(
                spark,
                spark.table(gold.SILVER_TABLE),
                spark.table(gold.BREAKPOINT_TABLE),
            )
            output = rows.collect()
            self.assertEqual(1, len(output))
            self.assertEqual(STATION, output[0].location_id)
            self.assertEqual(20.0, output[0].pm25_avg)
            self.assertEqual(40, output[0].aqi)

        def test_zero_is_accepted(self):
            self._replace(gold.SILVER_TABLE, _silver_rows([0.0]), SILVER_SCHEMA)
            output, _ = gold.build_hourly_aqi_source(
                spark,
                spark.table(gold.SILVER_TABLE),
                spark.table(gold.BREAKPOINT_TABLE),
            )
            rows = output.collect()
            self.assertEqual(1, len(rows))
            self.assertEqual((0.0, 0), (rows[0].pm25_avg, rows[0].aqi))

        def test_negative_measurement_is_rejected(self):
            self._numeric_case([-1.0])

        def test_null_measurement_is_rejected(self):
            self._numeric_case([None])

        def test_nan_is_rejected(self):
            self._numeric_case([float("nan")])

        def test_positive_infinity_is_rejected(self):
            self._numeric_case([float("inf")])

        def test_negative_infinity_is_rejected(self):
            self._numeric_case([float("-inf")])

        def test_finite_inputs_with_nonfinite_aggregate_are_rejected(self):
            self._numeric_case([sys.float_info.max, sys.float_info.max], overflow=True)

        def test_valid_bronze_metadata_covers_silver_location(self):
            rows = gold.build_dimension_source(
                spark, spark.table(gold.SILVER_TABLE)
            ).collect()
            self.assertEqual(1, len(rows))
            self.assertEqual(STATION, rows[0].location_id)
            self.assertEqual("reference", rows[0].station_type)

        def test_missing_bronze_metadata_is_rejected_by_real_builder(self):
            self._replace(gold.BRONZE_TABLE, [], BRONZE_SCHEMA)
            with self.assertRaisesRegex(
                RuntimeError, "(?i)metadata|coverage|missing|dimension"
            ):
                gold.build_dimension_source(
                    spark, spark.table(gold.SILVER_TABLE)
                ).collect()

        def test_missing_metadata_and_absent_target_key_cannot_write_orphan(self):
            self._replace(gold.BRONZE_TABLE, [], BRONZE_SCHEMA)
            self.assertEqual(0, spark.table(gold.DIMENSION_TABLE).count())
            self._assert_rejected_before_gold_writes(
                "(?i)metadata|coverage|missing|dimension"
            )
            with self.subTest(check="no orphan persisted"):
                self.assertEqual(0, spark.table(gold.HOURLY_TABLE).count())

        def test_absent_target_key_with_complete_metadata_is_created_safely(self):
            self.assertEqual(0, spark.table(gold.DIMENSION_TABLE).count())
            metrics = gold.run_gold_batch(spark)
            self.assertEqual(1, metrics["dimension_new_count"])
            self.assertEqual(1, metrics["hourly_new_count"])
            dimension = spark.table(gold.DIMENSION_TABLE)
            hourly = spark.table(gold.HOURLY_TABLE)
            self.assertEqual(1, hourly.count())
            self.assertEqual(
                0, hourly.join(dimension, "location_id", "left_anti").count()
            )

        def test_null_dimension_key_is_rejected_before_writes(self):
            self._replace(
                gold.DIMENSION_TABLE,
                [(None, "Invalid", 21.0, 105.8, "reference")],
                DIMENSION_SCHEMA,
            )
            self._assert_rejected_before_gold_writes("(?i)invalid|null|key")

        def test_duplicate_dimension_key_is_rejected_before_writes(self):
            row = (STATION, "Duplicate", 21.0, 105.8, "reference")
            self._replace(gold.DIMENSION_TABLE, [row, row], DIMENSION_SCHEMA)
            self._assert_rejected_before_gold_writes("(?i)invalid|duplicate|key")

        def test_null_name_is_repaired_from_unique_bronze_metadata(self):
            self._seed_null_name()
            self._assert_name_repair_and_stable_rerun(STATION)

        def test_identical_bronze_metadata_allows_name_repair(self):
            self._seed_null_name()
            self._replace(gold.BRONZE_TABLE, _bronze_rows() * 3, BRONZE_SCHEMA)
            self._assert_name_repair_and_stable_rerun(STATION)

        def test_null_name_with_missing_bronze_metadata_is_rejected(self):
            self._reject_repair_metadata([])

        def test_null_source_name_is_rejected_before_writes(self):
            for station in (STATION, OTHER_STATION):
                with self.subTest(station=station):
                    row = list(_bronze_rows((station,))[0])
                    row[2] = None
                    self._reject_repair_metadata([tuple(row)], station)

        def test_blank_source_name_is_rejected_before_writes(self):
            for station in (STATION, OTHER_STATION):
                for name in ("", "   ", "\t\n", "\u00a0"):
                    with self.subTest(station=station, name=repr(name)):
                        row = list(_bronze_rows((station,))[0])
                        row[2] = name
                        self._reject_repair_metadata([tuple(row)], station)

        def test_conflicting_source_names_are_rejected_before_writes(self):
            for station in (STATION, OTHER_STATION):
                with self.subTest(station=station):
                    rows = _bronze_rows((station,))
                    conflicting = list(rows[0])
                    conflicting[2] = "Conflicting station name"
                    self._reject_repair_metadata(rows + [tuple(conflicting)], station)

        def test_conflicting_source_coordinates_are_rejected_before_writes(self):
            for station in (STATION, OTHER_STATION):
                for index in (4, 5):
                    with self.subTest(station=station, coordinate=index):
                        rows = _bronze_rows((station,))
                        conflicting = list(rows[0])
                        conflicting[index] = str(float(conflicting[index]) + 0.1)
                        self._reject_repair_metadata(
                            rows + [tuple(conflicting)], station
                        )

        def test_invalid_source_coordinates_are_rejected_before_writes(self):
            for station in (STATION, OTHER_STATION):
                for index in (4, 5):
                    for value in (
                        None,
                        "",
                        "not-a-coordinate",
                        "NaN",
                        "Infinity",
                        "-Infinity",
                    ):
                        with self.subTest(
                            station=station, coordinate=index, value=value
                        ):
                            row = list(_bronze_rows((station,))[0])
                            row[index] = value
                            self._reject_repair_metadata([tuple(row)], station)

        def test_mixed_valid_and_invalid_bronze_metadata_is_rejected(self):
            for station in (STATION, OTHER_STATION):
                with self.subTest(station=station):
                    rows = _bronze_rows((station,))
                    invalid = list(rows[0])
                    invalid[2] = None
                    self._reject_repair_metadata(rows + [tuple(invalid)], station)

        def test_legacy_null_name_absent_from_silver_is_repaired(self):
            self._seed_null_name(OTHER_STATION)
            self._replace(
                gold.BRONZE_TABLE, _bronze_rows((STATION, OTHER_STATION)), BRONZE_SCHEMA
            )
            self._assert_name_repair_and_stable_rerun(OTHER_STATION)

        def test_legacy_null_name_without_metadata_rejects_before_writes(self):
            self._reject_repair_metadata([], OTHER_STATION)

        def test_legacy_repair_rejection_does_not_initialize_breakpoints(self):
            self._check_table(gold.BREAKPOINT_TABLE)
            spark.sql(f"DROP TABLE {gold.BREAKPOINT_TABLE}")
            self._reject_repair_metadata([], OTHER_STATION)
            self.assertFalse(spark.catalog.tableExists(gold.BREAKPOINT_TABLE))

        def test_legacy_null_name_with_empty_silver_is_repaired(self):
            self._seed_null_name()
            self._replace(gold.SILVER_TABLE, [], SILVER_SCHEMA)
            self._assert_name_repair_and_stable_rerun(STATION)

        def test_empty_silver_cannot_hide_unrepairable_null_name(self):
            self._seed_null_name()
            self._replace(gold.SILVER_TABLE, [], SILVER_SCHEMA)
            self._replace(gold.BRONZE_TABLE, [], BRONZE_SCHEMA)
            self._assert_rejected_before_gold_writes("(?i)missing|metadata")

        def test_existing_valid_name_and_payload_are_preserved(self):
            self._seed_null_name()
            self._replace(
                gold.DIMENSION_TABLE,
                [(STATION, "  Retained owner name  ", 21.25, 105.9, "reference")],
                DIMENSION_SCHEMA,
            )
            before = self._gold_state()
            metrics = gold.run_gold_batch(spark)
            self.assertEqual(before, self._gold_state())
            self.assertEqual(0, metrics["dimension_changed_count"])

        def test_valid_legacy_name_does_not_require_bronze_metadata(self):
            self._seed_null_name(OTHER_STATION)
            self._replace(
                gold.DIMENSION_TABLE,
                [
                    (STATION, "Retained station", 21.0, 105.8, "reference"),
                    (OTHER_STATION, "Valid legacy name", 21.25, 105.9, "reference"),
                ],
                DIMENSION_SCHEMA,
            )
            before = self._gold_state()
            gold.run_gold_batch(spark)
            self.assertEqual(before, self._gold_state())

        def test_blank_target_names_are_rejected_without_automatic_repair(self):
            for station in (STATION, OTHER_STATION):
                for name in ("", "   ", "\t\n", "\u00a0"):
                    with self.subTest(station=station, name=repr(name)):
                        self._seed_null_name(station)
                        rows = [(station, name, 21.25, 105.9, "reference")]
                        if station != STATION:
                            rows.append(
                                (STATION, "Retained station", 21.0, 105.8, "reference")
                            )
                        self._replace(gold.DIMENSION_TABLE, rows, DIMENSION_SCHEMA)
                        self._replace(
                            gold.BRONZE_TABLE,
                            _bronze_rows((STATION, OTHER_STATION)),
                            BRONZE_SCHEMA,
                        )
                        self._assert_rejected_before_gold_writes(
                            "(?i)invalid blank names"
                        )

        def test_invalid_legacy_target_coordinates_are_rejected(self):
            for index in (2, 3):
                for value in (None, float("nan"), float("inf"), float("-inf")):
                    with self.subTest(coordinate=index, value=str(value)):
                        self._seed_null_name(OTHER_STATION)
                        row = [OTHER_STATION, None, 21.25, 105.9, "reference"]
                        row[index] = value
                        self._replace(
                            gold.DIMENSION_TABLE,
                            [
                                (STATION, "Retained station", 21.0, 105.8, "reference"),
                                tuple(row),
                            ],
                            DIMENSION_SCHEMA,
                        )
                        self._replace(
                            gold.BRONZE_TABLE,
                            _bronze_rows((STATION, OTHER_STATION)),
                            BRONZE_SCHEMA,
                        )
                        self._assert_rejected_before_gold_writes(
                            "(?i)invalid numeric coordinates"
                        )

        def test_legacy_repair_cannot_introduce_an_unsupported_station_mapping(self):
            unknown_station = 999999
            self._reject_repair_metadata(
                _bronze_rows((unknown_station,)), unknown_station
            )

        def test_invalid_legacy_station_mapping_is_rejected_before_writes(self):
            for station_type in (None, "unsupported", "low_cost"):
                with self.subTest(station_type=station_type):
                    self._seed_null_name(OTHER_STATION)
                    self._replace(
                        gold.DIMENSION_TABLE,
                        [
                            (STATION, "Retained station", 21.0, 105.8, "reference"),
                            (OTHER_STATION, None, 21.25, 105.9, station_type),
                        ],
                        DIMENSION_SCHEMA,
                    )
                    self._replace(
                        gold.BRONZE_TABLE,
                        _bronze_rows((STATION, OTHER_STATION)),
                        BRONZE_SCHEMA,
                    )
                    self._assert_rejected_before_gold_writes(
                        "(?i)invalid station_type mapping"
                    )

        def test_unknown_legacy_target_key_is_rejected_before_writes(self):
            self._seed_null_name()
            self._replace(
                gold.DIMENSION_TABLE,
                [
                    (STATION, "Retained station", 21.0, 105.8, "reference"),
                    (999999, "Existing name", 21.25, 105.9, "reference"),
                ],
                DIMENSION_SCHEMA,
            )
            self._assert_rejected_before_gold_writes("(?i)invalid station_type mapping")

        def test_null_fact_key_cannot_leave_partial_dimension_write(self):
            self._replace(
                gold.HOURLY_TABLE, [(None, PAST_HOUR, 20.0, 40)], HOURLY_SCHEMA
            )
            self._assert_rejected_before_gold_writes("(?i)invalid|null|key")

        def test_duplicate_fact_key_cannot_leave_partial_dimension_write(self):
            row = (STATION, PAST_HOUR, 20.0, 40)
            self._replace(gold.HOURLY_TABLE, [row, row], HOURLY_SCHEMA)
            self._assert_rejected_before_gold_writes("(?i)invalid|duplicate|key")

        def test_invalid_existing_fact_value_is_rejected_before_writes(self):
            self._replace(
                gold.HOURLY_TABLE, [(STATION, PAST_HOUR, -1.0, 40)], HOURLY_SCHEMA
            )
            self._assert_rejected_before_gold_writes("(?i)invalid|negative|value")

        def test_existing_nan_aggregate_is_rejected_before_writes(self):
            self._replace(
                gold.HOURLY_TABLE,
                [(STATION, PAST_HOUR, float("nan"), 500)],
                HOURLY_SCHEMA,
            )
            self._assert_rejected_before_gold_writes("(?i)finite|invalid|value")

        def test_existing_infinite_aggregate_is_rejected_before_writes(self):
            self._replace(
                gold.HOURLY_TABLE,
                [(STATION, PAST_HOUR, float("inf"), 500)],
                HOURLY_SCHEMA,
            )
            self._assert_rejected_before_gold_writes("(?i)finite|invalid|value")

        def test_existing_orphan_is_rejected_before_writes(self):
            self._replace(
                gold.HOURLY_TABLE, [(OTHER_STATION, PAST_HOUR, 20.0, 40)], HOURLY_SCHEMA
            )
            self._assert_rejected_before_gold_writes("(?i)orphan|missing|dimension")

        def test_invalid_numeric_input_does_not_initialize_breakpoints(self):
            self._check_table(gold.BREAKPOINT_TABLE)
            spark.sql(f"DROP TABLE {gold.BREAKPOINT_TABLE}")
            self._replace(
                gold.SILVER_TABLE, _silver_rows([float("nan")]), SILVER_SCHEMA
            )
            self._assert_rejected_before_gold_writes("(?i)finite|invalid|PM2.5")
            self.assertFalse(spark.catalog.tableExists(gold.BREAKPOINT_TABLE))

        def test_missing_metadata_does_not_initialize_breakpoints(self):
            self._check_table(gold.BREAKPOINT_TABLE)
            spark.sql(f"DROP TABLE {gold.BREAKPOINT_TABLE}")
            self._replace(gold.BRONZE_TABLE, [], BRONZE_SCHEMA)
            self._assert_rejected_before_gold_writes("(?i)metadata|coverage|missing")
            self.assertFalse(spark.catalog.tableExists(gold.BREAKPOINT_TABLE))

        def test_invalid_fact_target_does_not_initialize_breakpoints(self):
            self._check_table(gold.BREAKPOINT_TABLE)
            spark.sql(f"DROP TABLE {gold.BREAKPOINT_TABLE}")
            self._replace(
                gold.HOURLY_TABLE, [(None, PAST_HOUR, 20.0, 40)], HOURLY_SCHEMA
            )
            self._assert_rejected_before_gold_writes("(?i)invalid|null|key")
            self.assertFalse(spark.catalog.tableExists(gold.BREAKPOINT_TABLE))

        def test_unchanged_rerun_preserves_rows_and_snapshots(self):
            gold.run_gold_batch(spark)
            before = self._gold_state()
            metrics = gold.run_gold_batch(spark)
            self.assertEqual(0, metrics["dimension_new_count"])
            self.assertEqual(0, metrics["dimension_changed_count"])
            self.assertEqual(0, metrics["hourly_new_count"])
            self.assertEqual(0, metrics["hourly_changed_count"])
            self.assertEqual(before, self._gold_state())

        def test_first_run_without_gold_or_breakpoint_tables(self):
            for table in (
                gold.DIMENSION_TABLE,
                gold.HOURLY_TABLE,
                gold.BREAKPOINT_TABLE,
            ):
                self._check_table(table)
                spark.sql(f"DROP TABLE {table}")
            metrics = gold.run_gold_batch(spark)
            self.assertEqual(1, metrics["dimension_new_count"])
            self.assertEqual(1, metrics["hourly_new_count"])
            self.assertEqual(7, spark.table(gold.BREAKPOINT_TABLE).count())
            self.assertEqual(1, spark.table(gold.HOURLY_TABLE).count())

        def test_largest_finite_single_measurement_is_accepted(self):
            self._replace(
                gold.SILVER_TABLE, _silver_rows([sys.float_info.max]), SILVER_SCHEMA
            )
            output, _ = gold.build_hourly_aqi_source(
                spark,
                spark.table(gold.SILVER_TABLE),
                spark.table(gold.BREAKPOINT_TABLE),
            )
            rows = output.collect()
            self.assertEqual(1, len(rows))
            self.assertEqual(sys.float_info.max, rows[0].pm25_avg)
            self.assertEqual(500, rows[0].aqi)

        def test_all_three_mapped_stations_are_supported(self):
            stations = (STATION, OTHER_STATION, 6123215)
            self._replace(gold.BRONZE_TABLE, _bronze_rows(stations), BRONZE_SCHEMA)
            rows = [
                row for station in stations for row in _silver_rows([20.0], station)
            ]
            self._replace(gold.SILVER_TABLE, rows, SILVER_SCHEMA)
            gold.run_gold_batch(spark)
            dimension = spark.table(gold.DIMENSION_TABLE)
            self.assertEqual(
                {STATION: "reference", OTHER_STATION: "reference", 6123215: "low_cost"},
                {row.location_id: row.station_type for row in dimension.collect()},
            )
            hourly = spark.table(gold.HOURLY_TABLE)
            self.assertEqual(3, hourly.count())
            self.assertEqual(
                0, hourly.join(dimension, "location_id", "left_anti").count()
            )

    try:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(GoldP0IntegrationTests)
        test_ids = [test.id() for test in suite]
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        records = []
        for test_id in test_ids:
            failures = [
                detail
                for test, detail in result.failures
                if test.id() == test_id or test.id().startswith(test_id + " (")
            ]
            errors = [
                detail
                for test, detail in result.errors
                if test.id() == test_id or test.id().startswith(test_id + " (")
            ]
            skipped = [
                reason for test, reason in result.skipped if test.id() == test_id
            ]
            records.append(
                {
                    "test": test_id,
                    "status": (
                        "ERROR"
                        if errors
                        else "FAIL"
                        if failures
                        else "SKIP"
                        if skipped
                        else "PASS"
                    ),
                    "failures": failures,
                    "errors": errors,
                    "skipped": skipped,
                }
            )
        print(
            "P0_SUITE_RESULT="
            + json.dumps(
                {
                    "tests_run": result.testsRun,
                    "failure_count": len(result.failures),
                    "error_count": len(result.errors),
                    "skipped_count": len(result.skipped),
                    "results": records,
                }
            ),
            flush=True,
        )
        return 0 if result.wasSuccessful() and not result.skipped else 1
    finally:
        spark.stop()


def main():
    if os.environ.get(OPT_IN) != "1":
        raise SystemExit(f"Refusing Spark execution: explicitly set {OPT_IN}=1 first")
    jar = Path(os.environ.get(JAR_VARIABLE, "")).expanduser().resolve()
    if not jar.is_file() or jar.name != "iceberg-spark-runtime-3.5_2.12-1.12.0.jar":
        raise SystemExit(
            f"{JAR_VARIABLE} must name a preinstalled Iceberg 1.12.0 runtime JAR"
        )
    if len(sys.argv) == 3 and sys.argv[1] == "--isolated-worker":
        return _worker(sys.argv[2], jar)
    if len(sys.argv) != 1:
        raise SystemExit("Unexpected integration-runner arguments")
    with tempfile.TemporaryDirectory(prefix="airlake-gold-p0-") as temporary:
        root = Path(temporary).resolve()
        if root.is_relative_to(REPOSITORY):
            raise SystemExit("Temporary directory must be outside the repository")
        # Do not inherit cloud credentials, remote gateways, extra JVM options,
        # user Spark/Hadoop config paths, or package-download settings.
        env = {
            name: os.environ[name]
            for name in (
                "PATH",
                "HOME",
                "JAVA_HOME",
                "LANG",
                "LC_ALL",
                "LC_CTYPE",
                "TZ",
                "SYSTEMROOT",
                "WINDIR",
            )
            if name in os.environ
        }
        env.update(
            {
                OPT_IN: "1",
                JAR_VARIABLE: str(jar),
                "SPARK_LOCAL_IP": "127.0.0.1",
                "SPARK_LOCAL_HOSTNAME": "localhost",
                "SPARK_LOCAL_DIRS": str(root / "spill"),
                "SPARK_LOG_DIR": str(root / "logs"),
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYSPARK_PYTHON": sys.executable,
                "PYSPARK_DRIVER_PYTHON": sys.executable,
                "TMPDIR": str(root),
                "PYSPARK_SUBMIT_ARGS": "--master local[1] pyspark-shell",
            }
        )
        return subprocess.run(
            [
                sys.executable,
                "-B",
                str(Path(__file__).resolve()),
                "--isolated-worker",
                str(root),
            ],
            cwd=root,
            env=env,
            check=False,
        ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
