import hashlib
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

BRONZE_TABLE = "local.bronze.aq_measurements"
SILVER_TABLE = "local.silver.aq_measurements"
BREAKPOINT_TABLE = "local.silver.aqi_breakpoints"
DIMENSION_TABLE = "local.gold.dim_location"
HOURLY_TABLE = "local.gold.pm25_aqi_hourly"

SILVER_SCHEMA = [
    ("location_id", "int"),
    ("sensors_id", "int"),
    ("parameter", "string"),
    ("value", "double"),
    ("units", "string"),
    ("measured_at", "timestamp"),
    ("ingested_at", "timestamp"),
]
BRONZE_DIMENSION_COLUMNS = [
    "location_id",
    "location",
    "lat",
    "lon",
]
BREAKPOINT_SCHEMA = [
    ("parameter", "string"),
    ("aqi_lo", "int"),
    ("aqi_hi", "int"),
    ("conc_lo", "double"),
    ("conc_hi", "double"),
    ("source", "string"),
    ("effective_from", "date"),
    ("effective_to", "date"),
    ("is_current", "boolean"),
]
DIMENSION_SCHEMA = [
    ("location_id", "int"),
    ("name", "string"),
    ("lat", "double"),
    ("lon", "double"),
    ("station_type", "string"),
]
HOURLY_SCHEMA = [
    ("location_id", "int"),
    ("hour_start", "timestamp"),
    ("pm25_avg", "double"),
    ("aqi", "int"),
]
BUSINESS_TYPES = {
    4946812: "reference",
    4946813: "reference",
    6123215: "low_cost",
}
BREAKPOINTS = [
    (0, 50, 0.0, 25.0),
    (50, 100, 25.0, 50.0),
    (100, 150, 50.0, 80.0),
    (150, 200, 80.0, 150.0),
    (200, 300, 150.0, 250.0),
    (300, 400, 250.0, 350.0),
    (400, 500, 350.0, 500.0),
]
BREAKPOINT_SOURCE = "QD1459-TCMT-2019"
BREAKPOINT_EFFECTIVE_FROM = date(2019, 11, 12)
PM25_UNITS = "µg/m³"


def _invalid_numeric(column: str):
    value = F.col(column)
    return value.isNull() | F.isnan(value) | (F.abs(value) == float("inf"))


def _blank_name(column: str):
    return F.col(column).rlike(r"(?U)^\s*$")


def _invalid_station_mapping():
    valid = None
    for location_id, station_type in BUSINESS_TYPES.items():
        match = (F.col("location_id") == location_id) & F.col(
            "station_type"
        ).eqNullSafe(F.lit(station_type))
        valid = match if valid is None else valid | match
    return F.lit(True) if valid is None else ~valid


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("AirLake-Gold-Batch")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "./warehouse")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def schema_signature(spark: SparkSession, table: str) -> list[tuple[str, str]]:
    return [
        (field.name, field.dataType.simpleString())
        for field in spark.table(table).schema.fields
    ]


def assert_schema(
    spark: SparkSession, table: str, expected: list[tuple[str, str]]
) -> None:
    actual = schema_signature(spark, table)
    if actual != expected:
        raise RuntimeError(f"{table} has schema {actual}, expected {expected}")


def ensure_silver_source(spark: SparkSession) -> DataFrame:
    if not spark.catalog.tableExists(SILVER_TABLE):
        raise RuntimeError(f"Required source table does not exist: {SILVER_TABLE}")
    if schema_signature(spark, SILVER_TABLE) != SILVER_SCHEMA:
        raise RuntimeError(
            f"{SILVER_TABLE} has schema {schema_signature(spark, SILVER_TABLE)}, "
            f"expected {SILVER_SCHEMA}"
        )
    return spark.table(SILVER_TABLE)


def ensure_breakpoint_table(
    spark: SparkSession, *, initialize: bool = True
) -> DataFrame:
    # Preflight uses the same seed/validation without creating or appending a table.
    if initialize:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
        spark.sql(
            f"""
        CREATE TABLE IF NOT EXISTS {BREAKPOINT_TABLE} (
            parameter STRING,
            aqi_lo INT,
            aqi_hi INT,
            conc_lo DOUBLE,
            conc_hi DOUBLE,
            source STRING,
            effective_from DATE,
            effective_to DATE,
            is_current BOOLEAN
        )
        USING iceberg
        """
        )
    breakpoints = None
    if spark.catalog.tableExists(BREAKPOINT_TABLE):
        assert_schema(spark, BREAKPOINT_TABLE, BREAKPOINT_SCHEMA)
        breakpoints = spark.table(BREAKPOINT_TABLE)

    if (
        breakpoints is None
        or breakpoints.filter(F.lower(F.trim("parameter")) == "pm25").count() == 0
    ):
        seed_schema = StructType(
            [
                StructField("parameter", StringType(), False),
                StructField("aqi_lo", IntegerType(), False),
                StructField("aqi_hi", IntegerType(), False),
                StructField("conc_lo", DoubleType(), False),
                StructField("conc_hi", DoubleType(), False),
                StructField("source", StringType(), False),
                StructField("effective_from", DateType(), False),
                StructField("effective_to", DateType(), True),
                StructField("is_current", BooleanType(), False),
            ]
        )
        seed_rows = [
            (
                "pm25",
                aqi_lo,
                aqi_hi,
                conc_lo,
                conc_hi,
                BREAKPOINT_SOURCE,
                BREAKPOINT_EFFECTIVE_FROM,
                None,
                True,
            )
            for aqi_lo, aqi_hi, conc_lo, conc_hi in BREAKPOINTS
        ]
        seed = spark.createDataFrame(seed_rows, seed_schema)
        if initialize:
            seed.writeTo(BREAKPOINT_TABLE).append()
            print(f"SEEDED_PM25_BREAKPOINTS={len(seed_rows)}")
            breakpoints = spark.table(BREAKPOINT_TABLE)
        else:
            breakpoints = seed

    breakpoints = breakpoints.filter(F.lower(F.trim("parameter")) == "pm25")
    if (
        breakpoints.filter(
            F.col("is_current")
            & (
                F.col("aqi_lo").isNull()
                | F.col("aqi_hi").isNull()
                | F.col("conc_lo").isNull()
                | F.col("conc_hi").isNull()
                | F.col("effective_from").isNull()
                | F.col("source").isNull()
                | (F.col("conc_hi") <= F.col("conc_lo"))
                | (F.col("aqi_hi") <= F.col("aqi_lo"))
            )
        )
        .limit(1)
        .count()
    ):
        raise RuntimeError(f"{BREAKPOINT_TABLE} contains invalid current PM2.5 rows")

    current = (
        breakpoints.filter(
            F.col("is_current") & (F.lower(F.trim("parameter")) == "pm25")
        )
        .orderBy("effective_from", "aqi_lo")
        .collect()
    )
    if len(current) != len(BREAKPOINTS):
        raise RuntimeError(
            f"Expected {len(BREAKPOINTS)} current PM2.5 breakpoints in "
            f"{BREAKPOINT_TABLE}; found {len(current)}"
        )
    for index, row in enumerate(current):
        expected = BREAKPOINTS[index]
        actual = (row["aqi_lo"], row["aqi_hi"], row["conc_lo"], row["conc_hi"])
        if actual != expected:
            raise RuntimeError(
                f"Current PM2.5 breakpoints in {BREAKPOINT_TABLE} do not match "
                f"the selected QD 1459 MVP contract at row {index + 1}: "
                f"{actual} != {expected}"
            )
    return breakpoints


def create_gold_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {DIMENSION_TABLE} (
            location_id INT,
            name STRING,
            lat DOUBLE,
            lon DOUBLE,
            station_type STRING
        )
        USING iceberg
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {HOURLY_TABLE} (
            location_id INT,
            hour_start TIMESTAMP,
            pm25_avg DOUBLE,
            aqi INT
        )
        USING iceberg
        """
    )
    assert_schema(spark, DIMENSION_TABLE, DIMENSION_SCHEMA)
    assert_schema(spark, HOURLY_TABLE, HOURLY_SCHEMA)


def build_dimension_source(
    spark: SparkSession,
    silver: DataFrame,
    existing_dimension: DataFrame | None = None,
) -> DataFrame:
    if not spark.catalog.tableExists(BRONZE_TABLE):
        raise RuntimeError(
            f"Required station metadata table does not exist: {BRONZE_TABLE}"
        )
    bronze = spark.table(BRONZE_TABLE)
    missing_columns = sorted(set(BRONZE_DIMENSION_COLUMNS) - set(bronze.columns))
    if missing_columns:
        raise RuntimeError(
            f"{BRONZE_TABLE} is missing station metadata columns: {missing_columns}"
        )

    silver_locations = silver.select("location_id").distinct()
    if silver_locations.filter(F.col("location_id").isNull()).limit(1).count():
        raise RuntimeError("Silver has null dimension location keys")
    required_locations = silver_locations
    if existing_dimension is not None:
        assert_target_integrity(existing_dimension, ["location_id"], DIMENSION_TABLE)
        required_locations = required_locations.unionByName(
            existing_dimension.filter(F.col("name").isNull()).select("location_id")
        ).distinct()
    raw_dimensions = (
        bronze.select(
            F.expr("try_cast(location_id AS INT)").alias("location_id"),
            F.trim("location").alias("name"),
            F.expr("try_cast(lat AS DOUBLE)").alias("lat"),
            F.expr("try_cast(lon AS DOUBLE)").alias("lon"),
        )
        .join(required_locations, on="location_id", how="inner")
        .distinct()
    )
    missing_locations = required_locations.join(
        raw_dimensions.select("location_id"), on="location_id", how="left_anti"
    )
    if missing_locations.limit(1).count():
        raise RuntimeError("Required dimension locations are missing Bronze metadata")
    if (
        raw_dimensions.filter(
            F.col("location_id").isNull()
            | F.col("name").isNull()
            | _blank_name("name")
            | _invalid_numeric("lat")
            | _invalid_numeric("lon")
        )
        .limit(1)
        .count()
    ):
        raise RuntimeError(
            "Bronze station metadata has null or invalid dimension fields"
        )

    conflicting_ids = (
        raw_dimensions.groupBy("location_id").count().filter(F.col("count") != 1)
    )
    if conflicting_ids.limit(1).count():
        raise RuntimeError(
            "Bronze has conflicting name/coordinates for a required location_id"
        )

    rows = raw_dimensions.collect()
    unknown_ids = sorted({row["location_id"] for row in rows} - set(BUSINESS_TYPES))
    if unknown_ids:
        raise RuntimeError(
            f"No documented station_type mapping exists for location IDs: {unknown_ids}"
        )

    dimension_schema = StructType(
        [
            StructField("location_id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("lat", DoubleType(), False),
            StructField("lon", DoubleType(), False),
            StructField("station_type", StringType(), False),
        ]
    )
    metadata = spark.createDataFrame(
        [
            (
                row["location_id"],
                row["name"],
                row["lat"],
                row["lon"],
                BUSINESS_TYPES[row["location_id"]],
            )
            for row in rows
        ],
        dimension_schema,
    )
    if existing_dimension is None:
        return metadata

    # Existing payload is retained; this policy authorizes only null-name repairs.
    joined = metadata.alias("b").join(
        existing_dimension.alias("t"),
        on=F.col("b.location_id") == F.col("t.location_id"),
        how="left",
    )
    existing_key = F.col("t.location_id").isNotNull()
    return joined.select(
        F.col("b.location_id").alias("location_id"),
        F.coalesce(F.col("t.name"), F.col("b.name")).alias("name"),
        *[
            F.when(existing_key, F.col(f"t.{column}"))
            .otherwise(F.col(f"b.{column}"))
            .alias(column)
            for column in ("lat", "lon", "station_type")
        ],
    )


def build_hourly_aqi_source(
    spark: SparkSession, silver: DataFrame, breakpoints: DataFrame
) -> tuple[DataFrame, dict[str, int]]:
    pm25 = silver.filter(F.lower(F.trim("parameter")) == "pm25")
    source_count = silver.count()
    pm25_count = pm25.count()
    invalid_count = pm25.filter(
        F.col("location_id").isNull()
        | F.col("measured_at").isNull()
        | _invalid_numeric("value")
        | (F.col("value") < 0)
        | F.col("units").isNull()
        | (F.trim("units") != PM25_UNITS)
    ).count()
    if invalid_count:
        raise RuntimeError(
            f"PM2.5 source has {invalid_count} rows with invalid keys, values, "
            f"or units; expected units {PM25_UNITS!r}"
        )

    hourly = pm25.groupBy(
        "location_id",
        F.date_trunc("hour", "measured_at").alias("hour_start"),
    ).agg(F.avg("value").alias("pm25_avg"))
    if (
        hourly.filter(_invalid_numeric("pm25_avg") | (F.col("pm25_avg") < 0))
        .limit(1)
        .count()
    ):
        raise RuntimeError("PM2.5 hourly aggregate is invalid or non-finite")
    breakpoint_window = Window.partitionBy("effective_from", "effective_to")
    applicable_breakpoints = breakpoints.withColumn(
        "_max_conc_hi", F.max("conc_hi").over(breakpoint_window)
    )

    candidates = (
        hourly.alias("h")
        .crossJoin(F.broadcast(applicable_breakpoints.alias("b")))
        .filter(
            (F.lower(F.trim(F.col("b.parameter"))) == "pm25")
            & (F.to_date("h.hour_start") >= F.col("b.effective_from"))
            & (
                F.col("b.effective_to").isNull()
                | (F.to_date("h.hour_start") <= F.col("b.effective_to"))
            )
            & (F.col("h.pm25_avg") >= F.col("b.conc_lo"))
            & (
                (F.col("h.pm25_avg") < F.col("b.conc_hi"))
                | (
                    (F.col("b.conc_hi") == F.col("b._max_conc_hi"))
                    & (F.col("h.pm25_avg") >= F.col("b.conc_hi"))
                )
            )
        )
        .select(
            F.col("h.location_id").alias("location_id"),
            F.col("h.hour_start").alias("hour_start"),
            F.col("h.pm25_avg").alias("pm25_avg"),
            F.when(
                F.col("h.pm25_avg") >= F.col("b._max_conc_hi"),
                F.col("b.aqi_hi"),
            )
            .otherwise(
                F.round(
                    (
                        (F.col("b.aqi_hi") - F.col("b.aqi_lo"))
                        / (F.col("b.conc_hi") - F.col("b.conc_lo"))
                    )
                    * (F.col("h.pm25_avg") - F.col("b.conc_lo"))
                    + F.col("b.aqi_lo")
                )
            )
            .cast("int")
            .alias("aqi"),
        )
    )

    hourly_count = hourly.count()
    candidate_count = candidates.count()
    duplicate_key_count = (
        candidates.groupBy("location_id", "hour_start")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if candidate_count != hourly_count or duplicate_key_count:
        raise RuntimeError(
            "AQI breakpoint matching did not produce exactly one result per "
            f"location/hour: hourly={hourly_count}, mapped={candidate_count}, "
            f"duplicate_keys={duplicate_key_count}"
        )

    return candidates, {
        "silver_source_count": source_count,
        "pm25_source_count": pm25_count,
        "non_pm25_source_count": source_count - pm25_count,
        "valid_pm25_count": pm25_count - invalid_count,
        "hourly_output_count": hourly_count,
        "invalid_pm25_count": invalid_count,
        "duplicate_hourly_key_count": duplicate_key_count,
    }


def assert_target_integrity(
    target: DataFrame, key_columns: list[str], table: str
) -> None:
    null_key_condition = None
    for column in key_columns:
        condition = F.col(column).isNull()
        null_key_condition = (
            condition if null_key_condition is None else null_key_condition | condition
        )
    null_keys = target.filter(null_key_condition).count()
    duplicate_keys = (
        target.groupBy(*key_columns).count().filter(F.col("count") > 1).count()
    )
    if null_keys or duplicate_keys:
        raise RuntimeError(
            f"{table} has invalid keys: null={null_keys}, duplicates={duplicate_keys}"
        )


def assert_gold_integrity(
    dimension: DataFrame | None,
    hourly: DataFrame | None,
    *,
    dimension_source: DataFrame | None = None,
) -> tuple[int, int]:
    if dimension is not None:
        assert_target_integrity(dimension, ["location_id"], DIMENSION_TABLE)
        if (
            dimension.filter(_invalid_numeric("lat") | _invalid_numeric("lon"))
            .limit(1)
            .count()
        ):
            raise RuntimeError(f"{DIMENSION_TABLE} has invalid numeric coordinates")
        if dimension.filter(_invalid_station_mapping()).limit(1).count():
            raise RuntimeError(f"{DIMENSION_TABLE} has an invalid station_type mapping")
        if dimension.filter(_blank_name("name")).limit(1).count():
            raise RuntimeError(f"{DIMENSION_TABLE} has invalid blank names")
        unrepairable = dimension.filter(F.col("name").isNull()).select("location_id")
        if dimension_source is not None:
            valid_names = dimension_source.filter(
                F.col("name").isNotNull() & ~_blank_name("name")
            ).select("location_id")
            unrepairable = unrepairable.join(
                valid_names, on="location_id", how="left_anti"
            )
        if unrepairable.limit(1).count():
            raise RuntimeError(
                f"{DIMENSION_TABLE} has null names without validated Bronze repairs"
            )
    if hourly is None:
        return 0, 0
    assert_target_integrity(hourly, ["location_id", "hour_start"], HOURLY_TABLE)
    invalid_hourly_output = hourly.filter(
        _invalid_numeric("pm25_avg")
        | (F.col("pm25_avg") < 0)
        | F.col("aqi").isNull()
        | (F.col("aqi") < 0)
        | (F.col("aqi") > 500)
    ).count()
    if invalid_hourly_output:
        raise RuntimeError(
            f"{HOURLY_TABLE} has {invalid_hourly_output} invalid output rows"
        )
    locations = hourly.select("location_id").distinct()
    orphan_location_count = (
        locations.count()
        if dimension is None
        else locations.join(
            dimension.select("location_id"), on="location_id", how="left_anti"
        ).count()
    )
    if orphan_location_count:
        raise RuntimeError(
            f"{HOURLY_TABLE} contains {orphan_location_count} locations missing "
            f"from {DIMENSION_TABLE}"
        )
    return orphan_location_count, invalid_hourly_output


def merge_if_changed(
    spark: SparkSession,
    target_table: str,
    source: DataFrame,
    key_columns: list[str],
    value_columns: list[str],
) -> tuple[int, int]:
    target = spark.table(target_table)
    assert_target_integrity(target, key_columns, target_table)
    match_condition = None
    for column in key_columns:
        condition = F.col(f"s.{column}") == F.col(f"t.{column}")
        match_condition = (
            condition if match_condition is None else match_condition & condition
        )
    joined = source.alias("s").join(target.alias("t"), on=match_condition, how="left")
    key_missing = F.col(f"t.{key_columns[0]}").isNull()
    values_differ = None
    for column in value_columns:
        difference = ~F.col(f"s.{column}").eqNullSafe(F.col(f"t.{column}"))
        values_differ = (
            difference if values_differ is None else values_differ | difference
        )
    new_count = joined.filter(key_missing).count()
    changed_count = joined.filter(~key_missing & values_differ).count()
    if new_count or changed_count:
        source.createOrReplaceTempView("gold_batch_source")
        update_assignments = ", ".join(
            f"target.{column} = source.{column}" for column in value_columns
        )
        insert_columns = key_columns + value_columns
        insert_names = ", ".join(insert_columns)
        insert_values = ", ".join(f"source.{column}" for column in insert_columns)
        match_condition = " AND ".join(
            f"target.{column} = source.{column}" for column in key_columns
        )
        difference_condition = " OR ".join(
            f"NOT (target.{column} <=> source.{column})" for column in value_columns
        )
        spark.sql(
            f"""
            MERGE INTO {target_table} AS target
            USING gold_batch_source AS source
            ON {match_condition}
            WHEN MATCHED AND ({difference_condition}) THEN
                UPDATE SET {update_assignments}
            WHEN NOT MATCHED THEN INSERT ({insert_names})
                VALUES ({insert_values})
            """
        )
    else:
        print(f"NO_CHANGE_SKIP_MERGE[{target_table}]=true")
    return new_count, changed_count


def content_digest(frame: DataFrame, order_columns: list[str]) -> str:
    rows = frame.orderBy(*order_columns).toJSON().collect()
    payload = "\n".join(rows).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def run_gold_batch(spark: SparkSession) -> dict[str, int | str]:
    silver = ensure_silver_source(spark)
    existing = {}
    for table, schema in (
        (DIMENSION_TABLE, DIMENSION_SCHEMA),
        (HOURLY_TABLE, HOURLY_SCHEMA),
    ):
        existing[table] = None
        if spark.catalog.tableExists(table):
            assert_schema(spark, table, schema)
            existing[table] = spark.table(table)
    dimension_source = build_dimension_source(spark, silver, existing[DIMENSION_TABLE])
    assert_gold_integrity(
        existing[DIMENSION_TABLE],
        existing[HOURLY_TABLE],
        dimension_source=dimension_source,
    )
    dimension_source_count = dimension_source.count()
    dimension_null_count = dimension_source.filter(
        F.col("location_id").isNull()
        | F.col("name").isNull()
        | F.col("lat").isNull()
        | F.col("lon").isNull()
        | F.col("station_type").isNull()
    ).count()
    if dimension_null_count:
        raise RuntimeError(
            f"Gold dimension source has {dimension_null_count} rows with null fields"
        )

    breakpoints = ensure_breakpoint_table(spark, initialize=False)
    hourly_source, metrics = build_hourly_aqi_source(spark, silver, breakpoints)
    # All source/target checks precede breakpoint initialization and Gold writes.
    # These separate Iceberg commits are still not a multi-table transaction.
    ensure_breakpoint_table(spark)
    create_gold_tables(spark)

    dim_new, dim_changed = merge_if_changed(
        spark,
        DIMENSION_TABLE,
        dimension_source,
        ["location_id"],
        ["name", "lat", "lon", "station_type"],
    )
    hourly_new, hourly_changed = merge_if_changed(
        spark,
        HOURLY_TABLE,
        hourly_source,
        ["location_id", "hour_start"],
        ["pm25_avg", "aqi"],
    )

    dimension = spark.table(DIMENSION_TABLE)
    hourly = spark.table(HOURLY_TABLE)
    orphan_location_count, invalid_hourly_output = assert_gold_integrity(
        dimension, hourly
    )

    metrics.update(
        {
            "dimension_source_count": dimension_source_count,
            "dimension_output_count": dimension.count(),
            "dimension_new_count": dim_new,
            "dimension_changed_count": dim_changed,
            "hourly_output_count": hourly.count(),
            "hourly_new_count": hourly_new,
            "hourly_changed_count": hourly_changed,
            "null_dimension_key_count": dimension.filter(
                F.col("location_id").isNull()
            ).count(),
            "null_hourly_key_count": hourly.filter(
                F.col("location_id").isNull() | F.col("hour_start").isNull()
            ).count(),
            "duplicate_dimension_key_count": dimension.groupBy("location_id")
            .count()
            .filter(F.col("count") > 1)
            .count(),
            "duplicate_hourly_key_count": hourly.groupBy("location_id", "hour_start")
            .count()
            .filter(F.col("count") > 1)
            .count(),
            "orphan_location_count": orphan_location_count,
            "invalid_hourly_output_count": invalid_hourly_output,
        }
    )
    metrics["dimension_sha256"] = content_digest(dimension, ["location_id"])
    metrics["hourly_sha256"] = content_digest(hourly, ["location_id", "hour_start"])
    for name, value in metrics.items():
        print(f"{name.upper()}={value}")
    print("AQI_CALCULATION=SIMPLIFIED_HOURLY_MEAN")
    print("AQI_BREAKPOINT_SOURCE=QD1459-TCMT-2019")
    print("ALERT_EVALUATION=DEFERRED_PENDING_THRESHOLD_POLICY")
    return metrics


def main() -> None:
    spark = create_spark_session()
    try:
        run_gold_batch(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
