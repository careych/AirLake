from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_TABLE = "local.bronze.aq_measurements"
SILVER_TABLE = "local.silver.aq_measurements"
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"
KEY_COLUMNS = ["location_id", "sensors_id", "measured_at"]
BRONZE_COLUMNS = [
    "location_id",
    "sensors_id",
    "location",
    "datetime",
    "lat",
    "lon",
    "parameter",
    "units",
    "value",
    "kafka_ts",
]
SILVER_SCHEMA = [
    ("location_id", "int"),
    ("sensors_id", "int"),
    ("parameter", "string"),
    ("value", "double"),
    ("units", "string"),
    ("measured_at", "timestamp"),
    ("ingested_at", "timestamp"),
]
REJECTION_REASONS = [
    "location_id_missing_or_invalid",
    "sensors_id_missing_or_invalid",
    "datetime_missing_or_invalid",
    "value_missing_or_non_numeric",
    "value_negative",
]


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("AirLake-Silver-Batch")
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


def classify_bronze_records(bronze: DataFrame) -> DataFrame:
    # Silver keeps its current rejection rules; Gold checks PM2.5 finiteness.
    classified = (
        bronze.withColumn("_location_id", F.expr("try_cast(location_id AS INT)"))
        .withColumn("_sensors_id", F.expr("try_cast(sensors_id AS INT)"))
        .withColumn(
            "_measured_at",
            F.try_to_timestamp(F.col("datetime"), F.lit(TIMESTAMP_FORMAT)),
        )
        .withColumn("_value", F.expr("try_cast(value AS DOUBLE)"))
    )

    return classified.withColumn(
        "_rejection_reason",
        F.when(
            F.col("_location_id").isNull(),
            F.lit(REJECTION_REASONS[0]),
        )
        .when(F.col("_sensors_id").isNull(), F.lit(REJECTION_REASONS[1]))
        .when(F.col("_measured_at").isNull(), F.lit(REJECTION_REASONS[2]))
        .when(F.col("_value").isNull(), F.lit(REJECTION_REASONS[3]))
        .when(F.col("_value") < 0, F.lit(REJECTION_REASONS[4])),
    )


def create_silver_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            location_id INT,
            sensors_id INT,
            parameter STRING,
            value DOUBLE,
            units STRING,
            measured_at TIMESTAMP,
            ingested_at TIMESTAMP
        )
        USING iceberg
        """
    )

    actual_schema = [
        (field.name, field.dataType.simpleString())
        for field in spark.table(SILVER_TABLE).schema.fields
    ]
    if actual_schema != SILVER_SCHEMA:
        raise RuntimeError(
            f"{SILVER_TABLE} has schema {actual_schema}, expected {SILVER_SCHEMA}"
        )


def assert_silver_key_integrity(silver: DataFrame) -> tuple[int, int]:
    null_key_count = silver.filter(
        F.col("location_id").isNull()
        | F.col("sensors_id").isNull()
        | F.col("measured_at").isNull()
    ).count()
    duplicate_key_count = (
        silver.groupBy(*KEY_COLUMNS).count().filter(F.col("count") > 1).count()
    )
    return null_key_count, duplicate_key_count


def run_silver_batch(spark: SparkSession) -> dict[str, int]:
    if not spark.catalog.tableExists(BRONZE_TABLE):
        raise RuntimeError(f"Required source table does not exist: {BRONZE_TABLE}")

    source = spark.table(BRONZE_TABLE)
    actual_source_columns = source.columns
    if actual_source_columns != BRONZE_COLUMNS:
        raise RuntimeError(
            f"{BRONZE_TABLE} columns are {actual_source_columns}, "
            f"expected {BRONZE_COLUMNS}"
        )

    source_count = source.count()
    classified = classify_bronze_records(source)
    rejection_rows = (
        classified.filter(F.col("_rejection_reason").isNotNull())
        .groupBy("_rejection_reason")
        .count()
        .collect()
    )
    rejected_by_reason = {
        row["_rejection_reason"]: row["count"] for row in rejection_rows
    }
    rejected_count = sum(rejected_by_reason.values())
    valid = classified.filter(F.col("_rejection_reason").isNull())
    # This count is derived, not an independent reconciliation query.
    valid_count = source_count - rejected_count

    for reason in REJECTION_REASONS:
        print(f"REJECTED[{reason}]={rejected_by_reason.get(reason, 0)}")

    if rejected_count:
        print("Rejected record examples (up to 10):")
        classified.filter(F.col("_rejection_reason").isNotNull()).select(
            "_rejection_reason", *BRONZE_COLUMNS
        ).show(10, truncate=False)

    tie_break_columns = [
        F.col("kafka_ts").desc_nulls_last(),
        *[
            F.col(column).asc_nulls_first()
            for column in BRONZE_COLUMNS
            if column != "kafka_ts"
        ],
    ]
    deduplication_window = Window.partitionBy(
        F.col("_location_id"),
        F.col("_sensors_id"),
        F.col("_measured_at"),
    ).orderBy(*tie_break_columns)

    deduplicated = (
        valid.withColumn("_row_number", F.row_number().over(deduplication_window))
        .filter(F.col("_row_number") == 1)
        .select(
            F.col("_location_id").alias("location_id"),
            F.col("_sensors_id").alias("sensors_id"),
            F.col("parameter"),
            F.col("_value").alias("value"),
            F.col("units"),
            F.col("_measured_at").alias("measured_at"),
            F.current_timestamp().alias("ingested_at"),
        )
    )
    deduplicated_count = deduplicated.count()
    duplicate_rows_removed = valid_count - deduplicated_count

    create_silver_table(spark)
    existing_silver = spark.table(SILVER_TABLE)
    existing_null_keys, existing_duplicate_keys = assert_silver_key_integrity(
        existing_silver
    )
    if existing_null_keys or existing_duplicate_keys:
        raise RuntimeError(
            f"Refusing to merge into invalid {SILVER_TABLE}: "
            f"null_keys={existing_null_keys}, "
            f"duplicate_keys={existing_duplicate_keys}"
        )

    new_records = deduplicated.join(
        existing_silver.select(*KEY_COLUMNS),
        on=KEY_COLUMNS,
        how="left_anti",
    )
    new_records_count = new_records.count()
    if new_records_count:
        new_records.createOrReplaceTempView("silver_batch_source")
        spark.sql(
            f"""
            MERGE INTO {SILVER_TABLE} AS target
            USING silver_batch_source AS source
            ON target.location_id = source.location_id
                AND target.sensors_id = source.sensors_id
                AND target.measured_at = source.measured_at
            WHEN NOT MATCHED THEN INSERT (
                location_id,
                sensors_id,
                parameter,
                value,
                units,
                measured_at,
                ingested_at
            ) VALUES (
                source.location_id,
                source.sensors_id,
                source.parameter,
                source.value,
                source.units,
                source.measured_at,
                source.ingested_at
            )
            """
        )
    else:
        print("No new Silver keys; skipping Iceberg MERGE.")

    silver = spark.table(SILVER_TABLE)
    output_count = silver.count()
    null_key_count, duplicate_key_count = assert_silver_key_integrity(silver)
    null_value_count = silver.filter(F.col("value").isNull()).count()
    negative_value_count = silver.filter(F.col("value") < 0).count()
    null_ingested_at_count = silver.filter(F.col("ingested_at").isNull()).count()
    null_parameter_count = silver.filter(F.col("parameter").isNull()).count()
    null_units_count = silver.filter(F.col("units").isNull()).count()
    if (
        null_key_count
        or duplicate_key_count
        or null_value_count
        or negative_value_count
        or null_ingested_at_count
    ):
        raise RuntimeError(
            f"Silver integrity checks failed: null_keys={null_key_count}, "
            f"duplicate_keys={duplicate_key_count}, "
            f"null_values={null_value_count}, "
            f"negative_values={negative_value_count}, "
            f"null_ingested_at={null_ingested_at_count}"
        )

    metrics = {
        "source_count": source_count,
        "valid_count": valid_count,
        "rejected_count": rejected_count,
        "duplicate_rows_removed": duplicate_rows_removed,
        "deduplicated_source_count": deduplicated_count,
        "new_records_count": new_records_count,
        "source_keys_already_present": deduplicated_count - new_records_count,
        "output_count": output_count,
        "null_key_count": null_key_count,
        "duplicate_key_count": duplicate_key_count,
        "null_value_count": null_value_count,
        "negative_value_count": negative_value_count,
        "null_ingested_at_count": null_ingested_at_count,
        "null_parameter_count": null_parameter_count,
        "null_units_count": null_units_count,
    }
    for name, value in metrics.items():
        print(f"{name.upper()}={value}")
    print("REJECTION_COUNTS_ARE_EXCLUSIVE=true")
    return metrics


def main() -> None:
    spark = create_spark_session()
    try:
        run_silver_batch(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
