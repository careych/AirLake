from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
KAFKA_TOPIC = "aq_measurements"
BRONZE_TABLE = "local.bronze.aq_measurements"
CHECKPOINT_PATH = "./checkpoints/bronze_aq"
# Paths are relative to the repository root. Reuse the checkpoint on restarts.

SOURCE_SCHEMA = """
    location_id STRING,
    sensors_id STRING,
    location STRING,
    datetime STRING,
    lat STRING,
    lon STRING,
    parameter STRING,
    units STRING,
    value STRING
"""


def main():
    spark = (
        SparkSession.builder.appName("AirLake-Bronze-Streaming")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "./warehouse")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
                location_id STRING,
                sensors_id STRING,
                location STRING,
                datetime STRING,
                lat STRING,
                lon STRING,
                parameter STRING,
                units STRING,
                value STRING,
                kafka_ts TIMESTAMP
            )
            USING iceberg
            """
        )

        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )

        bronze_df = (
            kafka_df.select(
                from_json(
                    col("value").cast("string"),
                    SOURCE_SCHEMA,
                ).alias("record"),
                col("timestamp").alias("kafka_ts"),
            )
            .select("record.*", "kafka_ts")
            # Current local policy drops null location IDs, including malformed JSON.
            # A lossless raw payload/rejection stream requires a separate contract.
            .filter(col("location_id").isNotNull())
        )

        query = (
            bronze_df.writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(processingTime="10 seconds")
            .toTable(BRONZE_TABLE)
        )

        print(f"Streaming Kafka topic: {KAFKA_TOPIC}")
        print(f"Writing to Iceberg table: {BRONZE_TABLE}")
        print(f"Checkpoint: {CHECKPOINT_PATH}")

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
