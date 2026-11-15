import ast
import hashlib
import inspect
import json
import os
import re
from pathlib import Path
from urllib.parse import urlparse

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


def run_bronze(
    spark,
    *,
    table=BRONZE_TABLE,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    topic=KAFKA_TOPIC,
    checkpoint=CHECKPOINT_PATH,
    available_now=False,
):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
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
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
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

    writer = (
        bronze_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
    )

    writer = (
        writer.trigger(availableNow=True)
        if available_now
        else writer.trigger(processingTime="10 seconds")
    )
    query = writer.toTable(table)

    print(f"Streaming Kafka topic: {topic}")
    print(f"Writing to Iceberg table: {table}")
    print(f"Checkpoint: {checkpoint}")

    query.awaitTermination()
    return {
        "input_rows": sum(p.get("numInputRows", 0) for p in query.recentProgress),
        "output_rows": spark.table(table).count(),
    }


def prepare_cloud_checkpoint(config, checkpoint, topic, bootstrap_servers):
    """Bind one retained local checkpoint to this exact bounded cloud query."""
    if not config.allow_cloud_write or config.confirm_account != config.account:
        raise ValueError("Checkpoint preparation requires explicit cloud confirmation")
    parts = urlparse(config.warehouse).path.strip("/").split("/")
    run_id = parts[1] if len(parts) >= 2 else ""
    root = Path.cwd().resolve()
    if (
        not re.fullmatch(r"\d{8}_[a-f0-9]{32}", run_id)
        or root.name != "airlake-it2-cloud-" + run_id
        or root.is_relative_to(Path(__file__).resolve().parents[1])
        or topic != "airlake_it2_" + run_id
    ):
        raise ValueError(
            "Checkpoint working directory/topic differs from the approved run"
        )
    if urlparse(str(checkpoint)).scheme:
        raise ValueError("This bounded test requires a local checkpoint path")
    expected = root / "bronze-checkpoint"
    candidate = Path(checkpoint).expanduser()
    if candidate.is_symlink() or candidate.resolve() != expected:
        raise ValueError(
            "Checkpoint must resolve inside the approved working directory"
        )
    marker = root / "bronze-checkpoint.binding.json"
    if marker.is_symlink() or (expected.exists() and not expected.is_dir()):
        raise ValueError("Unsafe checkpoint/binding path")
    if expected.exists() and any(p.is_symlink() for p in expected.rglob("*")):
        raise ValueError("Checkpoint contains a symlink")
    identity = {
        "version": 1,
        "account": config.account,
        "region": config.region,
        "warehouse": config.warehouse,
        "table": config.tables()["BRONZE_TABLE"],
        "topic": topic,
        "bootstrap_servers": bootstrap_servers,
        "source_schema": SOURCE_SCHEMA,
        "query_ast_sha256": hashlib.sha256(
            ast.dump(ast.parse(inspect.getsource(run_bronze))).encode()
        ).hexdigest(),
    }
    if marker.exists():
        try:
            compatible = json.loads(marker.read_text()) == identity
        except (ValueError, OSError):
            compatible = False
        if not compatible:
            raise ValueError(
                "Existing checkpoint binding is incompatible; do not reset offsets"
            )
        if not expected.is_dir():
            raise ValueError("Bound checkpoint is missing; refusing an offset reset")
    else:
        if expected.exists() and any(expected.iterdir()):
            raise ValueError(
                "Existing checkpoint has no verified binding; refusing reuse"
            )
        expected.mkdir(mode=0o700, exist_ok=True)
        with marker.open("x", encoding="utf-8") as handle:
            os.chmod(marker, 0o600)
            json.dump(identity, handle, sort_keys=True, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
    return str(expected)


def main():
    try:
        from scripts.pipeline_config import from_args, parser, print_plan
    except ModuleNotFoundError:
        from pipeline_config import from_args, parser, print_plan
    cli = parser("AirLake Kafka to Bronze")
    cli.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    cli.add_argument("--topic", default=KAFKA_TOPIC)
    cli.add_argument("--checkpoint", default=CHECKPOINT_PATH)
    cli.add_argument("--available-now", action="store_true")
    args = cli.parse_args()
    config = from_args(args)
    if args.plan:
        print_plan(config)
        return
    if config.mode == "glue":
        if not args.available_now:
            raise ValueError(
                "This cloud test requires bounded --available-now execution"
            )
        args.checkpoint = prepare_cloud_checkpoint(
            config, args.checkpoint, args.topic, args.bootstrap_servers
        )
    spark = config.create_spark("AirLake-Bronze-Streaming")
    try:
        run_bronze(
            spark,
            table=config.tables()["BRONZE_TABLE"],
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            checkpoint=args.checkpoint,
            available_now=args.available_now,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
