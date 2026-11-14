"""Frozen live raw manifest -> original Silver/Gold -> immutable versioned export."""

import json
import os
import sys
import time
from datetime import UTC, datetime

from final_demo.glue_job import records, snapshots
from live_data.contract import (
    ACCOUNT,
    BUCKET,
    DATABASE,
    MAX_INVOCATIONS,
    MAX_RECORDS,
    PREFIX,
    REGION,
    encode,
    sha,
)


def expected(facts, bronze):
    """Independent key/mean reconciliation; AQI remains the original Gold logic."""
    groups = {}
    for value in bronze:
        r = value["record"]
        dt = datetime.fromisoformat(r["datetime"]).replace(
            minute=0, second=0, microsecond=0
        )
        key = (int(r["location_id"]), dt)
        groups.setdefault(key, []).append(float(r["value"]))
    actual = {
        (r["location_id"], datetime.fromisoformat(r["hour_start"])): r for r in facts
    }
    if set(actual) != set(groups):
        raise ValueError("Gold key set differs from accepted raw observations")
    for key, values in groups.items():
        if abs(actual[key]["pm25_avg"] - sum(values) / len(values)) > 1e-10:
            raise ValueError("Hourly mean mismatch")


def execute(spark, values):
    # PySpark materializes collected TIMESTAMP values in the driver process TZ.
    # Keep its naive datetime conversion aligned with the UTC Spark session.
    os.environ["TZ"] = "UTC"
    time.tzset()
    from scripts import gold_batch as gold
    from scripts import silver_batch as silver

    if not values or len(values) > MAX_RECORDS * MAX_INVOCATIONS:
        raise ValueError("Raw manifest count outside bound")
    if (
        spark.conf.get("spark.sql.session.timeZone") != "UTC"
        or spark._jvm.org.apache.iceberg.IcebergBuild.version() != "1.12.0"
    ):
        raise ValueError("Wrong Spark/Iceberg configuration")
    bronze_id = f"live.{DATABASE}.bronze_measurements"
    silver.SILVER_TABLE = f"live.{DATABASE}.aq_measurements"
    silver.BRONZE_TABLE = bronze_id
    gold.BRONZE_TABLE = bronze_id
    gold.SILVER_TABLE = silver.SILVER_TABLE
    gold.BREAKPOINT_TABLE = f"live.{DATABASE}.aqi_breakpoints"
    gold.DIMENSION_TABLE = f"live.{DATABASE}.dim_location"
    gold.HOURLY_TABLE = f"live.{DATABASE}.pm25_aqi_hourly"
    rows = []
    keys = set()
    for value in values:
        r = value["record"]
        key = (r["location_id"], r["sensors_id"], r["datetime"])
        if key in keys:
            raise ValueError("Duplicate source identity in frozen manifest")
        keys.add(key)
        rows.append(
            tuple(r[n] for n in silver.BRONZE_COLUMNS[:-1])
            + (
                datetime.fromisoformat(value["ingested_at"])
                .astimezone(UTC)
                .replace(tzinfo=None),
            )
        )
    frame = spark.createDataFrame(
        rows,
        ", ".join(f"{n} STRING" for n in silver.BRONZE_COLUMNS[:-1])
        + ", kafka_ts TIMESTAMP",
    )
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS live.{DATABASE}")
    frame.writeTo(bronze_id).using("iceberg").tableProperty(
        "format-version", "2"
    ).create()
    first_silver = silver.run_silver_batch(spark)
    first = gold.run_gold_batch(spark)
    facts = records(spark.table(gold.HOURLY_TABLE))
    dimensions = records(spark.table(gold.DIMENSION_TABLE))
    expected(facts, values)
    if (
        first["silver_source_count"] != len(values)
        or first["orphan_location_count"]
        or first["duplicate_hourly_key_count"]
    ):
        raise ValueError("Reconciliation failed")
    tables = {
        "bronze": bronze_id,
        "silver": silver.SILVER_TABLE,
        "breakpoints": gold.BREAKPOINT_TABLE,
        "dimensions": gold.DIMENSION_TABLE,
        "hourly": gold.HOURLY_TABLE,
    }
    before = snapshots(spark, tables)
    silver.run_silver_batch(spark)
    second = gold.run_gold_batch(spark)
    after = snapshots(spark, tables)
    if before != after or first["hourly_sha256"] != second["hourly_sha256"]:
        raise ValueError("Unchanged rerun was not idempotent")
    for table in tables.values():
        props = {
            r["key"]: r["value"]
            for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
        }
        if props.get("format-version") != "2":
            raise ValueError("Expected Iceberg v2")
    export = {
        "schema_version": 1,
        "facts": facts,
        "dimensions": dimensions,
        "provenance": {
            "execution": "AWS_GLUE",
            "database": DATABASE,
            "snapshots": after,
            "computed_at": datetime.now(UTC).isoformat(),
            "classification": "bounded recently polled observations; freshness follows observation time",
            "aqi_method": "simplified_hourly_mean_mvp; NOT official VN_AQI",
        },
    }
    return export, {
        "status": "PASS",
        "source_count": len(values),
        "silver": first_silver,
        "gold": first,
        "snapshots_before": before,
        "snapshots_after": after,
        "export_sha256": sha(encode(export)),
    }


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from botocore.config import Config
    from pyspark import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "manifest_sha256", "manifest_key"])
    c = Config(retries={"total_max_attempts": 1})
    s = boto3.Session(region_name=REGION)
    if s.client("sts", config=c).get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong account")
    s3 = s.client("s3", config=c)

    def read(key):
        if not key.startswith(PREFIX):
            raise ValueError("Source escaped live scope")
        return s3.get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)[
            "Body"
        ].read()

    raw = read(args["manifest_key"])
    if sha(raw) != args["manifest_sha256"]:
        raise ValueError("Frozen manifest integrity mismatch")
    manifest = json.loads(raw)
    for name, digest in manifest["production_sha256"].items():
        module = __import__(name[:-3].replace("/", "."), fromlist=["__name__"])
        if sha(module.__loader__.get_data(module.__file__)) != digest:
            raise ValueError("Production transformation source hash mismatch")
    values = []
    if not 1 <= len(manifest["objects"]) <= MAX_INVOCATIONS * MAX_RECORDS:
        raise ValueError("Manifest bound")
    for entry in manifest["objects"]:
        if not entry["key"].startswith(PREFIX + "bronze/"):
            raise ValueError("Unexpected raw source")
        data = read(entry["key"])
        if sha(data) != entry["sha256"]:
            raise ValueError("Raw object integrity mismatch")
        values.append(json.loads(data))
    glue = s.client("glue", config=c)
    try:
        glue.get_database(Name=DATABASE)
    except glue.exceptions.EntityNotFoundException:
        pass
    else:
        raise ValueError("Output database exists; no automatic rerun")
    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    export, evidence = execute(spark, values)
    key = PREFIX + "exports/" + args["manifest_sha256"] + "/gold.json"

    def no_overwrite(params, **kwargs):
        params["headers"]["If-None-Match"] = "*"

    s3.meta.events.register("before-call.s3.PutObject", no_overwrite)
    for name, value in [
        (key, export),
        (PREFIX + "evidence/" + args["manifest_sha256"] + ".json", evidence),
    ]:
        s3.put_object(
            Bucket=BUCKET,
            Key=name,
            Body=encode(value),
            ExpectedBucketOwner=ACCOUNT,
            ServerSideEncryption="AES256",
        )
    print("AIRLAKE_LIVE_GLUE=" + json.dumps(evidence))
    # The export pointer is promoted separately only after independent Athena verification.


if __name__ == "__main__":
    main()
