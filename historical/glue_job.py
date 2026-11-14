"""Isolated archive backfill through unchanged production Silver/Gold functions."""

import hashlib
import json
import math
import os
import re
import sys
import time
from datetime import UTC, datetime

from final_demo.glue_job import records, snapshots
from historical.analytics import summarize


def execute(spark, values, audit, database, catalog="history", execution="AWS_GLUE"):
    if not re.fullmatch(r"airlake_history_[0-9]{8}_[a-f0-9]{8}", database):
        raise ValueError("Historical namespace allowlist")
    if catalog != "history" or execution not in ("AWS_GLUE", "LOCAL_ISOLATED_SPARK"):
        raise ValueError("Unexpected execution scope")
    if (
        spark.conf.get("spark.sql.session.timeZone") != "UTC"
        or not spark.version.startswith("3.5.")
        or spark._jvm.org.apache.iceberg.IcebergBuild.version() != "1.12.0"
    ):
        raise ValueError("Unverified Spark/Iceberg runtime")
    if not 1 <= len(values) <= 30000 or len(values) != audit["accepted_unique_records"]:
        raise ValueError("Historical record bound/count mismatch")
    os.environ["TZ"] = "UTC"
    time.tzset()
    from pyspark.sql import functions as F

    from scripts import gold_batch as gold
    from scripts import silver_batch as silver

    bronze = f"{catalog}.{database}.bronze_measurements"
    silver.BRONZE_TABLE = gold.BRONZE_TABLE = bronze
    silver.SILVER_TABLE = gold.SILVER_TABLE = f"{catalog}.{database}.aq_measurements"
    gold.BREAKPOINT_TABLE = f"{catalog}.{database}.aqi_breakpoints"
    gold.DIMENSION_TABLE = f"{catalog}.{database}.dim_location"
    gold.HOURLY_TABLE = f"{catalog}.{database}.pm25_aqi_hourly"
    daily = f"{catalog}.{database}.pm25_daily"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{database}")
    names = [
        bronze,
        silver.SILVER_TABLE,
        gold.BREAKPOINT_TABLE,
        gold.DIMENSION_TABLE,
        gold.HOURLY_TABLE,
        daily,
    ]
    if any(spark.catalog.tableExists(t) for t in names):
        raise ValueError("Target exists; no overwrite or automatic rerun")
    now = datetime.now(UTC).replace(tzinfo=None)
    rows = [
        tuple(v["record"][k] for k in silver.BRONZE_COLUMNS[:-1]) + (now,)
        for v in values
    ]
    frame = spark.createDataFrame(
        rows,
        ", ".join(k + " STRING" for k in silver.BRONZE_COLUMNS[:-1])
        + ", kafka_ts TIMESTAMP",
    )
    frame.coalesce(2).writeTo(bronze).using("iceberg").tableProperty(
        "format-version", "2"
    ).create()
    silver_result = silver.run_silver_batch(spark)
    first = gold.run_gold_batch(spark)
    facts = records(spark.table(gold.HOURLY_TABLE))
    dims = records(spark.table(gold.DIMENSION_TABLE))
    reference = {
        (r["location_id"], r["hour_start"]): r["pm25_avg"]
        for r in audit["hourly_reference"]
    }
    actual = {(r["location_id"], r["hour_start"]): r["pm25_avg"] for r in facts}
    if set(reference) != set(actual) or any(
        not math.isclose(actual[k], v, rel_tol=1e-12, abs_tol=1e-10)
        for k, v in reference.items()
    ):
        raise ValueError("Actual Gold differs from independent archive reference")
    if (
        first["silver_source_count"] != len(values)
        or first["orphan_location_count"]
        or first["duplicate_hourly_key_count"]
    ):
        raise ValueError("Silver/Gold integrity mismatch")
    daily_frame = (
        spark.table(gold.HOURLY_TABLE)
        .withColumn(
            "date_local",
            F.to_date(F.from_utc_timestamp("hour_start", "Asia/Ho_Chi_Minh")),
        )
        .groupBy("location_id", "date_local")
        .agg(
            F.avg("pm25_avg").alias("pm25_avg"),
            F.count("*").cast("int").alias("hours_present"),
        )
        .withColumn("hours_expected", F.lit(24))
        .withColumn("coverage_percent", F.col("hours_present") * 100.0 / F.lit(24))
        .withColumn("complete_24_hours", F.col("hours_present") == 24)
    )
    if (
        daily_frame.filter(
            F.isnan("pm25_avg")
            | F.col("pm25_avg").isNull()
            | (F.abs(F.col("pm25_avg")) == float("inf"))
            | (F.col("hours_present") > 24)
        )
        .limit(1)
        .count()
    ):
        raise ValueError("Invalid daily analytics")
    daily_frame.coalesce(1).writeTo(daily).using("iceberg").tableProperty(
        "format-version", "2"
    ).create()
    daily_rows = []
    for row in spark.table(daily).collect():
        r = row.asDict()
        r["date_local"] = r["date_local"].isoformat()
        daily_rows.append(r)
    analytics = summarize(
        facts,
        audit["window_start_utc"],
        audit["window_end_utc"],
        [int(k) for k in audit["stations"]],
    )
    expected_daily = {
        (r["location_id"], r["date_local"]): r
        for s in analytics["stations"].values()
        for r in s["daily"]
    }
    for r in daily_rows:
        e = expected_daily[(r["location_id"], r["date_local"])]
        if r["hours_present"] != e["hours_present"] or not math.isclose(
            r["pm25_avg"], e["pm25_avg"], rel_tol=1e-12, abs_tol=1e-10
        ):
            raise ValueError("Daily Spark/reference mismatch")
    tables = dict(
        zip(
            ("bronze", "silver", "breakpoints", "dimensions", "hourly", "daily"),
            names,
            strict=True,
        )
    )
    before = snapshots(spark, tables)
    silver.run_silver_batch(spark)
    again = gold.run_gold_batch(spark)
    # Derived daily rows are immutable for this frozen input; no second append.
    after = snapshots(spark, tables)
    if before != after or first["hourly_sha256"] != again["hourly_sha256"]:
        raise ValueError("Unchanged Silver/Gold rerun changed rows or snapshots")
    for t in names:
        props = {
            r["key"]: r["value"] for r in spark.sql("SHOW TBLPROPERTIES " + t).collect()
        }
        if props.get("format-version") != "2":
            raise ValueError("Expected Iceberg v2")
    export = {
        "schema_version": 2,
        "facts": facts,
        "dimensions": dims,
        "daily": daily_rows,
        "analytics": analytics,
        "provenance": {
            "execution": execution,
            "database": database,
            "computed_at": datetime.now(UTC).isoformat(),
            "source_records": len(values),
            "source_records_sha256": audit["records_sha256"],
            "source_files": audit["scanned_files"],
            "window_start_utc": audit["window_start_utc"],
            "window_end_utc": audit["window_end_utc"],
            "aqi_method": "simplified_hourly_mean_mvp; NOT official VN_AQI",
            "daily_method": "arithmetic mean of available Gold hour buckets, local calendar; incomplete days labeled; no regulatory claim",
            "source_time_semantics": "source datetime retained; Gold buckets source observation end timestamps per existing contract",
            "snapshots": after,
        },
    }
    return export, {
        "status": "PASS",
        "execution": execution,
        "source_count": len(values),
        "silver": silver_result,
        "gold": first,
        "daily_count": len(daily_rows),
        "snapshots_before": before,
        "snapshots_after": after,
        "daily_independent_comparison": "PASS",
        "export_sha256": hashlib.sha256(
            json.dumps(export, sort_keys=True, allow_nan=False).encode()
        ).hexdigest(),
    }


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from botocore.config import Config
    from pyspark import SparkContext

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "history_bucket",
            "history_prefix",
            "history_database",
            "manifest_sha256",
        ],
    )
    bucket, prefix = args["history_bucket"], args["history_prefix"]
    if bucket != "airlake-701364614078-lake" or not re.fullmatch(
        r"historical-demo/20261114_[a-f0-9]{8}/", prefix
    ):
        raise ValueError("AWS output scope rejected")
    c = Config(retries={"total_max_attempts": 1})
    s = boto3.Session(region_name="ap-southeast-2")
    s3 = s.client("s3", config=c)
    if s.client("sts", config=c).get_caller_identity()["Account"] != "701364614078":
        raise ValueError("Wrong AWS account")

    def read(suffix):
        return s3.get_object(
            Bucket=bucket, Key=prefix + suffix, ExpectedBucketOwner="701364614078"
        )["Body"].read()

    manifest = read("inputs/manifest.json")
    if hashlib.sha256(manifest).hexdigest() != args["manifest_sha256"]:
        raise ValueError("Frozen manifest mismatch")
    p = json.loads(manifest)
    if p["database"] != args["history_database"] or p["prefix"] != prefix:
        raise ValueError("Manifest and execution namespace differ")
    raw = read("inputs/records.jsonl")
    if hashlib.sha256(raw).hexdigest() != p["audit"]["records_sha256"]:
        raise ValueError("Source object hash mismatch")
    glue = s.client("glue", config=c)
    try:
        glue.get_database(Name=args["history_database"])
    except glue.exceptions.EntityNotFoundException:
        pass
    else:
        raise ValueError("Historical database exists; stop")
    from scripts import gold_batch, silver_batch

    for module in (gold_batch, silver_batch):
        name = "scripts/" + module.__name__.rsplit(".", 1)[-1] + ".py"
        if (
            hashlib.sha256(module.__loader__.get_data(module.__file__)).hexdigest()
            != p["production_sha256"][name]
        ):
            raise ValueError("Validated transformation source changed")

    def no_overwrite(params, **kwargs):
        params["headers"]["If-None-Match"] = "*"

    s3.meta.events.register("before-call.s3.PutObject", no_overwrite)
    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    export, evidence = execute(
        spark,
        [json.loads(line) for line in raw.splitlines()],
        p["audit"],
        args["history_database"],
    )
    for name, value in [("export.json", export), ("evidence.json", evidence)]:
        s3.put_object(
            Bucket=bucket,
            Key=prefix + "outputs/" + name,
            Body=json.dumps(value, sort_keys=True, allow_nan=False).encode(),
            ContentType="application/json",
            ExpectedBucketOwner="701364614078",
        )
    print(
        json.dumps(
            {
                "status": "PASS",
                "source_count": len(raw.splitlines()),
                "hourly_count": len(export["facts"]),
                "daily_count": len(export["daily"]),
            }
        )
    )


if __name__ == "__main__":
    main()
