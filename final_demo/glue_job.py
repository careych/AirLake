"""AWS Glue entrypoint: pinned Silver -> unchanged production Gold -> export.

Only runs deliberately as a Glue job. No precomputed Gold is used as input to
the transformation; the retained reference is used only for assertions.
"""

import json
import sys
from datetime import UTC, datetime

from final_demo.contract import (
    SOURCES,
    digest,
    encode,
    s3_parts,
    uri,
    validate_computed,
    validate_plan,
)


def records(frame):
    rows = []
    for row in frame.collect():
        value = row.asDict()
        if "hour_start" in value:
            value["hour_start"] = (
                value["hour_start"]
                .replace(tzinfo=UTC)
                .isoformat()
                .replace("+00:00", "Z")
            )
        rows.append(value)
    return rows


def snapshots(spark, tables):
    return {
        name: sorted(
            r["snapshot_id"]
            for r in spark.table(table + ".snapshots").select("snapshot_id").collect()
        )
        for name, table in tables.items()
    }


def execute(spark, p, reference):
    from scripts import gold_batch as gold
    from scripts.silver_batch import assert_silver_key_integrity

    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Spark must run in UTC")
    if not spark.version.startswith("3.5."):
        raise ValueError("Expected Glue Spark 3.5")
    if spark._jvm.org.apache.iceberg.IcebergBuild.version() != "1.12.0":
        raise ValueError("Custom Iceberg 1.12.0 was not loaded first")
    for name, expected in p["production_sha256"].items():
        module = (
            gold
            if name.endswith("gold_batch.py")
            else __import__("scripts.silver_batch", fromlist=["silver_batch"])
        )
        if digest(module.__loader__.get_data(module.__file__)) != expected:
            raise ValueError("Production transformation source hash mismatch")
    for layer, source in SOURCES.items():
        source_id = f"demo.{source['database']}.{source['table']}"
        frame = (
            spark.read.format("iceberg")
            .option("snapshot-id", source["snapshot"])
            .load(source_id)
            .cache()
        )
        if frame.limit(560).count() != 559:
            raise ValueError("Pinned source count differs from accepted 559")
        frame.createOrReplaceTempView("final_demo_" + layer)
    silver = spark.table("final_demo_silver")
    if assert_silver_key_integrity(silver) != (0, 0):
        raise ValueError("Pinned Silver is not deduplicated or has null keys")
    if {r[0] for r in silver.select("location_id").distinct().collect()} != {
        4946812,
        4946813,
        6123215,
    }:
        raise ValueError("Unexpected Silver station coverage")
    gold.SILVER_TABLE = "final_demo_silver"
    gold.BRONZE_TABLE = "final_demo_bronze"
    gold.BREAKPOINT_TABLE = f"demo.{p['database']}.aqi_breakpoints"
    gold.DIMENSION_TABLE = f"demo.{p['database']}.dim_location"
    gold.HOURLY_TABLE = f"demo.{p['database']}.pm25_aqi_hourly"
    # Validate all candidate computation against the reference BEFORE target DDL.
    gold.ensure_silver_source(spark)
    dimension = gold.build_dimension_source(spark, silver)
    hourly, _ = gold.build_hourly_aqi_source(
        spark, silver, gold.ensure_breakpoint_table(spark, initialize=False)
    )
    validate_computed(records(hourly), records(dimension), reference)
    first = gold.run_gold_batch(spark)
    tables = {
        "breakpoints": gold.BREAKPOINT_TABLE,
        "dimensions": gold.DIMENSION_TABLE,
        "hourly": gold.HOURLY_TABLE,
    }
    before = snapshots(spark, tables)
    facts, dimensions = (
        records(spark.table(gold.HOURLY_TABLE)),
        records(spark.table(gold.DIMENSION_TABLE)),
    )
    validate_computed(facts, dimensions, reference)
    if spark.table(gold.BREAKPOINT_TABLE).count() != 7:
        raise ValueError("Breakpoint count mismatch")
    for table in tables.values():
        properties = {
            r["key"]: r["value"]
            for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
        }
        if properties.get("format-version") != "2":
            raise ValueError("Output must be Iceberg format version 2")
    second = gold.run_gold_batch(spark)
    after = snapshots(spark, tables)
    if (
        before != after
        or first["hourly_sha256"] != second["hourly_sha256"]
        or first["dimension_sha256"] != second["dimension_sha256"]
    ):
        raise ValueError("Unchanged rerun changed Gold rows or snapshots")
    export = {
        "facts": facts,
        "dimensions": dimensions,
        "evidence_sha256": reference["evidence_sha256"],
        "provenance": {
            "run": p["run"],
            "execution": "AWS_GLUE",
            "glue_job": p["job"],
            "computed_at_utc": datetime.now(UTC).isoformat(),
            "database": p["database"],
            "source_snapshots": SOURCES,
            "output_snapshots": after,
            "aqi": p["aqi"],
        },
    }
    evidence = {
        "status": "PASS",
        "execution": "AWS_GLUE",
        "source_silver_count": 559,
        "silver_null_keys": 0,
        "silver_duplicate_keys": 0,
        "hourly_count": len(facts),
        "dimension_count": len(dimensions),
        "first_metrics": first,
        "second_metrics": second,
        "snapshots_before": before,
        "snapshots_after": after,
        "reference_comparison": "PASS; absolute float tolerance 1e-12",
        "export_sha256": digest(encode(export)),
    }
    return export, evidence


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from botocore.config import Config
    from pyspark import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "plan_uri", "plan_sha256"])
    sdk = boto3.Session(region_name="ap-southeast-2")
    config = Config(retries={"total_max_attempts": 1})
    s3, glue = sdk.client("s3", config=config), sdk.client("glue", config=config)

    def read(location):
        bucket, key = s3_parts(location)
        return s3.get_object(
            Bucket=bucket, Key=key, ExpectedBucketOwner="701364614078"
        )["Body"].read()

    raw = read(args["plan_uri"])
    if digest(raw) != args["plan_sha256"]:
        raise ValueError("Frozen plan hash mismatch")
    p = validate_plan(json.loads(raw))
    if (
        sdk.client("sts", config=config).get_caller_identity()["Account"]
        != p["account"]
    ):
        raise ValueError("Execution account mismatch")
    try:
        glue.get_database(Name=p["database"])
    except glue.exceptions.EntityNotFoundException:
        pass
    else:
        raise ValueError("Output namespace already exists; no retry or overwrite")
    for prefix in (
        p["prefix"] + "warehouse/",
        p["prefix"] + "exports/",
        p["prefix"] + "evidence/",
    ):
        if s3.list_objects_v2(Bucket=p["bucket"], Prefix=prefix, MaxKeys=1).get(
            "KeyCount", 0
        ):
            raise ValueError("Output prefix already has objects")
    raw_reference = read(uri(p, "artifacts/reference.json"))
    if digest(raw_reference) != p["artifacts"]["reference.json"]:
        raise ValueError("Reference artifact hash mismatch")
    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    export, evidence = execute(spark, p, json.loads(raw_reference))

    # Set the S3 overwrite precondition before signing, including on older SDKs.
    def no_overwrite(params, **kwargs):
        params["headers"]["If-None-Match"] = "*"

    s3.meta.events.register("before-call.s3.PutObject", no_overwrite)
    for key, value in (
        ("exports/gold.json", export),
        ("evidence/glue-result.json", evidence),
    ):
        s3.put_object(
            Bucket=p["bucket"],
            Key=p["prefix"] + key,
            Body=encode(value),
            ServerSideEncryption="AES256",
            ExpectedBucketOwner=p["account"],
        )
    print("AIRLAKE_FINAL_DEMO=" + json.dumps(evidence, sort_keys=True))


if __name__ == "__main__":
    main()
