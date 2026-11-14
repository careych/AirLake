"""One opt-in isolated Spark/Iceberg test of the NEW raw -> Silver -> Gold adapter.

Uses synthetic observations, a new external temporary warehouse and no AWS SDK.
No historical acceptance resources or project runtime paths are used.
"""

import argparse
import json
import tempfile
from pathlib import Path


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--runtime-jar", required=True)
    args = a.parse_args()
    from final_demo.prepare import JARS
    from live_data.contract import sha

    if sha(Path(args.runtime_jar).read_bytes()) != JARS["runtime.jar"]:
        raise ValueError("Runtime checksum mismatch")
    from pyspark.sql import SparkSession

    from ingestion.openaq import PILOTS
    from live_data.glue_job import execute

    with tempfile.TemporaryDirectory(prefix="airlake-live-spark-") as root:
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("AirLake-live-offline-regression")
            .config("spark.jars", args.runtime_jar)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.live", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.live.type", "hadoop")
            .config("spark.sql.catalog.live.warehouse", str(Path(root) / "iceberg"))
            .config("spark.sql.warehouse.dir", str(Path(root) / "spark"))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.local.dir", str(Path(root) / "scratch"))
            .getOrCreate()
        )
        values = []
        try:
            for loc, sensor in PILOTS.items():
                for hour, minute, value in [(3, 5, 10.0), (4, 5, 10.0), (4, 35, 20.0)]:
                    record = {
                        "location_id": str(loc),
                        "sensors_id": str(sensor),
                        "location": "SYNTHETIC " + str(loc),
                        "lat": "21.0",
                        "lon": "105.8",
                        "parameter": "pm25",
                        "units": "µg/m³",
                        "datetime": f"2026-11-14T{hour:02d}:{minute:02d}:00Z",
                        "value": str(value),
                    }
                    values.append(
                        {"record": record, "ingested_at": "2026-11-14T05:00:00Z"}
                    )
            export, evidence = execute(spark, values)
            assert len(export["facts"]) == 6 and len(export["dimensions"]) == 3
            assert evidence["source_count"] == 9
            assert evidence["snapshots_before"] == evidence["snapshots_after"]
            print(
                "AIRLAKE_OFFLINE_LIVE_INTEGRATION="
                + json.dumps(
                    {
                        "status": "PASS_SYNTHETIC_OFFLINE_ONLY",
                        "bronze": 9,
                        "silver": evidence["silver"]["output_count"],
                        "dimensions": 3,
                        "hourly": 6,
                        "unchanged_five_snapshots": True,
                    }
                )
            )
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
