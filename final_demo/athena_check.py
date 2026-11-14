"""One separately approved SELECT over AWS-computed Gold; never auto-retry."""

import argparse
import json
import time
import uuid
from pathlib import Path

from clean_api.data import stamp
from clean_api.verify import same_rows
from final_demo.contract import ACCOUNT, digest, validate_plan
from final_demo.control import Receipt, clients


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--packet", type=Path, required=True)
    a.add_argument("--execute", action="store_true")
    a.add_argument("--confirm-plan-sha256", required=True)
    a.add_argument("--approve-selects", type=int, choices=[1], required=True)
    args = a.parse_args()
    raw = (args.packet / "plan.json").read_bytes()
    p = validate_plan(json.loads(raw))
    if not args.execute or digest(raw) != args.confirm_plan_sha256:
        a.exit(
            1,
            "STOP: separate approval for exactly one SELECT and exact plan hash required\n",
        )
    report = Receipt(args.packet / "athena-result.json", digest(raw))
    try:
        c = clients()
        if c["sts"].get_caller_identity()["Account"] != ACCOUNT:
            raise ValueError("Wrong account")
        job = json.loads((args.packet / "job-result.json").read_text())
        if job["JobRunState"] != "SUCCEEDED" or job["validation"]["status"] != "PASS":
            raise ValueError("Glue success required")
        ids = job["validation"]["snapshots_after"]
        if any(
            len(ids[k]) != 1 or type(ids[k][0]) is not int or ids[k][0] <= 0
            for k in ("hourly", "dimensions")
        ):
            raise ValueError("Invalid output snapshots")
        import boto3
        from botocore.config import Config

        client = boto3.Session(profile_name="airlake", region_name=p["region"]).client(
            "athena", config=Config(retries={"total_max_attempts": 1})
        )
        group = client.get_work_group(WorkGroup="airlake")["WorkGroup"]
        settings = group["Configuration"]
        if (
            group["State"] != "ENABLED"
            or not settings.get("EnforceWorkGroupConfiguration")
            or settings["ResultConfiguration"]["OutputLocation"]
            != "s3://airlake-701364614078-athena-results/"
        ):
            raise ValueError("Workgroup scope mismatch")
        sql = (
            f"SELECT f.location_id,d.name AS station_name,to_iso8601(f.hour_start AT TIME ZONE 'UTC') AS hour_start,f.pm25_avg,f.aqi "
            f'FROM "AwsDataCatalog"."{p["database"]}"."pm25_aqi_hourly" FOR VERSION AS OF {ids["hourly"][0]} AS f '
            f'LEFT JOIN "AwsDataCatalog"."{p["database"]}"."dim_location" FOR VERSION AS OF {ids["dimensions"][0]} AS d ON f.location_id=d.location_id '
            "WHERE f.hour_start >= TIMESTAMP '2026-11-07 02:00:00 UTC' AND f.hour_start < TIMESTAMP '2026-11-08 03:00:00 UTC' "
            "AND f.location_id IN (4946812,4946813,6123215) ORDER BY f.location_id,f.hour_start LIMIT 73"
        )
        result = report.write(
            "athena.start_one_select",
            lambda: client.start_query_execution(
                QueryString=sql,
                ClientRequestToken=uuid.uuid4().hex,
                QueryExecutionContext={
                    "Catalog": "AwsDataCatalog",
                    "Database": p["database"],
                },
                WorkGroup="airlake",
                ResultConfiguration={"ExpectedBucketOwner": ACCOUNT},
                ResultReuseConfiguration={
                    "ResultReuseByAgeConfiguration": {"Enabled": False}
                },
            ),
        )
        report.value["query_id"] = result["QueryExecutionId"]
        report.save()
        for _ in range(60):
            value = client.get_query_execution(
                QueryExecutionId=report.value["query_id"]
            )["QueryExecution"]
            state = value["Status"]["State"]
            if state == "SUCCEEDED":
                break
            if state not in ("RUNNING", "QUEUED"):
                raise ValueError("Query failed; no retry")
            time.sleep(1)
        else:
            raise TimeoutError("Query may still run; no retry")
        if (
            value["Query"] != sql
            or value["WorkGroup"] != "airlake"
            or value["QueryExecutionContext"]["Database"] != p["database"]
        ):
            raise ValueError("Returned query scope differs")
        response = client.get_query_results(
            QueryExecutionId=report.value["query_id"], MaxResults=1000
        )
        if response.get("NextToken"):
            raise ValueError("Unexpected result size")
        resultset = response["ResultSet"]
        if [
            (x["Name"], x["Type"]) for x in resultset["ResultSetMetadata"]["ColumnInfo"]
        ] != [
            ("location_id", "integer"),
            ("station_name", "varchar"),
            ("hour_start", "varchar"),
            ("pm25_avg", "double"),
            ("aqi", "integer"),
        ]:
            raise ValueError("Athena result schema differs")
        rows = [
            [x.get("VarCharValue") for x in r["Data"]] for r in resultset["Rows"][1:]
        ]
        facts = [
            {
                "location_id": int(r[0]),
                "station_name": r[1],
                "hour_start": stamp(r[2]),
                "pm25_avg": float(r[3]),
                "aqi": int(r[4]),
            }
            for r in rows
        ]
        from clean_api.data import Dataset

        reference = json.loads((args.packet / "reference.json").read_text())
        expected = Dataset(
            reference["facts"],
            reference["dimensions"],
            evidence_sha256=reference["evidence_sha256"],
        )
        same_rows(facts, expected.rows)
        report.value.update(
            status="PASS",
            rows=72,
            statistics=value["Statistics"],
            result_object=value["ResultConfiguration"]["OutputLocation"],
            query_sha256=digest(sql.encode()),
        )
        report.save()
        print(json.dumps(report.value))
    except Exception as e:  # noqa: BLE001 -- retain ambiguous query ID, no error details or automatic retry
        report.value.update(
            status="FAILED_OR_UNCERTAIN", exception_type=type(e).__name__
        )
        report.save()
        a.exit(1, "STOP: inspect retained Athena receipt; no retry\n")


if __name__ == "__main__":
    main()
