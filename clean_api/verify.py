"""Opt-in, two-SELECT comparison with the running API. Never called by HTTP."""

import csv
import hashlib
import io
import json
import math
import os
import time
import uuid
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, build_opener

from clean_api.data import (
    ACCOUNT,
    DATABASE,
    DIM_SNAPSHOT,
    FACT_SNAPSHOT,
    REGION,
    stamp,
    utc,
)

RESULTS = "s3://airlake-701364614078-athena-results/"
FACT = f'"AwsDataCatalog"."{DATABASE}"."pm25_aqi_hourly"'
DIM = f'"AwsDataCatalog"."{DATABASE}"."dim_location"'
QUERIES = (
    (
        f"SELECT f.location_id, d.name AS station_name, "
        "to_iso8601(f.hour_start AT TIME ZONE 'UTC') AS hour_start, f.pm25_avg, f.aqi "
        f"FROM {FACT} FOR VERSION AS OF {FACT_SNAPSHOT} AS f "
        f"LEFT JOIN {DIM} FOR VERSION AS OF {DIM_SNAPSHOT} AS d ON f.location_id=d.location_id "
        "WHERE f.hour_start >= TIMESTAMP '2026-11-07 02:00:00 UTC' "
        "AND f.hour_start < TIMESTAMP '2026-11-08 03:00:00 UTC' "
        "AND f.location_id IN (4946812,4946813,6123215) "
        "ORDER BY f.location_id,f.hour_start LIMIT 73"
    ),
    (
        f"SELECT location_id,name,lat,lon,station_type FROM {DIM} "
        f"FOR VERSION AS OF {DIM_SNAPSHOT} "
        "WHERE location_id IN (4946812,4946813,6123215) ORDER BY location_id LIMIT 4"
    ),
)
SCHEMAS = (
    [
        ("location_id", "integer"),
        ("station_name", "varchar"),
        ("hour_start", "varchar"),
        ("pm25_avg", "double"),
        ("aqi", "integer"),
    ],
    [
        ("location_id", "integer"),
        ("name", "varchar"),
        ("lat", "double"),
        ("lon", "double"),
        ("station_type", "varchar"),
    ],
)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError("Local API redirects are not permitted")


def read_api(base, route):
    u = urlsplit(base)
    if (
        u.scheme != "http"
        or u.hostname not in ("127.0.0.1", "localhost")
        or u.username
        or u.password
        or u.path not in ("", "/")
        or u.query
        or u.fragment
    ):
        raise ValueError("Use a localhost HTTP API origin")
    with build_opener(ProxyHandler({}), NoRedirect()).open(
        base.rstrip("/") + route, timeout=10
    ) as response:
        content = response.read(1_000_001)
        if len(content) > 1_000_000:
            raise ValueError("API response exceeded verification bound")
        return content


def same_rows(actual, expected):
    if len(actual) != len(expected):
        raise ValueError("Row count differs from accepted data")
    for a, e in zip(actual, expected, strict=True):
        if set(a) != set(e):
            raise ValueError("Schema differs from accepted data")
        for key in e:
            if type(e[key]) is float:
                if (
                    type(a[key]) not in (int, float)
                    or not math.isfinite(a[key])
                    or not math.isclose(a[key], e[key], rel_tol=0, abs_tol=1e-12)
                ):
                    raise ValueError("Numeric value differs from accepted data")
            elif a[key] != e[key]:
                raise ValueError("Value differs from accepted data")


def fetch(client, sql, index, report, save, *, clock=time.monotonic, sleep=time.sleep):
    entry = {
        "number": index + 1,
        "sql_sha256": hashlib.sha256(sql.encode()).hexdigest(),
        "query_id": None,
        "status": "SUBMISSION_UNCERTAIN",
    }
    report["queries"].append(entry)
    report["phase"] = "query_submission"
    report["athena_selects_attempted"] += 1
    save()
    result = client.start_query_execution(
        QueryString=sql,
        ClientRequestToken=uuid.uuid4().hex,
        QueryExecutionContext={"Catalog": "AwsDataCatalog", "Database": DATABASE},
        WorkGroup="airlake",
        ResultConfiguration={"ExpectedBucketOwner": ACCOUNT},
        ResultReuseConfiguration={"ResultReuseByAgeConfiguration": {"Enabled": False}},
    )
    query_id = result["QueryExecutionId"]
    if not isinstance(query_id, str) or not query_id or len(query_id) > 128:
        raise ValueError("Invalid query identifier")
    entry.update(query_id=query_id, status="SUBMITTED")
    report["phase"] = "query_polling"
    save()
    deadline = clock() + 60
    for _ in range(61):
        if clock() >= deadline:
            raise TimeoutError("Query may still be running; no automatic retry")
        value = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        state = value["Status"]["State"]
        if state == "SUCCEEDED":
            context = value["QueryExecutionContext"]
            if (
                value["Query"] != sql
                or value["WorkGroup"] != "airlake"
                or context.get("Database") != DATABASE
                or context.get("Catalog", "").lower() != "awsdatacatalog"
            ):
                raise ValueError("Returned execution scope mismatch")
            output = value["ResultConfiguration"]["OutputLocation"]
            if not output.startswith(RESULTS) or output == RESULTS:
                raise ValueError("Unexpected Athena result location")
            if (
                value["Statistics"]
                .get("ResultReuseInformation", {})
                .get("ReusedPreviousResult", False)
            ):
                raise ValueError("Expected a fresh verification execution")
            entry.update(
                status=state,
                scan_bytes=value["Statistics"].get("DataScannedInBytes", 0),
                output_object=output,
            )
            save()
            break
        if state not in ("RUNNING", "QUEUED"):
            entry["status"] = state
            raise ValueError("Query did not succeed")
        sleep(1)
    else:
        raise TimeoutError("Polling bound exceeded")
    report["phase"] = "result_validation"
    rows, seen, token = [], set(), None
    for page in range(4):
        if clock() >= deadline:
            raise TimeoutError("Result retrieval timed out")
        args = {"QueryExecutionId": query_id, "MaxResults": 1000}
        if token:
            args["NextToken"] = token
        value = client.get_query_results(**args)
        result = value["ResultSet"]
        columns = [
            (v["Name"], v["Type"]) for v in result["ResultSetMetadata"]["ColumnInfo"]
        ]
        if columns != SCHEMAS[index]:
            raise ValueError("Athena result schema mismatch")
        raw = result["Rows"]
        if page == 0:
            if not raw or [v.get("VarCharValue") for v in raw[0]["Data"]] != [
                n for n, _ in columns
            ]:
                raise ValueError("Missing result header")
            raw = raw[1:]
        for row in raw:
            if len(row["Data"]) != 5:
                raise ValueError("Malformed result row")
            rows.append([v.get("VarCharValue") for v in row["Data"]])
        if len(rows) > (72 if index == 0 else 3):
            raise ValueError("Result row bound exceeded")
        token = value.get("NextToken")
        if not token:
            entry["rows"] = len(rows)
            save()
            return rows
        if not isinstance(token, str) or token in seen:
            raise ValueError("Invalid pagination token")
        seen.add(token)
    raise ValueError("Result page bound exceeded")


def check_api(dataset, base):
    """Compare actual JSON, CSV and station HTTP responses without AWS access."""
    api = json.loads(read_api(base, "/api/v1/gold/hourly?limit=100"))
    stations = json.loads(read_api(base, "/api/v1/stations"))
    same_rows(api["data"], dataset.rows)
    same_rows(stations["data"], dataset.stations)
    if (
        api.get("metadata") != dataset.metadata()
        or stations.get("metadata") != dataset.metadata()
    ):
        raise ValueError("API provenance or units do not match accepted data")
    if api.get("total") != len(dataset.rows) or api.get("next") is not None:
        raise ValueError("API pagination did not return the complete historical batch")
    csv_rows = list(
        csv.DictReader(
            io.StringIO(
                read_api(base, "/api/v1/gold/hourly?limit=100&format=csv").decode()
            )
        )
    )
    normalized = [
        {
            "location_id": int(r["location_id"]),
            "station_name": r["station_name"],
            "hour_start": r["hour_start"],
            "pm25_avg": float(r["pm25_avg"]),
            "aqi": int(r["aqi"]),
        }
        for r in csv_rows
    ]
    same_rows(normalized, dataset.rows)
    return api, stations


def smoke(dataset, base):
    check_api(dataset, base)
    health = json.loads(read_api(base, "/api/v1/health"))
    if health.get("status") != "ok" or health.get("athena_health") != "not_checked":
        raise ValueError("Health endpoint must distinguish API from Athena health")
    first = dataset.rows[0]
    filtered = json.loads(
        read_api(
            base, f"/api/v1/gold/hourly?location_id={first['location_id']}&limit=100"
        )
    )
    same_rows(
        filtered["data"],
        [r for r in dataset.rows if r["location_id"] == first["location_id"]],
    )
    query = urlencode(
        {
            "location_id": first["location_id"],
            "start": first["hour_start"],
            "end": (utc(first["hour_start"]) + timedelta(hours=1)).isoformat(),
        }
    )
    hourly = json.loads(read_api(base, "/api/v1/gold/hourly?" + query))
    same_rows(hourly["data"], [first])
    route, seen, rows = "/api/v1/gold/hourly?limit=30", set(), []
    for _ in range(35):
        if route in seen or not route.startswith("/api/v1/gold/hourly?"):
            raise ValueError("Invalid API pagination link")
        seen.add(route)
        page = json.loads(read_api(base, route))
        rows.extend(page["data"])
        route = page["next"]
        if route is None:
            break
    else:
        raise ValueError("API pagination exceeded its bound")
    same_rows(rows, dataset.rows)
    return {
        "status": "PASS",
        "hourly_rows": len(dataset.rows),
        "stations": len(dataset.stations),
        "json_csv_station_filter": "PASS",
        "source": "retained real cloud acceptance; no fresh Athena verification",
        "athena_selects": 0,
        "utc_filter": "PASS",
        "pagination_pages": len(seen),
    }


def verify(
    dataset,
    base,
    client,
    identity,
    report,
    save,
    *,
    approved_selects,
    clock=time.monotonic,
    sleep=time.sleep,
):
    if approved_selects != 2:
        raise ValueError("Exactly two SELECTs require explicit approval")
    api, stations = check_api(dataset, base)
    for sdk in (client, identity):
        if sdk.meta.region_name != REGION:
            raise ValueError("SDK region mismatch")
    if identity.get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("AWS account mismatch")
    group = client.get_work_group(WorkGroup="airlake")["WorkGroup"]
    config = group["Configuration"]
    if (
        group["State"] != "ENABLED"
        or config.get("EnforceWorkGroupConfiguration") is not True
        or config["EngineVersion"].get("EffectiveEngineVersion")
        != "Athena engine version 3"
        or config["ResultConfiguration"].get("OutputLocation") != RESULTS
    ):
        raise ValueError("Workgroup settings differ from approved scope")
    facts = fetch(client, QUERIES[0], 0, report, save, clock=clock, sleep=sleep)
    rows = [
        {
            "location_id": int(r[0]),
            "station_name": r[1],
            "hour_start": stamp(r[2]),
            "pm25_avg": float(r[3]),
            "aqi": int(r[4]),
        }
        for r in facts
    ]
    same_rows(rows, api["data"])
    dimensions = fetch(client, QUERIES[1], 1, report, save, clock=clock, sleep=sleep)
    rows = [
        {
            "location_id": int(r[0]),
            "name": r[1],
            "lat": float(r[2]),
            "lon": float(r[3]),
            "station_type": r[4],
        }
        for r in dimensions
    ]
    same_rows(rows, stations["data"])
    report.update(
        status="PASS",
        phase="complete",
        hourly_rows=len(facts),
        stations=len(dimensions),
        api_matches_pinned_aws_gold=True,
        json_csv_stations_match=True,
    )
    save()


def sdk_clients():
    if any(k.startswith("AWS_ENDPOINT_URL") for k in os.environ):
        raise ValueError("Custom AWS endpoints are not allowed")
    import boto3
    from botocore.config import Config

    session = boto3.Session(profile_name="airlake", region_name=REGION)
    config = Config(
        connect_timeout=10, read_timeout=20, retries={"total_max_attempts": 1}
    )
    return session.client("athena", config=config), session.client("sts", config=config)


def private_report(path):
    path = Path(path).absolute()
    if path.exists() or path.is_symlink():
        raise ValueError("Use a new report outside the repository")
    path = path.resolve()
    if any((p / ".git").exists() for p in (path.parent, *path.parents)):
        raise ValueError("Use a new report outside the repository")
    parent = path.parent
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if (
        parent.is_symlink()
        or parent.stat().st_uid != os.getuid()
        or parent.stat().st_mode & 0o077
    ):
        raise ValueError("Use an owned private directory (0700)")
    # Reserve before any API or AWS operation; never overwrite older evidence.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    report = {
        "status": "RUNNING",
        "phase": "initialization",
        "athena_selects_attempted": 0,
        "queries": [],
        "hourly_snapshot": FACT_SNAPSHOT,
        "dimension_snapshot": DIM_SNAPSHOT,
    }

    def save():
        temporary = path.with_suffix(".tmp")
        with os.fdopen(
            os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w"
        ) as handle:
            json.dump(report, handle, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)

    save()
    return report, save
