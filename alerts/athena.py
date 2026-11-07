"""Bounded Athena engine-3 reads of explicitly approved Iceberg snapshots.

Construction and SQL preparation are offline. Query submission requires an
explicit opt-in and injected clients; importing this module never creates an SDK.
"""

import re
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta

from alerts.gold import METHOD, UNITS, InvalidAlert, Observation, integer, utc


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]{0,254}", value
    ):
        raise InvalidAlert("Invalid Athena identifier")
    return value


def snapshot(value):
    if (
        not isinstance(value, str)
        or not re.fullmatch(r"[1-9][0-9]{0,18}", value)
        or int(value) > 2**63 - 1
    ):
        raise InvalidAlert("A verified positive Iceberg snapshot ID is required")
    return value


@dataclass(frozen=True)
class AthenaConfig:
    account: str
    region: str
    workgroup: str
    catalog: str
    database: str
    hourly_table: str
    dimension_table: str
    allowed_tables: tuple
    hourly_snapshot: str
    dimension_snapshot: str
    start: str
    end: str
    location_ids: tuple
    results_bucket: str
    results_prefix: str = ""
    max_rows: int = 1000
    timeout_seconds: int = 60
    historical_test_mode: bool = False

    def validate(self):
        if not re.fullmatch(r"[0-9]{12}", self.account) or not re.fullmatch(
            r"[a-z]{2}-[a-z]+-[0-9]", self.region
        ):
            raise InvalidAlert("Expected account and region must be explicit")
        for value in (
            self.workgroup,
            self.catalog,
            self.database,
            self.hourly_table,
            self.dimension_table,
        ):
            identifier(value)
        if self.hourly_table == self.dimension_table:
            raise InvalidAlert("Fact and dimension tables must differ")
        for table in (self.hourly_table, self.dimension_table):
            if (self.catalog, self.database, table) not in self.allowed_tables:
                raise InvalidAlert("Gold table is not allowlisted")
        snapshot(self.hourly_snapshot)
        snapshot(self.dimension_snapshot)
        integer(self.max_rows, 1, 1000)
        integer(self.timeout_seconds, 1, 300)
        if type(self.historical_test_mode) is not bool:
            raise InvalidAlert("Historical test mode must be explicit")
        if not isinstance(self.results_bucket, str) or not re.fullmatch(
            r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", self.results_bucket
        ):
            raise InvalidAlert("Expected workgroup result bucket is required")
        if (
            not isinstance(self.results_prefix, str)
            or (
                self.results_prefix
                and not re.fullmatch(
                    r"[A-Za-z0-9][A-Za-z0-9/_-]{0,200}/", self.results_prefix
                )
            )
            or "//" in self.results_prefix
        ):
            raise InvalidAlert("Invalid expected workgroup result prefix")
        if (
            not isinstance(self.location_ids, tuple)
            or not 1 <= len(self.location_ids) <= 100
            or len(set(self.location_ids)) != len(self.location_ids)
        ):
            raise InvalidAlert("A bounded unique station scope is required")
        for station in self.location_ids:
            integer(station, 1, 2**31 - 1)
        start, end = utc(self.start), utc(self.end)
        if (
            start >= end
            or end - start > timedelta(hours=24)
            or any(t.minute or t.second or t.microsecond for t in (start, end))
        ):
            raise InvalidAlert(
                "Query window must contain at most 24 complete UTC hours"
            )
        return self

    @property
    def source_table(self):
        return f"{self.catalog}.{self.database}.{self.hourly_table}"

    def sql(self):
        self.validate()

        def table(name):
            return ".".join(f'"{v}"' for v in (self.catalog, self.database, name))

        def stamp(value):
            return utc(value).strftime("%Y-%m-%d %H:%M:%S UTC")

        stations = ", ".join(str(i) for i in sorted(self.location_ids))
        # Pin BOTH tables: a latest dimension join would make a fact read irreproducible.
        return (
            "SELECT f.location_id, d.name, f.hour_start, f.pm25_avg, f.aqi\n"
            f"FROM {table(self.hourly_table)} FOR VERSION AS OF {self.hourly_snapshot} AS f\n"
            f"LEFT JOIN {table(self.dimension_table)} FOR VERSION AS OF {self.dimension_snapshot} AS d\n"
            "ON f.location_id = d.location_id\n"
            f"WHERE f.location_id IN ({stations})\n"
            f"AND f.hour_start >= TIMESTAMP '{stamp(self.start)}'\n"
            f"AND f.hour_start < TIMESTAMP '{stamp(self.end)}'\n"
            "ORDER BY f.location_id, f.hour_start\n"
            f"LIMIT {self.max_rows + 1}"
        )


COLUMNS = [
    ("location_id", "integer"),
    ("name", "varchar"),
    ("hour_start", "timestamp with time zone"),
    ("pm25_avg", "double"),
    ("aqi", "integer"),
]


class AthenaQueryExecutor:
    """One submission per read, bounded polling/results, no automatic cancellation.

    Supply SDK clients configured with total_max_attempts=1, finite HTTP timeouts
    and no custom endpoint. A timed-out query may keep running; retain query_id
    for a separately authorized inspection/cancellation. No raw SDK errors escape.
    """

    def __init__(
        self,
        client,
        identity_client,
        config,
        *,
        stations,
        rule,
        clock,
        execute_queries=False,
        test_now=None,
        monotonic=time.monotonic,
        sleep=time.sleep,
    ):
        self.config = config.validate()
        self.client, self.identity_client = client, identity_client
        self.stations, self.rule, self.clock = dict(stations), rule.validate(), clock
        self.execute_queries, self.test_now = execute_queries, test_now
        self.monotonic, self.sleep = monotonic, sleep
        self.query_id = None
        self.output_location = None
        if type(execute_queries) is not bool:
            raise InvalidAlert("Query execution opt-in must be explicit")
        if test_now is not None and not config.historical_test_mode:
            raise InvalidAlert("A historical clock requires explicit test mode")
        if config.historical_test_mode and test_now is None:
            raise InvalidAlert("Historical test mode requires a fixed reference time")
        if any(i not in self.stations for i in config.location_ids):
            raise InvalidAlert("Query station is not configured")

    def _call(self, client, operation, **kwargs):
        try:
            return getattr(client, operation)(**kwargs)
        except Exception:  # noqa: BLE001 -- SDK errors may contain sensitive service details
            raise InvalidAlert(
                f"Athena reader {operation} failed; inspect retained query ID"
            ) from None

    def _preflight(self):
        if not self.execute_queries:
            raise InvalidAlert("Athena query execution is disabled")
        now = utc(self.test_now if self.config.historical_test_mode else self.clock())
        start, end = utc(self.config.start), utc(self.config.end)
        if (
            end > now
            or (now - start - timedelta(hours=1)).total_seconds()
            > self.rule.maximum_age_seconds
        ):
            raise InvalidAlert("Query window is future or stale for this alert rule")
        if self.query_id is not None:
            raise InvalidAlert(
                "Use a new executor after a submitted query; no implicit retry"
            )
        for client in (self.client, self.identity_client):
            if (
                getattr(getattr(client, "meta", None), "region_name", None)
                != self.config.region
            ):
                raise InvalidAlert("SDK region differs from approved region")
        if (
            self._call(self.identity_client, "get_caller_identity").get("Account")
            != self.config.account
        ):
            raise InvalidAlert("AWS caller account differs from approved account")
        group = self._call(
            self.client, "get_work_group", WorkGroup=self.config.workgroup
        ).get("WorkGroup", {})
        settings = group.get("Configuration", {})
        if (
            group.get("State") != "ENABLED"
            or settings.get("EngineVersion", {}).get("EffectiveEngineVersion")
            != "Athena engine version 3"
            or settings.get("EnforceWorkGroupConfiguration") is not True
        ):
            raise InvalidAlert(
                "Enabled, enforced Athena engine-3 workgroup is required"
            )
        output = settings.get("ResultConfiguration", {}).get("OutputLocation", "")
        if output != f"s3://{self.config.results_bucket}/{self.config.results_prefix}":
            raise InvalidAlert("Workgroup output differs from approved result bucket")
        return now

    def read_pinned_hourly_rows(self):
        now = self._preflight()
        deadline = self.monotonic() + self.config.timeout_seconds
        # Token is retained even if submission is ambiguous. Never silently resubmit.
        self.query_id = "submission-uncertain"
        result = self._call(
            self.client,
            "start_query_execution",
            QueryString=self.config.sql(),
            ClientRequestToken=uuid.uuid4().hex,
            QueryExecutionContext={
                "Catalog": self.config.catalog,
                "Database": self.config.database,
            },
            WorkGroup=self.config.workgroup,
            ResultConfiguration={"ExpectedBucketOwner": self.config.account},
            ResultReuseConfiguration={
                "ResultReuseByAgeConfiguration": {"Enabled": False}
            },
        )
        query_id = result.get("QueryExecutionId")
        if not isinstance(query_id, str) or not re.fullmatch(
            r"[A-Za-z0-9-]{1,128}", query_id
        ):
            raise InvalidAlert(
                "Athena returned no valid query ID; submission is uncertain"
            )
        self.query_id = query_id
        for _ in range(self.config.timeout_seconds + 1):
            if self.monotonic() >= deadline:
                raise InvalidAlert("Athena polling timed out; query may still run")
            execution = self._call(
                self.client, "get_query_execution", QueryExecutionId=query_id
            ).get("QueryExecution", {})
            state = execution.get("Status", {}).get("State")
            if state == "SUCCEEDED":
                expected_context = {
                    "Catalog": self.config.catalog,
                    "Database": self.config.database,
                }
                if (
                    execution.get("Query") != self.config.sql()
                    or execution.get("WorkGroup") != self.config.workgroup
                    or execution.get("QueryExecutionContext") != expected_context
                ):
                    raise InvalidAlert(
                        "Athena execution does not match approved SQL/snapshots/context"
                    )
                output = execution.get("ResultConfiguration", {}).get(
                    "OutputLocation", ""
                )
                prefix = (
                    f"s3://{self.config.results_bucket}/{self.config.results_prefix}"
                )
                if (
                    not isinstance(output, str)
                    or not output.startswith(prefix)
                    or output == prefix
                ):
                    raise InvalidAlert(
                        "Athena result object is outside approved prefix"
                    )
                self.output_location = output
                return self._results(now, deadline)
            if state not in ("RUNNING", "QUEUED"):
                raise InvalidAlert(
                    "Athena query failed, cancelled or returned an unknown state"
                )
            self.sleep(1)
        raise InvalidAlert("Athena polling bound exceeded; query may still run")

    def _results(self, now, deadline):
        rows, seen, keys, token = [], set(), set(), None
        for page_index in range(11):
            if self.monotonic() >= deadline:
                raise InvalidAlert("Athena result retrieval timed out")
            kwargs = {"QueryExecutionId": self.query_id, "MaxResults": 1000}
            if token:
                kwargs["NextToken"] = token
            response = self._call(self.client, "get_query_results", **kwargs)
            result = response.get("ResultSet", {})
            columns = result.get("ResultSetMetadata", {}).get("ColumnInfo", [])
            if [(c.get("Name"), c.get("Type")) for c in columns] != COLUMNS:
                raise InvalidAlert("Athena Gold schema differs from verified schema")
            raw_rows = result.get("Rows", [])
            if page_index == 0:
                if not raw_rows or [
                    c.get("VarCharValue") for c in raw_rows[0].get("Data", [])
                ] != [n for n, _ in COLUMNS]:
                    raise InvalidAlert("Athena result header is missing or invalid")
                raw_rows = raw_rows[1:]
            for raw in raw_rows:
                data = raw.get("Data", [])
                if len(data) != 5 or any(
                    not isinstance(c.get("VarCharValue"), str) for c in data
                ):
                    raise InvalidAlert(
                        "Gold row has null or missing fields/dimension coverage"
                    )
                location, name, hour, pm25, aqi = [c["VarCharValue"] for c in data]
                if (
                    not re.fullmatch(r"[1-9][0-9]{0,9}", location)
                    or not re.fullmatch(r"[0-9]{1,3}", aqi)
                    or not re.fullmatch(
                        r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d{1,6})? UTC", hour
                    )
                ):
                    raise InvalidAlert(
                        "Gold result has invalid numeric or UTC timestamp encoding"
                    )
                try:
                    row = (
                        Observation(
                            int(location),
                            name,
                            hour.replace(" UTC", "+00:00").replace(" ", "T"),
                            float(pm25),
                            int(aqi),
                            UNITS,
                            METHOD,
                            self.config.source_table,
                            self.config.hourly_snapshot,
                        )
                        .validate(self.stations, self.rule, now)
                        .payload()
                    )
                except (ValueError, TypeError):
                    raise InvalidAlert(
                        "Gold result failed observation validation"
                    ) from None
                stamp = utc(row["hour_start"])
                if row["location_id"] not in self.config.location_ids or not utc(
                    self.config.start
                ) <= stamp < utc(self.config.end):
                    raise InvalidAlert("Gold result is outside approved query scope")
                key = (row["location_id"], row["hour_start"])
                if key in keys:
                    raise InvalidAlert("Duplicate Gold key or nonunique dimension join")
                keys.add(key)
                rows.append(row)
                if len(rows) > self.config.max_rows:
                    raise InvalidAlert("Gold result exceeds approved row limit")
            token = response.get("NextToken")
            if not token:
                if not rows:
                    raise InvalidAlert("Gold query returned no observations")
                return rows
            if not isinstance(token, str) or token in seen:
                raise InvalidAlert("Invalid or repeated Athena pagination token")
            seen.add(token)
        raise InvalidAlert("Athena result page limit exceeded")


class AthenaGoldSource:
    """Replaceable query source consumed by the existing alert evaluator."""

    def __init__(self, executor=None):
        self.executor = executor

    def read(self):
        if self.executor is None:
            raise InvalidAlert("Live Athena executor is not configured")
        return self.executor.read_pinned_hourly_rows()


def sdk_executor(
    config, *, stations, rule, clock, profile=None, execute_queries=False, test_now=None
):
    """Create bounded SDK clients deliberately; still no query until source.read().

    Use an execution role in AWS or an approved temporary-credential profile
    locally. No static credentials or alternative endpoints are accepted here.
    """
    import os

    if any(k.startswith("AWS_ENDPOINT_URL") for k in os.environ):
        raise InvalidAlert("Custom AWS endpoints are unsupported")
    config.validate()
    from boto3 import Session
    from botocore.config import Config

    session = Session(profile_name=profile, region_name=config.region)
    settings = Config(
        retries={"total_max_attempts": 1, "mode": "standard"},
        connect_timeout=5,
        read_timeout=10,
        ignore_configured_endpoint_urls=True,
    )
    return AthenaQueryExecutor(
        session.client("athena", config=settings),
        session.client("sts", config=settings),
        config,
        stations=stations,
        rule=rule,
        clock=clock,
        execute_queries=execute_queries,
        test_now=test_now,
    )
