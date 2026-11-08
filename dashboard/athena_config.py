"""Render configurable Athena connection/query assets offline; never connects."""

import argparse
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode, urlparse

SQL = Path(__file__).resolve().parent / "sql"


@dataclass(frozen=True)
class AthenaSettings:
    account: str = "701364614078"
    region: str = "ap-southeast-2"
    catalog: str = "AwsDataCatalog"
    database: str = "airlake_it2_20261106_f3b3b3e0677d4a6790e4a54faf68e4aa_gold"
    hourly_table: str = "pm25_aqi_hourly"
    dimension_table: str = "dim_location"
    hourly_snapshot: str = "1174385021871906125"
    dimension_snapshot: str = "4323462633891640540"
    start: str = "2025-07-03T15:00:00Z"
    end: str = "2025-07-04T14:00:00Z"
    workgroup: str = "airlake"
    results_location: str = "s3://airlake-701364614078-athena-results/"
    profile: str | None = None

    def validate(self):
        if not re.fullmatch(r"[0-9]{12}", self.account) or not re.fullmatch(
            r"[a-z]{2}-[a-z]+-[0-9]", self.region
        ):
            raise ValueError("Expected AWS account and region are required")
        for value in (
            self.catalog,
            self.database,
            self.hourly_table,
            self.dimension_table,
        ):
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,254}", value):
                raise ValueError("Invalid SQL identifier")
        if self.hourly_table == self.dimension_table:
            raise ValueError("Fact and dimension must differ")
        for value in (self.workgroup, self.profile):
            if value is not None and not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value
            ):
                raise ValueError("Invalid workgroup/profile name")
        for value in (self.hourly_snapshot, self.dimension_snapshot):
            if not re.fullmatch(r"[1-9][0-9]{0,18}", value) or int(value) > 2**63 - 1:
                raise ValueError("A positive Iceberg snapshot ID is required")
        times = [datetime.fromisoformat(value) for value in (self.start, self.end)]
        if any(
            t.utcoffset() != timedelta(0) or t.minute or t.second or t.microsecond
            for t in times
        ) or not timedelta(0) < times[1] - times[0] <= timedelta(days=1):
            raise ValueError("Use at most 24 complete UTC hours")
        uri = urlparse(self.results_location)
        if (
            uri.scheme != "s3"
            or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", uri.netloc)
            or uri.query
            or uri.fragment
            or not uri.path.endswith("/")
            or ".." in uri.path.split("/")
            or not re.fullmatch(r"/[A-Za-z0-9_./-]*", uri.path)
        ):
            raise ValueError(
                "An explicit S3 result prefix without credentials is required"
            )
        return self

    def sqlalchemy_uri(self):
        self.validate()
        params = {
            "s3_staging_dir": self.results_location,
            "work_group": self.workgroup,
            "catalog_name": self.catalog,
        }
        if self.profile is not None:
            params["profile_name"] = self.profile
        return (
            f"awsathena+rest://@athena.{self.region}.amazonaws.com:443/{self.database}?"
            + urlencode(params)
        )

    def render(self, kind="hourly"):
        self.validate()
        if kind not in ("hourly", "latest"):
            raise ValueError("Unknown query kind")
        text = (
            SQL / ("athena_gold.sql" if kind == "hourly" else "athena_latest.sql")
        ).read_text()
        defaults = AthenaSettings()
        replacements = {}
        for field in ("catalog", "database", "hourly_table", "dimension_table"):
            replacements['"' + getattr(defaults, field) + '"'] = (
                '"' + getattr(self, field) + '"'
            )
        for field in ("hourly_snapshot", "dimension_snapshot"):
            replacements["FOR VERSION AS OF " + getattr(defaults, field)] = (
                "FOR VERSION AS OF " + getattr(self, field)
            )
        for field in ("start", "end"):
            old = datetime.fromisoformat(getattr(defaults, field)).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
            new = datetime.fromisoformat(getattr(self, field)).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
            replacements["TIMESTAMP '" + old + "'"] = "TIMESTAMP '" + new + "'"
        if self.start != defaults.start or self.end != defaults.end:
            replacements["Historical July 2025 Gold; NOT a live air-quality feed"] = (
                f"Configured Gold window {self.start[:10]} to {self.end[:10]}; live freshness unverified"
            )
        pattern = re.compile("|".join(re.escape(key) for key in replacements))
        return pattern.sub(lambda match: replacements[match.group(0)], text)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    defaults = asdict(AthenaSettings())
    for name, value in defaults.items():
        p.add_argument("--" + name.replace("_", "-"), default=value)
    args = p.parse_args(argv)
    settings = AthenaSettings(**vars(args)).validate()
    print(
        json.dumps(
            {
                "mode": "OFFLINE ASSET RENDERING; NO AWS REQUESTS",
                "expected_account": settings.account,
                "sqlalchemy_uri": settings.sqlalchemy_uri(),
                "hourly_sql": settings.render(),
                "latest_sql": settings.render("latest"),
                "output_policy": "Workgroup-enforced settings remain authoritative; approval/identity checks are required before connecting",
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
