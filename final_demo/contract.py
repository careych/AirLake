"""One isolated scope, with fixed validated sources and unchanged Gold rules."""

import hashlib
import json
import re
from urllib.parse import urlsplit

from clean_api.data import ACCOUNT, REGION, Dataset
from clean_api.verify import same_rows

BUCKET = "airlake-701364614078-lake"
SOURCE_RUN = "20261108_37179997ad2b42959f4387637e003ab8"
SOURCE_ROOT = f"integration-real/{SOURCE_RUN}/warehouse/"
SOURCES = {
    "bronze": {
        "database": f"airlake_it2_{SOURCE_RUN}_bronze",
        "table": "aq_measurements",
        "snapshot": "82685161818144704",
    },
    "silver": {
        "database": f"airlake_it2_{SOURCE_RUN}_silver",
        "table": "aq_measurements",
        "snapshot": "5024076747683534255",
    },
}


def digest(value):
    return hashlib.sha256(value).hexdigest()


def encode(value):
    return (
        json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode()


def new_plan(run):
    p = {
        "schema_version": 1,
        "run": run,
        "account": ACCOUNT,
        "region": REGION,
        "bucket": BUCKET,
        "prefix": f"final-demo/{run}/",
        "database": f"airlake_final_{run}",
        "job": f"airlake-final-{run}",
        "glue_role": f"airlake-final-glue-{run}",
        "api": f"airlake-final-api-{run}",
        "api_role": f"airlake-final-api-{run}",
        "sources": json.loads(json.dumps(SOURCES)),
        "expected": {"silver": 559, "dimensions": 3, "hourly": 72, "breakpoints": 7},
        "glue_version": "5.0",
        "workers": 2,
        "worker_type": "G.1X",
        "timeout_minutes": 10,
        "max_retries": 0,
        "log_prefix": f"/airlake/final-demo/{run}",
        "aqi": "simplified_hourly_mean_mvp; NOT official VN_AQI",
        "artifacts": {},
    }
    validate_plan(p)
    return p


def validate_plan(p):
    if not re.fullmatch(r"20261114_[a-f0-9]{8}", p.get("run", "")):
        raise ValueError("Invalid final-demo run")
    run = p["run"]
    expected = {
        "account": ACCOUNT,
        "region": REGION,
        "bucket": BUCKET,
        "prefix": f"final-demo/{run}/",
        "database": f"airlake_final_{run}",
        "job": f"airlake-final-{run}",
        "glue_role": f"airlake-final-glue-{run}",
        "api": f"airlake-final-api-{run}",
        "api_role": f"airlake-final-api-{run}",
        "sources": SOURCES,
        "glue_version": "5.0",
        "workers": 2,
        "worker_type": "G.1X",
        "timeout_minutes": 10,
        "max_retries": 0,
        "log_prefix": f"/airlake/final-demo/{run}",
        "expected": {"silver": 559, "dimensions": 3, "hourly": 72, "breakpoints": 7},
    }
    if any(p.get(k) != v for k, v in expected.items()):
        raise ValueError("Plan escapes the isolated fixed scope")
    return p


def s3_parts(uri):
    u = urlsplit(uri)
    if (
        u.scheme != "s3"
        or u.netloc != BUCKET
        or u.query
        or u.fragment
        or ".." in u.path.split("/")
    ):
        raise ValueError("Invalid S3 location")
    return u.netloc, u.path.lstrip("/")


def uri(p, key):
    return f"s3://{BUCKET}/{p['prefix']}{key}"


def validate_computed(facts, dimensions, reference):
    data = Dataset(facts, dimensions, evidence_sha256=reference["evidence_sha256"])
    expected = Dataset(
        reference["facts"],
        reference["dimensions"],
        evidence_sha256=reference["evidence_sha256"],
    )
    if len(data.rows) != 72 or len(data.stations) != 3:
        raise ValueError("Computed Gold count differs")
    same_rows(data.rows, expected.rows)
    same_rows(data.stations, expected.stations)
    return data
