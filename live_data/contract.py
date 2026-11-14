"""A fixed, isolated demo scope; measurement time is never replaced by polling time."""

import hashlib
import json
import re
from datetime import UTC, datetime, timedelta

from ingestion.openaq import finite, utc

ACCOUNT = "701364614078"
REGION = "ap-southeast-2"
BUCKET = "airlake-701364614078-lake"
RUN = "20261114_9e2c740b"
NAME = "airlake-live-" + RUN
PREFIX = "live-demo/" + RUN + "/"
DATABASE = "airlake_live_" + RUN
TABLE = NAME + "-state"
SECRET_NAME = "airlake/live-demo/" + RUN + "/openaq"
MAX_INVOCATIONS = 6
MAX_RECORDS = 300
MAX_PAGES = 1
PAGE_SIZE = 100
OVERLAP_SECONDS = 7200
FRESH_SECONDS = 10800
MAX_AGE_SECONDS = 86400


def encode(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, allow_nan=False
    ).encode()


def sha(value):
    return hashlib.sha256(value).hexdigest()


def timestamp(value):
    return utc(value).isoformat().replace("+00:00", "Z")


def now_utc():
    return datetime.now(UTC).replace(microsecond=0)


def config(env):
    if env.get("AIRLAKE_LIVE_RUN") != RUN:
        raise ValueError("Unapproved live run")
    arn = env.get("OPENAQ_SECRET_ARN", "")
    if not re.fullmatch(
        rf"arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret:{re.escape(SECRET_NAME)}-[A-Za-z0-9]{{6}}",
        arn,
    ):
        raise ValueError("An exact isolated OpenAQ secret ARN is required")
    deadline = utc(env.get("AIRLAKE_LIVE_DEADLINE", ""))
    return arn, deadline


def freshness(observed_at, now, cadence_seconds=None):
    """Cadence-aware UI status, not a health or safety classification."""
    if observed_at is None:
        return {"status": "no_data", "age_seconds": None, "threshold_seconds": None}
    age = (utc(now) - utc(observed_at)).total_seconds()
    threshold = FRESH_SECONDS
    if cadence_seconds is not None:
        finite(cadence_seconds, 1, MAX_AGE_SECONDS)
        threshold = max(900, 2 * cadence_seconds)
    return {
        "status": "invalid_future"
        if age < 0
        else "recent"
        if age <= threshold
        else "stale",
        "age_seconds": age,
        "threshold_seconds": threshold,
        "criterion": "age <= max(15min, 2 * estimated observation spacing); 3h fallback if cadence unknown; not a freshness SLA",
        "cadence_estimate_seconds": cadence_seconds,
    }


def validate_window(start, end):
    start, end = utc(start), utc(end)
    if not timedelta(0) < end - start <= timedelta(seconds=MAX_AGE_SECONDS):
        raise ValueError("Source window must be bounded to 24 hours")
    return start, end
