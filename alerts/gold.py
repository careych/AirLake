"""Evaluate verified Gold facts and publish versioned SNS messages."""

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import islice

METHOD = "simplified_hourly_mean_mvp"
UNITS = "µg/m³"


class InvalidAlert(ValueError):
    """Input does not satisfy the alert boundary contract."""


def strict_json(text):
    if not isinstance(text, str) or len(text.encode()) > 200_000:
        raise InvalidAlert("Invalid message size or encoding")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise InvalidAlert("Duplicate JSON key")
            result[key] = value
        return result

    def constant(_):
        raise InvalidAlert("Non-finite JSON number")

    try:
        return json.loads(text, object_pairs_hook=pairs, parse_constant=constant)
    except (TypeError, ValueError) as error:
        raise InvalidAlert("Invalid JSON payload") from error


def canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def integer(value, lo, hi):
    if type(value) is not int or not lo <= value <= hi:
        raise InvalidAlert("Invalid integer field")
    return value


def utc(value):
    try:
        result = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(result, datetime) or result.utcoffset() != timedelta(0):
            raise InvalidAlert("An explicit UTC timestamp is required")
        return result.astimezone(UTC)
    except (ValueError, TypeError) as error:
        raise InvalidAlert("Invalid UTC timestamp") from error


def label(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", value
    ):
        raise InvalidAlert("Invalid rule identifier/version")
    return value


@dataclass(frozen=True)
class Rule:
    rule_id: str
    version: str
    minimum_aqi: int
    maximum_age_seconds: int

    def validate(self):
        label(self.rule_id)
        label(self.version)
        integer(self.minimum_aqi, 0, 500)
        integer(self.maximum_age_seconds, 1, 86400 * 7)
        return self

    def payload(self):
        self.validate()
        return {
            "rule_id": self.rule_id,
            "version": self.version,
            "minimum_aqi": self.minimum_aqi,
            "maximum_age_seconds": self.maximum_age_seconds,
        }


@dataclass(frozen=True)
class Observation:
    location_id: int
    name: str
    hour_start: str
    pm25_avg: float
    aqi: int
    units: str
    aqi_method: str
    source_table: str
    source_snapshot_id: str

    @classmethod
    def from_row(cls, row):
        if not isinstance(row, dict) or set(row) != set(cls.__dataclass_fields__):
            raise InvalidAlert("Gold observation fields are incomplete or unknown")
        normalized = dict(row)
        normalized["hour_start"] = (
            utc(row["hour_start"]).isoformat().replace("+00:00", "Z")
        )
        return cls(**normalized)

    def validate(self, stations, rule, now):
        rule.validate()
        integer(self.location_id, 1, 2**31 - 1)
        if self.location_id not in stations or self.name != stations[self.location_id]:
            raise InvalidAlert("Unknown station identity")
        if (
            not isinstance(self.name, str)
            or not self.name.strip()
            or len(self.name) > 300
        ):
            raise InvalidAlert("Invalid station name")
        hour, current = utc(self.hour_start), utc(now)
        if hour.minute or hour.second or hour.microsecond:
            raise InvalidAlert("Gold hour must be aligned to UTC")
        age = (current - hour - timedelta(hours=1)).total_seconds()
        if age < 0 or age > rule.maximum_age_seconds:
            raise InvalidAlert("Gold hour is not closed or is stale")
        if (
            type(self.pm25_avg) not in (float, int)
            or not math.isfinite(self.pm25_avg)
            or self.pm25_avg < 0
        ):
            raise InvalidAlert("PM2.5 must be finite and nonnegative")
        integer(self.aqi, 0, 500)
        if self.units != UNITS or self.aqi_method != METHOD:
            raise InvalidAlert("Unsupported PM2.5 units or AQI provenance")
        if not isinstance(self.source_table, str) or not re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){1,2}",
            self.source_table,
        ):
            raise InvalidAlert("Invalid Gold source identifier")
        if (
            not isinstance(self.source_snapshot_id, str)
            or not re.fullmatch(r"[1-9][0-9]{0,18}", self.source_snapshot_id)
            or int(self.source_snapshot_id) > 2**63 - 1
        ):
            raise InvalidAlert("Invalid source snapshot identifier")
        return self

    def payload(self):
        value = dict(self.__dict__)
        value["hour_start"] = utc(self.hour_start).isoformat().replace("+00:00", "Z")
        value["pm25_avg"] = 0.0 if self.pm25_avg == 0 else float(self.pm25_avg)
        return value


@dataclass(frozen=True)
class Alert:
    observation: Observation
    rule: Rule

    @property
    def event_id(self):
        observation = self.observation.payload()
        # A new table snapshot alone does not make unchanged hourly data a correction.
        observation.pop("source_snapshot_id")
        content = {
            "schema_version": 1,
            "observation": observation,
            "rule": self.rule.payload(),
        }
        return hashlib.sha256(canonical(content).encode()).hexdigest()

    def payload(self):
        return {
            "schema_version": 1,
            "event_id": self.event_id,
            "observation": self.observation.payload(),
            "rule": self.rule.payload(),
        }

    @classmethod
    def parse(cls, value, stations, now):
        if (
            not isinstance(value, dict)
            or set(value) != {"schema_version", "event_id", "observation", "rule"}
            or type(value["schema_version"]) is not int
            or value["schema_version"] != 1
        ):
            raise InvalidAlert("Unsupported alert envelope")
        try:
            rule = Rule(**value["rule"]).validate()
        except (TypeError, KeyError) as error:
            raise InvalidAlert("Invalid alert rule") from error
        observation = Observation.from_row(value["observation"]).validate(
            stations, rule, now
        )
        alert = cls(observation, rule)
        if value["event_id"] != alert.event_id or observation.aqi < rule.minimum_aqi:
            raise InvalidAlert("Event identity or rule eligibility mismatch")
        return alert

    def text(self):
        o = self.observation
        return (
            f"⚠️ AIRLAKE: {o.name} ({o.location_id})\n"
            f"UTC hour: {o.payload()['hour_start']}\n"
            f"PM2.5 mean: {o.pm25_avg:g} {o.units}; hourly MVP AQI: {o.aqi}\n"
            f"Rule: {self.rule.rule_id}/{self.rule.version}; revision: {self.event_id[:12]}\n"
            f"Gold snapshot: {o.source_snapshot_id}\n"
            "Simplified hourly mean; not official VN_AQI."
        )


def evaluate(source, rule, stations, now):
    """Preflight the entire bounded source before returning any publishable event."""
    rule.validate()
    rows = list(islice(source.read(), 1001))
    if not rows or len(rows) > 1000:
        raise InvalidAlert("Expected between 1 and 1000 Gold observations")
    observations = [Observation.from_row(r).validate(stations, rule, now) for r in rows]
    if len({(o.source_table, o.source_snapshot_id) for o in observations}) != 1:
        raise InvalidAlert("Read must identify one pinned Gold table snapshot")
    unique = {}
    for o in observations:
        key = (o.location_id, o.payload()["hour_start"])
        if key in unique and unique[key].payload() != o.payload():
            raise InvalidAlert("Conflicting duplicate Gold hour")
        unique[key] = o
    return [
        Alert(o, rule) for _, o in sorted(unique.items()) if o.aqi >= rule.minimum_aqi
    ]


def resource_arn(value, service, account, region):
    if not isinstance(account, str) or not re.fullmatch(r"[0-9]{12}", account):
        raise InvalidAlert("Expected AWS account is required")
    if not isinstance(region, str) or not re.fullmatch(
        r"[a-z]{2}-[a-z]+-[0-9]+", region
    ):
        raise InvalidAlert("Expected AWS region is required")
    prefix = f"arn:aws:{service}:{region}:{account}:"
    if (
        not isinstance(value, str)
        or not value.startswith(prefix)
        or not re.fullmatch(r"[A-Za-z0-9_.-]{1,256}", value.removeprefix(prefix))
    ):
        raise InvalidAlert("Resource ARN differs from expected account/region")
    return value


class SNSPublisher:
    def __init__(self, client, topic_arn, ledger, *, account, region, stations, clock):
        self.client, self.ledger = client, ledger
        self.topic_arn = resource_arn(topic_arn, "sns", account, region)
        if self.topic_arn.endswith(".fifo"):
            raise InvalidAlert("This adapter requires a standard SNS topic")
        self.stations, self.clock = stations, clock

    def publish(self, alert):
        alert.observation.validate(self.stations, alert.rule, self.clock())
        checked = Alert.parse(alert.payload(), self.stations, self.clock())
        key = (
            "sns#"
            + checked.event_id
            + "#"
            + hashlib.sha256(self.topic_arn.encode()).hexdigest()
        )
        claim = self.ledger.claim(key)
        if claim.status == "sent":
            return "duplicate"
        if claim.status != "acquired":
            raise RuntimeError("Publication lease busy; retry later")
        try:
            result = self.client.publish(
                TopicArn=self.topic_arn,
                Subject="AirLake hourly PM2.5 alert",
                Message=canonical(checked.payload()),
                MessageAttributes={
                    "event_id": {"DataType": "String", "StringValue": checked.event_id},
                    "location_id": {
                        "DataType": "String",
                        "StringValue": str(checked.observation.location_id),
                    },
                },
            )
            if not isinstance(result, dict) or not result.get("MessageId"):
                raise RuntimeError("SNS response did not confirm publication")
        except Exception:  # noqa: BLE001 -- sanitize service errors and preserve retry state
            self.ledger.retry(key, claim.owner)
            raise RuntimeError(
                "SNS publication failed or is uncertain; no automatic retry"
            ) from None
        self.ledger.complete(key, claim.owner)
        return "published"


def publish_batch(source, rule, stations, now, publisher):
    alerts = evaluate(source, rule, stations, now)
    return [publisher.publish(alert) for alert in alerts]
