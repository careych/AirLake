"""Scoped AWS persistence; no SDK retry after uncertain writes."""

import json
import uuid
from datetime import timedelta
from decimal import Decimal

from live_data.contract import (
    ACCOUNT,
    BUCKET,
    MAX_INVOCATIONS,
    PREFIX,
    REGION,
    TABLE,
    encode,
    timestamp,
)


def clients():
    import boto3
    from botocore.config import Config

    session = boto3.Session(region_name=REGION)
    c = Config(retries={"total_max_attempts": 1}, connect_timeout=3, read_timeout=15)
    return {
        n: session.client(n, config=c) for n in ("dynamodb", "s3", "secretsmanager")
    }


def pack(item):
    from boto3.dynamodb.types import TypeSerializer

    return {k: TypeSerializer().serialize(v) for k, v in item.items()}


def unpack(item):
    from boto3.dynamodb.types import TypeDeserializer

    value = {k: TypeDeserializer().deserialize(v) for k, v in item.items()}
    # DynamoDB decodes numeric attributes as Decimal; freshness expects the
    # integral cadence written by this poller, not the SDK's transport type.
    cadence = value.get("cadence_seconds")
    if cadence is not None:
        if (
            not isinstance(cadence, Decimal)
            or not cadence.is_finite()
            or cadence != cadence.to_integral_value()
            or not 1 <= cadence <= 86400
        ):
            raise ValueError("Invalid stored observation cadence")
        value["cadence_seconds"] = int(cadence)
    return value


def conditional(error):
    return (
        getattr(error, "response", {}).get("Error", {}).get("Code")
        == "ConditionalCheckFailedException"
    )


class AWSStore:
    def __init__(self, c):
        self.ddb, self.s3 = c["dynamodb"], c["s3"]

        # Conditional PUT header also works on older managed Lambda SDK models.
        def no_overwrite(params, **kwargs):
            params["headers"]["If-None-Match"] = "*"

        self.s3.meta.events.register("before-call.s3.PutObject", no_overwrite)

    def read(self, pk, sk):
        item = self.ddb.get_item(
            TableName=TABLE, Key=pack({"pk": pk, "sk": sk}), ConsistentRead=True
        ).get("Item")
        return unpack(item) if item else {}

    def acquire(self, now):
        owner = uuid.uuid4().hex
        self.ddb.update_item(
            TableName=TABLE,
            Key=pack({"pk": "RUN", "sk": "LEASE"}),
            UpdateExpression="SET owner_id=:owner, lease_until=:until, active=:active ADD attempts :one",
            ConditionExpression="attribute_not_exists(halted) AND (attribute_not_exists(active) OR active=:idle) AND (attribute_not_exists(lease_until) OR lease_until < :now) AND (attribute_not_exists(attempts) OR attempts < :max)",
            ExpressionAttributeValues=pack(
                {
                    ":owner": owner,
                    ":active": True,
                    ":idle": False,
                    ":until": int((now + timedelta(seconds=240)).timestamp()),
                    ":now": int(now.timestamp()),
                    ":one": 1,
                    ":max": MAX_INVOCATIONS,
                }
            ),
        )
        return owner

    def halt(self, owner):
        self.ddb.update_item(
            TableName=TABLE,
            Key=pack({"pk": "RUN", "sk": "LEASE"}),
            UpdateExpression="SET halted=:halt",
            ConditionExpression="owner_id=:owner",
            ExpressionAttributeValues=pack({":halt": True, ":owner": owner}),
        )

    def release(self, owner):
        self.ddb.update_item(
            TableName=TABLE,
            Key=pack({"pk": "RUN", "sk": "LEASE"}),
            UpdateExpression="SET lease_until=:zero, active=:idle",
            ConditionExpression="owner_id=:owner",
            ExpressionAttributeValues=pack(
                {":zero": 0, ":owner": owner, ":idle": False}
            ),
        )

    def cursor(self, sensor):
        value = self.read("CURSOR", str(sensor)).get("value", {})
        if "page" in value:
            value["page"] = int(value["page"])
        return value

    def save_cursor(self, sensor, value, owner):
        self.ddb.transact_write_items(
            TransactItems=[
                {
                    "ConditionCheck": {
                        "TableName": TABLE,
                        "Key": pack({"pk": "RUN", "sk": "LEASE"}),
                        "ConditionExpression": "owner_id=:owner",
                        "ExpressionAttributeValues": pack({":owner": owner}),
                    }
                },
                {
                    "Put": {
                        "TableName": TABLE,
                        "Item": pack(
                            {"pk": "CURSOR", "sk": str(sensor), "value": value}
                        ),
                    }
                },
            ]
        )

    def raw(self, key, value):
        if not key.startswith(PREFIX + "bronze/"):
            raise ValueError("Raw key escaped scope")
        try:
            self.s3.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=encode(value),
                ExpectedBucketOwner=ACCOUNT,
                ServerSideEncryption="AES256",
            )
            return True, value
        except Exception as error:
            if getattr(error, "response", {}).get("Error", {}).get("Code") not in (
                "PreconditionFailed",
                "412",
            ):
                raise
        raw = self.s3.get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)[
            "Body"
        ].read(100001)
        if len(raw) > 100000:
            raise ValueError("Oversized raw object")
        existing = json.loads(raw)
        if (
            existing.get("record") != value["record"]
            or existing.get("record_sha256") != value["record_sha256"]
        ):
            raise ValueError(
                "Source correction conflict; no overwrite of first accepted observation"
            )
        return False, existing

    def latest(self, location, value, key):
        r = value["record"]
        item = {
            "pk": "LATEST",
            "sk": str(location),
            "location_id": location,
            "sensor_id": int(r["sensors_id"]),
            "station": r["location"],
            "observed_at": value["observed_at"],
            "ingested_at": value["ingested_at"],
            "pm25": Decimal(r["value"]),
            "units": r["units"],
            "source": value["source"],
            "raw_key": key,
            "record_sha256": value["record_sha256"],
            "cadence_seconds": value["source"].get("observed_spacing_seconds"),
        }
        try:
            self.ddb.put_item(
                TableName=TABLE,
                Item=pack(item),
                ConditionExpression="attribute_not_exists(observed_at) OR observed_at < :observed",
                ExpressionAttributeValues=pack({":observed": value["observed_at"]}),
            )
        except Exception as error:
            if not conditional(error):
                raise

    def poll_status(self, location, now, status):
        self.ddb.put_item(
            TableName=TABLE,
            Item=pack(
                {
                    "pk": "POLL",
                    "sk": str(location),
                    "last_poll_at": timestamp(now),
                    "status": status,
                }
            ),
        )
