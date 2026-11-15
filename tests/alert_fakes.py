"""Offline service doubles; fixtures never initialize AWS or real sockets."""

import copy
import json
from datetime import UTC, datetime
from pathlib import Path

from alerts.delivery import DeliveryError, DynamoLedger, DynamoSubscriptions
from alerts.gold import METHOD, UNITS, Rule
from bot_lambda.src.lambda_function import Application

ROOT = Path(__file__).resolve().parents[1]
ACCOUNT = "000000000000"
REGION = "ap-southeast-2"
TOPIC = f"arn:aws:sns:{REGION}:{ACCOUNT}:fixture-alerts"
QUEUE = f"arn:aws:sqs:{REGION}:{ACCOUNT}:fixture-alerts"
RULE = Rule("fixture-only", "test-v1", 40, 7200)


def gold_fixture():
    evidence = json.loads(
        (ROOT / "tests/evidence/real_openaq_cloud_2026-11-07.json").read_text()
    )
    local = json.loads(
        (ROOT / "tests/evidence/real_openaq_2026-11-06.json").read_text()
    )
    fact = local["hourly_rows"][0]
    row = dict(
        fact,
        name=local["dimension_rows"][0]["name"],
        units=UNITS,
        aqi_method=METHOD,
        source_table=evidence["table_states"]["HOURLY_TABLE"]["identifier"],
        source_snapshot_id=str(
            evidence["table_states"]["HOURLY_TABLE"]["before_unchanged_rerun"][
                "snapshot_ids"
            ][0]
        ),
    )
    return row


class Clock:
    def __init__(self):
        self.now = datetime(2025, 7, 3, 16, 30, tzinfo=UTC)

    def __call__(self):
        return self.now


class Source:
    def __init__(self, rows):
        self.rows = rows

    def read(self):
        return copy.deepcopy(self.rows)


class ConditionalFailure(Exception):
    def __init__(self):
        self.response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class LedgerTable:
    """Small DynamoDB conditional-write double, not a live SDK validation."""

    def __init__(self):
        self.items = {}
        self.fail_complete = False
        self.calls = []

    def put_item(self, **kwargs):
        self.calls.append(("put", kwargs))
        item = kwargs["Item"]
        old = self.items.get(item["delivery_id"])
        if old and not (
            old["state"] in ("RETRY", "INFLIGHT")
            and old["lease_until"] <= kwargs["ExpressionAttributeValues"][":now"]
        ):
            raise ConditionalFailure()
        self.items[item["delivery_id"]] = copy.deepcopy(item)
        return {}

    def get_item(self, **kwargs):
        assert kwargs["ConsistentRead"] is True
        return {"Item": copy.deepcopy(self.items.get(kwargs["Key"]["delivery_id"], {}))}

    def update_item(self, **kwargs):
        self.calls.append(("update", kwargs))
        item = self.items[kwargs["Key"]["delivery_id"]]
        values = kwargs["ExpressionAttributeValues"]
        if item["state"] != "INFLIGHT" or item["owner"] != values[":owner"]:
            raise ConditionalFailure()
        if self.fail_complete and values[":state"] == "SENT":
            self.fail_complete = False
            raise TimeoutError("synthetic state-write timeout")
        item.update(state=values[":state"], lease_until=0)
        return {}


class SubscriptionTable:
    def __init__(self, pages=None):
        self.pages = pages or [[{"chat_id": "101", "station_ids": ["4946812"]}]]
        self.scans = []
        self.writes = []

    def scan(self, **kwargs):
        self.scans.append(kwargs)
        assert kwargs["ConsistentRead"] is True
        page = int(kwargs.get("ExclusiveStartKey", {}).get("chat_id", "0"))
        response = {"Items": copy.deepcopy(self.pages[page])}
        if page + 1 < len(self.pages):
            response["LastEvaluatedKey"] = {"chat_id": str(page + 1)}
        return response

    def put_item(self, **kwargs):
        self.writes.append(kwargs)
        item = kwargs["Item"]
        if any(r["chat_id"] == item["chat_id"] for p in self.pages for r in p):
            raise ConditionalFailure()
        self.pages[0].append(copy.deepcopy(item))
        return {}

    def delete_item(self, **kwargs):
        self.pages = [
            [r for r in p if r["chat_id"] != kwargs["Key"]["chat_id"]]
            for p in self.pages
        ]


class Telegram:
    def __init__(self):
        self.attempts = []
        self.sent = []
        self.fail_for = set()

    def send(self, chat_id, text):
        self.attempts.append((chat_id, text))
        if chat_id in self.fail_for:
            raise DeliveryError("Synthetic Telegram failure")
        self.sent.append((chat_id, text))


class SNS:
    def __init__(self):
        self.messages = []
        self.fail = False

    def publish(self, **kwargs):
        self.messages.append(kwargs)
        if self.fail:
            raise TimeoutError("Synthetic SNS timeout")
        return {"MessageId": "fixture-sns-id"}


def record(payload, *, message_id="sqs-1", raw=False):
    body = (
        payload
        if raw
        else {
            "Type": "Notification",
            "TopicArn": TOPIC,
            "Message": json.dumps(payload, allow_nan=False),
        }
    )
    return {
        "messageId": message_id,
        "eventSource": "aws:sqs",
        "eventSourceARN": QUEUE,
        "body": json.dumps(body, allow_nan=False),
    }


def setup_app(pages=None, *, partial=True):
    row = gold_fixture()
    clock, table, sender = Clock(), LedgerTable(), Telegram()
    subs_table = SubscriptionTable(pages)
    stations = {
        row["location_id"]: row["name"],
        4946813: "Synthetic second configured station",
    }
    ledger = DynamoLedger(table, clock, lease_seconds=60, retention_seconds=86400)
    subscriptions = DynamoSubscriptions(subs_table, stations)
    app = Application(
        subscriptions,
        sender,
        ledger,
        stations=stations,
        rules=[RULE],
        clock=clock,
        queue_arn=QUEUE,
        topic_arn=TOPIC,
        account=ACCOUNT,
        region=REGION,
        partial_batch=partial,
        webhook_secret="fixture-webhook-secret",
    )
    return row, clock, table, subs_table, sender, ledger, app
