"""Injected Telegram transport and DynamoDB subscription/delivery adapters.

Pagination and bounded HTTP response checking adapt Khuê's fixes to Thịnh's bot.
No clients, credential lookups or service calls occur at import time.
"""

import json
import re
import urllib.request
import uuid
from dataclasses import dataclass
from decimal import Decimal

from alerts.gold import InvalidAlert, canonical, integer, utc


class DeliveryError(RuntimeError):
    """Sanitized retryable delivery/state failure; never includes transport URLs."""


def conditional_failure(error):
    return (
        getattr(error, "response", {}).get("Error", {}).get("Code")
        == "ConditionalCheckFailedException"
    )


def chat_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"-?[0-9]{1,20}", value):
        raise InvalidAlert("Invalid subscription chat identifier")
    if (
        not -(2**63) <= int(value) < 2**63
        or int(value) == 0
        or str(int(value)) != value
    ):
        raise InvalidAlert("Chat identifier must be canonical and nonzero")
    return value


class TelegramSender:
    def __init__(self, token, *, opener=urllib.request.urlopen, timeout=10):
        if not isinstance(token, str) or not re.fullmatch(
            r"[A-Za-z0-9:_-]{1,200}", token
        ):
            raise InvalidAlert("Telegram token must be configured")
        if type(timeout) is not int or not 1 <= timeout <= 10:
            raise InvalidAlert("Telegram timeout must be bounded")
        self._url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.opener, self.timeout = opener, timeout

    def send(self, recipient, text):
        chat_id(recipient)
        if not isinstance(text, str) or not text or len(text) > 3500:
            raise InvalidAlert("Notification text exceeds the bounded format")
        request = urllib.request.Request(
            self._url,
            data=json.dumps({"chat_id": recipient, "text": text}).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with self.opener(request, timeout=self.timeout) as response:
                raw = response.read(65537)
                if len(raw) > 65536:
                    raise DeliveryError("Telegram response exceeds size limit")
                payload = json.loads(raw)
                if not 200 <= response.status < 300 or payload.get("ok") is not True:
                    raise DeliveryError("Telegram rejected delivery")
        except Exception:  # noqa: BLE001 -- sanitize service errors and preserve retry state
            raise DeliveryError("Telegram request failed or is uncertain") from None


@dataclass(frozen=True)
class Claim:
    status: str
    owner: str = ""


class DynamoLedger:
    """Conditional per-destination leases; SENT survives ordinary queue retries.

    This is at-least-once delivery, not an atomic transaction with SNS/Telegram.
    A crash/timeout after remote acceptance and before SENT can cause a duplicate.
    TTL must exceed the approved queue/DLQ redrive horizon; never clear on reruns.
    """

    def __init__(self, table, clock, *, lease_seconds, retention_seconds):
        integer(lease_seconds, 60, 3600)
        integer(retention_seconds, lease_seconds + 1, 86400 * 365)
        self.table, self.clock = table, clock
        self.lease_seconds, self.retention_seconds = lease_seconds, retention_seconds

    def claim(self, key):
        now = int(utc(self.clock()).timestamp())
        owner = uuid.uuid4().hex
        try:
            self.table.put_item(
                Item={
                    "delivery_id": key,
                    "owner": owner,
                    "state": "INFLIGHT",
                    "lease_until": now + self.lease_seconds,
                    "expires_at": now + self.retention_seconds,
                },
                ConditionExpression="attribute_not_exists(delivery_id) OR (#s IN (:inflight, :retry) AND lease_until <= :now)",
                ExpressionAttributeNames={"#s": "state"},
                ExpressionAttributeValues={
                    ":inflight": "INFLIGHT",
                    ":retry": "RETRY",
                    ":now": now,
                },
            )
        except Exception as error:  # noqa: BLE001 -- injected SDK boundary; never expose credentials
            if not conditional_failure(error):
                raise DeliveryError("Delivery lease unavailable") from None
            try:
                item = self.table.get_item(
                    Key={"delivery_id": key}, ConsistentRead=True
                ).get("Item", {})
            except Exception:  # noqa: BLE001 -- sanitize service errors and preserve retry state
                raise DeliveryError("Delivery status unavailable") from None
            return Claim("sent" if item.get("state") == "SENT" else "busy")
        return Claim("acquired", owner)

    def _finish(self, key, owner, state):
        try:
            self.table.update_item(
                Key={"delivery_id": key},
                UpdateExpression="SET #s = :state, lease_until = :zero",
                ConditionExpression="#s = :inflight AND #o = :owner",
                ExpressionAttributeNames={"#s": "state", "#o": "owner"},
                ExpressionAttributeValues={
                    ":state": state,
                    ":zero": 0,
                    ":inflight": "INFLIGHT",
                    ":owner": owner,
                },
            )
        except Exception:  # noqa: BLE001 -- sanitize service errors and preserve retry state
            raise DeliveryError("Delivery state transition failed") from None

    def complete(self, key, owner):
        self._finish(key, owner, "SENT")

    def retry(self, key, owner):
        self._finish(key, owner, "RETRY")


def subscription_number(value):
    if isinstance(value, Decimal):
        if not value.is_finite() or value != value.to_integral_value():
            raise InvalidAlert("Invalid subscription threshold")
        value = int(value)
    return integer(value, 0, 500)


class DynamoSubscriptions:
    """Keep the teammate's chat_id key and paginate before any alert fanout."""

    def __init__(self, table, stations, *, max_pages=10, max_recipients=100):
        integer(max_pages, 1, 100)
        integer(max_recipients, 1, 1000)
        self.table, self.stations = table, stations
        self.max_pages, self.max_recipients = max_pages, max_recipients

    def eligible(self, alert):
        recipients = set()
        scan_kwargs = {"ConsistentRead": True, "Limit": 100}
        seen = set()
        for _ in range(self.max_pages):
            response = self.table.scan(**scan_kwargs)
            for item in response.get("Items", []):
                recipient = chat_id(item.get("chat_id"))
                enabled = item.get("enabled", True)
                if type(enabled) is not bool:
                    raise InvalidAlert("Invalid subscription enable flag")
                # Legacy /subscribe records meant all configured stations.
                station_ids = item.get("station_ids", [str(i) for i in self.stations])
                if (
                    not isinstance(station_ids, list)
                    or not station_ids
                    or any(
                        not isinstance(i, str)
                        or not i.isdecimal()
                        or int(i) not in self.stations
                        for i in station_ids
                    )
                ):
                    raise InvalidAlert("Invalid subscription station scope")
                minimum = item.get("minimum_aqi")
                if minimum is not None:
                    minimum = subscription_number(minimum)
                if (
                    enabled
                    and str(alert.observation.location_id) in station_ids
                    and (minimum is None or alert.observation.aqi >= minimum)
                ):
                    recipients.add(recipient)
                if len(recipients) > self.max_recipients:
                    raise DeliveryError(
                        "Fanout limit exceeded; split work before retry"
                    )
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                return sorted(recipients)
            marker = canonical(last_key)
            if marker in seen:
                raise DeliveryError("Repeated subscription pagination key")
            seen.add(marker)
            scan_kwargs["ExclusiveStartKey"] = last_key
        raise DeliveryError("Subscription scan page limit exceeded")

    def subscribe(self, recipient, station_ids):
        chat_id(recipient)
        if not station_ids or any(i not in self.stations for i in station_ids):
            raise InvalidAlert("Unknown subscription station")
        try:
            self.table.put_item(
                Item={
                    "chat_id": recipient,
                    "station_ids": [str(i) for i in sorted(set(station_ids))],
                    "enabled": True,
                },
                ConditionExpression="attribute_not_exists(chat_id)",
            )
        except Exception as error:  # noqa: BLE001 -- injected SDK boundary; never expose credentials
            if conditional_failure(error):
                return False
            raise DeliveryError("Subscription write failed") from None
        return True

    def unsubscribe(self, recipient):
        self.table.delete_item(Key={"chat_id": chat_id(recipient)})
