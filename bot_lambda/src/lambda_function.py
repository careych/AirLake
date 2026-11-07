"""Gold alert consumer adapted from Thịnh's bot and Khuê's reliability fixes.

Cloud runtime construction is deliberate and lazy. Local tests inject the entire
application; importing this module never initializes boto3 or sends a message.
"""

import base64
import hmac
import os
import re
from datetime import UTC, datetime

from alerts.delivery import (
    DeliveryError,
    DynamoLedger,
    DynamoSubscriptions,
    TelegramSender,
    chat_id,
)
from alerts.gold import (
    Alert,
    InvalidAlert,
    Rule,
    integer,
    resource_arn,
    strict_json,
    utc,
)


def validate_stations(stations):
    if (
        not isinstance(stations, dict)
        or not stations
        or any(
            type(k) is not int
            or not 1 <= k <= 2**31 - 1
            or not isinstance(v, str)
            or not v.strip()
            or len(v) > 300
            for k, v in stations.items()
        )
    ):
        raise InvalidAlert("Known station names must be configured")


def test_recipient_policy(test_mode, recipient):
    if type(test_mode) is not bool:
        raise InvalidAlert("Recipient test mode must be explicit")
    if test_mode:
        return chat_id(recipient)
    if recipient is not None:
        raise InvalidAlert("Test recipient requires explicit test mode")
    return None


def observation_clock(test_mode, reference_time, live_clock):
    """Historical replay changes observation age only, never delivery lease/TTL time."""
    if reference_time is None:
        return live_clock
    if not test_mode:
        raise InvalidAlert("Historical reference time requires one-recipient test mode")
    reference = utc(reference_time)
    return lambda: reference


class Application:
    def __init__(
        self,
        subscriptions,
        sender,
        ledger,
        *,
        stations,
        rules,
        clock,
        queue_arn,
        topic_arn,
        account,
        region,
        partial_batch,
        webhook_secret,
        test_mode=False,
        test_recipient=None,
        historical_demo=False,
    ):
        validate_stations(stations)
        self.test_recipient = test_recipient_policy(test_mode, test_recipient)
        if type(historical_demo) is not bool or (historical_demo and not test_mode):
            raise InvalidAlert(
                "Historical delivery requires explicit recipient test mode"
            )
        self.historical_demo = historical_demo
        if type(partial_batch) is not bool:
            raise InvalidAlert("Partial-batch mode must be explicit")
        self.subscriptions, self.sender, self.ledger = subscriptions, sender, ledger
        self.stations, self.clock = stations, clock
        if not rules or any(not isinstance(r, Rule) for r in rules):
            raise InvalidAlert("Approved alert rules must be configured")
        self.rules = {(r.rule_id, r.version): r.validate() for r in rules}
        if len(self.rules) != len(rules):
            raise InvalidAlert("Duplicate configured rule version")
        self.queue_arn = resource_arn(queue_arn, "sqs", account, region)
        self.topic_arn = resource_arn(topic_arn, "sns", account, region)
        if self.topic_arn.endswith(".fifo") or self.queue_arn.endswith(".fifo"):
            raise InvalidAlert("This adapter requires standard SNS/SQS resources")
        self.partial_batch, self.webhook_secret = partial_batch, webhook_secret

    def parse_record(self, record):
        if (
            record.get("eventSource") != "aws:sqs"
            or record.get("eventSourceARN") != self.queue_arn
        ):
            raise InvalidAlert("Unexpected queue source")
        body = strict_json(record.get("body"))
        if isinstance(body, dict) and "Message" in body:
            if (
                body.get("Type") != "Notification"
                or body.get("TopicArn") != self.topic_arn
            ):
                raise InvalidAlert("Unexpected SNS notification source")
            body = strict_json(body["Message"])
        # Raw SNS delivery has the versioned event directly in the SQS body.
        alert = Alert.parse(body, self.stations, self.clock())
        if self.rules.get((alert.rule.rule_id, alert.rule.version)) != alert.rule:
            raise InvalidAlert("Alert rule is not approved for this consumer")
        return alert

    def deliver(self, record, remaining_ms=None):
        alert = self.parse_record(record)
        recipients = self.subscriptions.eligible(alert)
        if self.test_recipient is not None:
            # Eligibility remains required: the guard never creates a subscription.
            recipients = [r for r in recipients if r == self.test_recipient]
        failed = False
        for recipient in recipients:
            if remaining_ms is not None and remaining_ms() < 60_000:
                raise DeliveryError(
                    "Invocation budget insufficient; retry remaining work"
                )
            key = "telegram#" + alert.event_id + "#" + recipient
            claim = self.ledger.claim(key)
            if claim.status == "sent":
                continue
            if claim.status != "acquired":
                failed = True
                continue
            try:
                text = alert.text()
                if self.historical_demo:
                    text = (
                        "HISTORICAL DEMONSTRATION — NOT A CURRENT AIR-QUALITY WARNING\n"
                        + text
                    )
                self.sender.send(recipient, text)
            except Exception:  # noqa: BLE001 -- sanitize service errors and preserve retry state
                self.ledger.retry(key, claim.owner)
                failed = True
                continue
            # Failed/uncertain completion leaves the lease intact. Never claim
            # an atomic transaction between a remote send and this state write.
            self.ledger.complete(key, claim.owner)
        if failed:
            raise DeliveryError("Some recipient deliveries require retry")

    def sqs(self, event, remaining_ms=None):
        records = event.get("Records")
        if not isinstance(records, list) or len(records) > 10:
            raise InvalidAlert("Expected a bounded SQS batch")
        ids = [r.get("messageId") if isinstance(r, dict) else None for r in records]
        if any(not isinstance(i, str) or not i or len(i) > 128 for i in ids) or len(
            ids
        ) != len(set(ids)):
            raise InvalidAlert("Cannot acknowledge unidentified/duplicate SQS records")
        failures = []
        for record in records:
            try:
                self.deliver(record, remaining_ms)
            except Exception:  # noqa: BLE001 -- sanitize service errors and preserve retry state
                failures.append({"itemIdentifier": record["messageId"]})
        if failures and not self.partial_batch:
            raise DeliveryError("SQS batch failed; whole-batch retry is required")
        return {"batchItemFailures": failures}

    def webhook(self, event):
        headers = event.get("headers") or {}
        if not isinstance(headers, dict) or any(
            not isinstance(k, str) for k in headers
        ):
            return {"statusCode": 403, "body": "Forbidden"}
        secrets = [
            v
            for k, v in headers.items()
            if k.lower() == "x-telegram-bot-api-secret-token"
        ]
        supplied = secrets[0] if len(secrets) == 1 else ""
        if not self.webhook_secret:
            return {"statusCode": 503, "body": "Webhook is not configured"}
        if not isinstance(supplied, str) or not hmac.compare_digest(
            supplied.encode(), self.webhook_secret.encode()
        ):
            return {"statusCode": 403, "body": "Forbidden"}
        try:
            body = event.get("body", "")
            if event.get("isBase64Encoded"):
                if not isinstance(body, str) or len(body) > 300_000:
                    raise InvalidAlert("Webhook exceeds size limit")
                body = base64.b64decode(body, validate=True).decode()
            update = strict_json(body)
            if not isinstance(update, dict):
                raise InvalidAlert("Invalid Telegram update")
            update_id = integer(update.get("update_id"), 0, 2**63 - 1)
            message = update["message"]
            recipient = chat_id(str(message["chat"]["id"]))
            if self.test_recipient is not None and recipient != self.test_recipient:
                return {"statusCode": 403, "body": "Recipient outside test scope"}
            text = message["text"]
            if not isinstance(text, str) or len(text) > 1000:
                raise InvalidAlert("Invalid bot command")
            words = text.strip().split()
            if not words:
                raise InvalidAlert("Empty bot command")
            command = words[0].split("@", 1)[0]
            if command == "/subscribe":
                selected = (
                    list(self.stations)
                    if len(words) == 1
                    else [int(w) for w in words[1:]]
                )
                if not selected or any(i not in self.stations for i in selected):
                    raise InvalidAlert("Unknown station")
        except (ValueError, TypeError, KeyError, UnicodeError):
            return {"statusCode": 400, "body": "Invalid Telegram update or station"}
        key = f"webhook#{update_id}#{recipient}"
        try:
            claim = self.ledger.claim(key)
            if claim.status == "sent":
                return {"statusCode": 200, "body": "OK"}
            if claim.status != "acquired":
                return {"statusCode": 503, "body": "Retry later"}
            try:
                if command == "/subscribe":
                    created = self.subscriptions.subscribe(recipient, selected)
                    reply = (
                        "Bạn đã đăng ký nhận cảnh báo thành công!"
                        if created
                        else "Bạn đã đăng ký rồi!"
                    )
                elif command == "/unsubscribe":
                    self.subscriptions.unsubscribe(recipient)
                    reply = "Đã hủy đăng ký cảnh báo."
                else:
                    reply = "Gõ /subscribe [location_id] để nhận cảnh báo; /unsubscribe để hủy."
                self.sender.send(recipient, reply)
            except Exception:
                self.ledger.retry(key, claim.owner)
                raise
            self.ledger.complete(key, claim.owner)
        except Exception:  # noqa: BLE001 -- sanitize service errors and preserve retry state
            return {"statusCode": 503, "body": "Bot operation requires retry"}
        return {"statusCode": 200, "body": "OK"}


def application_from_environment(context=None):
    if os.environ.get("AIRLAKE_ALERT_DELIVERY_ENABLED") != "1":
        raise InvalidAlert("Live alert delivery is disabled")
    region, account = os.environ["AWS_REGION"], os.environ["AIRLAKE_ACCOUNT_ID"]
    prefix = f"arn:aws:lambda:{region}:{account}:function:"
    if not str(getattr(context, "invoked_function_arn", "")).startswith(prefix):
        raise InvalidAlert("Lambda invocation differs from expected account/region")
    if any(
        k in os.environ
        for k in (
            "AWS_PROFILE",
            "AWS_DEFAULT_PROFILE",
            "AWS_CONFIG_FILE",
            "AWS_SHARED_CREDENTIALS_FILE",
        )
    ) or any(k.startswith("AWS_ENDPOINT_URL") for k in os.environ):
        raise InvalidAlert("Only the Lambda execution role is supported")
    resource_arn(os.environ["AIRLAKE_ALERT_TOPIC_ARN"], "sns", account, region)
    resource_arn(os.environ["AIRLAKE_ALERT_QUEUE_ARN"], "sqs", account, region)
    # Lambda supplies SDK/role credentials; no clients exist during imports/tests.
    from boto3 import Session
    from botocore.config import Config

    stations = {
        int(k): v for k, v in strict_json(os.environ["AIRLAKE_ALERT_STATIONS"]).items()
    }
    rules = [
        Rule(**r).validate() for r in strict_json(os.environ["AIRLAKE_ALERT_RULES"])
    ]
    validate_stations(stations)
    if not rules or len({(r.rule_id, r.version) for r in rules}) != len(rules):
        raise InvalidAlert("Approved rule versions must be unique and nonempty")
    from bot_lambda.src.create_table import table_request

    table_request(os.environ["AIRLAKE_SUBSCRIPTION_TABLE"])
    table_request(os.environ["AIRLAKE_DELIVERY_TABLE"], delivery_ledger=True)
    if os.environ["AIRLAKE_SUBSCRIPTION_TABLE"] == os.environ["AIRLAKE_DELIVERY_TABLE"]:
        raise InvalidAlert("Subscription and delivery tables must be distinct")
    if os.environ["AIRLAKE_ALERT_TOPIC_ARN"].endswith(".fifo") or os.environ[
        "AIRLAKE_ALERT_QUEUE_ARN"
    ].endswith(".fifo"):
        raise InvalidAlert("Standard SNS/SQS resources are required")
    mode = os.environ.get("AIRLAKE_ALERT_TEST_MODE", "0")
    if mode not in ("0", "1"):
        raise InvalidAlert("Recipient test mode must be 0 or 1")
    recipient = test_recipient_policy(
        mode == "1", os.environ.get("AIRLAKE_ALERT_TEST_RECIPIENT")
    )
    alert_clock = observation_clock(
        mode == "1",
        os.environ.get("AIRLAKE_ALERT_TEST_REFERENCE_TIME"),
        lambda: datetime.now(UTC),
    )
    secret_arn = os.environ.get("TELEGRAM_TOKEN_SECRET_ARN")
    if mode == "1":
        scope = os.environ.get("AIRLAKE_ALERT_STAGING_NAME", "")
        if scope != "airlake-alert-staging-v1":
            raise InvalidAlert("A dedicated staging scope is required")
        expected = {
            "AIRLAKE_ALERT_TOPIC_ARN": f"arn:aws:sns:{region}:{account}:{scope}",
            "AIRLAKE_ALERT_QUEUE_ARN": f"arn:aws:sqs:{region}:{account}:{scope}",
            "AIRLAKE_SUBSCRIPTION_TABLE": scope + "-subscriptions",
            "AIRLAKE_DELIVERY_TABLE": scope + "-ledger",
        }
        if (
            any(os.environ.get(k) != v for k, v in expected.items())
            or not str(context.invoked_function_arn).startswith(prefix + scope + ":")
            and str(context.invoked_function_arn) != prefix + scope
        ):
            raise InvalidAlert("Test delivery must use isolated staging resources")
        if (
            "TELEGRAM_TOKEN" in os.environ
            or not isinstance(secret_arn, str)
            or not re.fullmatch(
                rf"arn:aws:secretsmanager:{region}:{account}:secret:airlake/alert-staging-v1/telegram-[A-Za-z0-9]{{6}}",
                secret_arn,
            )
        ):
            raise InvalidAlert(
                "Staging requires a separately approved test-bot secret reference"
            )
    session = Session(region_name=region)
    config = Config(
        retries={"total_max_attempts": 1, "mode": "standard"},
        connect_timeout=5,
        read_timeout=10,
    )
    if secret_arn:
        try:
            token = session.client("secretsmanager", config=config).get_secret_value(
                SecretId=secret_arn
            )["SecretString"]
        except Exception:  # noqa: BLE001 -- never expose secret lookup errors or values
            raise InvalidAlert("Test bot secret lookup failed") from None
    else:
        token = os.environ["TELEGRAM_TOKEN"]
    sender = TelegramSender(token)
    dynamo = session.resource("dynamodb", config=config)
    clock = lambda: datetime.now(UTC)
    ledger = DynamoLedger(
        dynamo.Table(os.environ["AIRLAKE_DELIVERY_TABLE"]),
        clock,
        lease_seconds=int(os.environ["AIRLAKE_DELIVERY_LEASE_SECONDS"]),
        retention_seconds=int(os.environ["AIRLAKE_DEDUPE_RETENTION_SECONDS"]),
    )
    return Application(
        DynamoSubscriptions(
            dynamo.Table(os.environ["AIRLAKE_SUBSCRIPTION_TABLE"]),
            stations,
            only_recipient=recipient,
        ),
        sender,
        ledger,
        stations=stations,
        rules=rules,
        clock=alert_clock,
        queue_arn=os.environ["AIRLAKE_ALERT_QUEUE_ARN"],
        topic_arn=os.environ["AIRLAKE_ALERT_TOPIC_ARN"],
        account=account,
        region=region,
        partial_batch=os.environ.get("AIRLAKE_PARTIAL_BATCH_FAILURES") == "1",
        webhook_secret=os.environ.get("TELEGRAM_WEBHOOK_SECRET"),
        test_mode=mode == "1",
        test_recipient=recipient,
        historical_demo=os.environ.get("AIRLAKE_ALERT_TEST_REFERENCE_TIME") is not None,
    )


def lambda_handler(event, context, *, app=None):
    application = app if app is not None else application_from_environment(context)
    remaining = getattr(context, "get_remaining_time_in_millis", None)
    return (
        application.sqs(event, remaining)
        if "Records" in event
        else application.webhook(event)
    )
