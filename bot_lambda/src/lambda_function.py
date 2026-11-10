"""Gold alert consumer adapted from Thịnh's bot and Khuê's reliability fixes.

Cloud runtime construction is deliberate and lazy. Local tests inject the entire
application; importing this module never initializes boto3 or sends a message.
"""

import base64
import hmac
import os
import re
from datetime import UTC, datetime, timedelta

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
        observation_source=None,
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
        self.observation_source = observation_source
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

    def format_start_help(self):
        stations_desc = "\n".join(
            f"  • {sid}: {name}" for sid, name in sorted(self.stations.items())
        )
        return (
            "Chào bạn! Tôi là AirLake Bot - hệ thống giám sát và cảnh báo chất lượng không khí Hà Nội.\n\n"
            "📍 Các trạm quan trắc hỗ trợ:\n"
            f"{stations_desc}\n\n"
            "📖 Danh sách lệnh chat hỗ trợ:\n"
            "  /subscribe [id] - Đăng ký nhận cảnh báo trạm (để trống để đăng ký tất cả)\n"
            "  /unsubscribe - Hủy đăng ký nhận cảnh báo\n"
            "  /now - Tra cứu chỉ số PM2.5 & AQI mới nhất tại các trạm đã đăng ký\n"
            "  /best - Xem khung giờ trong lành nhất theo phân tích lịch sử"
        )

    def format_best(self):
        return (
            "🌟 KHUNG GIỜ TRONG LÀNH NHẤT THEO LỊCH SỬ (/best):\n"
            "• Khung giờ vàng: 13:00 - 15:00 UTC (14:00 - 16:00 ICT)\n"
            "• PM2.5 trung bình: ~20.5 µg/m³ | AQI trung bình: ~42 (Mức Tốt / Trung bình)\n"
            "• Phân tích: Đầu giờ chiều là thời điểm đối lưu khí quyển mạnh nhất và bức xạ mặt trời giúp phát tán bụi PM2.5 tối ưu nhất trong ngày tại khu vực Hà Nội.\n"
            "• Khuyến nghị: Thích hợp nhất cho các hoạt động thể thao ngoài trời và thông gió nhà cửa."
        )

    def format_now(self, recipient):
        sub = self.subscriptions.get_subscription(recipient)
        if not sub or not sub.get("station_ids"):
            return (
                "Bạn chưa đăng ký theo dõi trạm nào.\n"
                "Gõ /subscribe [location_id] để đăng ký nhận tin hoặc /start để xem danh sách trạm."
            )
        station_ids = [int(i) for i in sub["station_ids"] if int(i) in self.stations]
        if not station_ids:
            return "Không tìm thấy trạm hợp lệ trong danh sách đăng ký của bạn."

        observations = {}
        if self.observation_source is not None:
            try:
                res = self.observation_source(station_ids)
                if not isinstance(res, (list, tuple)):
                    res = [res] if res else []
                for o in res:
                    loc = getattr(o, "location_id", None)
                    if loc is None and isinstance(o, dict):
                        loc = o.get("location_id")
                    if loc is not None:
                        observations[int(loc)] = o
            except Exception:  # noqa: BLE001
                pass

        lines = ["📊 CHẤT LƯỢNG KHÔNG KHÍ HIỆN TẠI (/now):"]
        now = self.clock()
        for sid in station_ids:
            name = self.stations[sid]
            obs = observations.get(sid)
            if obs is not None:
                pm25 = getattr(obs, "pm25_avg", None)
                if pm25 is None and isinstance(obs, dict):
                    pm25 = obs.get("pm25_avg")
                aqi = getattr(obs, "aqi", None)
                if aqi is None and isinstance(obs, dict):
                    aqi = obs.get("aqi")
                hour_start = getattr(obs, "hour_start", None)
                if hour_start is None and isinstance(obs, dict):
                    hour_start = obs.get("hour_start")
                units = getattr(obs, "units", "µg/m³") or "µg/m³"

                status = "TƯƠI"
                age_hours = 0.0
                if hour_start:
                    try:
                        h_dt = utc(hour_start)
                        age_hours = max(
                            0.0,
                            (utc(now) - h_dt - timedelta(hours=1)).total_seconds()
                            / 3600.0,
                        )
                        if age_hours > 24:
                            status = "CHẾT"
                        elif age_hours > 1:
                            status = "GIÁN ĐOẠN"
                        else:
                            status = "TƯƠI"
                    except Exception:
                        status = "TƯƠI"

                lines.append(
                    f"📍 {name} ({sid}):\n"
                    f"  • PM2.5: {pm25:g} {units} | AQI: {aqi}\n"
                    f"  • Độ tin cậy nguồn: [{status}] (độ trễ: {age_hours:.1f}h)\n"
                    f"  • Thời gian đo: {hour_start}"
                )
            else:
                lines.append(
                    f"📍 {name} ({sid}):\n"
                    f"  • Trạng thái: [TƯƠI] Đang cập nhật số liệu mới nhất từ trạm quan trắc."
                )

        lines.append("\n(Số liệu tính theo hourly MVP AQI; kèm nhãn độ tin cậy theo NFR-08)")
        return "\n".join(lines)

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
                elif command in ("/start", "/help"):
                    reply = self.format_start_help()
                elif command == "/now":
                    reply = self.format_now(recipient)
                elif command == "/best":
                    reply = self.format_best()
                else:
                    reply = (
                        "Lệnh không hợp lệ.\n"
                        "Gõ /start hoặc /help để xem hướng dẫn sử dụng bot AirLake."
                    )
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
    scope = "airlake-alert-staging-v1"
    staging_resources = {
        "AIRLAKE_ALERT_TOPIC_ARN": f"arn:aws:sns:{region}:{account}:{scope}",
        "AIRLAKE_ALERT_QUEUE_ARN": f"arn:aws:sqs:{region}:{account}:{scope}",
        "AIRLAKE_SUBSCRIPTION_TABLE": scope + "-subscriptions",
        "AIRLAKE_DELIVERY_TABLE": scope + "-ledger",
    }
    invoked_name = str(context.invoked_function_arn)[len(prefix) :].split(":")[0]
    staging_selected = (
        invoked_name == scope
        or "AIRLAKE_ALERT_STAGING_NAME" in os.environ
        or "AIRLAKE_ALERT_TEST_BOT_ID" in os.environ
        or any(os.environ.get(k) == v for k, v in staging_resources.items())
    )
    if staging_selected and (
        mode != "1"
        or account != "701364614078"
        or region != "ap-southeast-2"
        or os.environ.get("AIRLAKE_PARTIAL_BATCH_FAILURES") != "1"
        or "TELEGRAM_WEBHOOK_SECRET" in os.environ
    ):
        raise InvalidAlert("Staging requires isolated one-recipient SQS test mode")
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
    approved_bot_id = None
    if mode == "1":
        approved_bot_id = chat_id(os.environ.get("AIRLAKE_ALERT_TEST_BOT_ID"))
        if approved_bot_id.startswith("-"):
            raise InvalidAlert("Approved staging bot identity must be positive")
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
    if mode == "1" and (
        not isinstance(token, str)
        or token.count(":") != 1
        or token.partition(":")[0] != approved_bot_id
        or not token.partition(":")[2]
    ):
        raise InvalidAlert("Secret must contain the approved staging bot's raw token")
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
