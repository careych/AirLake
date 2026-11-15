"""AirLake Telegram bot: webhook commands, alert evaluation and delivery.

One Lambda, three triggers:
- Function URL webhook (Telegram secret-token header required) -> commands/buttons
- EventBridge schedule {"action": "evaluate"} -> read the published projection,
  decide alerts per subscription, enqueue one SQS message per alert
- SQS -> send each alert once (idempotency key), retry transient failures,
  redrive to the DLQ after repeated failure
"""

import hashlib
import hmac
import json
import os
import pkgutil
from datetime import UTC, datetime

from ingestion.openaq import stamp, utc
from telegram_bot import messages as m
from telegram_bot.client import Rejected, Retryable, Telegram
from telegram_bot.policy import decide, initial_state
from telegram_bot.store import Store

STALE_PROJECTION_SECONDS = 1800  # refresh runs every 15 minutes
PATTERN = json.loads(pkgutil.get_data("telegram_bot", "hour_pattern.json"))


def now_utc():
    return datetime.now(UTC).replace(microsecond=0)


class Bot:
    def __init__(self, store, telegram, projection, queue, clock=now_utc):
        self.store, self.telegram, self.projection = store, telegram, projection
        self.queue, self.clock = queue, clock

    # ---- webhook -------------------------------------------------------
    def webhook(self, event, secret):
        headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
        supplied = headers.get("x-telegram-bot-api-secret-token", "")
        if not secret or not hmac.compare_digest(
            str(supplied).encode(), secret.encode()
        ):
            return {"statusCode": 403, "body": "Forbidden"}
        if event.get("requestContext", {}).get("http", {}).get("method") != "POST":
            return {"statusCode": 405, "body": "POST only"}
        body = event.get("body") or ""
        if len(body) > 100_000:
            return {"statusCode": 413, "body": "Too large"}
        try:
            update = json.loads(body)
            update_id = int(update["update_id"])
        except (ValueError, KeyError, TypeError):
            return {"statusCode": 400, "body": "Bad update"}
        if not self.store.once(f"update#{update_id}", days=2):
            return {
                "statusCode": 200,
                "body": "OK",
            }  # Telegram retry of a handled update
        try:
            self.handle(update)
        except Retryable:
            self.store.release(f"update#{update_id}")
            return {"statusCode": 503, "body": "Retry"}
        except Rejected:
            pass  # e.g. the user blocked the bot; nothing to retry
        except Exception as e:  # noqa: BLE001 -- odd update shapes must not cause endless Telegram retries
            print(
                json.dumps(
                    {
                        "metric": "AirLakeTelegramUpdateIgnored",
                        "error": type(e).__name__,
                    }
                )
            )
        return {"statusCode": 200, "body": "OK"}

    def handle(self, update):
        if "callback_query" in update:
            q = update["callback_query"]
            if q.get("message", {}).get("chat", {}).get("type", "private") != "private":
                return None
            chat = str(q["message"]["chat"]["id"])
            self.telegram.answer(q["id"])
            return self.button(chat, str(q.get("data", "")))
        message = update.get("message") or {}
        if message.get("chat", {}).get("type") != "private" or "text" not in message:
            return None  # groups and non-text updates are ignored
        chat = str(message["chat"]["id"])
        text = message["text"].strip()[:200]
        command = text.split()[0].split("@")[0].lower() if text else ""
        if not command.startswith("/"):
            return self.free_text(chat, text)
        self.store.clear_flow(chat)
        if command == "/start":
            payload = text.split()[1] if len(text.split()) > 1 else ""
            if (
                payload.startswith("st_")
                and payload[3:].isdigit()
                and int(payload[3:]) in m.STATIONS
            ):
                self.telegram.send(chat, m.welcome())
                return self.telegram.send(chat, *m.choose_threshold(int(payload[3:])))
            return self.telegram.send(chat, m.welcome(), m.MAIN_MENU)
        if command == "/help":
            return self.telegram.send(chat, m.HELP)
        if command == "/latest":
            return self.button(chat, "latest")
        if command in ("/subscribe", "/dangky"):
            return self.button(chat, "sub")
        if command in ("/status", "/settings"):
            return self.button(chat, "status")
        if command == "/hours":
            return self.button(chat, "hours")
        if command == "/unsubscribe":
            return self.button(chat, "unsub")
        return self.telegram.send(
            chat, "Mình chưa hiểu lệnh này. Gõ /help để xem các lệnh."
        )

    def free_text(self, chat, text):
        station = self.store.flow(chat)
        if station is None:
            return self.telegram.send(
                chat, "Chọn một mục bên dưới hoặc gõ /help.", m.MAIN_MENU
            )
        if not text.isdigit() or not 5 <= int(text) <= 500:
            return self.telegram.send(
                chat, "Vui lòng nhập một số từ 5 đến 500 (µg/m³)."
            )
        self.store.clear_flow(chat)
        return self.telegram.send(chat, *m.confirm(station, int(text)))

    def button(self, chat, data):
        parts = data.split(":")
        action = parts[0]
        if action == "latest":
            return self.telegram.send(
                chat, m.latest(self.projection(), self.clock()), m.MAIN_MENU
            )
        if action == "hours":
            return self.telegram.send(chat, m.hours(PATTERN))
        if action == "sub":
            return self.telegram.send(chat, *m.choose_station())
        if action == "status":
            text, markup = m.status(
                self.store.subscription(chat), self.projection(), self.clock()
            )
            return self.telegram.send(chat, text, markup)
        if action in ("st", "custom", "th", "ok"):
            station = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
            if station not in m.STATIONS:
                return self.telegram.send(
                    chat, "Trạm không hợp lệ. Gõ /subscribe để chọn lại."
                )
            if action == "st":
                return self.telegram.send(chat, *m.choose_threshold(station))
            if action == "custom":
                self.store.set_flow(chat, station)
                return self.telegram.send(
                    chat, "Nhập ngưỡng bạn muốn, là một số từ 5 đến 500 (µg/m³):"
                )
            threshold = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            if not 5 <= threshold <= 500:
                return self.telegram.send(
                    chat, "Ngưỡng không hợp lệ. Gõ /subscribe để chọn lại."
                )
            if action == "th":
                return self.telegram.send(chat, *m.confirm(station, threshold))
            self.store.subscribe(chat, station, threshold, initial_state())
            return self.telegram.send(
                chat, m.subscribed(station, threshold), m.MAIN_MENU
            )
        if action in ("pause", "resume"):
            if not self.store.subscription(chat):
                return self.telegram.send(
                    chat, "Bạn chưa đăng ký cảnh báo. Gõ /subscribe để bắt đầu."
                )
            self.store.set_active(chat, action == "resume")
            return self.telegram.send(
                chat,
                "Đã tạm dừng cảnh báo."
                if action == "pause"
                else "Đã bật lại cảnh báo.",
            )
        if action == "unsub":
            return self.telegram.send(chat, *m.unsubscribe_confirm())
        if action == "unsub_ok":
            self.store.unsubscribe(chat)
            return self.telegram.send(
                chat,
                "Đã hủy cảnh báo và xóa thông tin đăng ký của bạn. Gõ /start nếu muốn dùng lại.",
            )
        if action == "cancel":
            return self.telegram.send(chat, "Đã hủy thao tác.", m.MAIN_MENU)
        return self.telegram.send(chat, "Mình chưa hiểu lựa chọn này. Gõ /help.")

    # ---- alert evaluation (scheduled) -----------------------------------
    def evaluate(self):
        now = self.clock()
        projection = self.projection()
        if (
            now - utc(projection["published_at"])
        ).total_seconds() > STALE_PROJECTION_SECONDS:
            return {
                "status": "SKIPPED_STALE_PROJECTION",
                "projection": projection["published_at"],
                "queued": 0,
            }
        observations = {
            o["station_id"]: o
            for o in projection["observations"]
            if o.get("observed_at")
            and (now - utc(o["observed_at"])).total_seconds()
            <= (o.get("freshness") or {}).get("threshold_seconds", 10800)
        }  # freshness re-checked at evaluation time, not only at publication
        queued = 0
        for sub in self.store.active():
            obs = observations.get(sub["station_id"])
            if not obs:
                continue
            alert, state = decide(sub.get("state"), obs, sub["threshold"], now)
            if state == sub.get("state"):
                continue
            if alert:
                key = hashlib.sha256(
                    f"{sub['chat']}|{obs['station_id']}|{obs['observed_at']}|{sub['threshold']}".encode()
                ).hexdigest()
                self.queue.send(
                    {
                        "alert_id": key,
                        "chat": sub["chat"],
                        "station_id": obs["station_id"],
                        "pm25": obs["pm25"],
                        "threshold": sub["threshold"],
                        "observed_at": obs["observed_at"],
                    }
                )
                queued += 1
            # Enqueue first: a lost state update can only cause a duplicate
            # message, which the delivery idempotency key suppresses.
            self.store.save_state(sub["chat"], state, sub["version"])
        return {
            "status": "EVALUATED",
            "projection": projection["published_at"],
            "queued": queued,
        }

    # ---- delivery (SQS) -------------------------------------------------
    def deliver(self, records):
        failures = []
        for record in records:
            try:
                a = json.loads(record["body"])
                if a["station_id"] not in m.STATIONS:
                    continue
                if not self.store.once("alert#" + a["alert_id"]):
                    continue  # already delivered
                try:
                    text = m.alert(
                        a["station_id"],
                        a["pm25"],
                        a["threshold"],
                        a["observed_at"],
                        self.clock(),
                    )
                    self.telegram.send(a["chat"], text)
                except Retryable:
                    self.store.release("alert#" + a["alert_id"])
                    raise
                except Rejected:
                    self.store.set_active(a["chat"], False)  # blocked bot: stop alerts
            except Exception:  # noqa: BLE001 -- SQS retries the record, then the DLQ keeps it
                failures.append({"itemIdentifier": record["messageId"]})
        return {"batchItemFailures": failures}


class Queue:
    def __init__(self, sqs, url):
        self.sqs, self.url = sqs, url

    def send(self, message):
        self.sqs.send_message(QueueUrl=self.url, MessageBody=json.dumps(message))


def from_environment():
    import boto3

    secrets = boto3.client("secretsmanager")

    def secret(arn):
        value = secrets.get_secret_value(SecretId=arn)["SecretString"].strip()
        if not value or "\n" in value or value.startswith("{"):
            raise ValueError("Expected a plaintext secret")
        return value

    s3 = boto3.client("s3")

    def projection():
        body = s3.get_object(
            Bucket=os.environ["AIRLAKE_BUCKET"], Key="nowcast/v1/current.json"
        )["Body"].read(2_000_001)
        return json.loads(body)

    bot = Bot(
        Store(boto3.resource("dynamodb").Table(os.environ["AIRLAKE_TELEGRAM_TABLE"])),
        Telegram(secret(os.environ["AIRLAKE_TELEGRAM_TOKEN_SECRET_ARN"])),
        projection,
        Queue(boto3.client("sqs"), os.environ["AIRLAKE_TELEGRAM_QUEUE_URL"]),
    )
    return bot, secret(os.environ["AIRLAKE_TELEGRAM_WEBHOOK_SECRET_ARN"])


_cached = None


def lambda_handler(event, context):
    global _cached
    if _cached is None:
        _cached = from_environment()
    bot, webhook_secret = _cached
    if "Records" in event:
        return bot.deliver(event["Records"])
    if event.get("action") == "evaluate":
        result = bot.evaluate()
        print(
            json.dumps(
                {"metric": "AirLakeTelegramEvaluate", **result, "at": stamp(now_utc())}
            )
        )
        return result
    return bot.webhook(event, webhook_secret)
