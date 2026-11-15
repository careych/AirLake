"""Telegram product: onboarding, alert policy, delivery; fakes only, no network."""

import json
import unittest
from datetime import UTC, datetime, timedelta

from telegram_bot import messages as m
from telegram_bot.app import Bot
from telegram_bot.client import Rejected, Retryable
from telegram_bot.policy import decide, initial_state
from telegram_bot.store import Store

T0 = datetime(2026, 11, 15, 3, 10, tzinfo=UTC)
CHAT = "123456789"
SECRET = "SYNTHETIC-webhook-secret"


class ConditionFailed(Exception):
    response = {"Error": {"Code": "ConditionalCheckFailedException"}}  # noqa: RUF012


class FakeTable:
    def __init__(self):
        self.items = {}

    def get_item(self, Key):
        item = self.items.get(Key["pk"])
        return {"Item": dict(item)} if item else {}

    def put_item(self, Item, ConditionExpression=None):
        if (
            ConditionExpression == "attribute_not_exists(pk)"
            and Item["pk"] in self.items
        ):
            raise ConditionFailed()
        self.items[Item["pk"]] = dict(Item)

    def delete_item(self, Key):
        self.items.pop(Key["pk"], None)

    def update_item(
        self,
        Key,
        UpdateExpression,
        ExpressionAttributeValues,
        ConditionExpression=None,
        ExpressionAttributeNames=None,
    ):
        item = self.items.get(Key["pk"])
        v = ExpressionAttributeValues
        if item is None or (
            ConditionExpression == "version = :v" and item["version"] != v[":v"]
        ):
            raise ConditionFailed()
        if ":a" in v:
            item.update(active=v[":a"], version=item["version"] + 1)
        else:
            item.update(state=v[":s"], version=v[":next"])

    def scan(self, FilterExpression, ExpressionAttributeValues, ExclusiveStartKey=None):
        return {
            "Items": [
                dict(i)
                for k, i in self.items.items()
                if k.startswith("sub#") and i.get("active") is True
            ]
        }


class FakeTelegram:
    def __init__(self):
        self.sent, self.answered, self.fail = [], [], None

    def send(self, chat, text, markup=None):
        if self.fail:
            raise self.fail
        self.sent.append((chat, text, markup))

    def answer(self, callback_id):
        self.answered.append(callback_id)


class FakeQueue:
    def __init__(self):
        self.messages = []

    def send(self, message):
        self.messages.append(message)


def observation(
    station=6123215,
    value=60.0,
    at="2026-11-15T03:00:00Z",
    freshness="recent",
    status="ok",
):
    return {
        "kind": "observed",
        "station_id": station,
        "pm25": value,
        "observed_at": at,
        "fetch_status": status,
        "freshness": {"status": freshness},
    }


def projection(*obs):
    return {
        "published_at": "2026-11-15T03:05:00Z",
        "observations": list(obs) or [observation()],
        "model": {"current": {"pm25": 30.0, "valid_at": "2026-11-15T03:00:00Z"}},
    }


class Harness:
    def __init__(self):
        self.clock = [T0]
        self.table, self.telegram, self.queue = FakeTable(), FakeTelegram(), FakeQueue()
        self.current = projection()
        self.store = Store(self.table, clock=lambda: self.clock[0].timestamp())
        self.bot = Bot(
            self.store,
            self.telegram,
            lambda: self.current,
            self.queue,
            clock=lambda: self.clock[0],
        )
        self.update_id = 0

    def post(self, update, secret=SECRET):
        self.update_id += 1
        update = {"update_id": self.update_id, **update}
        return self.bot.webhook(
            {
                "headers": {"X-Telegram-Bot-Api-Secret-Token": secret},
                "requestContext": {"http": {"method": "POST"}},
                "body": json.dumps(update),
            },
            SECRET,
        )

    def text(self, text, chat_type="private"):
        return self.post(
            {"message": {"chat": {"id": int(CHAT), "type": chat_type}, "text": text}}
        )

    def tap(self, data):
        return self.post(
            {
                "callback_query": {
                    "id": "cb",
                    "data": data,
                    "message": {"chat": {"id": int(CHAT)}},
                }
            }
        )

    def last(self):
        return self.telegram.sent[-1][1]


class WebhookTests(unittest.TestCase):
    def test_secret_token_and_method_are_required(self):
        h = Harness()
        self.assertEqual(h.post({"message": {}}, secret="wrong")["statusCode"], 403)
        self.assertEqual(
            h.bot.webhook({"headers": {}, "body": "{}"}, SECRET)["statusCode"], 403
        )
        get = {
            "headers": {"x-telegram-bot-api-secret-token": SECRET},
            "requestContext": {"http": {"method": "GET"}},
            "body": "{}",
        }
        self.assertEqual(h.bot.webhook(get, SECRET)["statusCode"], 405)
        self.assertEqual(h.telegram.sent, [])

    def test_start_shows_menu_and_groups_are_ignored(self):
        h = Harness()
        h.text("/start")
        self.assertIn("2 điểm đo", h.last())
        self.assertIn("inline_keyboard", h.telegram.sent[-1][2])
        h.text("/start", chat_type="group")
        self.assertEqual(len(h.telegram.sent), 1)

    def test_repeated_update_is_handled_once(self):
        h = Harness()
        body = {
            "update_id": 7,
            "message": {"chat": {"id": int(CHAT), "type": "private"}, "text": "/help"},
        }
        event = {
            "headers": {"x-telegram-bot-api-secret-token": SECRET},
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps(body),
        }
        h.bot.webhook(event, SECRET)
        h.bot.webhook(event, SECRET)
        self.assertEqual(len(h.telegram.sent), 1)

    def test_transient_send_failure_lets_telegram_retry_the_update(self):
        h = Harness()
        h.telegram.fail = Retryable("synthetic")
        body = {
            "update_id": 9,
            "message": {"chat": {"id": int(CHAT), "type": "private"}, "text": "/help"},
        }
        event = {
            "headers": {"x-telegram-bot-api-secret-token": SECRET},
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps(body),
        }
        self.assertEqual(h.bot.webhook(event, SECRET)["statusCode"], 503)
        h.telegram.fail = None
        self.assertEqual(h.bot.webhook(event, SECRET)["statusCode"], 200)
        self.assertEqual(len(h.telegram.sent), 1)


class OnboardingTests(unittest.TestCase):
    def test_subscribe_status_edit_pause_and_unsubscribe(self):
        h = Harness()
        h.tap("sub")
        self.assertIn("trạm nào", h.last())
        h.tap("st:6123215")
        self.assertIn("µg/m³", h.last())
        h.tap("th:6123215:50")
        self.assertIn("Xác nhận", h.last())
        self.assertIsNone(
            h.store.subscription(CHAT)
        )  # nothing stored before confirmation
        h.tap("ok:6123215:50")
        sub = h.store.subscription(CHAT)
        self.assertEqual(
            (sub["station_id"], sub["threshold"], sub["active"]), (6123215, 50, True)
        )
        h.text("/status")
        self.assertIn("Ngưỡng: 50 µg/m³", h.last())
        h.tap("ok:99385:35")
        self.assertEqual(h.store.subscription(CHAT)["station_id"], 99385)
        h.tap("pause")
        self.assertFalse(h.store.subscription(CHAT)["active"])
        h.tap("resume")
        self.assertTrue(h.store.subscription(CHAT)["active"])
        h.text("/unsubscribe")
        self.assertIsNotNone(h.store.subscription(CHAT))  # confirmation required
        h.tap("unsub_ok")
        self.assertIsNone(h.store.subscription(CHAT))
        self.assertEqual(h.telegram.answered.count("cb"), 8)

    def test_custom_threshold_validation(self):
        h = Harness()
        h.tap("custom:99385")
        h.text("abc")
        self.assertIn("từ 5 đến 500", h.last())
        h.text("700")
        self.assertIn("từ 5 đến 500", h.last())
        h.text("60")
        self.assertIn("Ngưỡng: 60 µg/m³", h.last())

    def test_unknown_station_and_bad_threshold_are_refused(self):
        h = Harness()
        h.tap("ok:4946812:50")
        h.tap("ok:6123215:9999")
        self.assertIsNone(h.store.subscription(CHAT))

    def test_latest_separates_measurements_from_model_and_hours_are_historical(self):
        h = Harness()
        h.current = projection(observation(), observation(99385, None, None, "no_data"))
        h.text("/latest")
        text = h.last()
        self.assertIn("60,0 µg/m³", text)
        self.assertIn("chưa có số đo", text)
        self.assertIn("không phải số đo", text)
        h.text("/hours")
        self.assertIn("không phải dự báo", h.last())


class PolicyTests(unittest.TestCase):
    def test_one_alert_per_crossing_with_rearm_and_cooldown(self):
        s, t = initial_state(), T0
        a, s = decide(s, observation(value=60, at="2026-11-15T03:00:00Z"), 50, t)
        self.assertTrue(a)
        a, s = decide(s, observation(value=60, at="2026-11-15T03:00:00Z"), 50, t)
        self.assertFalse(a)  # same observation
        a, s = decide(
            s,
            observation(value=80, at="2026-11-15T04:00:00Z"),
            50,
            t + timedelta(hours=1),
        )
        self.assertFalse(a)  # still above: no repeat
        a, s = decide(
            s,
            observation(value=45, at="2026-11-15T05:00:00Z"),
            50,
            t + timedelta(hours=2),
        )
        self.assertFalse(s["armed"])  # 45 is not below 80% of 50
        a, s = decide(
            s,
            observation(value=30, at="2026-11-15T05:30:00Z"),
            50,
            t + timedelta(hours=2, minutes=30),
        )
        self.assertTrue(s["armed"])
        a, s = decide(
            s,
            observation(value=55, at="2026-11-15T05:45:00Z"),
            50,
            t + timedelta(hours=2, minutes=45),
        )
        self.assertFalse(a)  # re-armed but within the 3-hour cooldown
        a, s = decide(
            s,
            observation(value=56, at="2026-11-15T06:10:00Z"),
            50,
            t + timedelta(hours=3, minutes=10),
        )
        self.assertTrue(a)

    def test_community_sensor_needs_two_readings_ten_minutes_apart(self):
        s = initial_state()
        a, s = decide(s, observation(99385, 60, "2026-11-15T03:00:00Z"), 50, T0)
        self.assertFalse(a)
        a, s = decide(s, observation(99385, 61, "2026-11-15T03:05:00Z"), 50, T0)
        self.assertFalse(a)
        a, s = decide(s, observation(99385, 62, "2026-11-15T03:12:00Z"), 50, T0)
        self.assertTrue(a)

    def test_stale_failed_or_model_values_never_alert(self):
        for obs in (
            observation(freshness="stale"),
            observation(status="fetch_failed"),
            observation(status="no_new_reading"),
            {**observation(), "kind": "model"},
            observation(value=None),
        ):
            self.assertFalse(decide(initial_state(), obs, 50, T0)[0])


class EvaluateAndDeliverTests(unittest.TestCase):
    def subscribed(self, threshold=50):
        h = Harness()
        h.tap(f"ok:6123215:{threshold}")
        h.telegram.sent.clear()
        return h

    def test_full_chain_alert_sent_once_with_real_values(self):
        h = self.subscribed()
        self.assertEqual(h.bot.evaluate()["queued"], 1)
        self.assertEqual(h.bot.evaluate()["queued"], 0)  # repeated evaluation
        records = [{"messageId": "1", "body": json.dumps(h.queue.messages[0])}]
        self.assertEqual(h.bot.deliver(records), {"batchItemFailures": []})
        self.assertEqual(
            h.bot.deliver(records), {"batchItemFailures": []}
        )  # SQS redelivery
        self.assertEqual(len(h.telegram.sent), 1)
        chat, text, _ = h.telegram.sent[0]
        self.assertEqual(chat, CHAT)
        for part in (
            "PM2.5: 60,0 µg/m³",
            "Ngưỡng bạn chọn: 50 µg/m³",
            "Đo lúc: 10:00 15/11",
            "/unsubscribe",
        ):
            self.assertIn(part, text)
        for jargon in ("Lambda", "queue", "Iceberg", "Glue", "threshold", "an toàn"):
            self.assertNotIn(jargon, text)

    def test_paused_or_unsubscribed_users_get_nothing(self):
        h = self.subscribed()
        h.tap("pause")
        self.assertEqual(h.bot.evaluate()["queued"], 0)
        h.tap("resume")
        h.tap("unsub_ok")
        self.assertEqual(h.bot.evaluate()["queued"], 0)

    def test_below_threshold_or_stale_does_not_queue(self):
        h = self.subscribed(threshold=100)
        self.assertEqual(h.bot.evaluate()["queued"], 0)
        h = self.subscribed()
        h.current = projection(observation(freshness="stale"))
        self.assertEqual(h.bot.evaluate()["queued"], 0)

    def test_transient_failure_retries_and_blocked_user_is_paused(self):
        h = self.subscribed()
        h.bot.evaluate()
        record = [{"messageId": "1", "body": json.dumps(h.queue.messages[0])}]
        h.telegram.fail = Retryable("synthetic 429")
        self.assertEqual(
            h.bot.deliver(record), {"batchItemFailures": [{"itemIdentifier": "1"}]}
        )
        h.telegram.fail = None
        self.assertEqual(h.bot.deliver(record), {"batchItemFailures": []})
        self.assertEqual(len(h.telegram.sent), 1)
        h2 = self.subscribed()
        h2.bot.evaluate()
        h2.telegram.fail = Rejected(403)
        h2.bot.deliver([{"messageId": "1", "body": json.dumps(h2.queue.messages[0])}])
        self.assertFalse(h2.store.subscription(CHAT)["active"])

    def test_station_names_are_only_licensed_stations(self):
        self.assertEqual(set(m.STATIONS), {6123215, 99385})


if __name__ == "__main__":
    unittest.main()
