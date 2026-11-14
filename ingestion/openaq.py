"""Bounded OpenAQ v3 → the existing nine-field Bronze Kafka contract."""

import argparse
import fcntl
import hashlib
import json
import math
import os
import re
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path

PILOTS = {4946812: 13502151, 4946813: 13502154, 6123215: 14581375}
FIELDS = (
    "location_id",
    "sensors_id",
    "location",
    "datetime",
    "lat",
    "lon",
    "parameter",
    "units",
    "value",
)
BASE = "https://api.openaq.org/v3"


def utc(value):
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    if (
        result.tzinfo is None
        or result.utcoffset() != timedelta(0)
        or result.microsecond
    ):
        raise ValueError("Whole-second explicit UTC timestamps are required by Silver")
    return result.astimezone(UTC)


def stamp(value):
    return utc(value).isoformat().replace("+00:00", "Z")


def finite(value, low, high):
    if (
        type(value) not in (int, float)
        or not math.isfinite(value)
        or not low <= value <= high
    ):
        raise ValueError("Invalid finite numeric field")
    return value


class UnavailableStation(RuntimeError):
    """A configured source no longer exists; retain its cursor and continue others."""


def retry_delay(headers, attempt, now=None):
    """At most 30 seconds; a longer source cooldown requires a later bounded run."""
    value = next(
        (str(v) for k, v in headers.items() if k.lower() == "retry-after"), None
    )
    if value is None:
        delay = 2**attempt
    elif value.isdecimal() and len(value) <= 3:
        delay = int(value)
    else:
        try:
            deadline = parsedate_to_datetime(value)
            if deadline.tzinfo is None:
                raise ValueError("Retry time needs timezone")
            delay = math.ceil((deadline - (now or datetime.now(UTC))).total_seconds())
        except (ValueError, TypeError, OverflowError):
            raise RuntimeError("Invalid source retry delay") from None
    if delay > 30:
        raise RuntimeError("OpenAQ rate-limit delay exceeds bounded run; resume later")
    return max(1, delay)


class API:
    def __init__(self, key, session=None, sleep=time.sleep):
        if not key:
            raise ValueError("OpenAQ credential missing; no synthetic fallback")
        if session is None:
            import requests

            session = requests.Session()
        self.session, self.key, self.sleep = session, key, sleep

    def get(self, path, params=None):
        for attempt in range(3):
            try:
                response = self.session.get(
                    BASE + path,
                    params=params,
                    headers={"X-API-Key": self.key},
                    timeout=(5, 20),
                    allow_redirects=False,
                )
            except Exception:  # noqa: BLE001 -- transport details can contain credentials
                if attempt == 2:
                    raise RuntimeError("OpenAQ transport failed") from None
                self.sleep(2**attempt)
                continue
            if response.status_code == 404:
                raise UnavailableStation("Configured OpenAQ source returned HTTP 404")
            if response.status_code == 200:
                data = response.json()
                if not isinstance(data, dict) or not isinstance(
                    data.get("results"), list
                ):
                    raise ValueError("Malformed OpenAQ response")
                return data
            if response.status_code not in (429, 500, 502, 503, 504):
                raise RuntimeError(
                    f"OpenAQ request rejected: HTTP {response.status_code}"
                )
            if attempt == 2:
                raise RuntimeError("OpenAQ transient/rate-limit retry budget exhausted")
            self.sleep(retry_delay(response.headers, attempt))

        raise RuntimeError("No OpenAQ response")

    def metadata(self, location, sensor):
        values = self.get(f"/locations/{location}")["results"]
        if len(values) != 1:
            raise ValueError("Unique station metadata required")
        row = values[0]
        selected = [s for s in row.get("sensors", []) if s.get("id") == sensor]
        if (
            row.get("id") != location
            or len(selected) != 1
            or row.get("isMobile") is not False
        ):
            raise ValueError("Pilot sensor/location metadata mismatch")
        if (
            selected[0].get("parameter", {}).get("name") != "pm25"
            or selected[0]["parameter"].get("units") != "µg/m³"
        ):
            raise ValueError("Pilot PM2.5 units mismatch")
        if not isinstance(row.get("name"), str) or not row["name"].strip():
            raise ValueError("Valid source station name required")
        coordinates = row.get("coordinates") or {}
        finite(coordinates.get("latitude"), -90, 90)
        finite(coordinates.get("longitude"), -180, 180)
        return row


def bronze_record(item, metadata, sensor, start, end):
    location = metadata["id"]
    if PILOTS.get(location) != sensor:
        raise ValueError("Unsupported station/sensor pair")
    if (
        item.get("locationsId", location) != location
        or item.get("sensorsId", sensor) != sensor
    ):
        raise ValueError("Measurement station/sensor identity mismatch")
    parameter = item.get("parameter", {})
    if parameter.get("name") != "pm25" or parameter.get("units") != "µg/m³":
        raise ValueError("Measurement parameter/units mismatch")
    if item.get("flagInfo", {}).get("hasFlags") is not False:
        raise ValueError("Flagged or unspecified-quality measurement")
    period = item.get("period", {})
    if period.get("label") != "raw":
        raise ValueError("Raw source measurements required")
    measured = utc(period.get("datetimeTo", {}).get("utc", ""))
    if not start <= measured <= end:
        raise ValueError("Measurement outside requested window")
    value = finite(item.get("value"), 0, float("inf"))
    # API period end matches datetime in the accepted historical CSV contract.
    return dict(
        zip(
            FIELDS,
            [
                str(location),
                str(sensor),
                metadata["name"],
                stamp(measured),
                str(metadata["coordinates"]["latitude"]),
                str(metadata["coordinates"]["longitude"]),
                "pm25",
                "µg/m³",
                str(value),
            ],
            strict=True,
        )
    )


class State:
    def __init__(self, root, topic, broker):
        root = Path(root).expanduser()
        if root.is_symlink() or not root.name.startswith("airlake-live-"):
            raise ValueError("An external airlake-live-* directory is required")
        root = root.resolve()
        if any((p / ".git").exists() for p in (root, *root.parents)):
            raise ValueError("State must be outside every Git checkout")
        root.mkdir(parents=True, exist_ok=True)
        root.chmod(0o700)
        self.root = root
        self.lock = (root / "ingestion.lock").open("a+")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            raise ValueError("Another ingestion process owns this cursor") from None
        path = root / "cursor.sqlite"
        if path.is_symlink():
            raise ValueError("Unsafe cursor path")
        self.db = sqlite3.connect(path, timeout=1)
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS binding (identity TEXT); CREATE TABLE IF NOT EXISTS cursor (sensor INTEGER PRIMARY KEY, start TEXT,end TEXT,page INTEGER); CREATE TABLE IF NOT EXISTS outbox (id TEXT PRIMARY KEY,sensor INTEGER,record TEXT,provenance TEXT,sent INTEGER DEFAULT 0);"
        )
        identity = json.dumps(
            {
                "topic": topic,
                "broker": broker,
                "pilots": PILOTS,
                "schema": FIELDS,
                "timestamp": "period.datetimeTo.utc",
                "page_size": 100,
            },
            sort_keys=True,
        )
        rows = self.db.execute("SELECT identity FROM binding").fetchall()
        if rows and rows != [(identity,)]:
            self.close()
            raise ValueError("Cursor binding differs; do not reset/reuse it")
        if not rows:
            self.db.execute("INSERT INTO binding VALUES (?)", (identity,))
            self.db.commit()

    def close(self):
        self.db.close()
        self.lock.close()

    def window(self, sensor, start, end):
        row = self.db.execute(
            "SELECT start,end,page FROM cursor WHERE sensor=?", (sensor,)
        ).fetchone()
        if row:
            if end < utc(row[1]):
                raise ValueError(
                    "Requested end precedes retained cursor; do not rewind"
                )
            return utc(row[0]), utc(row[1]), row[2]
        self.db.execute(
            "INSERT INTO cursor VALUES (?,?,?,1)", (sensor, stamp(start), stamp(end))
        )
        self.db.commit()
        return start, end, 1

    def stage(self, sensor, records, path, page):
        with self.db:
            for row in records:
                encoded = json.dumps(
                    row, sort_keys=True, ensure_ascii=False, allow_nan=False
                )
                event = hashlib.sha256(encoded.encode()).hexdigest()
                provenance = json.dumps(
                    {
                        "endpoint": BASE + path,
                        "page": page,
                        "retrieved_at": stamp(datetime.now(UTC).replace(microsecond=0)),
                    }
                )
                self.db.execute(
                    "INSERT OR IGNORE INTO outbox(id,sensor,record,provenance) VALUES(?,?,?,?)",
                    (event, sensor, encoded, provenance),
                )

    def drain(self, sensor, publish, budget):
        pending = self.db.execute(
            "SELECT id,record FROM outbox WHERE sensor=? AND sent=0 ORDER BY rowid LIMIT ?",
            (sensor, budget),
        ).fetchall()
        for event, encoded in pending:
            publish(json.loads(encoded))
            # Commit only after broker acknowledgement. Crash ambiguity is at-least-once.
            with self.db:
                self.db.execute("UPDATE outbox SET sent=1 WHERE id=?", (event,))
        return len(pending)


def ingest(api, state, publish, *, start, end, max_records=300, max_pages=10):
    start, end = utc(start), utc(end)
    if (
        not 3 <= max_records <= 1000
        or not 1 <= max_pages <= 100
        or not timedelta(0) < end - start <= timedelta(days=1)
    ):
        raise ValueError("Bounded 1-day window and 3..1000 records required")
    report = {"source": "LIVE OpenAQ API v3", "published": 0, "stations": []}
    for index, (location, sensor) in enumerate(PILOTS.items()):
        budget = (max_records - report["published"]) // (len(PILOTS) - index)
        info = {
            "location_id": location,
            "sensor_id": sensor,
            "published": 0,
            "fetched": 0,
            "complete": False,
        }
        report["stations"].append(info)
        if not budget:
            continue
        try:
            metadata = api.metadata(location, sensor)
        except UnavailableStation:
            info["status"] = "SOURCE_UNAVAILABLE_HTTP_404"
            continue
        begin, finish, page = state.window(sensor, start, end)
        info["source_name"] = metadata["name"]
        info["source_latest_utc"] = (metadata.get("datetimeLast") or {}).get("utc")
        sent = state.drain(sensor, publish, budget)
        info["published"] += sent
        report["published"] += sent
        for _ in range(max_pages):
            if info["published"] >= budget:
                break
            path = f"/sensors/{sensor}/measurements"
            try:
                response = api.get(
                    path,
                    {
                        "datetime_from": stamp(begin),
                        "datetime_to": stamp(finish),
                        "limit": 100,
                        "page": page,
                    },
                )
            except UnavailableStation:
                info["status"] = "SOURCE_UNAVAILABLE_HTTP_404"
                break
            values = response["results"]
            info["fetched"] += len(values)
            if len(values) > 100:
                raise ValueError("API page exceeds requested limit")
            rows = [
                bronze_record(item, metadata, sensor, begin, finish) for item in values
            ]
            state.stage(sensor, rows, path, page)
            sent = state.drain(sensor, publish, budget - info["published"])
            info["published"] += sent
            report["published"] += sent
            pending = state.db.execute(
                "SELECT count(*) FROM outbox WHERE sensor=? AND sent=0", (sensor,)
            ).fetchone()[0]
            if pending:
                break
            if len(values) < 100:
                # Revisit an hour of overlap for late arrivals/corrections; identities suppress repeats.
                next_start = max(begin, finish - timedelta(hours=1))
                with state.db:
                    state.db.execute(
                        "UPDATE cursor SET start=?,end=?,page=1 WHERE sensor=?",
                        (
                            stamp(next_start),
                            stamp(min(end, next_start + timedelta(days=1))),
                            sensor,
                        ),
                    )
                info["complete"] = True
                break
            page += 1
            with state.db:
                state.db.execute(
                    "UPDATE cursor SET page=? WHERE sensor=?", (page, sensor)
                )
        info["pending"] = state.db.execute(
            "SELECT count(*) FROM outbox WHERE sensor=? AND sent=0", (sensor,)
        ).fetchone()[0]
    report["outbox_confirmed_total"] = state.db.execute(
        "SELECT count(*) FROM outbox WHERE sent=1"
    ).fetchone()[0]
    report["outbox_pending_total"] = state.db.execute(
        "SELECT count(*) FROM outbox WHERE sent=0"
    ).fetchone()[0]
    return report


class KafkaSink:
    def __init__(self, topic, broker, create=False, bound_state=False):
        if not re.fullmatch(r"airlake_live_[a-z0-9_]{8,64}", topic) or broker not in (
            "localhost:19092",
            "127.0.0.1:19092",
        ):
            raise ValueError("An isolated local topic is required")
        from confluent_kafka.admin import AdminClient, NewTopic

        from real_producer import Producer

        admin = AdminClient({"bootstrap.servers": broker})
        topics = admin.list_topics(timeout=10).topics
        if topic in topics and not bound_state:
            raise ValueError("Existing topic without matching cursor is unsafe")
        if topic not in topics:
            if not create:
                raise ValueError(
                    "Create the isolated topic explicitly with --create-topic"
                )
            admin.create_topics(
                [NewTopic(topic, 1, 1, config={"retention.ms": "86400000"})]
            )[topic].result(15)
        self.producer = Producer(
            {
                "bootstrap.servers": broker,
                "enable.idempotence": True,
                "acks": "all",
                "delivery.timeout.ms": 30000,
            }
        )
        self.topic = topic

    def __call__(self, row):
        result = []
        self.producer.produce(
            self.topic,
            key=row["location_id"] + ":" + row["sensors_id"],
            value=json.dumps(row, ensure_ascii=False, allow_nan=False),
            callback=lambda error, _msg: result.append(error),
        )
        remaining = self.producer.flush(35)
        if remaining or len(result) != 1 or result[0] is not None:
            raise RuntimeError(
                "Kafka delivery failed or uncertain; cursor not advanced"
            )


def key_from_environment(env_file=None):
    key = os.environ.get("OPENAQ_API_KEY") or os.environ.get("OPENAQ_KEY")
    if key:
        return key
    if env_file:
        for line in Path(env_file).read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                name, value = line.split("=", 1)
                if name.strip() in ("OPENAQ_KEY", "OPENAQ_API_KEY"):
                    return value.strip().strip("\"'")
    raise ValueError(
        "Set OPENAQ_KEY/OPENAQ_API_KEY or use an explicit local --env-file; no AWS secret lookup"
    )


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--broker", default="localhost:19092")
    p.add_argument("--env-file", type=Path)
    p.add_argument("--max-records", type=int, default=300)
    p.add_argument("--create-topic", action="store_true")
    p.add_argument("--start")
    p.add_argument("--end")
    a = p.parse_args(argv)
    now = datetime.now(UTC).replace(microsecond=0)
    end = utc(a.end) if a.end else now - timedelta(minutes=5)
    start = utc(a.start) if a.start else end - timedelta(hours=24)
    api = API(key_from_environment(a.env_file))
    marker = a.root / "topic-binding.json"
    if marker.is_symlink():
        raise ValueError("Unsafe topic binding")
    was_bound = (a.root / "cursor.sqlite").is_file() and marker.is_file()
    if marker.is_file() and json.loads(marker.read_text()) != {
        "topic": a.topic,
        "broker": a.broker,
    }:
        raise ValueError("Topic ownership binding mismatch")
    state = State(a.root, a.topic, a.broker)
    sink = KafkaSink(a.topic, a.broker, a.create_topic, was_bound)
    if not marker.exists():
        with marker.open("x") as handle:
            json.dump({"topic": a.topic, "broker": a.broker}, handle)
            handle.flush()
            os.fsync(handle.fileno())
    try:
        report = ingest(
            api, state, sink, start=start, end=end, max_records=a.max_records
        )
        report.update(
            observed_at_utc=stamp(now),
            window_start_utc=stamp(start),
            window_end_utc=stamp(end),
            topic=a.topic,
        )
        (state.root / "ingestion-report.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False) + "\n"
        )
        print(json.dumps(report, indent=2, ensure_ascii=False))
    finally:
        state.close()


if __name__ == "__main__":
    main()
