import argparse
import csv
import glob
import gzip
import json
import os
import math
import sys
import time

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient

SPEED = float(sys.argv[1]) if len(sys.argv) > 1 else 0
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
p = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
files = sorted(glob.glob("data/raw/*/*/*/*.csv.gz"))
print(f"Tìm thấy {len(files)} file")

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Send real OpenAQ CSV.GZ records to Kafka."
    )
    parser.add_argument(
        "speed",
        nargs="?",
        type=float,
        default=0,
        help="Seconds to pause after each 2000 rows.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:19092",
        help="Kafka bootstrap servers; CLI-only, defaults to localhost:19092.",
    )
    parser.add_argument(
        "--topic",
        default="aq_measurements_test1",
        help="Kafka topic. Defaults to the test topic.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=2000,
        help="Maximum source rows to send; use 0 for no limit.",
    )
    args = parser.parse_args(argv)

    if not math.isfinite(args.speed) or args.speed < 0:
        parser.error("speed must be finite and >= 0")
    if args.max_rows < 0:
        parser.error("--max-rows must be >= 0")
    if not args.bootstrap_servers.strip():
        parser.error("--bootstrap-servers must not be blank")
    if not args.topic.strip():
        parser.error("--topic must not be blank")

    return args


def main():
    args = parse_args()
    files = sorted(glob.glob("data/raw/*/*/*/*.csv.gz"))

    if not files:
        sys.exit("ERROR: No CSV.GZ files found.")

    # Check broker connectivity and confirm the topic already exists.
    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    try:
        metadata = admin.list_topics(timeout=10)
    except KafkaException as exc:
        sys.exit(f"ERROR: Cannot connect to Kafka: {exc}")

    if args.topic not in metadata.topics:
        sys.exit(
            f"ERROR: Topic {args.topic!r} does not exist. "
            "Create it before running the producer."
        )
    if metadata.topics[args.topic].error is not None:
        sys.exit(
            f"ERROR: Topic metadata is unavailable: {metadata.topics[args.topic].error}"
        )

    print(f"Kafka: {args.bootstrap_servers}")
    print(f"Topic: {args.topic}")
    print(f"CSV.GZ files: {len(files)}")
    print(f"Maximum rows: {args.max_rows or 'unlimited'}")

    stats = {"delivered": 0, "failed": 0}

    def delivery_report(err, msg):
        if err is not None:
            stats["failed"] += 1
            print(f"DELIVERY FAILED: {err}", file=sys.stderr)
        else:
            stats["delivered"] += 1

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": "airlake-real-producer",
        }
    )

    submitted = 0
    stop = False

    try:
        for fp in files:
            with gzip.open(fp, "rt", encoding="utf-8-sig", newline="") as f:
                for row in csv.DictReader(f):
                    key = f"{row['location_id']}:{row['sensors_id']}"
                    value = json.dumps(row, ensure_ascii=False)

                    # Retry when the local producer queue is full.
                    while True:
                        try:
                            producer.produce(
                                args.topic,
                                key=key,
                                value=value,
                                callback=delivery_report,
                            )
                            break
                        except BufferError:
                            producer.poll(1)

                    submitted += 1
                    producer.poll(0)

                    if submitted % 2000 == 0:
                        print(
                            f"Submitted: {submitted} | "
                            f"Delivered: {stats['delivered']} | "
                            f"Failed: {stats['failed']}"
                        )
                        if args.speed > 0:
                            time.sleep(args.speed)

                    if args.max_rows and submitted >= args.max_rows:
                        stop = True
                        break

            if stop:
                break

    except (OSError, csv.Error, KeyError, ValueError, KafkaException) as exc:
        print(f"ERROR while reading or producing: {exc}", file=sys.stderr)
        producer.flush(30)
        raise

    remaining = producer.flush(30)

    print("\n========== SUMMARY ==========")
    print(f"Rows submitted: {submitted}")
    print(f"Delivery successes: {stats['delivered']}")
    print(f"Delivery failures: {stats['failed']}")
    print(f"Messages still queued after flush: {remaining}")

    if remaining or stats["failed"] or stats["delivered"] != submitted:
        sys.exit("ERROR: Delivery was not fully successful.")

    print("SUCCESS: All submitted messages were delivered.")


if __name__ == "__main__":
    main()
