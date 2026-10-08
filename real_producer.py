# real_producer.py   |   python real_producer.py [giây_nghỉ_mỗi_2000_dòng]
import csv
import glob
import gzip
import json
import sys
import time

from confluent_kafka import Producer

SPEED = float(sys.argv[1]) if len(sys.argv) > 1 else 0
p = Producer({"bootstrap.servers": "localhost:9092"})
files = sorted(glob.glob("data/raw/*/*/*/*.csv.gz"))
print(f"Tìm thấy {len(files)} file")

sent = 0
for fp in files:
    with gzip.open(fp, "rt", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = f"{row['location_id']}:{row['sensors_id']}"
            p.produce(
                "aq_measurements", key=key, value=json.dumps(row, ensure_ascii=False)
            )
            sent += 1
            if sent % 2000 == 0:
                p.poll(0)
                print(f"đã gửi {sent}")
                if SPEED > 0:
                    time.sleep(SPEED)
p.flush()
print(f"Hoàn tất: đã gửi {sent} bản ghi thật")
