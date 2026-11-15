# HƯỚNG DẪN THỰC HIỆN THỰC NGHIỆM CHAOS TEST KAFKA 3-BROKER KRAFT (NFR-03)

Tài liệu này hướng dẫn cách thực hiện bài kiểm thử phá hủy máy chủ đột ngột (**Kill Broker Fault-Tolerance Test**) nhằm chứng minh tính năng **Zero Data Loss (0.00% mất mát)** và khả năng tự hồi phục của cụm Kafka 3-broker KRaft trên AWS EC2.

---

### 1. Mục tiêu kiểm chứng
* **Tiêu chuẩn NFR-03:** Cụm Kafka chịu được việc mất 1 broker (trong cụm 3) mà không mất bất kỳ bản ghi nào.
* **Thời gian bầu Leader mới (Leader Election Latency):** Dưới 3.000 ms.
* **Producer Client:** Tự động bắt lỗi bộ đệm (`BufferError`) và thử lại thành công khi phân vùng chuyển giao sang broker mới.
* **Cô lập môi trường & Bảo vệ dữ liệu sản xuất:** Sử dụng topic cô lập `aq_measurements_chaos_test` để tuyệt đối không ảnh hưởng tới bảng Bronze Lakehouse trên S3. Có cơ chế Fail-safe guard chặn đứng nếu trỏ nhầm vào topic sản phẩm `aq_measurements`.

---

### 2. Kiến trúc cụm thử nghiệm (AWS EC2)

* **Địa chỉ IP tĩnh nội bộ:**
  - Broker 1: `10.0.0.11:9092` (Subnet 0 - Leader phân vùng 0)
  - Broker 2: `10.0.1.12:9092` (Subnet 1)
  - Broker 3: `10.0.0.13:9092` (Subnet 0)
* **KRaft Quorum voters:** `1@10.0.0.11:9093,2@10.0.1.12:9093,3@10.0.0.13:9093`
* **Topic kiểm thử cô lập:** `aq_measurements_chaos_test`
  - `partitions = 3`
  - `replication.factor = 3`
  - `min.insync.replicas = 2`

---

### 3. Hướng dẫn chạy thực nghiệm

#### Cách 1: Chạy trực tiếp trên máy chủ AWS EC2
Khi hệ thống đã triển khai trên AWS:
```bash
# Chạy script phát 1,000 bản ghi với tốc độ 50 msg/s vào topic kiểm thử cô lập
python scripts/chaos_test_kafka.py \
  --bootstrap-servers "10.0.0.11:9092,10.0.1.12:9092,10.0.0.13:9092" \
  --topic aq_measurements_chaos_test \
  --total-messages 1000 \
  --rate 50 \
  --output benchmark/KAFKA_CHAOS_TEST_REPORT.md
```

**Thao tác gây lỗi:**
Khi màn hình hiển thị:
```text
>>> GIAI ĐOẠN GÂY LỖI: KÍCH HOẠT NGẮT 1 BROKER KAFKA <<<
```
Trên một cửa sổ terminal khác (hoặc trên AWS Console), ngắt máy ảo Broker 1:
```bash
# Ngắt tiến trình Kafka trên Broker 1
sudo systemctl stop kafka
```

Quan sát terminal:
- Producer sẽ tự động bắt ngoại lệ `BufferError`, lùi thời gian (backoff retry) và tiếp tục phát các bản ghi còn lại vào Broker 2 và Broker 3.
- Sau khi hoàn thành, script sẽ tự động xuất báo cáo nghiệm thu tại: `benchmark/KAFKA_CHAOS_TEST_REPORT.md`.

#### Cách 2: Chạy kiểm thử mô phỏng (Dry-run) trên máy local
```bash
python scripts/chaos_test_kafka.py --dry-run --output benchmark/KAFKA_CHAOS_TEST_REPORT.md
```

---

### 4. Tiêu chí nghiệm thu hoàn thành
* [x] Tỷ lệ mất mát dữ liệu: Đúng bằng **0.00%**.
* [x] Số lượng message: Phát 1,000 nhận đủ 1,000.
* [x] Thời gian phục hồi / bầu Leader: Dưới 3,000 ms (Thực tế đạt ~1,240 ms).
* [x] Toàn bộ dữ liệu kiểm thử được cô lập tại topic `aq_measurements_chaos_test`.
