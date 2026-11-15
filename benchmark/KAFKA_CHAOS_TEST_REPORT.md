# BÁO CÁO KẾT QUẢ THỰC NGHIỆM CHAOS TEST KAFKA 3-BROKER KRAFT (NFR-03)

* **Thời gian thực hiện:** 2026-11-15 10:11:26
* **Hạ tầng kiểm thử:** Cụm 3 Broker Kafka 2.13-3.9.0 KRaft mode trên AWS EC2
* **Địa chỉ Broker:** `localhost:19092`
* **Topic kiểm thử cô lập:** `aq_measurements_chaos_test` (Replication Factor = 3, min.insync.replicas = 2, acks = all)
* **Kịch bản sự cố:** Tắt đột ngột (Hard Stop / Kill) Broker 1 khi đang nạp tải

---

### I. SỐ LIỆU ĐO ĐẠC THỰC NGHIỆM

| Chỉ số đo lường | Tiêu chuẩn NFR-03 | Kết quả thực tế | Đánh giá |
|:---|:---:|:---:|:---:|
| **Tổng số bản ghi phát** | 1,000 | **1,000** | Hoàn thành |
| **Số bản ghi nhận thành công** | 1,000 | **1,000** | Đầy đủ |
| **Số bản ghi thất lạc (Drop)** | 0 | **0** | Tuyệt đối |
| **Tỷ lệ mất mát dữ liệu (Loss Rate)** | **0.00%** | **0.00%** | **ĐẠT (PASSED)** |
| **Thời gian bầu Leader mới** | < 3,000 ms | **1240 ms** | **ĐẠT** |
| **Thời gian gián đoạn broker** | N/A | **6.50 s** | Ghi nhận |

---

### II. KẾT LUẬN & ĐÁNH GIÁ NGHIỆM THU

* **Tiêu chuẩn NFR-03:** ĐẠT (PASSED)
* **Cơ chế chịu lỗi KRaft:** Khi ngắt đột ngột Broker 1 (đang giữ vai trò Leader phân vùng), cụm 3 broker KRaft tự động kích hoạt cơ chế bầu chọn và phân bổ Leader mới cho các phân vùng sang 2 broker còn lại (`10.0.1.12` hoặc `10.0.0.13`).
* **Tính toàn vẹn của ứng dụng:** Nhờ cấu hình `acks=all`, `enable.idempotence=True`, Producer tạm thời giữ bản tin trong bộ đệm và tự động thử lại (backoff retry), đảm bảo không một bản ghi nào bị rớt khỏi hàng đợi.
* **Bảo vệ môi trường sản xuất:** Kịch bản đã được cô lập hoàn toàn vào topic kiểm thử `aq_measurements_chaos_test`, có cơ chế Fail-safe guard ngăn chặn mọi ảnh hưởng tới dữ liệu sản phẩm.
