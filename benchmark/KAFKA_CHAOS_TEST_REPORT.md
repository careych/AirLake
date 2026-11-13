# BÁO CÁO THỰC NGHIỆM CHỊU LỖI KAFKA 3-BROKER KRAFT (CHAOS TEST)

**Thời gian thực nghiệm:** 2026-11-13 22:33:04 UTC  
**Dự án:** AirLake — Data Lakehouse AWS  
**Cụm thử nghiệm:** Apache Kafka KRaft 3-Broker HA Cluster  
**Địa chỉ Broker:** `localhost:19092`  
**Topic:** `aq_measurements` (Replication Factor = 3, min.insync.replicas = 2, acks = all)  

---

### I. KẾT QUẢ ĐO ĐẠC ĐỊNH LƯỢNG

| Chỉ số đo đạc | Giá trị cam kết | Kết quả thực tế | Đánh giá kỹ thuật |
| :--- | :---: | :---: | :--- |
| **Tổng số bản ghi phát (Submitted)** | Configurable | **1,000** | Hoàn tất phát tin liên tục |
| **Số bản ghi xác nhận ghi đĩa (Delivered)** | == Submitted | **1,000** | 100% bản ghi được ghi an toàn |
| **Số bản ghi thất thoát (Failed)** | 0 | **0** | 🟢 ĐẠT (Zero Data Loss) |
| **Tỷ lệ mất mát dữ liệu (Data Loss Rate)** | 0.00% | **0.00%** | Cam kết Zero Data Loss đạt chuẩn |
| **Thời gian gián đoạn / Ngắt Broker 1** | ~5-10s | **6.50s** | Gián đoạn có chủ đích |
| **Thời gian bầu Leader mới (Election Latency)** | < 3,000ms | **1240.0ms** | Quorum KRaft tự động chuyển giao |
| **Trạng thái Producer Client** | Không crash | **Tự động phục hồi** | Không phát sinh ngoại lệ Fatal |

---

### II. KẾT LUẬN & ĐÁNH GIÁ NGHIỆM THU

* **Tiêu chuẩn NFR-03:** 🟢 ĐẠT TIÊU CHÍ NFR-03
* **Cơ chế chịu lỗi KRaft:** Khi ngắt đột ngột Broker 1 (đang giữ vai trò Leader phân vùng), cụm 3 broker KRaft tự động kích hoạt cơ chế bầu chọn và phân bổ Leader mới cho các phân vùng sang 2 broker còn lại (`10.0.0.12` hoặc `10.0.0.13`).
* **Tính toàn vẹn của ứng dụng:** Nhờ cấu hình `acks=all`, `enable.idempotence=True`, Producer tạm thời giữ bản tin trong bộ đệm và tự động thử lại (backoff retry), đảm bảo không một bản ghi nào bị rớt khỏi hàng đợi.
