flowchart TD
    subgraph P1["👤 1. Người dân / Sinh viên (Nhóm cần cảnh báo nhanh)"]
        A1["Mở Telegram, tìm bot 'AirLake Hà Nội'"] --> A2["Lệnh /start: Chọn trạm (Nhân Chính / Giải Phóng / OceanPark)"]
        A2 --> A3["Cài đặt ngưỡng cảnh báo AQI cá nhân hoá"]
        A3 --> A4{"Tương tác hàng ngày"}
        A4 -->|Chủ động tra cứu| A5["Lệnh /now (Xem tức thời) & /best (Giờ sạch nhất)"]
        A4 -->|Hệ thống giám sát tự động| A6["AQI vượt ngưỡng liên tục đủ lâu"]
        A6 --> A7["🔔 Bot tự động đẩy thông báo cảnh báo qua SQS/Telegram"]
        A7 --> A8["Người dùng điều chỉnh lịch trình / bảo vệ hô hấp"]
    end

    subgraph P2["🔬 2. Nhà nghiên cứu / Sinh viên phân tích"]
        B1["Mở AirLake Web / Superset Dashboard"] --> B2["Xem AQI hiện tại & biểu đồ xu hướng 24h theo trạm"]
        B2 --> B3["Kiểm tra độ tin cậy: Xem trạng thái Tươi / Gián đoạn / Chết"]
        B3 --> B4["Truy vấn SQL lịch sử phân tích chuyên sâu (Trino / Athena)"]
        B4 --> B5["Tải dữ liệu mở (Chỉ trạm OceanPark - CC BY 4.0)"]
    end

    subgraph P3["🛠️ 3. Đội ngũ Vận hành (Nhóm tác giả)"]
        C1["Giám sát độ tươi (Freshness) per-sensor (≤1h: Tươi, ≤24h: Gián đoạn, >24h: Chết)"]
        C1 --> C2["CloudWatch Alarm phát hiện cảm biến chết/lỗi"]
        C2 --> C3["Kiểm tra chi phí AWS Budgets & hạ tầng EC2/Kafka/Spark"]
    end