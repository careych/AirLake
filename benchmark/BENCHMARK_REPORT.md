# Báo Cáo So Sánh Thực Nghiệm Hiệu Năng & Chi Phí: Trino vs AWS Athena

- **Thời gian thực hiện**: 2026-11-14 05:57:47
- **Cơ sở dữ liệu Gold**: `airlake_it2_20261108_37179997ad2b42959f4387637e003ab8_gold`
- **Endpoint Trino**: `3.107.183.65:8080` (EC2 `m7i-flex.large`, đơn giá: ~$0.1008/giờ)
- **AWS Athena**: Serverless engine (Đơn giá: $5.00/TB quét, min 10MB/query)
- **Số lần chạy lặp lại (Runs)**: 1
- **Chế độ kiểm thử**: Mô phỏng (Dry-run)

## 1. Bảng Tổng Hợp Kết Quả Thực Nghiệm

| STT | Kịch Bản Truy Vấn | Athena Thời Gian (s) | Athena Quét (MB) | Athena Chi Phí ($) | Trino Thời Gian (s) | Trino Dòng Trả Về | Trino Chi Phí ($) | Tăng Tốc (Speedup) |
|:---:|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | `1_Truy_van_co_ban` | 2.15 | 15.00 | $0.000072 | 0.62 | 5,000 | $0.000017 | **3.47x** |
| 2 | `2_Gom_nhom_don_gian` | 3.48 | 42.00 | $0.000200 | 1.18 | 120 | $0.000033 | **2.95x** |
| 3 | `3_Truy_van_phuc_tap` | 5.12 | 90.00 | $0.000429 | 1.45 | 10 | $0.000041 | **3.53x** |

### Thống Kê Tổng Hợp
- **Tổng thời gian thực thi**: Athena: `10.75s` vs Trino: `3.25s` (Trino nhanh hơn trung bình **3.31x**).
- **Tổng dung lượng quét Athena**: `147.00 MB` (~`$0.000701`).
- **Tổng chi phí tính toán Trino**: `$0.000091`.

## 2. Phân Tích Kỹ Thuật & Đánh Giá Kiến Trúc

### 2.1. Độ Trễ (Latency)
- **Trino**: Trino giữ tiến trình JVM chạy thường trực (long-running cluster) kết hợp in-memory execution, loại bỏ hoàn toàn độ trễ khởi động (cold start). Các truy vấn phục vụ dashboard (như Superset) đạt thời gian phản hồi sub-second hoặc ~1s.
- **AWS Athena**: Là kiến trúc Serverless, Athena mất khoảng 1.5s - 2.5s overhead cho mỗi truy vấn để cấp phát worker và phân bổ tài nguyên. Tuy nhiên, với các truy vấn quét dữ liệu lớn phân tán (full-scan), Athena tự động mở rộng theo tải mà không cần cấu hình cụ thể.

### 2.2. Chi Phí (Cost Efficiency)
- **AWS Athena**: Tính phí thuần túy theo $5.00/TB quét thực tế. Khi áp dụng định dạng Parquet nén kèm phân vùng (partition pruning), dung lượng quét giảm từ 70% - 90%, rất tiết kiệm cho các báo cáo định kỳ theo giờ hoặc tác vụ kiểm toán dữ liệu.
- **Trino**: Tính phí theo giờ máy chủ EC2 (`m7i-flex.large`: ~$0.1008/giờ, tương đương ~$72.5/tháng nếu chạy 24/7). Khi tích hợp Auto Scaling Group (ASG) và lịch trình bật/tắt (Scheduled Action 07:00 - 19:00), chi phí giảm xuống còn ~$36/tháng, rất tối ưu cho các hệ thống dashboard có tần suất truy vấn cao liên tục.

### 2.3. Khuyến Nghị Phối Hợp Kiến Trúc (Architecture Recommendation)
1. **Apache Superset BI Dashboard**: Sử dụng **Trino** làm query engine mặc định để mang lại trải nghiệm tương tác mượt mà, thời gian tải biểu đồ nhanh chóng (< 1.5s).
2. **Tác vụ Ad-hoc & Phân Tích Thăm Dò Nâng Cao**: Sử dụng **AWS Athena** cho các tác vụ ad-hoc từ data analyst hoặc batch data science không yêu cầu độ trễ tương tác thực thời.
3. **Dự phòng sự cố (Failover/Fallback)**: Nếu Trino cluster gặp sự cố bảo trì hoặc ngoài khung giờ hoạt động, hệ thống có thể linh hoạt fallback truy vấn trực tiếp vào AWS Athena vì cả hai cùng chia sẻ chung AWS Glue Data Catalog và cấu trúc bảng Apache Iceberg.
