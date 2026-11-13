# AirLake: Cloud contracts

Tài liệu này là thỏa thuận chung của nhóm về tên bảng, vị trí S3 và người được phép ghi từng dataset. Mọi thay đổi gửi bằng Pull Request, Minh Khuê (hạ tầng) và Mai Anh (mô hình dữ liệu) cùng duyệt.

Cột **Trạng thái** có ba giá trị: `Đã xác nhận` (có trong file context hoặc trong hạ tầng đang chạy), `Đề xuất` (đã đặt tên, cần người phụ trách xác nhận), `Chưa có` (chưa ai quyết định).

## 1. Thông số chung

| Mục | Giá trị | Trạng thái |
|---|---|---|
| Region AWS | `ap-southeast-2` (Sydney). **File context ghi `us-east-1`, cần sửa cho khớp** | Đã xác nhận |
| Bucket lake | `airlake-<ACCOUNT_ID>-lake` | Đã xác nhận |
| Bucket artifact (mã Spark, zip Lambda) | `airlake-<ACCOUNT_ID>-artifacts` | Đã xác nhận |
| Bucket kết quả Athena | `airlake-<ACCOUNT_ID>-athena-results` (xóa sau 7 ngày) | Đã xác nhận |
| Glue database | `airlake_bronze`, `airlake_silver`, `airlake_gold` | Đã xác nhận |
| Athena workgroup | `airlake` (giới hạn 10 GB quét mỗi truy vấn) | Đã xác nhận |
| Kafka topic | `aq_measurements` | Đã xác nhận |

## 2. Dataset

| Dataset | Bảng Glue | Vị trí S3 | Người ghi | Người đọc | Trạng thái |
|---|---|---|---|---|---|
| Bronze | `airlake_bronze.aq_measurements` | `s3://<lake>/warehouse/airlake_bronze.db/aq_measurements/` (vị trí mặc định của Iceberg GlueCatalog) | Spark Streaming, role `airlake-ec2-spark` (Mai Anh) | Spark (Silver), Trino, Athena | Tên bảng đã xác nhận, vị trí là đề xuất |
| Silver | `airlake_silver.<tên bảng do Mai Anh đặt>` | `s3://<lake>/warehouse/airlake_silver.db/<bảng>/` | Spark, role `airlake-ec2-spark` (Mai Anh) | Spark (Gold), Trino, Athena | Chưa có |
| Gold | `airlake_gold.<tên bảng do Mai Anh đặt>` | `s3://<lake>/warehouse/airlake_gold.db/<bảng>/` | Spark, role `airlake-ec2-spark` (Mai Anh) | Trino, Athena, Superset, bot Telegram (Phúc Thịnh) | Chưa có |
| Checkpoint Spark | không phải bảng | `s3://<lake>/checkpoints/<tên job>/` | Spark, role `airlake-ec2-spark` | Spark | Đề xuất |
| Mã job Spark, zip Lambda | không phải bảng | `s3://<artifacts>/jobs/`, `s3://<artifacts>/lambda/` | Người triển khai (qua PR) | Spark, Lambda | Đề xuất |

### Schema Bronze (từ file context, mục 3.5)

Toàn bộ cột là `STRING` (Bronze không ép kiểu), cộng cột kỹ thuật `kafka_ts`:

`location_id, sensors_id, location, datetime, lat, lon, parameter, units, value` và `kafka_ts TIMESTAMP`.

`datetime` mang múi giờ `+07:00`. Việc ép kiểu và đổi sang UTC thuộc về lớp Silver.

## 3. Quyền ghi và đọc (theo role)

| Thành phần | Role AWS | Được ghi | Được đọc |
|---|---|---|---|
| Spark Streaming và batch | `airlake-ec2-spark` | `lake/` (warehouse, checkpoints), bảng Glue `airlake_*` | `artifacts/` |
| Trino | `airlake-ec2-trino` | Không ghi gì | `lake/`, Glue |
| Superset | `airlake-ec2-superset` | `athena-results/` | `lake/`, Glue, Athena |
| Kafka | `airlake-ec2-kafka` | Không có quyền S3 | Không có quyền S3 |
| Poller (Lambda hoặc EC2) | do Khánh Nam đặt tên, qua PR | Ghi vào Kafka, không ghi S3 | Tham số `/airlake/openaq_key` |
| Bot Telegram, cảnh báo | do Phúc Thịnh đặt tên, qua PR | DynamoDB, SNS (nếu dùng) | Bảng Gold, tham số `/airlake/telegram_token` |

**Nguyên tắc:** bảng Bronze, Silver, Gold chỉ có **một người ghi** là job Spark của Mai Anh. Ai cần ghi thêm phải đề nghị qua PR để Minh Khuê cập nhật policy.

## 4. Metric độ tươi (CloudWatch)

| Mục | Giá trị | Trạng thái |
|---|---|---|
| Namespace | `AirLake` | Đã xác nhận (alarm đã tạo) |
| Tên metric | `SensorAgeHours` | Đã xác nhận (alarm đã tạo) |
| Dimension | `location_id`, `sensors_id`, `parameter` | Đã xác nhận |
| Giá trị | Số giờ kể từ lần đo mới nhất của cảm biến | Đề xuất |
| Người phát metric | Mai Anh (job Silver) | Cần xác nhận |
| Ngưỡng | ≤1 giờ tươi, ≤24 giờ gián đoạn, >24 giờ chết. OceanPark dùng ngưỡng cảnh báo 2 giờ vì đo mỗi giờ | Đã xác nhận (theo context) |

Ba cảm biến PM2.5 đang có alarm: Nhân Chính (`4946812` / `13502151`), Giải Phóng (`4946813` / `13502154`), OceanPark (`6123215` / `14581375`).

## 5. Tham số bí mật (SSM Parameter Store, loại SecureString)

| Tham số | Ai đặt giá trị | Ai đọc |
|---|---|---|
| `/airlake/openaq_key` | Người giữ khóa OpenAQ | Poller (Khánh Nam) |
| `/airlake/telegram_token` | Người tạo bot (Phúc Thịnh) | Bot cảnh báo |

Giá trị thật không bao giờ đưa vào Git, chat hay file Terraform.

## 6. Cách thay đổi hợp đồng

1. Sửa file này trong một Pull Request.
2. Ghi rõ bảng hoặc prefix nào đổi và ai bị ảnh hưởng.
3. Minh Khuê cập nhật IAM nếu cần, Mai Anh xác nhận phần dữ liệu.
4. Sau khi merge, chạy `bash infra/scripts/status-report.sh` để đối chiếu với thực tế.
