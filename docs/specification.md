# AirLake — Bản đặc tả kỹ thuật (Technical Specification)

**Phiên bản:** 1.0 · **Dự án:** AirLake — Nền tảng dữ liệu và cảnh báo chất lượng không khí Hà Nội trên AWS · **Môn:** Điện toán đám mây

> Bản đặc tả viết ở giai đoạn thiết kế (tháng 10/2026). Từ Tuần 8 nhóm chuyển sang các dịch vụ AWS được quản lý, nên hệ thống cuối khác bản này ở một số thành phần; xem [architecture.md](architecture.md).

---

## MỤC LỤC

1. Giới thiệu & thuật ngữ
2. Tổng quan sản phẩm
3. Yêu cầu chức năng (FR)
4. Yêu cầu phi chức năng (NFR)
5. Kiến trúc tính năng (sơ đồ ASCII)
6. Kiến trúc triển khai (sơ đồ ASCII, theo từng phân hệ)
7. Đặc tả dữ liệu — mọi bảng, mọi cột
8. Đặc tả nguồn dữ liệu nguồn (OpenAQ) — thông tin đã xác minh
9. Đặc tả giao diện (bot Telegram, nội bộ giữa các thành phần)
10. Đặc tả hạ tầng AWS
11. Đặc tả bảo mật
12. Đặc tả CI/CD
13. Đặc tả giám sát & vận hành
14. Kế hoạch kiểm thử
15. Ước tính chi phí
16. Quản lý rủi ro
17. Phân vai trò & kế hoạch triển khai
18. Tiêu chí nghiệm thu từng giai đoạn
19. Phụ lục tra cứu nhanh

---

## 1. Giới thiệu & thuật ngữ

| Thuật ngữ | Giải thích |
|---|---|
| Bronze / Silver / Gold | Ba lớp lưu trữ dữ liệu theo kiến trúc Medallion: thô → đã làm sạch → đã tổng hợp sẵn sàng phục vụ |
| AQI | Chỉ số chất lượng không khí (Air Quality Index) |
| VN_AQI | Chuẩn AQI của Việt Nam theo Quyết định 1459/QĐ-TCMT (2019) |
| Cảm biến (sensor) | Thiết bị đo một chất cụ thể (PM2.5, CO...) tại một trạm; một trạm có nhiều cảm biến |
| Trạm (location) | Vị trí đặt thiết bị quan trắc, có thể có nhiều cảm biến |
| Snapshot | Trạng thái dữ liệu của một bảng Iceberg tại một thời điểm ghi cụ thể |
| ASG | Auto Scaling Group — nhóm máy tự động tăng/giảm số lượng theo tải |
| SCD Type 2 | Mô hình lưu lịch sử thay đổi của dữ liệu tham chiếu bằng cách thêm dòng mới thay vì ghi đè |

---

## 2. Tổng quan sản phẩm

**Vấn đề:** các ứng dụng đo bụi hiện có chỉ hiển thị một con số tức thời, không có lịch sử, không cho biết độ tin cậy của dữ liệu.

**Giải pháp:** AirLake thu thập liên tục dữ liệu chất lượng không khí Hà Nội, lưu trữ dài hạn, tính AQI theo chuẩn Việt Nam, cảnh báo chủ động qua Telegram, và minh bạch về tình trạng hoạt động của từng nguồn đo.

**Người dùng:** (1) người dân/sinh viên Hà Nội, đặc biệt nhóm nhạy cảm hô hấp; (2) sinh viên/nhà nghiên cứu cần dữ liệu lịch sử sạch; (3) đội vận hành nội bộ.

---

## 3. Yêu cầu chức năng (Functional Requirements)

| Mã | Yêu cầu |
|---|---|
| FR-01 | Hệ thống thu thập dữ liệu quan trắc gần thời gian thực từ OpenAQ API — mỗi 5 phút cho trạm chuẩn, mỗi giờ cho cảm biến giá rẻ |
| FR-02 | Hệ thống nạp được toàn bộ lịch sử từ kho lưu trữ S3 công khai của OpenAQ |
| FR-03 | Hệ thống chuẩn hoá đơn vị, ép kiểu, khử trùng dữ liệu thô (Bronze → Silver) |
| FR-04 | Hệ thống phân loại độ tin cậy từng cảm biến theo 3 mức: TƯƠI (≤1h) / GIÁN ĐOẠN (≤24h) / CHẾT (>24h) |
| FR-05 | Hệ thống tính AQI theo giờ cho từng trạm theo chuẩn VN_AQI (PM2.5) |
| FR-06 | Hệ thống phát hiện AQI vượt ngưỡng liên tục ≥2 giờ và sinh sự kiện cảnh báo |
| FR-07 | Người dùng đăng ký nhận cảnh báo qua Telegram, tự chọn trạm và ngưỡng |
| FR-08 | Người dùng tra cứu nhanh bằng lệnh chat: giá trị mới nhất (`/now`), khung giờ trong lành nhất theo lịch sử (`/best`) |
| FR-09 | Người dùng xem dashboard: AQI hiện tại theo trạm + biểu đồ xu hướng 24 giờ |
| FR-10 | Người dùng (vai phân tích) chạy truy vấn SQL tuỳ ý trên dữ liệu lịch sử qua Superset |
| FR-11 | Người dùng xem được tình trạng "sức khoẻ" từng trạm/cảm biến (tính năng minh bạch, khác biệt) |
| FR-12 | Hệ thống cho phép tải dữ liệu đã làm sạch của nguồn có giấy phép mở (chỉ AirGradient/CC BY 4.0), kèm ghi công |

---

## 4. Yêu cầu phi chức năng (Non-functional Requirements)

| Mã | Yêu cầu | Cách đo |
|---|---|---|
| NFR-01 | Độ trễ từ thời điểm đo đến khi hiển thị trên dashboard ở mức chấp nhận được | Đo `ingested_at - measured_at`, báo cáo p50/p95 ở Giai đoạn C |
| NFR-02 | Lớp truy vấn (Trino) tự động mở rộng theo tải truy vấn đồng thời | k6 tăng dần số truy vấn song song, quan sát số worker ASG |
| NFR-03 | Kafka chịu được mất 1 broker (trong cụm 3) mà không mất dữ liệu | Kill 1 broker giữa lúc đang ghi, kiểm tra số bản ghi không đổi |
| NFR-04 | Có thể khôi phục Silver/Gold về trạng thái trước đó khi phát hiện lỗi | Demo time travel: rollback về snapshot cũ |
| NFR-05 | Không dùng access key tĩnh; dữ liệu mã hoá khi lưu trữ (KMS) và truyền tải (TLS) | Rà soát Terraform + IAM policy |
| NFR-06 | Chi phí vận hành nằm trong free credit AWS trong suốt dự án | Theo dõi Cost Explorer hàng tuần, budget alert $20/$50/$80 |
| NFR-07 | Toàn bộ hạ tầng dựng lại bằng 1 lệnh, xoá sạch bằng 1 lệnh | `terraform apply` / `terraform destroy` chạy thành công từ đầu |
| NFR-08 | Mọi số liệu hiển thị kèm thông tin độ tin cậy nguồn | Dashboard/bot luôn hiển thị cờ TƯƠI/GIÁN ĐOẠN/CHẾT cạnh giá trị |

---

## 5. Kiến trúc tính năng (logic, chưa gắn dịch vụ cụ thể)

```
┌─────────────────────┐
│  Nguồn dữ liệu       │
│  quan trắc Hà Nội    │
└──────────┬───────────┘
           │
           ▼
┌─────────────────────┐
│  THU THẬP DỮ LIỆU    │
└──────────┬───────────┘
           │
           ▼
┌─────────────────────┐
│  XỬ LÝ & LÀM SẠCH    │
│  DÒNG DỮ LIỆU         │
└──────────┬───────────┘
           │
           ▼
┌───────────────────────────────────────┐
│  LƯU TRỮ THEO LỚP                       │
│  Bronze ──► Silver ──► Gold             │
└───────┬─────────────────────┬───────────┘
        │                     │
        ▼                     ▼
┌────────────────┐   ┌──────────────────┐
│  CACHE           │   │  LỚP TRUY VẤN &    │
│  (giá trị mới    │   │  PHÂN TÍCH          │
│  nhất, sẵn dùng) │   │                    │
└────────┬─────────┘   └─────────┬──────────┘
         │                       │
         └───────────┬───────────┘
                      ▼
             ┌─────────────────┐
             │  LỚP PHỤC VỤ      │
             └────────┬──────────┘
                       │
         ┌─────────────┴──────────────┐
         ▼                             ▼
┌──────────────────┐         ┌─────────────────────┐
│  DASHBOARD          │         │  KHỐI PHÁT HIỆN &     │◄─ đọc Gold
│  (trực quan hoá)    │         │  PHÁT CẢNH BÁO        │
└─────────┬────────────┘         └──────────┬───────────┘
          │                                 │
          ▼                                 ▼
   ┌─────────────┐                  ┌───────────────┐
   │ Người dùng    │                  │ Thông báo cá     │
   │ web           │                  │ nhân hoá         │
   └─────────────┘                  └───────┬────────┘
                                             ▼
                                      ┌─────────────┐
                                      │ Người dùng    │
                                      │ Telegram      │
                                      └─────────────┘

┌─────────────────────────────────────────────────────┐
│  GIÁM SÁT SỨC KHOẺ HỆ THỐNG & ĐỘ TIN CẬY DỮ LIỆU        │
│  (theo dõi xuyên suốt: Thu thập, Xử lý, Lưu trữ)         │
└─────────────────────────────────────────────────────┘
```

---

## 6. Kiến trúc triển khai — theo từng phân hệ (có giao thức/giao diện giữa các thành phần)

### 6.1. Thu thập & Lưu trữ
```
┌──────────────────┐            ┌───────────────────────────┐
│  OpenAQ API        │──HTTPS────►│  EventBridge Scheduler      │
│  (REST, 60 req/phút)│  5 phút   │  + AWS Lambda (Poller)       │
└──────────────────┘            └──────────────┬──────────────┘
                                                │ Kafka Producer
┌──────────────────┐                           ▼
│  S3 Archive         │                 ┌──────────────────────┐
│  openaq-data-       │                 │  Apache Kafka           │
│  archive (lịch sử)  │                 │  (EC2, 3 broker)        │
│  --no-sign-request  │                 │  topic: aq_measurements │
└─────────┬──────────┘                 └───────────┬─────────────┘
          │ Spark Batch                             │ Kafka Consumer
          │ (đọc S3 trực tiếp)                       ▼
          │                                ┌──────────────────────┐
          └───────────────────────────────►│ Spark Structured       │
                                            │ Streaming (EC2 ASG)    │
                                            └───────────┬─────────────┘
                                                        │ Iceberg / S3 API
                                                        ▼
                                            ┌────────────────────────────┐
                                            │ Amazon S3 + AWS Glue          │
                                            │ Data Catalog                  │
                                            │ Bronze / Silver / Gold         │
                                            │ (Apache Iceberg)               │
                                            └────────────────────────────┘
```

### 6.2. Truy vấn & Dashboard
```
┌────────────────────────────┐
│ S3 + Glue (Iceberg)           │
└──────┬──────────────┬─────────┘
       │ JDBC          │ Athena API
       ▼               ▼
┌──────────────┐  ┌───────────────────┐
│ Trino          │  │ Amazon Athena        │
│ (EC2 ASG,      │  │ (serverless)         │
│ tự dựng)       │  │                      │
└──────┬─────────┘  └─────────┬────────────┘
       │ JDBC                  │ JDBC
       └──────────┬────────────┘
                   ▼
         ┌───────────────────┐
         │ Apache Superset      │
         │ (EC2, sau ALB+WAF)   │
         └─────────┬────────────┘
                    │ HTTPS
                    ▼
            ┌───────────────┐
            │ Người dùng web  │
            └───────────────┘
```

### 6.3. Cảnh báo (Bot Telegram)
```
┌─────────────────────────┐
│ S3+Glue (Gold table:       │
│ pm25_aqi_hourly)            │
└─────────────┬──────────────┘
              │ Spark job kiểm tra ngưỡng (định kỳ, EventBridge Scheduler)
              ▼
       ┌─────────────┐
       │ Amazon SNS     │
       └──────┬──────────┘
              │ trigger
              ▼
   ┌───────────────────┐         ┌────────────────────────┐
   │ AWS Lambda            │──đọc──►│ Amazon DynamoDB            │
   │ (gửi cảnh báo)        │        │ (bảng subscriptions)       │
   └──────────┬─────────────┘◄──ghi─┤                            │
              │ qua SQS               └────────────▲───────────────┘
              ▼                                     │ đọc/ghi
       ┌─────────────┐                    ┌─────────┴──────────────┐
       │ Amazon SQS     │                   │ AWS Lambda                │
       └──────┬──────────┘                   │ (xử lý lệnh bot)          │
              │ Telegram Bot API (HTTPS)      └────────────▲───────────────┘
              ▼                                             │ invoke
    ┌─────────────────────┐                      ┌─────────┴──────────┐
    │ Người dùng Telegram    │──Webhook (HTTPS)──►│ API Gateway            │
    └─────────────────────┘                      └────────────────────────┘
```

### 6.4. Vận hành (giám sát, bảo mật, IaC)
```
┌─────────────────────┐        ┌────────────────────────────┐
│ Amazon CloudWatch      │◄──giám sát──┤ Kafka, Spark, Trino, Superset │
│ (metric, alarm)        │              │ (các máy EC2)                 │
└─────────────────────┘        └────────────────────────────┘

┌─────────────────────┐        ┌────────────────────────────┐
│ Terraform (IaC)         │──triển khai──►│ Toàn bộ hạ tầng AWS           │
└──────────▲──────────────┘        └────────────────────────────┘
           │ áp dụng qua
┌──────────┴──────────────┐
│ GitHub Actions + OIDC      │
│ (CI/CD)                    │
└─────────────────────────┘

Xuyên suốt: IAM least-privilege · AWS KMS (mã hoá) · Secrets Manager
(không dùng khoá tĩnh) · AWS WAF (chặn trước ALB/Superset) ·
SSM Session Manager (thay SSH)
```

---

## 7. Đặc tả dữ liệu — mọi bảng, mọi cột

### 7.1. Bronze — `local.bronze.aq_measurements` (giữ nguyên thô, toàn bộ STRING)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| location_id | STRING | ID trạm |
| sensors_id | STRING | ID cảm biến |
| location | STRING | Tên trạm (kèm hậu tố nội bộ của OpenAQ) |
| datetime | STRING | Có múi giờ, dạng `...+07:00` |
| lat, lon | STRING | Toạ độ |
| parameter | STRING | Tên chất đo (pm25, co, no2...) |
| units | STRING | Đơn vị đo |
| value | STRING | Giá trị đo |
| kafka_ts | TIMESTAMP | Thời điểm Kafka nhận bản ghi (thêm khi ghi Bronze) |

### 7.2. Silver — `local.silver.aq_measurements` (đã ép kiểu, khử trùng)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| location_id | INT | |
| sensors_id | INT | |
| parameter | STRING | |
| value | DOUBLE | Đã lọc giá trị âm/null |
| units | STRING | |
| measured_at | TIMESTAMP | Đã chuyển từ chuỗi có +07:00 |
| ingested_at | TIMESTAMP | Thời điểm Silver xử lý |

Khoá khử trùng: `(location_id, sensors_id, measured_at)`, chiến lược `MERGE INTO ... WHEN NOT MATCHED THEN INSERT`.

### 7.3. Silver — `local.silver.sensor_health`

| Cột | Kiểu | Ghi chú |
|---|---|---|
| location_id, sensors_id, parameter | INT/INT/STRING | Khoá |
| last_measured_at | TIMESTAMP | |
| age_hours | DOUBLE | |
| status | STRING | `TUOI` / `GIAN_DOAN` / `CHET` |

### 7.4. Silver — `local.silver.aqi_breakpoints` (bảng tham chiếu, SCD Type 2)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| parameter | STRING | Hiện tại chỉ `pm25` |
| aqi_lo, aqi_hi | INT | Khoảng điểm AQI |
| conc_lo, conc_hi | DOUBLE | Khoảng nồng độ tương ứng (µg/m³) |
| source | STRING | `QD1459-TCMT-2019` |
| effective_from, effective_to | DATE | NULL ở effective_to nếu đang hiệu lực |
| is_current | BOOLEAN | |

### 7.5. Gold — `local.gold.dim_location` (dimension table)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| location_id | INT | |
| name | STRING | |
| lat, lon | DOUBLE | |
| station_type | STRING | `reference` hoặc `low_cost` |

### 7.6. Gold — `local.gold.pm25_aqi_hourly` (fact table)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| location_id | INT | Khoá ngoại tới `dim_location` |
| hour_start | TIMESTAMP | |
| pm25_avg | DOUBLE | Trung bình giờ (MVP — chưa áp trọng số NowCast) |
| aqi | INT | Tính theo `aqi_breakpoints` |

### 7.7. Gold — bảng đánh giá cảnh báo (`alert_eval`, tính từ `pm25_aqi_hourly`)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| location_id, hour_start, aqi | — | Kế thừa |
| prev_aqi | INT | AQI giờ trước (window `lag`) |
| should_alert | BOOLEAN | `aqi>=150 AND prev_aqi>=150` (ví dụ ngưỡng) |

### 7.8. DynamoDB — `subscriptions` (lưu đăng ký cảnh báo của người dùng)

| Khoá/cột | Kiểu | Ghi chú |
|---|---|---|
| station_id (PK) | String | Khoá phân vùng |
| chat_id (SK) | String | Khoá sắp xếp — ID chat Telegram |
| threshold | Number | Ngưỡng AQI người dùng đặt |
| last_alert_ts | String | Chống gửi trùng liên tục |

---

## 8. Đặc tả nguồn dữ liệu (OpenAQ) — đã xác minh thật

### 8.1. API & giới hạn
- `https://api.openaq.org/v3` — cần khoá miễn phí (đăng ký tại `explore.openaq.org`).
- Giới hạn: **60 lượt gọi/phút**.

### 8.2. Ba trạm còn hoạt động (trong 37 trạm trong bán kính 25km, 34 trạm còn lại đã ngừng hoạt động từ ~01/2024)

| Trạm | location_id | Loại | Hoạt động từ | Nhịp đo |
|---|---|---|---|---|
| Công viên Nhân Chính | 4946812 | Trạm chuẩn, Hanoi AQMN | 2025-07-03 | ~5 phút |
| Số 1 đường Giải Phóng | 4946813 | Trạm chuẩn, Hanoi AQMN | 2025-07-03 | ~5 phút |
| OceanPark | 6123215 | Cảm biến giá rẻ, AirGradient | 2025-11-08 | 1 giờ |

### 8.3. Cảm biến con theo từng trạm (sensors_id, trạng thái đã xác minh)

| Trạm | parameter | sensors_id | Trạng thái |
|---|---|---|---|
| Nhân Chính | co | 13502163 | CHẾT (~133 ngày) |
| Nhân Chính | no2 | 13502162 | Tươi |
| Nhân Chính | o3 | 13502148 | Tươi |
| Nhân Chính | pm10 | 13502153 | Tươi |
| Nhân Chính | **pm25** | **13502151** | Tươi |
| Nhân Chính | so2 | 13502157 | Tươi |
| Giải Phóng | co | 13502156 | CHẾT (~163 ngày) |
| Giải Phóng | no2 | 13502155 | GIÁN ĐOẠN (~51h) |
| Giải Phóng | o3 | 13502149 | Tươi |
| Giải Phóng | pm10 | 13502152 | Tươi |
| Giải Phóng | **pm25** | **13502154** | Tươi |
| Giải Phóng | so2 | 13502164 | Tươi |
| OceanPark | pm1 | 14581374 | Tươi |
| OceanPark | **pm25** | **14581375** | Tươi |
| OceanPark | relativehumidity | 14581376 | Tươi |
| OceanPark | temperature | 14581377 | Tươi |
| OceanPark | um003 (số hạt PM0.3/cm³) | 14581378 | Tươi |

### 8.4. Giấy phép
- OceanPark/AirGradient: **CC BY 4.0** (hiệu lực từ 2023-07-15) → được phép tải/công bố lại, ghi công.
- Nhân Chính, Giải Phóng: `licenses: null`, chủ sở hữu "Unknown Governmental Organization" → chỉ hiển thị, không công bố lại.

### 8.5. Kho lưu trữ lịch sử
`s3://openaq-data-archive/records/csv.gz/locationid={id}/year={yyyy}/month={mm}/location-{id}-{yyyymmdd}.csv.gz` — công khai, `--no-sign-request`, xác nhận đủ dữ liệu từ ngày bắt đầu mỗi trạm đến 2026-09-30.

### 8.6. Schema CSV
`location_id, sensors_id, location, datetime, lat, lon, parameter, units, value` — dạng dài (mỗi dòng = 1 lần đo của 1 cảm biến), `datetime` có `+07:00`.

### 8.7. Khối lượng ước tính
~1–1,5 triệu bản ghi lịch sử tính đến hiện tại.

---

## 9. Đặc tả giao diện

### 9.1. Lệnh bot Telegram
| Lệnh | Tham số | Hành vi |
|---|---|---|
| `/start` | — | Chào, liệt kê danh sách trạm |
| `/subscribe` | `<trạm> <ngưỡng>` | Ghi vào `subscriptions` |
| `/now` | — | Trả giá trị PM2.5 mới nhất của trạm đã đăng ký |
| `/best` | — | Trả khung giờ trung bình thấp nhất trong lịch sử |

### 9.2. Giao diện nội bộ giữa các thành phần
| Từ | Đến | Giao thức/định dạng |
|---|---|---|
| Lambda Poller | Kafka | Kafka producer protocol, JSON, key=`location_id:sensors_id` |
| Kafka | Spark Streaming | Kafka consumer protocol |
| Spark | S3/Glue | Iceberg table API (ghi Parquet + metadata) |
| Trino/Athena | Superset | JDBC |
| Lambda cảnh báo | DynamoDB | AWS SDK (boto3), đọc/ghi item |
| Lambda | Telegram | Telegram Bot API qua HTTPS (`sendMessage`) |
| Telegram | API Gateway | Webhook HTTPS POST |

---

## 10. Đặc tả hạ tầng AWS

| Dịch vụ | Vai trò | Free Tier |
|---|---|---|
| EC2 + Auto Scaling Group | Kafka, Spark, Trino, Superset | Có |
| Application Load Balancer | Trước Superset | Có |
| AWS WAF | Chặn trước ALB | Có (trừ AWS Shield) |
| S3 | Lưu trữ Iceberg + checkpoint | Có |
| AWS Glue Data Catalog | Catalog cho Iceberg | Có |
| Amazon Athena | Truy vấn serverless | Có |
| AWS Lambda | Poller, xử lý bot, gửi cảnh báo | Có |
| API Gateway | Webhook Telegram | Có |
| Amazon DynamoDB | Lưu đăng ký người dùng | Có |
| Amazon SNS / SQS | Phân phối sự kiện cảnh báo | Có |
| EventBridge Scheduler | Lên lịch poller + job batch Silver/Gold | Có |
| Amazon CloudWatch | Giám sát, alarm | Có |
| AWS Secrets Manager | Khoá OpenAQ, token Telegram | Có |
| AWS KMS | Mã hoá dữ liệu | Có |
| IAM | Phân quyền | Có |
| Terraform (ngoài AWS) | IaC | — |
| GitHub Actions (ngoài AWS) | CI/CD | — |

**Không dùng:** Kubernetes/EKS, Kafka/MSK quản lý sẵn, Kinesis, Firehose, Airflow (lõi), NAT Gateway (tránh để giảm chi phí).

---

## 11. Đặc tả bảo mật

| Hạng mục | Biện pháp |
|---|---|
| Xác thực EC2 → AWS API | IAM instance profile, không access key tĩnh |
| Truy cập máy chủ | SSM Session Manager, không mở cổng SSH |
| Bí mật (khoá OpenAQ, token Telegram) | AWS Secrets Manager |
| Mã hoá khi lưu | AWS KMS cho S3/EBS |
| Mã hoá khi truyền | TLS cho mọi kết nối (HTTPS, JDBC qua TLS) |
| Chặn tấn công web | AWS WAF trước ALB/Superset |
| Phân quyền | IAM least-privilege theo từng vai trò dịch vụ |
| CI/CD | GitHub OIDC, không lưu access key trong repo |

---

## 12. Đặc tả CI/CD

Pipeline GitHub Actions: `lint` → `test` → `terraform plan` → `quét bảo mật (Checkov)` → `terraform apply` (qua OIDC, không khoá tĩnh) → `deploy job Spark/Lambda`.

---

## 13. Đặc tả giám sát & vận hành

| Đối tượng giám sát | Chỉ số | Hành động khi bất thường |
|---|---|---|
| Độ tươi dữ liệu theo cảm biến | `age_hours` | CloudWatch alarm khi >24h → thông báo đội vận hành |
| Hàng đợi SQS | Độ sâu hàng đợi | Alarm nếu tăng bất thường (dấu hiệu Lambda gửi cảnh báo bị nghẽn) |
| EC2 (Kafka/Trino/Spark) | CPU, bộ nhớ | ASG tự scale theo ngưỡng CPU |
| Chi phí | Billing | Budget alert $20/$50/$80 |

---

## 14. Kế hoạch kiểm thử

| Loại kiểm thử | Nội dung |
|---|---|
| Kiểm thử tải (k6) | Tăng dần số truy vấn đồng thời tới Trino, quan sát số worker ASG và độ trễ p95 |
| Kiểm thử chịu lỗi | Kill 1 broker Kafka, kill 1 worker Spark — kiểm tra không mất/không trùng dữ liệu |
| Minh hoạ schema evolution | `ALTER TABLE ADD COLUMN`, chứng minh đọc được dữ liệu cũ |
| Minh hoạ khôi phục (time travel) | Ghi sai dữ liệu có chủ đích, rollback về snapshot trước |
| So sánh Trino với Athena | Cùng truy vấn, đo thời gian, chi phí, công sức vận hành |
| Kiểm thử chất lượng dữ liệu | Input có giá trị âm/trùng/thiếu cột → kiểm tra Silver xử lý đúng |
| Kiểm thử tính idempotent | Chạy lại job Silver 2 lần liên tiếp cùng input, số dòng không đổi |

---

## 15. Ước tính chi phí (ước lượng, cần đối chiếu lại bằng AWS Pricing Calculator)

| Thành phần | Ước tính/tháng nếu bật 24/7 |
|---|---|
| EC2 (Kafka, Spark, Trino, Superset) | ~$60–75 |
| ALB | ~$16–20 |
| S3 + Glue + Athena | Vài USD (theo lượng quét) |
| Lambda, DynamoDB, SQS/SNS | Gần như miễn phí ở quy mô dự án |

**Nguyên tắc kiểm soát:** phát triển cục bộ bằng Docker, chỉ `terraform apply` khi test/demo, `terraform destroy` cuối mỗi buổi, dùng Spot cho worker, tránh NAT Gateway, budget alert $20/$50/$80.

---

## 16. Quản lý rủi ro

| Rủi ro | Khả năng | Phương án |
|---|---|---|
| Nguồn OpenAQ ngừng cập nhật | Thấp | Đã có 15 tháng lịch sử tải sẵn, đủ demo |
| Kafka/Trino tự dựng không ổn định | Trung bình | Ghi lại làm bằng chứng học thật; hạ xuống 1 broker nếu cần |
| Trễ tiến độ | Trung bình | Cắt theo thứ tự: WAF → giữ 1 trong 2 (Trino/Athena) → Kafka 1 broker |
| Một người ôm hết việc | Trung bình | Mỗi vai trò tự chạy và báo kết quả bằng số liệu cụ thể |
| Vượt chi phí free tier | Thấp (nếu tuân thủ nguyên tắc Mục 15) | Budget alert, destroy cuối buổi |
| Môi trường Windows gây lỗi PySpark | Trung bình | Chạy trong WSL2 Ubuntu, không chạy thuần Windows |

---

## 17. Phân vai trò & kế hoạch triển khai (tóm tắt — chi tiết xem báo cáo tiến độ đã nộp)

| Vai trò | Phụ trách | Giai đoạn chính |
|---|---|---|
| A — Mai Anh | Mô hình dữ liệu, Silver, Gold | A (Tuần 1-3), toàn trình review |
| B — Minh Khuê | Terraform, IAM, CI/CD, chi phí | A, B |
| C — Khánh Nam | Kafka, poller, hỗ trợ Trino | A, B |
| D — Phúc Thịnh | Superset, Bot, kiểm thử, báo cáo | B, C |

7 tuần: A (Tuần 1-3) → B (Tuần 3-5, bot/Superset chạy song song từ Tuần 3) → C (Tuần 6) → Hoàn thiện (Tuần 7).

---

## 18. Tiêu chí nghiệm thu từng giai đoạn

| Giai đoạn | Đạt khi |
|---|---|
| A | Pipeline Bronze→Silver→Gold chạy trên AWS, số liệu khớp nguồn, Terraform apply/destroy một lệnh |
| B | Kafka chịu mất 1 broker, Trino tự scale, bot gửi được cảnh báo thật, dashboard truy cập được |
| C | Có số liệu load test, chaos test, so sánh Trino/Athena, bảng chi phí thật |
| Hoàn thiện | Báo cáo đầy đủ, video demo dự phòng, tập trình bày ≥2 lần |

---

## 19. Phụ lục tra cứu nhanh

| Mục | Giá trị |
|---|---|
| OpenAQ API | `https://api.openaq.org/v3` |
| S3 archive | `s3://openaq-data-archive/records/csv.gz/locationid={id}/...` |
| location_id (Nhân Chính/Giải Phóng/OceanPark) | 4946812 / 4946813 / 6123215 |
| sensors_id PM2.5 (theo thứ tự trên) | 13502151 / 13502154 / 14581375 |
| Kafka topic | `aq_measurements` |
| Rate limit OpenAQ | 60 req/phút |
| Chuẩn AQI | VN_AQI, QĐ 1459/QĐ-TCMT 2019 |
| Region AWS | `us-east-1` |
