# AirLake — Bản đặc tả kỹ thuật (Technical Specification)

**Phiên bản:** 1.0 · **Dự án:** AirLake — Nền tảng dữ liệu và cảnh báo chất lượng không khí Hà Nội trên AWS · **Môn:** Điện toán đám mây

---

## Implementation status and evidence

This document contains target requirements, local data contracts and historical
survey notes. FR/NFR, AWS diagrams, security/CI/CD design and phase acceptance are
targets, not claims that the local branch has deployed them.

| Scope | Implemented / reviewed | Evidence boundary |
| --- | --- | --- |
| Local ingestion | CSV replay and Kafka-to-Iceberg Bronze | Source and inert-client checks; no new Kafka/restart runtime acceptance |
| Local Silver | UTC parsing, typed values, deterministic dedup, insert-only MERGE | Reviewed source; finite-value filtering, unit conversion and persistent rejects are deferred |
| Local Gold | Dimension/name repair and simplified hourly PM2.5 AQI | Retained 2026-10-25 result: 48 integration and 9 orchestration methods passed; exact source hashes in tests/evidence |
| AWS infrastructure | Source audited on khue/nam | Separate owner work; no deployment acceptance in this PR |
| Bot/serving | Source previously audited on thinh; target interfaces below | Latest branch content not merged; schema/reliability/security review pending |

See [README.md](README.md) for operational commands,
[tests/README.md](tests/README.md) for verification scope and evidence format, and
[AirLake_PROJECT_CONTEXT.md](AirLake_PROJECT_CONTEXT.md) for team ownership and
historical context. Local Gold acceptance does not establish official VN_AQI,
cloud deployment, fault tolerance or production readiness. The historically
exposed bot token remains a publication blocker until its owner confirms rotation.

---

## MỤC LỤC

1. Giới thiệu & thuật ngữ
2. Tổng quan sản phẩm
3. Yêu cầu chức năng (FR)
4. Yêu cầu phi chức năng (NFR)
5. Kiến trúc tính năng (sơ đồ ASCII)
6. Kiến trúc triển khai (sơ đồ ASCII, theo từng phân hệ)
7. Đặc tả dữ liệu — mọi bảng, mọi cột
8. Đặc tả nguồn dữ liệu (OpenAQ) — khảo sát lịch sử
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

**Giải pháp mục tiêu (chưa nghiệm thu đầy đủ):** AirLake thu thập liên tục dữ liệu chất lượng không khí Hà Nội, lưu trữ dài hạn, tính AQI theo chuẩn Việt Nam, cảnh báo chủ động qua Telegram, và minh bạch về tình trạng hoạt động của từng nguồn đo.

**Người dùng:** (1) người dân/sinh viên Hà Nội, đặc biệt nhóm nhạy cảm hô hấp; (2) sinh viên/nhà nghiên cứu cần dữ liệu lịch sử sạch; (3) đội vận hành nội bộ.

---

## 3. Yêu cầu chức năng mục tiêu (Functional Requirements)

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

## 4. Yêu cầu phi chức năng mục tiêu (Non-functional Requirements)

| Mã | Yêu cầu | Cách đo |
|---|---|---|
| NFR-01 | Độ trễ từ thời điểm đo đến khi hiển thị trên dashboard ở mức chấp nhận được | Đo `ingested_at - measured_at`, báo cáo p50/p95 ở Giai đoạn C |
| NFR-02 | Lớp truy vấn (Trino) tự động mở rộng theo tải truy vấn đồng thời | k6 tăng dần số truy vấn song song, quan sát số worker ASG |
| NFR-03 | Kafka chịu được mất 1 broker (trong cụm 3) mà không mất dữ liệu | Kill 1 broker giữa lúc đang ghi, kiểm tra số bản ghi không đổi |
| NFR-04 | Có thể khôi phục Silver/Gold về trạng thái trước đó khi phát hiện lỗi | Demo time travel: rollback về snapshot cũ |
| NFR-05 | Không dùng access key tĩnh; dữ liệu mã hoá khi lưu trữ (KMS) và truyền tải (TLS) | Rà soát Terraform + IAM policy |
| NFR-06 | Chi phí nằm trong ngân sách được B xác nhận; không mặc định có free credit | Theo dõi chi phí, budget alert $20/$50/$80 là kế hoạch ban đầu |
| NFR-07 | Toàn bộ hạ tầng dựng lại bằng 1 lệnh, xoá sạch bằng 1 lệnh | `terraform apply` / `terraform destroy` chạy thành công từ đầu |
| NFR-08 | Mọi số liệu hiển thị kèm thông tin độ tin cậy nguồn | Dashboard/bot luôn hiển thị cờ TƯƠI/GIÁN ĐOẠN/CHẾT cạnh giá trị |

---

## 5. Kiến trúc tính năng mục tiêu (không phải trạng thái triển khai)

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

## 6. Kiến trúc triển khai mục tiêu — theo từng phân hệ

### 6.1. Thu thập & Lưu trữ
```
┌──────────────────┐            ┌───────────────────────────┐
│  OpenAQ API        │──HTTPS────►│  EventBridge Scheduler      │
│  (REST, quota API) │  5 phút   │  + AWS Lambda (Poller)       │
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

### 7.1. Bronze — `local.bronze.aq_measurements` (parsed source strings)

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

Local behavior: `bronze_local.py` drops records whose parsed location_id is null, including malformed JSON, and does not retain Kafka payload/partition/offset. Other parsed measurement fields remain strings. A fully lossless raw/reject stream is a pending contract; this cleanup does not change the filter. Reuse the existing checkpoint on restart. startingOffsets=earliest governs a new query, not recovery from an existing checkpoint.

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

`datetime` được parse theo ISO-8601 có offset tường minh (ví dụ `+07:00`) với Spark session time zone đặt là UTC; `measured_at` biểu diễn cùng một thời điểm tuyệt đối ở UTC. Bản ghi bị loại nếu `location_id`/`sensors_id` không chuyển được sang INT, `datetime` thiếu/không parse được, hoặc `value` thiếu/không chuyển được sang DOUBLE/âm. Các lý do loại được tính loại trừ lẫn nhau theo thứ tự: location ID, sensor ID, datetime, value thiếu/không phải số, value âm. Không áp ngưỡng giá trị tối đa khi chưa có quy tắc nghiệp vụ được duyệt; `parameter` và `units` nullable được giữ nguyên vì đặc tả hiện tại không quy định loại chúng.

Nếu Bronze có nhiều dòng cùng khoá nhưng payload khác nhau, Silver chọn dòng có `kafka_ts` mới nhất; nếu bằng nhau, chọn dòng có thứ tự từ điển nhỏ nhất trên toàn bộ các cột Bronze. Đây là quy tắc ổn định để phân xử trường hợp chưa được quy định nghiệp vụ. `MERGE` insert-only không ghi đè một khoá đã có trong Silver; nếu không có khoá mới thì job bỏ qua thao tác ghi để tránh tạo snapshot rỗng. Việc sửa lại một phép đo cùng khoá cần một quy tắc cập nhật được thống nhất riêng.

Silver does not uniformly reject NaN/+Infinity after DOUBLE casting, and it does not convert units. Gold independently rejects non-finite PM2.5 measurements and aggregates. Adding Silver finiteness/unit rules, supporting additional timestamp formats or correcting existing keys requires dedicated tests and a reviewed contract. Reported valid_count is derived from source/rejected counts, not an independent reconciliation query.

### 7.3. Silver — `local.silver.sensor_health` (target schema; no local writer)

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

The unchanged project seed uses concentrations (µg/m³) 0, 25, 50, 80, 150, 250, 350, 500 paired with AQI 0, 50, 100, 150, 200, 300, 400, 500. It stores seven intervals with source=QD1459-TCMT-2019, effective_from=2019-11-12, effective_to=NULL and is_current=true. This is the existing MVP configuration, not a fresh primary-source verification or a confirmed legal effective date. Review the primary regulation and breakpoint-history rules before claiming regulatory/SCD completeness; this cleanup does not change values, intervals or formulas.

### 7.5. Gold — `local.gold.dim_location` (dimension table)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| location_id | INT | |
| name | STRING | |
| lat, lon | DOUBLE | |
| station_type | STRING | `reference` hoặc `low_cost` |

**Chính sách tên trạm do chủ dự án phê duyệt:** tên hiện có hợp lệ được giữ nguyên. Chỉ `name=NULL` mới được sửa tự động, khi Bronze xác định duy nhất một bộ metadata hợp lệ cho trạm. Các dòng Bronze hợp lệ lặp giống nhau được chấp nhận; metadata thiếu, tên null/rỗng/chỉ có khoảng trắng, toạ độ không ép được sang DOUBLE hữu hạn, hoặc tên/toạ độ mâu thuẫn đều làm batch bị từ chối trước mọi thao tác ghi.

Phạm vi metadata bắt buộc là hợp của các `location_id` trong Silver và **mọi khoá dimension hiện có với `name=NULL`**, kể cả khi khoá không có trong Silver hoặc Silver rỗng. Legacy null-name có metadata hợp lệ, duy nhất được sửa tên; thiếu hoặc không hợp lệ thì từ chối batch. Tên hợp lệ của legacy record ngoài Silver không cần tra lại Bronze. Tên target rỗng nhưng không null bị từ chối, không tự chuyển thành null hoặc sửa theo chính sách này. Toạ độ và `station_type` hiện có được giữ nguyên; không tự sửa metadata lịch sử khác. Target có khoá/toạ độ không hợp lệ hoặc `station_type` không khớp mapping trạm ở Mục 8.2 phải bị từ chối.

Triển khai Gold cục bộ kiểm tra schema, metadata/repair plan, khoá target, số hữu hạn và quan hệ fact–dimension trước khi khởi tạo breakpoint, tạo bảng hoặc MERGE. Sau ghi, dimension không được còn tên null/rỗng. Đây là kiểm tra trước ghi cho các lỗi xác định được, không phải giao dịch nguyên tử hay rollback giữa nhiều bảng Iceberg; lỗi I/O hoặc thay đổi đồng thời sau preflight vẫn có thể để lại ghi từng phần. Các yêu cầu AWS/FR ở phần khác là mục tiêu riêng, không được xem là đã nghiệm thu chỉ từ kiểm thử Gold cục bộ.

### 7.6. Gold — `local.gold.pm25_aqi_hourly` (fact table)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| location_id | INT | Khoá ngoại tới `dim_location` |
| hour_start | TIMESTAMP | |
| pm25_avg | DOUBLE | Trung bình giờ (MVP — chưa áp trọng số NowCast) |
| aqi | INT | Tính theo `aqi_breakpoints` |

Triển khai MVP cục bộ: `gold_batch.py` dùng metadata Bronze và mapping trạm ở Mục 8.2 để tạo dimension mới; tên null hiện có được sửa theo Mục 7.5, còn payload hiện có khác được giữ nguyên. PM2.5 và kết quả tổng hợp phải hữu hạn, không âm; null, NaN và vô cực bị từ chối trước ghi. Thời gian được nhóm theo giờ UTC. Mỗi giờ có ít nhất một quan trắc được giữ lại và `pm25_avg` là trung bình số học của các giá trị có đơn vị `µg/m³`; hiện chưa có ngưỡng completeness cho giờ thiếu quan trắc. AQI dùng nội suy tuyến tính giữa các breakpoint và làm tròn thành số nguyên; giá trị từ 500 µg/m³ trở lên được giới hạn ở AQI 500.

**MVP limitation:** this is simplified hourly-mean AQI, not official VN_AQI. NowCast and regulatory completeness are not implemented. The official calculation, required input coverage and effective dates need primary-source review before replacing the approved MVP.

### 7.7. Gold — bảng đánh giá cảnh báo (`alert_eval`, deferred target)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| location_id, hour_start, aqi | — | Kế thừa |
| prev_aqi | INT | AQI giờ trước (window `lag`) |
| should_alert | BOOLEAN | `aqi>=150 AND prev_aqi>=150` (ví dụ ngưỡng) |

`150` chỉ là ví dụ trong đặc tả, không phải ngưỡng chung đã duyệt. Chưa tạo `alert_eval` cho đến khi thống nhất dùng ngưỡng toàn cục hay ngưỡng riêng từ `subscriptions`.

### 7.8. DynamoDB — `subscriptions` (target interface, not current bot schema)

| Khoá/cột | Kiểu | Ghi chú |
|---|---|---|
| station_id (PK) | String | Khoá phân vùng |
| chat_id (SK) | String | Khoá sắp xếp — ID chat Telegram |
| threshold | Number | Ngưỡng AQI người dùng đặt |
| last_alert_ts | String | Chống gửi trùng liên tục |

---

## 8. Đặc tả nguồn dữ liệu (OpenAQ) — khảo sát lịch sử

The following station/sensor details were retained from the initial survey. Cadence, freshness, license and availability observations are historical, not live checks. The original roughly 51-hour NO2 observation was labeled intermittent; that contradicts the shared >24-hour CHET threshold and is corrected below. Supported station IDs/types remain the project mapping.

### 8.1. API & giới hạn
- API v3: `https://api.openaq.org/v3`; credentials and current account terms must be checked separately.
- The original survey reported 60 requests/minute. This is not a current API/account limit verified by this PR.

### 8.2. Project station mapping and historically reported cadence

| Trạm | location_id | Loại | Hoạt động từ | Nhịp đo |
|---|---|---|---|---|
| Công viên Nhân Chính | 4946812 | Trạm chuẩn, Hanoi AQMN | 2025-07-03 | ~5 phút |
| Số 1 đường Giải Phóng | 4946813 | Trạm chuẩn, Hanoi AQMN | 2025-07-03 | ~5 phút |
| OceanPark | 6123215 | Cảm biến giá rẻ, AirGradient | 2025-11-08 | 1 giờ |

### 8.3. Historical sensor survey (not current sensor health)

| Trạm | parameter | sensors_id | Trạng thái |
|---|---|---|---|
| Nhân Chính | co | 13502163 | CHẾT (~133 ngày) |
| Nhân Chính | no2 | 13502162 | Tươi |
| Nhân Chính | o3 | 13502148 | Tươi |
| Nhân Chính | pm10 | 13502153 | Tươi |
| Nhân Chính | **pm25** | **13502151** | Tươi |
| Nhân Chính | so2 | 13502157 | Tươi |
| Giải Phóng | co | 13502156 | CHẾT (~163 ngày) |
| Giải Phóng | no2 | 13502155 | Reported age ~51h; CHẾT under the >24h contract |
| Giải Phóng | o3 | 13502149 | Tươi |
| Giải Phóng | pm10 | 13502152 | Tươi |
| Giải Phóng | **pm25** | **13502154** | Tươi |
| Giải Phóng | so2 | 13502164 | Tươi |
| OceanPark | pm1 | 14581374 | Tươi |
| OceanPark | **pm25** | **14581375** | Tươi |
| OceanPark | relativehumidity | 14581376 | Tươi |
| OceanPark | temperature | 14581377 | Tươi |
| OceanPark | um003 (số hạt PM0.3/cm³) | 14581378 | Tươi |

### 8.4. Historical license observations

The survey reported CC BY 4.0 metadata for OceanPark/AirGradient and a null license value for Nhân Chính/Giải Phóng. These observations were not refreshed here and do not establish present display/export permission. Verify source licenses before redistribution and retain required AirGradient, Hanoi Air Quality Monitoring Network and OpenAQ attribution. A null license value is not a grant. No raw data is included in this PR.

### 8.5. Kho lưu trữ lịch sử
`s3://openaq-data-archive/records/csv.gz/locationid={id}/year={yyyy}/month={mm}/location-{id}-{yyyymmdd}.csv.gz` is the archive pattern recorded by the earlier survey, which reported coverage through 2026-09-30. Current availability and completeness were not checked here. Dataset acquisition is separate from the local test suite.

### 8.6. Schema CSV
`location_id, sensors_id, location, datetime, lat, lon, parameter, units, value` — dạng dài (mỗi dòng = 1 lần đo của 1 cảm biến), `datetime` có `+07:00`.

### 8.7. Khối lượng ước tính
The original context estimated 1–1.5 million historical observations; this is not a current measured table count.

---

## 9. Đặc tả giao diện mục tiêu

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

## 10. Đặc tả hạ tầng AWS mục tiêu

| Dịch vụ | Vai trò mục tiêu |
|---|---|
| EC2 + Auto Scaling Group | Kafka, Spark, Trino, Superset |
| Application Load Balancer | Trước Superset |
| AWS WAF | Chặn trước ALB |
| S3 | Lưu trữ Iceberg + checkpoint |
| AWS Glue Data Catalog | Catalog cho Iceberg |
| Amazon Athena | Truy vấn serverless |
| AWS Lambda | Poller, xử lý bot, gửi cảnh báo |
| API Gateway | Webhook Telegram |
| Amazon DynamoDB | Lưu đăng ký người dùng |
| Amazon SNS / SQS | Phân phối sự kiện cảnh báo |
| EventBridge Scheduler | Lên lịch poller + job batch Silver/Gold |
| Amazon CloudWatch | Giám sát, alarm |
| AWS Secrets Manager | Khoá OpenAQ, token Telegram |
| AWS KMS | Mã hoá dữ liệu |
| IAM | Phân quyền |
| Terraform (ngoài AWS) | IaC |
| GitHub Actions (ngoài AWS) | CI/CD |

Account eligibility, free credits and prices are not verified by this specification. Audited khue/nam source contains infrastructure scaffolding; it does not prove applications are running or accepted.

**Không dùng:** Kubernetes/EKS, Kafka/MSK quản lý sẵn, Kinesis, Firehose, Airflow (lõi), NAT Gateway (tránh để giảm chi phí).

---

## 11. Đặc tả bảo mật mục tiêu

The requirements below remain targets. Audited source uses SSM SecureString rather than Secrets Manager, AES256 S3 configuration rather than explicit KMS, and HTTP/plaintext listeners. These differences require owner/security review before deployment; they are not silently treated as approved equivalents. Local Compose is development-only.

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

## 12. Đặc tả CI/CD mục tiêu

Pipeline GitHub Actions: `lint` → `test` → `terraform plan` → `quét bảo mật (Checkov)` → `terraform apply` (qua OIDC, không khoá tĩnh) → `deploy job Spark/Lambda`.

This is the target pipeline. The audited remote workflow defines Terraform format/init-without-backend/validate, lint, and soft-failing Checkov. Python regression gates, OIDC deployment, plan/apply and application rollout are not established by that workflow. No GitHub Actions or AWS execution is claimed here.

---

## 13. Đặc tả giám sát & vận hành mục tiêu

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
| Gold P0 — tên dimension | Sửa null-name với metadata Bronze duy nhất; chấp nhận dòng lặp giống nhau; kiểm tra cả legacy key ngoài Silver và Silver rỗng; không sửa tên target blank hoặc metadata lịch sử khác |
| Gold P0 — từ chối trước ghi | Metadata thiếu/null/blank/mâu thuẫn/toạ độ không hợp lệ, target không hợp lệ, PM2.5 hoặc AVG không hữu hạn → RuntimeError trước khởi tạo breakpoint/tạo bảng/MERGE; rows và snapshot IDs không đổi |
| Gold P0 — chạy lại sau sửa tên | Dữ liệu đã sửa giữ nguyên và không thêm snapshot khi chạy lại với cùng input; kiểm thử dùng catalog/warehouse tạm ngoài repository, xem `tests/README.md` |

---

## 15. Cost planning and resource lifecycle

The original document estimated EC2 at $60–75/month and ALB at $16–20/month. Those historical estimates were not validated and must not be used as current prices or a sufficient project budget. B owns the account/region, instance sizing, credits, current cost estimate and alerts. The original $20/$50/$80 alert thresholds remain planning inputs, not proof that alerts exist.

Earlier planning proposed Spot and end-of-session destruction; audited source instead has on-demand ASGs and night schedules under organization constraints. B must reconcile the resource lifecycle and state/data retention policy before any deployment or destructive operation. Local development and avoiding a NAT Gateway remain project choices, not free-tier guarantees.

---

## 16. Quản lý rủi ro

| Rủi ro | Khả năng | Phương án |
|---|---|---|
| Nguồn OpenAQ ngừng cập nhật | Chưa đánh giá | Kiểm tra lại độ phủ archive; khảo sát lịch sử không chứng minh dữ liệu hiện tại |
| Kafka/Trino tự dựng không ổn định | Trung bình | Ghi lại làm bằng chứng học thật; hạ xuống 1 broker nếu cần |
| Trễ tiến độ | Trung bình | Cắt theo thứ tự: WAF → giữ 1 trong 2 (Trino/Athena) → Kafka 1 broker |
| Một người ôm hết việc | Trung bình | Mỗi vai trò tự chạy và báo kết quả bằng số liệu cụ thể |
| Vượt ngân sách | Chưa đánh giá | B xác nhận ngân sách, cảnh báo và vòng đời tài nguyên; không mặc định free tier |
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

## 18. Tiêu chí nghiệm thu từng giai đoạn (targets, not achieved status)

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
| Rate limit OpenAQ | Historical survey: 60 req/phút; current limit unverified |
| Chuẩn AQI | Target: VN_AQI; implemented: simplified hourly MVP |
| Region AWS | Audited Terraform default: `ap-southeast-2`; B must confirm deployment region |
