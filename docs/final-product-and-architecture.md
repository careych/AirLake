# AirLake final product: coverage, UX choices and architecture status

Status 2026-11-15. VERIFIED = checked in this repository or read-only on AWS
account 701364614078 (ap-southeast-2) today. PLANNED = prepared, not deployed.

## 1. Why the public site shows Dragon and #99385

| Location | Kind | Data in AirLake | Public | Reason |
|---|---|---|---|---|
| Dragon 6123215 (Ocean Park, Gia Lâm) | Low-cost sensor (AirGradient) | September history (720 h, Glue run 2) + live | Yes | CC BY 4.0 on OpenAQ |
| Sensor.Community #99385 (Tây Hồ, approx.) | Low-cost SDS011, outdoor | Live since 2026-11-15; September archive used for validation | Live only | DbCL 1.0 / ODbL 1.0 |
| Nhân Chính 4946812, Giải Phóng 4946813 | Hanoi network reference monitors | September history (720 h / 693 h) in Iceberg and the private IAM API | No | No licence on OpenAQ; Basic Auth is not permission |
| AirNow 7441 (US Embassy) | Reference monitor | Not loaded | — | Public domain, but stopped in April 2025 |
| CAMS Global grid cell | Model, ~45 km | Live forecast | Yes, as a model | Not a station; never counted as one |

The restricted stations were removed from the public and demo bundles on
2026-11-15 by owner decision; their data is not deleted and stays available to
signed-in operators through the AWS_IAM history API and Athena. Publishing them
needs written permission from the network operator.

## 2. Product patterns adopted (from widely used air-quality services)

Common patterns in services such as IQAir/AirVisual, WAQI and OpenAQ Explorer,
adapted without copying layouts, wording or data:
- One large current value per station with its time and age first; details on demand.
- Station-centred map with source and freshness on tap, not district rankings.
- Hourly forecast strip/line kept visually distinct from measurements.
- Short alert set-up in a messaging app with explicit thresholds.
- Downloads and API next to licence and units.
AirLake differs where honesty requires it: model and measurement never share a
chart series, and no colour badge implies "safe".

## 3. Architecture against the original report

| Planned component | Status | Notes |
|---|---|---|
| EventBridge Scheduler | VERIFIED live | nowcast refresh every 15 min |
| Lambda ingestion/serving | VERIFIED live | refresh, public API, private API |
| S3 + Iceberg Bronze/Silver/Gold, Glue (Spark), Athena | VERIFIED | September backfill run 2, two Athena checks |
| DynamoDB | VERIFIED (live state, staging tables) | Telegram product table VERIFIED live (2026-11-15) |
| SNS/SQS/DLQ alert transport | Staging VERIFIED deployed (Khuê) | product SQS queue + DLQ VERIFIED live; SNS not used by the product bot |
| Telegram bot | Legacy prototype and Khuê's staging pipeline exist | product bot @AirLakeHanoiBot VERIFIED live, E2E passed 2026-11-15 (`telegram_bot/`) |
| Amplify hosting | VERIFIED live | public + Basic-Auth demo |
| IAM, Secrets Manager, CloudWatch alarms | VERIFIED | scoped roles; 3 nowcast alarms (no notification target) |
| Kafka | Local only (`infra/local`) | replaced on AWS by scheduled pull; volumes are tiny |
| Trino | Replaced by Athena (Trino-based, serverless) | |
| Superset | Local only | not exposed publicly |
| API Gateway | Function URLs used | legacy HTTP API exists for the old bot |
| Terraform, CI/CD | Not implemented | releases use frozen, hash-pinned plans with receipts |
| ALB/WAF | Not used | public API bounded by handler limits and reserved concurrency |
| Free-form SQL for researchers | Not public | Athena for signed-in operators; public API offers filtered JSON/CSV |

## 4. Deviations from the original report (owner decisions, 2026-11-15)

- **Source polling: about 5 minutes planned, 15 minutes operated.** The
  EventBridge schedule checks the sources every 15 minutes. This is an
  ingestion check frequency, not a promise of 15-minute measurement freshness:
  Dragon publishes one hourly mean (reaching AirLake 16–20 min after the
  hour) and #99385 reports every ~150 s. Freshness is judged per source from
  the observation time (see `rolling-history-and-freshness.md`).
- **AQI.** The report planned an hourly AQI. The project-specific value in the
  September lakehouse tables (`aqi`, `aqi_mvp`) is NOT official VN_AQI and is
  no longer shown to residents; residents see PM2.5 in µg/m³ with time and
  completeness. The value stays in the validated September evidence and the
  archive export, labelled as an internal pipeline check.
- **Telegram ownership.** Khuê designed and deployed the staging alert
  pipeline (SNS/SQS/Lambda/DynamoDB ledger, runner, CloudWatch evidence),
  which shaped the delivery contract. On the owner's instruction the product
  bot @AirLakeHanoiBot was then built and released end to end from
  `telegram_bot/` by Carey's session; Khuê's staging resources were left
  untouched.
- **Public data.** Two licensed sources are public, each as its own dataset:
  Dragon under CC BY 4.0 and Sensor.Community #99385 under ODbL 1.0 / DbCL 1.0
  (the AirLake-derived database for #99385 is offered under ODbL 1.0).
  Hanoi network stations stay private.
