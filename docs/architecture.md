# Architecture

AirLake runs on AWS in `ap-southeast-2` (Sydney). There are no always-on
servers: the live path uses Lambda, S3 and Amplify, and Glue runs only as a
batch job.

## Live data flow

1. **Every 15 minutes** EventBridge Scheduler invokes the refresh Lambda
   (`nowcast/refresh.py`). It
   - fetches the latest hourly value of Gia Lâm · Ocean Park from OpenAQ (API
     key in Secrets Manager) and the latest reading of Sensor.Community 99385
     (Tây Hồ);
   - when Open-Meteo reports a new CAMS run (every 12 h), downloads the central
     forecast and one multi-location request for all 126 area centres, and
     stores each run once;
   - writes each new observation as an immutable S3 object and publishes a new
     projection (immutable copy first, then the `current.json` pointer with an
     `If-Match` condition);
   - after publishing, extends the per-station history series. A failing
     source never blocks publication; the old value is kept and becomes
     "stale".
2. **Public API** (`nowcast/public_api.py`, Lambda Function URL, reserved
   concurrency 10) serves `/v1/now`, `/v1/stations`, `/v1/history` and
   `/v1/archive/september-2026` from fixed S3 keys only, with CORS for the two
   Amplify origins, ETag and bounded responses. See [public-api.md](public-api.md).
3. **Website** (`citizen/web/`) is a static bundle on Amplify Hosting with two
   branches: `public` and `demo` (Basic Auth). It reads the public API.
4. **Telegram bot** (`telegram_bot/`) is one Lambda with three triggers: the
   webhook (checked by a secret header), a scheduled evaluation (cron 5/15)
   and an SQS queue with a DLQ for delivery. DynamoDB holds subscriptions,
   state and idempotency keys. See [telegram-bot.md](telegram-bot.md).

## Historical lakehouse (September 2026)

OpenAQ archive files for three Hanoi stations (Nhân Chính, Giải Phóng, Gia Lâm)
are processed by a Glue Spark job (`historical/glue_job.py`) into six Iceberg
tables in the Glue Data Catalog:

| Layer | Tables | Content (run 2) |
|---|---|---|
| Bronze | `bronze_measurements` | 17,181 raw PM2.5 records with source and timestamps |
| Silver | `aq_measurements` | normalised, validated, deduplicated records |
| Gold | `pm25_aqi_hourly`, `pm25_daily`, `dim_location`, `aqi_breakpoints` | 2,133 hourly and 90 daily rows, location dimension, breakpoint table |

The job also writes a JSON export. Two Athena queries compare every hourly and
daily row with the export before it is used. The export feeds a private API
(Lambda Function URL with `AWS_IAM` auth), the only place where the restricted
network stations are served, and the public archive route (Gia Lâm rows only).
The `aqi` column in the Gold tables is an internal pipeline check, not the
official VN_AQI, and is not shown to residents.

The Bronze/Silver/Gold logic was first built and tested locally with Kafka,
Spark and Iceberg (`scripts/bronze_local.py`, `silver_batch.py`,
`gold_batch.py`, `infra/local/docker-compose.yml`) for progress report 2.

## AWS services

| Service | Role |
|---|---|
| EventBridge Scheduler | refresh every 15 minutes; alert evaluation (cron 5/15) |
| Lambda | refresh, public API, Telegram bot, private API |
| S3 (versioned) | observations, model runs, projections, history series, Iceberg tables, September export |
| Glue, Glue Data Catalog, Iceberg | September 2026 batch, six tables |
| Athena | row-by-row checks of the Glue output |
| DynamoDB | alert subscriptions, state, deduplication keys (TTL) |
| SQS + DLQ | alert delivery with retry |
| Amplify Hosting | static website, `public` and Basic-Auth `demo` branches |
| Secrets Manager, IAM | OpenAQ key and bot secrets; one least-privilege role per function |
| CloudWatch | logs (14 days) and three alarms: API throttles, unusual traffic, refresh errors |

## Compared with the original design

The original design ([specification.md](specification.md)) planned a
self-managed stack. From Week 8 the team moved to managed AWS services, which
fit the data volume (two public sensors) and the remaining time better.

| Planned | Final |
|---|---|
| Kafka cluster for ingestion | scheduled Lambda pull every 15 minutes; Kafka used only locally |
| Spark on EC2 | Glue (serverless Spark) for the batch lakehouse |
| Trino on an EC2 Auto Scaling group | Athena |
| Superset dashboard | static website on Amplify; Superset used only locally |
| API Gateway, ALB/WAF | Lambda Function URLs with handler limits and reserved concurrency |
| SNS + SQS alert transport | SQS with a DLQ; SNS was used in the staging alert pipeline |
| Hourly AQI for residents | PM2.5 in µg/m³ with time, freshness and completeness |
| Source polling about every 5 minutes | every 15 minutes; freshness is judged per source from the observation time |
| Terraform, CI | Terraform stacks and a CI workflow were written and tested on the `khue` branch; the final release uses the scripted releases described below |

### Earlier AWS work kept in the repository

- **Staging alert pipeline** (`alerts/`, `bot_lambda/`, `scripts/prepare_alert_staging.py`):
  SNS, SQS with a DLQ, Lambda and a DynamoDB delivery ledger, checked with
  Athena on staging data (PR #19). It defined the delivery contract that the
  product bot follows.
- **Live polling and first AWS demo** (`live_data/`, `final_demo/`): a
  five-minute polling trial and a first Glue/Athena demo with a private API,
  replaced by `nowcast/` and `historical/`.
- **Local tools** (`ingestion/`, `clean_api/`, `dashboard/`): local
  incremental OpenAQ ingestion, a localhost API and the local Superset
  dashboard.

## Releases

Every AWS change goes through the release scripts in `scripts/` and the
`deploy` modules (`nowcast.deploy`, `telegram_bot.deploy`, `historical.cloud`,
`citizen/release.py`). Each release has a plan whose SHA-256 must be confirmed
(`--confirm-plan-sha256`), a read-only preflight, one receipt per write, a stop
on the first error and a prepared rollback (previous Lambda code, IAM policy
and Amplify bundle). Receipts are in `tests/evidence/`.

## Security

- Public data is limited to Gia Lâm (CC BY 4.0) and Sensor.Community 99385
  (ODbL/DbCL) measurements, CAMS model values and the Gia Lâm part of the
  September export. Restricted network stations are available only through
  the IAM-signed private API and Athena.
- The `demo` branch is behind Basic Auth. This is access control, not a
  redistribution licence.
- Secrets live in Secrets Manager and are never logged; handlers return
  generic errors.
- Security headers: strict CSP, `X-Frame-Options: DENY`,
  `Permissions-Policy` allowing geolocation only on the site itself. Location
  is requested only after a button press and rounded to about 1 km.
