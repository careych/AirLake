# AirLake architecture (final, 2026-11-15)

Status labels: **DEPLOYED** (running on AWS and verified by release receipts
or read-only checks on 2026-11-15), **HISTORICAL** (ran earlier, kept as
evidence, not part of the live service), **LOCAL** (development only),
**NOT IMPLEMENTED** (in the original plan only), **UNKNOWN** (exists, origin
not recorded in this repository). Account 701364614078, region ap-southeast-2.

## Live data flow

1. **Every 15 minutes** EventBridge Scheduler (`airlake-nowcast-20261115-15m`)
   invokes the refresh Lambda (`nowcast/refresh.py`). It
   - fetches Dragon's latest hourly value from OpenAQ (API key in Secrets
     Manager) and Sensor.Community 99385's latest reading;
   - when Open-Meteo reports a new CAMS run (every 12 h), downloads the
     central forecast and one multi-location request for all 126 area centres,
     validates and stores each raw run once in S3;
   - writes each new observation as an immutable S3 object, publishes a new
     projection (immutable copy first, then the `current.json` pointer with an
     `If-Match` condition);
   - after publication, extends the per-station history series (OpenAQ
     `/hours`, Sensor.Community daily archive, provisional samples). A failing
     source or model fetch never blocks publication; it is recorded and the
     page shows an unavailable or stale state.
2. **Public API** (`nowcast/public_api.py`, Lambda Function URL, auth NONE,
   reserved concurrency 10) serves `/v1/now`, `/v1/stations`, `/v1/history`
   and `/v1/archive/september-2026` from fixed S3 objects only (scoped IAM
   `s3:GetObject` on exact keys), with CORS for the two Amplify origins, ETag
   and bounded responses.
3. **Website** (Amplify app, branches `public` and Basic-Auth `demo`) is a
   static bundle (`citizen/web/`) that reads the public API.
4. **Telegram** (`telegram_bot/`, one Lambda with three triggers):
   Function URL webhook checked by a secret header; EventBridge
   (`airlake-telegram-20261115-evaluate`, cron 5/15) evaluates subscriptions
   against the latest projection; alerts go through SQS with a DLQ and are
   delivered once using DynamoDB idempotency keys.

## Historical lakehouse (September 2026)

OpenAQ archive files for three Hanoi locations were processed by a Glue Spark
job into six Iceberg tables in the Glue Catalog (database
`airlake_history_20261114_7a6e91c2`: bronze_measurements, aq_measurements,
aqi_breakpoints, dim_location, pm25_aqi_hourly, pm25_daily). Run 2: 17,181
records → 2,133 hourly and 90 daily rows, checked row by row by two Athena
queries. The job also wrote a verified JSON export, which feeds
- the private research API (Lambda Function URL with `AWS_IAM` auth), the only
  place restricted-station data is served, and
- the public `/v1/archive/september-2026` route (Dragon rows only).

## Component status

| Component | Status | Notes |
|---|---|---|
| EventBridge Scheduler (nowcast 15 min, Telegram cron 5/15) | DEPLOYED | both ENABLED |
| Lambda refresh, public API, Telegram bot | DEPLOYED | code hashes in `tests/evidence/final_release_2026-11-15.json` |
| S3 (versioned bucket) | DEPLOYED | `nowcast/v1/` projections, observations, model runs, history series; historical exports |
| Glue Spark + Iceberg + Glue Catalog | DEPLOYED (batch, run on 2026-11-15) | not scheduled; re-running needs approval |
| Athena | DEPLOYED (acceptance queries) | |
| Lambda private API (AWS_IAM) | DEPLOYED | September export incl. restricted stations |
| DynamoDB `airlake-telegram-20261115` | DEPLOYED | subscriptions, state, idempotency (TTL) |
| SQS alerts + DLQ | DEPLOYED | |
| Amplify Hosting | DEPLOYED | job 7 on both branches |
| Secrets Manager, IAM, CloudWatch logs and 3 alarms | DEPLOYED | alarms have no notification target |
| Bounded live polling trial (`airlake-live-20261114_9e2c740b-*`, 5-minute schedule) | HISTORICAL | schedule DISABLED; see [live-operations](live-operations.md) |
| First AWS final demo and private API (`airlake-final-api-20261114_*`) | HISTORICAL (function still exists) | see [aws-final-demo](aws-final-demo.md) |
| Khuê's alert staging pipeline (`airlake-alert-staging-v1*`: SNS, SQS, Lambda, DynamoDB ledger) | HISTORICAL | shaped the delivery contract; untouched by the product bot |
| Legacy bot `AirLake_Bot_API` (API Gateway HTTP API, DynamoDB `AirLake_Subscriptions`) | HISTORICAL | early prototype; not used by the product |
| EventBridge schedule `airlake-stop-ec2` | UNKNOWN | ENABLED; not created by this repository |
| Kafka (`infra/local/`), Spark local pipeline, Superset dashboard | LOCAL | see [local lakehouse history](history/local-lakehouse-pipeline.md), [gold-dashboard](gold-dashboard.md) |
| Trino on EC2 | NOT IMPLEMENTED | Athena (serverless, Trino-based) used instead |
| ALB / WAF | NOT IMPLEMENTED | public API bounded in the handler and by reserved concurrency |
| Terraform, CI/CD | NOT IMPLEMENTED | releases use frozen, hash-pinned plans with receipts |
| SNS in the product alert path | NOT USED | product bot uses SQS directly |

## Security boundaries

- Public data: only Dragon (CC BY 4.0) and Sensor.Community 99385 (ODbL/DbCL)
  measurements, CAMS model values and the Dragon part of the September export.
- Restricted Hanoi network stations: private IAM API and Athena only.
- The `demo` branch is behind Amplify Basic Auth; this is access control, not
  a redistribution licence.
- Secrets live in Secrets Manager and are never logged; handlers return
  generic errors without SDK payloads.
- Browser geolocation is requested only after a button press and rounded to
  ~1 km before use or local storage (`Permissions-Policy: geolocation=(self)`).
