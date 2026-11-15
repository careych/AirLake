# AirLake final evidence package (2026-11-15)

Facts below come from release receipts in `tests/evidence/`, read-only AWS
checks and the hosted browser acceptance of 2026-11-15. Each claim is marked
**VERIFIED** (checked on AWS or the live site), **LOCAL** (tests or local
browser only) or **NOT DONE**. Team roles and contributions are not included
here: Report 2 is the authoritative source for them.

## Live endpoints (VERIFIED)

| What | Where |
|---|---|
| Public website (Amplify `public`, job 6) | https://public.dhyvgpdsnj4zi.amplifyapp.com |
| Protected copy (Amplify `demo`, job 6, Basic Auth: anonymous 401) | https://demo.dhyvgpdsnj4zi.amplifyapp.com |
| Public read-only data API (Lambda Function URL) | https://npe6uhbknh4qojnmylv3w436mi0epxfb.lambda-url.ap-southeast-2.on.aws/v1 |
| Telegram bot | https://t.me/AirLakeHanoiBot |

Account 701364614078, region ap-southeast-2.

## AWS services actually running

| Service | Resource | Role | Status |
|---|---|---|---|
| EventBridge Scheduler | `airlake-nowcast-20261115-15m` (rate 15 min) | source check and publication | VERIFIED ENABLED |
| EventBridge Scheduler | `airlake-telegram-20261115-evaluate` (cron 5/15) | alert evaluation | VERIFIED ENABLED |
| Lambda | `airlake-nowcast-20261115-refresh` | fetch OpenAQ, Sensor.Community, CAMS; validate; write S3; extend history | VERIFIED |
| Lambda + Function URL | `airlake-nowcast-20261115-public-api` (reserved concurrency 10) | public API `/v1/now`, `/v1/stations`, `/v1/history`, `/v1/archive/september-2026` | VERIFIED |
| Lambda + Function URL | `airlake-telegram-20261115-bot` | webhook (secret-token header), evaluation, delivery | VERIFIED |
| SQS | `airlake-telegram-20261115-alerts` + `-dlq` | alert delivery with redrive; both 0 messages | VERIFIED |
| DynamoDB | Telegram table (TTL) | subscriptions, state, idempotency keys | VERIFIED (created in plan 8cfe8871) |
| S3 (versioned) | `airlake-701364614078-lake` | `nowcast/v1/` observations, projections, model runs, history series; historical exports | VERIFIED |
| Glue (Spark) + Iceberg + Glue Catalog | database `airlake_history_20261114_7a6e91c2` | September 2026 lakehouse: 17,181 records → 2,133 hourly / 90 daily (run 2) | VERIFIED (run 2 evidence) |
| Athena | two acceptance queries on run 2 | row-by-row check of the export | VERIFIED (run 2 evidence) |
| API (AWS_IAM) | private historical API | restricted-station data for signed requests only | VERIFIED (run 2 evidence) |
| Amplify Hosting | app `dhyvgpdsnj4zi` | static site, security headers, Basic Auth on `demo` | VERIFIED |
| Secrets Manager | OpenAQ key, bot token, webhook secret | values never printed or committed | VERIFIED (metadata only) |
| CloudWatch | logs (14-day retention) and 3 alarms | throttles, traffic, refresh errors; no notification target | VERIFIED |

Not running on AWS (architecture deviations): Kafka (local only), Superset
(local only), Trino on EC2 (replaced by Athena), ALB/WAF, Terraform, CI/CD.
SNS is not used by the product bot (Khuê's staging pipeline used it).

## Data sources and licences (VERIFIED)

| Data | Source | Licence | Public? |
|---|---|---|---|
| Gia Lâm · Ocean Park (xã Gia Lâm) | AirGradient "Dragon" via OpenAQ, location 6123215 | CC BY 4.0, changes stated | yes |
| Tây Hồ (phường Tây Hồ, approximate) | Sensor.Community sensor 99385 (SDS011) | ODbL 1.0 / DbCL 1.0; AirLake derived database under ODbL 1.0 | yes |
| Regional estimate and forecast | CAMS Global via Open-Meteo | CC BY 4.0, Copernicus credit | yes, labelled model |
| Nhân Chính, Giải Phóng (Hanoi network) | OpenAQ | no redistribution licence | private IAM API only |
| Areas (126 phường/xã) | OpenStreetMap via Overpass | ODbL 1.0 | yes |

Each history response or file holds one station with its own licence, URIs
and attribution; JSON/CSV carry the licence in every row and a
`Link: rel="license"` header.

## Observed history (VERIFIED on the live API)

| Station | Rows on 2026-11-15 10:01Z | 7 days 08–14/11 | 30 days 16/10–14/11 | Sept 2026 |
|---|---|---|---|---|
| Gia Lâm | 1,781 hourly since 2026-09-01 | 168/168 h, 7/7 valid days | 685/720 h (95.1 %), 28/30 valid, 27 complete; gaps 25–26/10 (34 h) and 30/10 11:00 (1 h) | 720/720 |
| Tây Hồ | 1,761 hourly (7 provisional) | 151/168 h, 6/7 valid | 668/720 h (92.8 %), 27/30 valid; sensor silent 06/11 05:52Z–08/11 10:14Z | 719/720 |

Rules: hours keyed by UTC start; OpenAQ's end-of-hour stamps converted;
no interpolation or model values in observed series; valid day ≥ 18
non-provisional hours; provisional (`sc_sampled`, `openaq_latest`) never
replaces `/hours` or archive values. September Gia Lâm in the new series
equals the Glue/Athena export for 719/720 hours after the one-hour label
correction; the remaining pair is the month-edge record (see
`rolling-history-and-freshness.md`).

## Model coverage (VERIFIED)

All 126 areas map to 5 native CAMS 0.4° (~45 km) cells (38, 29, 28, 26, 5
areas). Areas in one cell share one value; cell edges cross central Hanoi
(about 21.0°N, 105.8°E). CAMS vs sensors, September 2026: bias +16.9 µg/m³
(Gia Lâm) and +32.3 µg/m³ (Tây Hồ), daily r 0.54 / 0.81
(`cams_validation_2026-09.json`).

## Telegram (VERIFIED, 2026-11-15)

Real E2E with the owner's consenting account: subscribe Gia Lâm at 25 µg/m³,
one alert received (43.8 µg/m³, 12:00 15/11), no duplicate on re-evaluation,
no alert after /unsubscribe; DLQ 0. Evaluations since then: EVALUATED with 0
queued and 0 errors. No test messages were sent after acceptance.

## Final release acceptance (2026-11-15, plan ceb5c35a)

- Operations: refresh and public-api code `OfJOlM2s…`, one refresh
  (PUBLISHED 09:59:52Z, model areas ok, 5 groups), public-api reserved
  concurrency 2 → 10, Amplify job 6 on both branches, demo Basic Auth 401
  before and after (unchanged).
- Hosted browser (60 GETs, desktop and 390 px, two sessions at once, rapid
  7/30-day switching): all 7 history views rendered; A–D journeys shown
  in screenshots; no horizontal overflow.
- CloudWatch public-api: 1 throttle at 09:59Z during the release minute,
  before the concurrency change; 31 invocations during the browser test at
  10:02Z with 0 throttles and 0 errors.

## Limitations

- Two low-cost outdoor sensors; not reference-grade, no humidity correction,
  14 km apart and systematically offset (paired r 0.86, means 44.6 vs 20.5).
- CAMS is a ~45 km model with seasonal bias; shown as a trend, never as a
  ward measurement.
- Today's Tây Hồ hours are provisional until the next daily archive.
- CloudWatch alarms have no notification target; the legacy bot's token is
  still a plaintext environment variable (rotation not authorised yet).
- The September archive CSV keeps an `aqi` column documented as an internal
  pipeline value, not VN_AQI, in the API docs and archive JSON.
