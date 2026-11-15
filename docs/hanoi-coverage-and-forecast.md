# Hanoi coverage, model forecast and live publication

Status 2026-11-15: implemented and tested locally; AWS refresh, public API and
site release require an approved deployment. Labels: VERIFIED (checked against
the source on 2026-11-15), PLANNED.

## 1. Source inventory

| Source | Kind | Hanoi coverage | Latest seen | Cadence | Licence / permission | Access | Decision |
|---|---|---|---|---|---|---|---|
| Dragon 6123215 (AirGradient via OpenAQ) | Low-cost sensor, PM2.5 µg/m³ | 20.9933, 105.9441 (Gia Lâm) | 2026-11-15 | hourly | CC BY 4.0 | OpenAQ v3, API key (server side) | Publish |
| Sensor.Community 99385 (SDS011) | Low-cost sensor, P2 µg/m³ | 21.070, 105.824 (Tây Hồ, rounded) | 2026-11-15 | ~150 s | ODC DbCL 1.0 | Open API + daily CSV archive, no key, User-Agent required | Publish |
| Sensor.Community 44434 | Indoor sensor | 20.964, 105.830 | 2026-11-15 | ~150 s | DbCL 1.0 | same | Excluded: indoor |
| AirNow 7441 (US Embassy) | Reference monitor | Hanoi | 2025-04-09 | hourly | US Public Domain | OpenAQ | Historical only (ended) |
| Hanoi Air Quality Monitoring Network (incl. Nhân Chính, Giải Phóng; 38 locations) | Reference monitors | City-wide | 2026-11-15 for 4 | hourly | No licence on OpenAQ | OpenAQ | Not published; needs written permission |
| StateAir, SPARTAN, HabitatMap | Mixed | Hanoi | 2016-2020 | — | No licence | OpenAQ | Not used |
| CAMS Global via Open-Meteo | Model estimate and forecast, PM2.5 µg/m³ | 0.4° (~45 km) grid | run every 12 h | 3-hourly native, hourly interpolated, 5 days | CC BY 4.0 (Open-Meteo), Copernicus attribution; free tier non-commercial, <10,000 calls/day | Open API, no key | Publish as model |

Sensor.Community: 2 PM sensors within 40 km, one outdoor. September archive:
719/720 hours; hourly r = 0.80 against Dragon with peak correlation at lag 0-1 h
(timestamps are UTC), but its level is 46-59% of the other stations. It is
labelled as low-cost; absolute values are not adjusted.

CAMS grid check: eight Hanoi points returned four distinct series. Hoàn Kiếm,
Long Biên and Sóc Sơn share one; Hà Đông shares another with Xuân Mai. District
rankings from the model would reflect grid boundaries, so the site uses one
central Hanoi-area forecast and shows the cell scale on the map.

Not used: protected websites, scraping, paid APIs, or sources whose
redistribution terms could not be verified.

## 2. Forecast validation (`tests/evidence/cams_validation_2026-09.json`)

Archived CAMS (most recent run per hour) against September 2026 hour buckets:

| Station | Hours | Bias | MAE | r hourly | r daily |
|---|---|---|---|---|---|
| Dragon | 720 | +16.9 | 28.0 | 0.39 | 0.54 |
| Sensor.Community 99385 | 719 | +32.3 | 33.2 | 0.53 | 0.81 |
| Network reference A (aggregate only) | 720 | +21.5 | 28.9 | 0.40 | 0.82 |
| Network reference B (aggregate only) | 693 | +17.5 | 30.8 | 0.25 | 0.76 |

A ratio fitted on 1-15 September reduced MAE on 16-30 September at all four
stations, but on 15 November the model was below both live sensors (40.6 vs
60.9-103.2 µg/m³): the bias changes sign with season. No calibration is
applied. The forecast is labelled external and used for relative higher/lower
periods and day-to-day comparison. Archived values are short-lead, so this does
not measure 3-5 day skill.

## 3. Citizen website (implemented)

- Hôm nay: one card per licensed sensor (value, observed time, age, sensor
  type, source/licence, reference band) and a separate dashed model card;
  a sentence compares the model with live observations.
- Dự báo: 24-hour model chart, daily means/ranges, relative 3-hour windows only
  when they differ by at least 20%, validation summary and limitations.
- Lịch sử: Athena-verified September history, unchanged.
- Bản đồ: Leaflet (self-hosted) with licensed stations and the model scale.
- Tình trạng: observed/ingested times per sensor; model run/available times;
  projection generated time.

## 4. AWS refresh and serving (PLANNED, `nowcast/refresh.py`, `nowcast/public_api.py`)

EventBridge Scheduler (every 15 min) -> Lambda refresh -> S3
`nowcast/v1/` (immutable observations per source, one raw object per CAMS run,
immutable projections, replaced `current.json`) -> public Lambda URL
(`/v1/now`, `/v1/history`) -> Amplify site reading `/v1/now`.

Realistic freshness: Dragon about 1 h after the hour; Sensor.Community up to
~20 min (15-min schedule); CAMS 12-hourly runs, available ~10 h after
initialisation. The site shows each timestamp; the schedule is not a data SLA.

Calls per day: 96 metadata + ~2 forecast (Open-Meteo), 96 OpenAQ, 96
Sensor.Community. Lambda/S3 cost is within the free tier for this volume.

## 5. Research access

`/v1/history` (public, no credentials): Dragon hourly/daily, JSON or CSV,
UTC `start`/`end`, `limit` 1-1000, `offset` 0-3000; any other station returns
404. `/v1` returns the schema. The IAM-protected private API is unchanged and
remains the only route to restricted stations.

## 6. Telegram contract (with Khuê's PR #19)

Observed alerts: trigger on a licensed sensor's new hourly observation above
the subscriber threshold, keyed by (subscriber, station, observed hour) for
deduplication; text states the observation time. Forecast alerts (optional):
once per model run when the next-24-h model maximum exceeds the threshold,
worded as a model forecast. Commands: /start, choose station, set threshold,
/status, /stop. Not deployed or verified; no messages sent.

## 7. Five-minute demo checklist

1. Phone: public HTTPS URL, Hôm nay with two observed cards and model card.
2. Dự báo: 24 h chart and validation limits.
3. Lịch sử: 7/30-day September trends, heatmap, WHO reference days.
4. Researcher: `/v1/history?granularity=daily&format=csv` download.
5. AWS console: Scheduler, Lambda logs, S3 `nowcast/v1/`, Glue run 2,
   Athena query IDs, Amplify job, CloudWatch.
6. Telegram only if E2E is verified.
