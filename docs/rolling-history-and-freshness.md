# Rolling history, freshness and location (release 2026-11-16)

## Why

On 2026-11-15 the public site's history page showed only September 2026
(the first licensed backfill). Its "7 ngày" / "30 ngày" buttons meant "the
last 7/30 days of that September file", the home 24-hour chart had 4–6 real
points on a 24-hour axis, and the only interpretation was an experimental
AQI ("91 (chỉ số thử nghiệm)") and a QĐ 1459 category applied to a single
hourly reading.

## Data

| Station | Source | Basis | Licence |
|---|---|---|---|
| 6123215 Gia Lâm · Ocean Park (xã Gia Lâm) | OpenAQ daily archive (S3 `openaq-data-archive`), then OpenAQ `/v3/sensors/14581375/hours` for the ~4 days the archive lags | 1 value per hour | CC BY 4.0, "AirGradient via OpenAQ" |
| 99385 Tây Hồ (phường Tây Hồ, approximate) | Sensor.Community daily archive CSV; AirLake's own 15-minute samples for today (provisional) | mean of ≥ 12 readings (~24 expected); provisional: ≥ 2 samples | ODbL 1.0 / DbCL 1.0; derived database offered under ODbL 1.0 |

Series objects: `s3://airlake-701364614078-lake/nowcast/v1/history/<station>.json`,
one row per hour, keyed by the **UTC start** of the hour. Backfill
(`scripts/backfill_history.py`, 2026-11-15): 4 OpenAQ API calls, 103 OpenAQ
archive files, 79 Sensor.Community archive files (1 s apart). Result to
2026-11-15 08:00Z: Dragon 1,779 hours (one 34-hour outage 25–26/10); #99385
1,754 hours to the end of 14/11 UTC (sensor silent 06/11 05:52Z–08/11 10:14Z,
archive 404 for 07/11). Five archive days that first failed with network
errors (07, 10, 11, 22, 23/09) were recovered by one bounded retry.

The refresh Lambda extends both series after each publication (never
before, never blocking it): Dragon `/hours` for the last 72 h when an hour is
due; #99385 provisional hours from the projection's 24-hour sample list, and
the previous two UTC days' archive files once per hour until they appear.

### Hour labels: defect found in the September lakehouse export

OpenAQ stamps an hourly value with the **end** of the hour (verified: archive
`2026-11-11T01:00+07:00` == `/hours` period `17:00Z–18:00Z`). The September
Glue/Athena export used that stamp as `hour_start`, so every Dragon hour there
is labelled one hour late and daily means include 00:00–01:00 of the next day.
The new series fixes the label; after shifting, 719/720 September Dragon
values match the Glue export exactly. The remaining pair is the month edge:
the Glue window selected records by their end stamp, so it holds the OpenAQ
record `2026-09-01T00:00+07:00` = 63.1 (really 31/08 23:00–24:00) and lacks
`2026-10-01T00:00+07:00` = 24.1 (really 30/09 23:00–24:00); the new series
holds the latter and not the former.
The frozen export is not rewritten; the site describes its convention.

OpenAQ `/hours` rounds values ≥ 100 µg/m³ to whole numbers (12/720 September
hours, ≤ 0.5 µg/m³), so archive values replace `/hours` values when available.

## Cleaning rules

- No interpolation, no forward fill, no model values in observed series.
- Flags only, values unchanged: `very_low` (< 2 µg/m³; 42 Dragon / 25 #99385
  hours), `isolated_peak` (≥ 3× and ≥ 50 above the median of ±3 h; none),
  `provisional`.
- 9–10/11 daily means of ~3–4 µg/m³ at Dragon are mirrored at #99385
  (2.9 µg/m³ on 9/11), so they are kept as plausible weather-driven lows.
- Daily: Vietnam calendar day (UTC+7, no DST); `valid_day` ≥ 18/24 hours
  (75 %, as US EPA daily PM2.5 validity), `complete_day` 24/24. WHO 2021's
  15 µg/m³ is compared only with valid days or a rolling 24-hour mean with
  ≥ 18 hours.
- The experimental AQI and the QĐ 1459 hourly category are no longer shown
  to residents; the September export keeps its `aqi` column, documented as
  not VN_AQI.

## Two sensors disagree

Paired hours Sept 1–Nov 14: 1,719; mean Dragon 44.6 vs #99385 20.5 µg/m³,
median difference 22.6, Pearson r 0.86. They rise and fall together but
differ by a roughly constant offset: different sensor models (AirGradient vs
SDS011), no humidity correction, 14 km apart, Dragon an hourly mean vs
#99385 means of ~150 s readings. The site never averages them and does not
claim Gia Lâm air is objectively worse.

## Freshness

Measured delivery (24 projections on 2026-11-15): Dragon values reached
AirLake 16–20 min after the end of their hour; #99385 readings 2–4 min after
measurement. With a 15-minute refresh:

| Source | Mới (fresh) | Chậm cập nhật (delayed) | Dữ liệu cũ (stale) |
|---|---|---|---|
| Dragon (age of hour end) | ≤ 90 min | ≤ 3 h | > 3 h |
| #99385 | ≤ 25 min | ≤ 60 min | > 60 min |

`Chưa có dữ liệu` when there is no valid reading; readings more than 5 min
in the future are rejected at ingestion. Age is always measured from
`observed_at`, never `fetched_at`/`published_at`. The page also warns when
the projection itself is over 45 min old. The Telegram bot uses the fresh
limit as alert eligibility.

## Location

`citizen/web/areas.json`: the 126 current phường/xã of Hà Nội (51 + 75 after
1 July 2025) with approximate centres, from OpenStreetMap via one Overpass
query (ODbL, © OpenStreetMap contributors). No address geocoder is used (the
public Nominatim service forbids autocomplete). Browser geolocation is asked
only after a button press, rounded to 0.01° (~1 km), and only the rounded
point or area id is kept in `localStorage`. "Near" = within 4 km
(neighbourhood scale of US EPA siting); otherwise the page says no public
station is close enough and offers the CAMS regional forecast, which is
labelled as a ~45 km model cell.

## Public API

```
GET /v1/stations
GET /v1/history?station_id=6123215                       # last 7 complete Vietnam days
GET /v1/history?station_id=99385&start=2026-10-16&end=2026-11-15&granularity=daily&format=csv
GET /v1/history?station_id=6123215&start=2026-11-14T00:00:00Z&end=2026-11-15T00:00:00Z
GET /v1/archive/september-2026?format=csv                # frozen Glue export
```

At most 100 days, 1,000 rows per page (`offset` up to 3,000), ETag/304,
5-minute cache. Each response carries coverage (hours present/expected,
valid/complete days, gaps), licence, attribution and methodology.
