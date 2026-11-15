# Public data API

Base URL: `https://npe6uhbknh4qojnmylv3w436mi0epxfb.lambda-url.ap-southeast-2.on.aws`
(Lambda Function URL, read-only, no key). Source: `nowcast/public_api.py`,
`nowcast/rolling_api.py`. `GET /v1` returns the same reference as JSON.

## Routes

| Route | Content |
|---|---|
| `GET /v1/now` | Current projection: the two sensors' latest observations with `observed_at`, `fetched_at`, freshness and licence; the CAMS model estimate and forecast; `model_areas` (CAMS estimate for each of the 126 phường/xã, grouped by native 0.4° cell). No parameters. |
| `GET /v1/stations` | Public stations, licence, location precision and history availability. No parameters. |
| `GET /v1/history` | Rolling hourly or daily history for one station since 2026-09-01 (Vietnam time), with coverage. |
| `GET /v1/archive/september-2026` | Frozen Glue/Athena export for Dragon, September 2026. |

## `/v1/history` parameters

| Parameter | Values |
|---|---|
| `station_id` | required: `6123215` (Gia Lâm, CC BY 4.0) or `99385` (Tây Hồ, ODbL 1.0) |
| `start`, `end` | `YYYY-MM-DD` (Vietnam date, end exclusive) or `YYYY-MM-DDTHH:00:00Z`; default the last 7 complete Vietnam days; at most 100 days; not before 2026-09-01, not in the future |
| `granularity` | `hourly` (default) or `daily` |
| `format` | `json` (default) or `csv` |
| `limit`, `offset` | 1–1000 (default 1000), 0–3000; JSON responses carry a `next` link |

Hourly fields: `hour_start_utc` (UTC start of the hour), `hour_start_local`,
`pm25` (µg/m³), `n_readings`, `basis`, `flags`. Daily fields: `date_local`,
`pm25`, `hours`, `provisional_hours`, `valid_day` (≥ 18 non-provisional hours),
`complete_day` (24), `min_hourly`, `max_hourly`.

`basis`: `openaq_archive`, `openaq_hourly` (values ≥ 100 rounded by OpenAQ),
`openaq_latest` (provisional), `sc_archive` (≥ 12 readings in the hour),
`sc_sampled` (provisional). `flags`: `very_low` (< 2 µg/m³), `isolated_peak`,
`provisional`; flagged values are never changed.

Every JSON response includes `station` (licence, licence URI, attribution,
changes made), `range`, `coverage` (hours present/expected, provisional hours,
valid/complete days, gaps) and `methodology`. CSV rows repeat the licence,
licence URI and licence notice; responses carry `Link: <licence URI>; rel="license"`.

## Examples

```bash
API=https://npe6uhbknh4qojnmylv3w436mi0epxfb.lambda-url.ap-southeast-2.on.aws
curl "$API/v1/stations"
curl "$API/v1/history?station_id=6123215"                                  # last 7 complete days, hourly JSON
curl "$API/v1/history?station_id=99385&start=2026-10-16&end=2026-11-15&granularity=daily&format=csv"
curl "$API/v1/history?station_id=6123215&start=2026-11-14T00:00:00Z&end=2026-11-15T00:00:00Z"
```

Verified on 2026-11-15 (live): Tây Hồ since 2026-09-01 returned 1,761 hourly
rows in two pages (7 provisional); Gia Lâm 30-day daily CSV returned 30 rows
(28 valid, 27 complete).

## Errors and limits

`400` invalid parameters, `404` unknown route or unlicensed station, `405`
non-GET, `503` temporarily unavailable (generic body, `retry-after: 60`).
Responses are at most 1 MB, cached for 5 minutes (60 s for `/v1/now`) and
support `If-None-Match` (304). The function has a reserved concurrency of 10;
the website retries a refused request once and then shows a retry button.

There is no public SQL endpoint. Restricted Hanoi network stations are served
only by the separate private API that requires AWS SigV4 (IAM) signatures.
