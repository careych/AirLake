# Telegram alerts from the live layer: integration contract (PROPOSED)

Owner of delivery: Khuê (SNS -> SQS -> Telegram Lambda, DynamoDB ledger and
subscriptions; PR #19). Owner of observations: the nowcast refresh. Nothing
here is deployed; no message has been sent.

## What exists

- VERIFIED live: `nowcast/v1/observations/{source}/{date}/{observed_at}-{hash}.json`
  (one immutable object per licensed observation) and `nowcast/v1/current.json`
  (schema 2), refreshed every 15 minutes since 2026-11-15.
- Khuê's `alerts.gold.Observation` expects an Iceberg Gold row:
  `source_table` (Glue table pattern) and `source_snapshot_id` (Iceberg
  snapshot), `aqi_method = simplified_hourly_mean_mvp`.

## Gap

Live observations are not Iceberg rows, so they cannot pass the current
`Observation.validate` without a contract change. Two options:

1. Extend the contract with a second provenance form: `source_object`
   (S3 key under `nowcast/v1/observations/`) and `source_sha256`, keeping the
   Gold form unchanged. Smallest change; no new Glue/Athena run per alert.
2. Land live observations in Gold Iceberg first (Glue or Athena INSERT every
   refresh). Heavier and slower; not recommended for 15-minute data.

Recommendation: option 1, reviewed and merged by Khuê.

## Alert semantics

- Observed alert: a new Dragon hourly observation (Sensor.Community is
  low-cost and reads ~50% lower; offered only as an explicitly labelled
  second station) whose PM2.5 hourly value maps to an experimental AQI at or
  above the subscriber threshold. Event id = sha256(station, observed hour,
  rule version); the ledger's conditional write prevents duplicates.
  Text: station, value, observation time (Vietnam time), "not official VN_AQI".
- Forecast notice (optional, separate rule id): once per CAMS run when the
  next-24-hour model maximum exceeds the threshold; text says "mô hình dự báo",
  the run time and the ~45 km scale. Never mixed with observed alerts.
- No alert from stale data (observation older than 2 h) or `fetch_failed`.

## Next steps (each needs coordination and, for AWS, approval)

1. Khuê reviews option 1; Carey adds the provenance form and tests.
2. Staging deploy of Khuê's PR #19 (already merged, not deployed).
3. One end-to-end test to Khuê's staging bot with a synthetic threshold.
