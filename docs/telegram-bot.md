# Telegram bot @AirLakeHanoiBot

Source: `telegram_bot/` (one Lambda). Deployed 2026-11-15; end-to-end
acceptance in `tests/evidence/telegram_release_2026-11-15.json`.

## Triggers

| Trigger | Path |
|---|---|
| Telegram webhook (Lambda Function URL) | requests without the secret header `X-Telegram-Bot-Api-Secret-Token` get 403; each `update_id` is handled once |
| EventBridge Scheduler `airlake-telegram-20261115-evaluate` (`cron(5/15 * * * ? *)`) | reads the published projection from S3 and evaluates active subscriptions |
| SQS `airlake-telegram-20261115-alerts` (DLQ `-dlq`) | sends each queued alert once, using a DynamoDB idempotency key; failures retry and then land in the DLQ |

## Commands

`/start` (also `?start=st_6123215` / `st_99385` deep links from the website),
`/help`, `/latest`, `/subscribe` (`/dangky`), `/status` (`/settings`), `/hours`,
`/unsubscribe` (deletes the subscription). Only private chats are handled.
Thresholds: 25, 35, 50, 75, 100 µg/m³ or a custom value from 5 to 500.

## Alert policy (`telegram_bot/policy.py`)

- Only real observations from the two public sensors; never the CAMS model.
- Only new readings whose freshness is current at evaluation time; delayed or
  stale readings never alert.
- Gia Lâm values are hourly means. A Tây Hồ reading is one ~150 s sample, so
  two new readings at least 10 minutes apart must both reach the threshold.
- One alert per crossing: after an alert the subscription re-arms only when a
  reading falls below 80 % of the threshold, and at most one alert per 3 hours.

## Status

- End-to-end acceptance passed on 2026-11-15 (subscribe, one real alert,
  no duplicate, unsubscribe, no further alert).
- Defect found on 2026-11-15 during the documentation audit: after the
  projection's freshness states changed from `recent` to `fresh`/`delayed`/
  `stale`, the policy still required `recent`, so no alert could fire. Fixed
  in commit `30b8535` (accepts `fresh`, keeps `recent` for older projections)
  and deployed the same day (bot code `/adfqOaq…`,
  `tests/evidence/bot_policy_fix_2026-11-15.json`). The deployed code was
  checked offline against the live projection; the next scheduled evaluation
  ran without errors. No new end-to-end Telegram delivery was run.
