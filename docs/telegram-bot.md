# Telegram bot @AirLakeHanoiBot

Source: `telegram_bot/` (one Lambda). Release receipt:
`tests/evidence/telegram_release_2026-11-15.json`.

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

## Testing

- End-to-end test on 2026-11-15 with a team member's account: subscribe to
  Gia Lâm at 25 µg/m³, one real alert (43.8 µg/m³), no duplicate on the next
  evaluation, no message after unsubscribing; DLQ empty.
- In the final review the team found that the alert policy still expected the
  old freshness state `recent` after the projection switched to
  `fresh`/`delayed`/`stale`, so no alert could be sent. The policy was fixed
  with a regression test and deployed the same day
  (`tests/evidence/bot_policy_fix_2026-11-15.json`). The deployed code gives
  the expected result on the live projection and the next scheduled
  evaluation ran without errors. A full message delivery has not been re-run
  since the fix.
