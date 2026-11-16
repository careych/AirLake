# Deployment

These are the steps used to build the running system. Every command that
writes to AWS takes a reviewed plan and its hash (`--confirm-plan-sha256`),
runs a read-only preflight first and writes one receipt per change to
`tests/evidence/`.

1. **Prepare.** AWS account and CLI profile; Python 3.12 and `uv`;
   `uv sync --frozen`; `uv run python -m unittest discover -s tests`. Register
   an OpenAQ v3 key and create the bot with BotFather; store the key and the
   token in Secrets Manager.
2. **September lakehouse.** Prepare the input from the OpenAQ archive
   (`historical/prepare.py`), then run `python -m historical.cloud` with the
   steps `prepare`, `preflight`, `run-glue`, `status`, `verify-athena`,
   `publish-api` and `smoke-api`: create and run the Glue job, check the
   output with Athena and publish the private API.
3. **Near-real-time path.** `python -m nowcast.deploy prepare`, `preflight`,
   `execute` create the IAM roles, the refresh and public API Lambdas, the
   Function URL, log groups and alarms. `verify-live` checks the first
   projection, then `enable-schedule` turns on the 15-minute schedule.
4. **Telegram bot.** `python -m telegram_bot.deploy prepare`, `preflight`,
   `execute` create the DynamoDB table (TTL), the SQS queue and DLQ, the
   webhook secret, the Lambda and its Function URL. `configure-bot` registers
   the webhook and the command list. Then `smoke`, an end-to-end test with
   `evaluate` and `e2e-check`, and finally `enable-schedule`.
5. **Rolling history.** `scripts/backfill_history.py` builds the initial
   series outside Git; `scripts/release_rolling_history.py` updates the bot,
   grants read access to exactly the two history keys, uploads the series and
   updates the refresh Lambda and the API.
6. **Website.** `historical/build.py` bundles the site with the API and bot
   addresses; `citizen/release.py` deploys once per Amplify branch and keeps
   the Basic Auth of the `demo` branch.
7. **Final release.** `scripts/release_fix_20261115.py` (per-area estimates,
   reserved concurrency 10, Amplify job 6), `scripts/release_site_only.py`
   (Amplify job 7) and `scripts/release_bot_fix.py` (alert policy fix).

Re-running the Glue job or any release needs a new plan.
