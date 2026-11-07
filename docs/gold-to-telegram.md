# Gold-to-Telegram local integration

This slice implements Gold observation validation → explicit alert rule → SNS
JSON → raw or wrapped SNS/SQS body → Lambda handler → Telegram transport.
AWS and Telegram are mocked in local acceptance. No deployment, live notification,
new Gold write or modification of retained acceptance resources occurred.

## Source reuse

Base main: `5007ea503d427bd8bb75a303bc14b3b4905cea2a` (PR #4 merge).

- Thịnh: `8af4f39cdd7abc3b5f6deb26d644f665105599d3`,
  `bot_lambda/src/lambda_function.py` and `create_table.py`: SNS-in-SQS parsing,
  `/subscribe`, chat-ID partition key, conditional subscription creation and
  Telegram request construction. Negative-measurement rejection is enforced in
  the Gold boundary instead of treating arbitrary sensor POSTs as bot commands.
- Khuê: `b5a188d1ffdf1c00a01b2a4e925660b9c8c9314c`, the same paths:
  10-second HTTP timeout, response status/`ok` checks, propagated failures and
  `LastEvaluatedKey` pagination. These are retained and extended with limits,
  per-recipient leases, strict event validation and webhook authentication.
- Khuê's paired `pyproject.toml`/`uv.lock` supplies boto3 while preserving the
  current PySpark pin. Neither teammate branch/history was merged or cherry-picked.

No historical environment files, tokens, Terraform plan/state or deployment code
were copied. BotFather token revocation remains owner-reported; this change does
not purge historical Git exposure or edit the deployed Lambda.

## Gold read boundary and explicit rules

Actual `pm25_aqi_hourly` fields consumed:

| Field | Gold type | Adapter requirement |
| --- | --- | --- |
| `location_id` | int | Positive, in the configured station/name registry |
| `hour_start` | timestamp | Explicit UTC, whole hour, closed and not stale |
| `pm25_avg` | double | Finite, nonnegative; interpreted only with verified units |
| `aqi` | int | 0–500; existing hourly MVP value, never recalculated here |

`name` is verified dimension enrichment, not an hourly fact column. `units`,
`aqi_method`, `source_table` and decimal-string `source_snapshot_id` are required
read provenance, not newly added Gold columns. Accepted units are `µg/m³` and
method is `simplified_hourly_mean_mvp`. Unknown lineage must fail rather than
silently assigning those labels. There is no official VN_AQI claim.

A replaceable source implements `read()`. `evaluate` accepts 1–1,000 rows from
one pinned table snapshot, rejects missing/unknown fields, conflicting duplicate
hours and invalid/stale rows before returning any publishable events. Identical
duplicate hours collapse. There is no completeness or correction calculation here.

`AthenaGoldSource` is an explicit future executor boundary. Without an injected
`read_pinned_hourly_rows()` executor it raises. Live Athena query submission,
pagination, snapshot pinning/time travel and dimension enrichment are **not
implemented**. A future executor must bound its query, verify actual Gold/units
lineage and return one snapshot; it must not substitute latest unpinned results.

Construct `Rule(rule_id, version, minimum_aqi, maximum_age_seconds)` explicitly.
There are no production defaults. Eligibility is `aqi >= minimum_aqi`; freshness
is elapsed time since the end of a closed UTC hour, inclusive of the configured
maximum age. Age must be positive policy input (bounded to seven days). Current
hour/future data rejects. Consumer configuration must contain the exact same
rule definition/version; a message cannot invent a lower threshold.

Tests use `fixture-only/test-v1`, minimum AQI 40 and maximum age 7,200 seconds.
These are **test-only values**, not approved health thresholds. The normal E2E
row is the retained July 2025 observation (21.7875 µg/m³, AQI 44); its clock is
frozen to 2025-07-03 16:30 UTC. Corrections and secondary subscriptions are
explicitly synthetic. Archived data will be stale at today's real clock.

## Event, publication and correction semantics

Wire payload:

```json
{
  "schema_version": 1,
  "event_id": "<canonical SHA-256>",
  "observation": {
    "location_id": 4946812,
    "name": "<verified dimension name>",
    "hour_start": "<closed UTC hour ending in Z>",
    "pm25_avg": "<finite JSON number>",
    "aqi": "<integer>",
    "units": "µg/m³",
    "aqi_method": "simplified_hourly_mean_mvp",
    "source_table": "<catalog.database.pm25_aqi_hourly>",
    "source_snapshot_id": "<decimal string>"
  },
  "rule": {
    "rule_id": "<configured identifier>",
    "version": "<configured version>",
    "minimum_aqi": "<explicit integer>",
    "maximum_age_seconds": "<explicit integer>"
  }
}
```

Angle-bracket values show types/roles, not a publishable event. Event IDs hash
canonical JSON containing schema version, rule and observation except snapshot
ID. Canonical timestamps/numbers avoid representation-only changes. An unchanged
hour remains the same event even when another table commit changes snapshot ID.
Changed mean/AQI/name or rule version produces a distinct content revision.
An A→B→A reversion reuses A's ID and is suppressed while its ledger entry remains.
Standard queues can reorder revisions; no newest-revision ordering or superseded-
snapshot lookup is implemented. Confirm these semantics before production. An eligible
correction sends a new revision; a correction below threshold emits no automatic
retraction. No cross-hour cooldown or hysteresis policy is approved/implemented.

`SNSPublisher` takes an injected client and ledger. It publishes the JSON as
plain SNS `Message`, with event/station string attributes and a fixed subject.
Only standard SNS/SQS resources are supported; FIFO rejects. Resource ARNs must
match the explicitly supplied account/region. Callers must configure the SDK for
one total attempt and bounded timeouts; the publisher calls `publish` once and
never retries the job. Partial multi-event publication is not atomic. Rerunning
skips already confirmed publications and can retry unfinished ones.

The SHA-256 event ID is a deduplication key, **not a signature or authorization**.
Restrict topic/queue send permissions; do not expose this payload as a public
unauthenticated ingestion API.

## Consumption, subscriptions and delivery state

`lambda_handler(..., app=injected_application)` is the local entrypoint. Each SQS
record must identify the configured queue and have a unique nonempty message ID.
Wrapped SNS delivery must match `Type=Notification` and the configured topic;
its `Message` contains event JSON. Raw delivery puts that JSON directly in body.
Legacy unstructured alert text is rejected; do not connect the operations-alert
SNS topic to this application queue.

Subscriptions retain Thịnh's string `chat_id` partition key. Optional
`station_ids` is a nonempty list of configured decimal-string location IDs;
`enabled=false` opts out, and optional integer `minimum_aqi` adds a subscriber
filter. Legacy chat-only rows retain their original all-configured-stations
opt-in. Confirm that compatibility policy before deployment. `/subscribe`
registers all configured stations; `/subscribe <id> ...` scopes a new subscription.
Existing subscriptions are not silently overwritten: unsubscribe then resubscribe
to change scope. `/unsubscribe` removes the subscription; `/start`/`/help` and
unknown commands return help. There is no `/now`, `/best` or personal rule UI.

The whole bounded subscriber scan is paginated before fanout. Repeated cursors,
malformed rows, more than 10 pages or 100 eligible recipients fail closed.
No recipients is a successful no-send result; new subscribers are not backfilled.
Subscriptions are read at processing time, not as an atomic multi-page snapshot.

A separate DynamoDB table uses string `delivery_id`, `state`, `owner`,
`lease_until` and TTL attribute `expires_at`. Keys distinguish SNS publication,
Telegram event/chat delivery and Telegram update/chat commands. Conditional puts
claim absent or expired INFLIGHT/RETRY leases; only the current owner can mark
SENT or RETRY. Confirmed SENT deliveries are skipped; busy leases fail the SQS
record instead of acknowledging it. A failed recipient is retried while already
successful recipients are skipped. No in-process retry loop exists.

This is **at-least-once**, not exactly-once. Remote acceptance followed by a
network timeout, crash or failed SENT write can produce a duplicate after lease
expiry. The tests demonstrate this window explicitly. Expired TTL items can remain
until DynamoDB deletes them, so TTL is retention, not a retry scheduler. Configure
retention beyond the entire approved SNS/SQS/DLQ redrive and manual replay horizon.
Never clear the ledger on reruns. See [DynamoDB TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html).

SQS partial responses contain only failed `messageId` values. Set
`AIRLAKE_PARTIAL_BATCH_FAILURES=1` **only together with** the event mapping's
`FunctionResponseTypes=[ReportBatchItemFailures]`. Otherwise errors raise for
whole-batch retry. Batch size is at most 10. Low Lambda remaining time stops new
recipient work; successful recipients remain deduplicated on retry. Queue
visibility, Lambda timeout/concurrency, lease duration and DLQ redrive must be
reviewed together. Stale/invalid events fail and require DLQ review, not silent
success. See [AWS SQS error handling](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html).

## Webhook and runtime configuration

The webhook requires `X-Telegram-Bot-Api-Secret-Token` matching
`TELEGRAM_WEBHOOK_SECRET`. Missing configuration returns 503; absent/wrong or
ambiguous headers return 403 before subscription work. Validate Telegram update
IDs/body before commands, return 400 for malformed updates, and return 503 on
subscription/send failures. Duplicate completed update IDs are acknowledged
without repeating the confirmation. Telegram must be configured to send that
secret by its owner; no webhook registration was performed here.

Imports create no clients. Future Lambda construction requires:

```text
AIRLAKE_ALERT_DELIVERY_ENABLED=1
AIRLAKE_ACCOUNT_ID=<approved account>
AWS_REGION=<approved region>
AIRLAKE_ALERT_STATIONS=<JSON object of approved location IDs to exact names>
AIRLAKE_ALERT_RULES=<JSON list of explicitly approved Rule objects>
AIRLAKE_ALERT_TOPIC_ARN=<dedicated standard SNS topic ARN>
AIRLAKE_ALERT_QUEUE_ARN=<dedicated standard SQS queue ARN>
AIRLAKE_SUBSCRIPTION_TABLE=<approved chat_id table>
AIRLAKE_DELIVERY_TABLE=<distinct approved delivery_id table>
AIRLAKE_DELIVERY_LEASE_SECONDS=<at least 60; exceeds bounded send/state duration>
AIRLAKE_DEDUPE_RETENTION_SECONDS=<approved retention horizon>
AIRLAKE_PARTIAL_BATCH_FAILURES=<0 or 1, consistent with mapping>
TELEGRAM_TOKEN=<injected by the approved secret-management/deployment process>
TELEGRAM_WEBHOOK_SECRET=<required only for webhook processing>
```

The factory checks the Lambda invocation ARN against expected account/region and
rejects profile/endpoint overrides. Use the execution role, not static keys or a
copied Mac login profile. Clients use bounded timeouts and one SDK attempt.
Package only `alerts/`, `bot_lambda/` and the required boto3 dependencies from the
lock; do not package the full Spark environment/JARs/data into Lambda. Packaging
and live service configuration are deployment work, not proven by mocked tests.

`create_table.py` reuses the teammate table schema but has no automatic CLI or
import-time provisioning. Its helper requires an injected client and explicit
confirmation; no table creation was called against AWS.

## Local verification

Python 3.12, the project environment and its locked dev dependencies are required.
A future `uv sync --frozen` installs dependencies; this sprint reused the existing
project interpreter and performed `uv lock --check --offline` without installing.
From this worktree, use its `.venv/bin/python` after setup, or the existing project
interpreter explicitly:

```sh
python -B -m unittest discover -s tests -p 'test_gold_to_telegram.py' -v
python -B -m unittest discover -s tests -p 'test_*alert*.py' -v
python -B -m unittest discover -s tests -p 'test_*.py' -v
ruff check --no-cache real_producer.py scripts tests alerts bot_lambda
ruff format --check --no-cache real_producer.py scripts tests alerts bot_lambda
uv lock --check --offline
```

Tests forbid real sockets, use fake SNS/DynamoDB/Telegram clients and cover both
SNS envelope modes, rule/input/staleness validation, corrections, partial failures,
recipient retries, conditional leases, pagination, webhook auth and commands.
The existing opt-in Gold P0 suite still uses fresh external Hadoop catalogs;
its production files and historical evidence remain unchanged. See
[the local result record](../tests/evidence/gold_to_alert_local_2026-11-07.json).

## Boundary to confirm with Khuê before cloud deployment

Proposed least-privilege scopes, not grants or verified effective permissions:

| Principal | Actions | Scope |
| --- | --- | --- |
| Publisher | `sns:Publish` | One approved application topic ARN |
| Publisher ledger | `dynamodb:PutItem`, `GetItem`, `UpdateItem` | Dedicated delivery table; restrict leading keys to `sns#*` where applicable |
| Consumer | `sqs:ReceiveMessage`, `DeleteMessage`, `GetQueueAttributes` | One dedicated queue, with the event mapping and redrive configured |
| Consumer subscriptions | `dynamodb:Scan`, `PutItem`, `DeleteItem` | Dedicated subscription table; no lake/table administration |
| Consumer ledger | `dynamodb:PutItem`, `GetItem`, `UpdateItem` | Dedicated delivery table, `telegram#*` and `webhook#*` keys |
| Logging | `logs:CreateLogStream`, `PutLogEvents` | Approved function log group; deployment owns group creation |
| Queue policy | SNS service `sqs:SendMessage` | Exact source topic ARN/account condition |
| Future Athena reader | Reviewed Athena/workgroup, Glue and scoped S3 reads/results permissions | Separate read/query adapter review; not a broad consumer-role grant |

For the future reader, scope `athena:StartQueryExecution`, `GetQueryExecution`
and `GetQueryResults` to the chosen workgroup; scope Glue `GetDatabase`/`GetTable`
reads to the approved catalog/database/table ARNs and S3 `ListBucket`/`GetObject`
to the actual Gold/dimension data and metadata prefixes. Athena also needs the
approved result-object writes/reads. Workgroup-enforced output settings take
precedence over a client-supplied prefix; review that scope rather than assuming
a new result prefix can be selected. No Athena reader permissions are needed by
the Telegram consumer itself.

KMS or SSM/Secrets Manager permissions depend on the selected encryption/secret
injection arrangement and require Khuê's review. No secret lookup or IAM change
was performed. Confirm rule thresholds/age, supported stations, legacy subscriptions,
correction/retraction/cooldown policy, SDK packaging, endpoint egress/TLS, webhook
registration, queue retention/DLQ, partial-response flag and lease/TTL timing.

Smallest proposed AWS test: one new standard SNS topic, SQS queue plus DLQ, Lambda,
subscription table, delivery ledger and log group; one approved private test chat.
Use a fresh scope, an explicitly approved fresh Gold fact and at most one initial
notification plus duplicate/retry checks. No API Gateway is needed for that first
SQS delivery test. Archived July 2025 facts cannot pass today's freshness guard;
obtain fresh data or separately approve a dedicated test-clock harness, without
weakening the production guard. No shared bot deployment, old smoke resources,
EC2, Terraform or Gold writes are part of this proposed slice. All provisioning,
AWS messages and real Telegram sends need separate approval.
