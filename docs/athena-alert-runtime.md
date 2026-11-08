# Athena alert runtime and isolated staging

For the current executable wiring, secret references, exact deployment-command
preparation and latest preflight status, see [the Sprint 3A.3 MVP guide](athena-to-telegram-mvp.md).
The infrastructure inspection below records the earlier Sprint 3A.2 observation.

Sprint 3A.2 implements the live-query adapter, not a live alert deployment.
PR #5 is merged into main at `f3f86f8b2a22953a2e181c5705b8ea72ed7d48eb`.
The new worktree changes neither Gold calculations nor historical acceptance.
All Athena, SNS and Telegram integration results in this sprint are **mocked**.
Read-only AWS metadata was inspected separately; no query or notification was sent.

## Reader contract

`AthenaConfig` explicitly allowlists both `(catalog, database, table)` triples.
The actual Athena catalog is `AwsDataCatalog`; `cloudreal` in older Spark evidence
is a Spark alias, not the Athena catalog name. Use the actual Athena catalog in
new event provenance and keep that choice stable across reruns.

The adapter joins these verified columns:

| Table | Consumed fields |
| --- | --- |
| `pm25_aqi_hourly` | `location_id INT`, `hour_start TIMESTAMP`, `pm25_avg DOUBLE`, `aqi INT` |
| `dim_location` | `location_id INT`, `name STRING` |

The cloud acceptance records Athena's hour type as `timestamp with time zone`.
Glue exposes it as `timestamp`; the reader requires Athena's UTC encoding and
whole-hour alignment. It enriches rows with the **project contract** units
`µg/m³` and method `simplified_hourly_mean_mvp`; those are not stored Gold columns
or a regulatory certification. Names must match the explicit station registry.
A left join plus validation detects missing metadata instead of dropping facts.
Duplicate result keys, including dimension fanout, reject the complete result.

Both table snapshots must be supplied from reviewed evidence/metadata. Athena
engine 3 supports `FOR VERSION AS OF <bigint>` after each table name, including
joins ([AWS version-travel documentation](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-time-travel-and-version-travel-queries.html)).
The implementation prepares that syntax; it has not executed this new joined query.
Snapshot existence, availability and service acceptance need the approved cloud test.
The event retains the fact snapshot; retain the configuration/SQL alongside the
query ID to preserve the dimension snapshot and query window too.

Reads require 1–100 approved stations, at most 24 complete UTC hours, an explicit
freshness rule and at most 1,000 rows. `LIMIT max_rows+1` detects overflow.
Time/station predicates limit scope; `LIMIT` alone does not cap bytes scanned.
Schema, nulls, finiteness, station identity, window membership and AQI provenance
are checked before the evaluator can publish anything. Empty results fail closed.
Pagination is bounded and rejects repeated cursors. HTTP clients have one SDK
attempt, 5-second connect and 10-second read timeouts. Query polling/results have
a configurable deadline (60 seconds in the proposed plan); an in-flight HTTP
request can exceed the deadline by its socket timeout.

Construction and `config.sql()` are offline. `sdk_executor(...)` deliberately
constructs SDK clients and may resolve credentials; it is not a dry-run entrypoint.
`execute_queries` defaults to `False`. The read method checks caller account,
SDK region, enforced engine-3 workgroup and **exact** result bucket/prefix before
one submission. It disables result reuse. It never silently resubmits, cancels
or deletes results. A submission timeout retains `submission-uncertain`; otherwise
`executor.query_id` remains available on failure. The caller must persist that ID
and stop on an ambiguous submission, rather than construct another executor to retry.

### Prepare the approved historical query offline

```python
import json
from alerts.athena import AthenaConfig

plan = json.load(open("docs/plans/athena-alert-staging.json"))
settings = dict(plan["reader"])
settings["allowed_tables"] = tuple(tuple(t) for t in settings["allowed_tables"])
settings["location_ids"] = tuple(settings["location_ids"])
config = AthenaConfig(**settings)
print(config.sql())  # No SDK client, credential lookup or AWS operation.
```

For a later separately approved execution, construct `sdk_executor` with an
approved temporary-credential profile or execution role, explicit station/rule
configuration, `execute_queries=True`, and the historical `test_now` from the
plan. Wrap it in `AthenaGoldSource` and use the existing `evaluate`/`publish_batch`
interfaces. These calls are **NOT EXECUTED** here. A caller must never publish
before all returned observations pass evaluation. No scheduler or continuously
running publisher is installed by this change.

Production reads use the real current UTC clock. Old July 2025 observations fail
freshness validation today. Historical reader mode requires both
`historical_test_mode=True` and a fixed `test_now`. It does not alter downstream
consumer freshness automatically. The staging Lambda additionally requires:

- `AIRLAKE_ALERT_TEST_MODE=1` and exactly one canonical
  `AIRLAKE_ALERT_TEST_RECIPIENT`, provided privately by the owner.
- `AIRLAKE_ALERT_TEST_REFERENCE_TIME` only for the separately approved historical
  replay. Without it, the real clock is used. Outside recipient test mode it rejects.
- An explicitly approved **test-only** rule; the plan's `fixture-only/test-v1`
  threshold 40 is a routing fixture, not a production health recommendation.

The fixed reference affects observation validation only. DynamoDB leases and
`expires_at` always use actual UTC time. The recipient must still have an eligible
subscription. The guard also rejects other webhook recipients before state changes
or sends. Disable test mode by removing the recipient/reference settings and
setting the mode to `0`; never carry historical clocks into production.

## Read-only infrastructure findings: 2026-11-07

Account `701364614078`, region `ap-southeast-2`, CLI profile `airlake` were verified.
Khuê's retained `origin/khue:status-report.txt` at `b5a188d` is a UTF-16 historical
report; its infrastructure claims do not replace the fresh API observations below.

| Resource | Actual observation | Deployment consequence |
| --- | --- | --- |
| `AirLake_Alerts` | One SQS subscription to `AirLake_Bot_Queue`; raw delivery false; no subscription filter/redrive policy | New handler supports this wrapper, but shared topic must not receive tests |
| `AirLake_Bot_Queue` | Visibility 30s, retention 345600s, managed SSE, no redrive policy | Requires explicit queue DLQ and timing review |
| `AirLake_Bot_API` | Python 3.12; `lambda_function.lambda_handler`; timeout 3s; 128 MB; only environment name `TELEGRAM_TOKEN`; no layers | Incompatible with new handler configuration and its 60s remaining-budget guard |
| Mapping `8dde34f1-e036-40e5-82c5-49209877759e` | Enabled; batch 10; batching window 0; `FunctionResponseTypes=[]` | Partial failures are not enabled |
| HTTP API `rsvjj5fff3` | `ANY /AirLake_Bot_API`, authorization NONE, AWS_PROXY to the same Lambda, payload v2.0, integration timeout 30000ms | Existing webhook shares this function; do not overwrite it |
| `AirLake_Subscriptions` | String HASH key `chat_id`, on-demand, TTL disabled; COUNT-only probe confirms at least one subscriber | No subscriber identity was requested; accidental fanout is a real risk |
| Delivery ledger | `ListTables` returned only `AirLake_Subscriptions` | Dedicated ledger is not deployed |
| Existing Lambda role | `PowerUserRole`, attached `PowerUserAccess`; no inline policies returned | Do not reuse this privilege model for staging |
| Workgroup `airlake` | Enabled, engine 3, enforced output at the results-bucket root, 10 GiB cutoff | Separate workgroup/prefix avoids broad result-object privileges |

The deployed package was downloaded privately and its SHA-256 matched Lambda's
`CodeSha256`. It contains one small handler and relies on Lambda's bundled SDK.
Static inspection confirms both SQS and webhook paths, but no partial-response,
pagination, ledger or webhook-secret implementation. No webhook request or
Telegram API was called. API Gateway authorization NONE does not itself prove
an exploit; the absent handler secret check is the application-level blocker.
A REST-API lookup returned NotFound because this is an HTTP API; the v2 lookup
confirmed the integration. Zero approximate queued/inflight counts are a point-in-time
observation, not proof that shared traffic cannot arrive later.

## Deployment proposal — NOT EXECUTED

Use [the local staging plan](plans/athena-alert-staging.json). All names are
proposals and must be checked for absence before any provisioning. The plan is
not Terraform and contains no deployment automation or secret values.

Create a separate standard SNS topic, SQS queue and DLQ, Python 3.12 Lambda,
subscription table, ledger, log group and Athena engine-3 workgroup. Do not connect
an API Gateway or change the existing bot webhook. Prefer a separate staging bot
credential, supplied privately; never retrieve or copy the deployed token here.
Create exactly one approved subscription instead of copying the shared table.

- Lambda: 120s timeout, 256 MB, reserved concurrency 2.
- Mapping: batch 1, window 0, maximum concurrency 2,
  `FunctionResponseTypes=["ReportBatchItemFailures"]`.
- Queue: visibility 720s, retention 4 days, redrive after 5 receives.
- DLQ: retention 14 days, redrive allowed only from the staging source queue.
- Ledger: string HASH `delivery_id`, PAY_PER_REQUEST, TTL on numeric `expires_at`,
  180s leases and 60-day retention. Manual redrive must stop after the approved
  30-day horizon; delayed SNS delivery and retention must fit that horizon. Do not
  interpret TTL deletion as immediate or promise lifetime duplicate suppression.
- Workgroup: enforced `s3://airlake-701364614078-athena-results/alert-staging/v1/`,
  engine 3 and 100 MiB query cutoff. A small query may still hit the service's
  minimum billing; a timeout does not stop a running query.
- Queue policy: SNS service principal, exact topic ARN and source-account condition.
  Separate reader/publisher and consumer IAM policy proposals are in the plan.

The 120s timeout accommodates the existing pre-send 60s budget guard and bounded
DynamoDB/Telegram operations for one recipient. HTTP socket timeout is not an
end-to-end delivery deadline. Leases outlive the Lambda invocation; visibility
allows recovery after a failed invocation. AWS recommends visibility at least
six times Lambda timeout plus the batching window
([SQS configuration](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html)).
AWS must enable partial responses as well as the application flag
`AIRLAKE_PARTIAL_BATCH_FAILURES=1`
([partial-batch response documentation](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html)).

Reader/publisher privileges cover only the chosen workgroup, two Gold Glue tables,
read-only Gold data/metadata prefixes, one result prefix, staging SNS topic and
`sns#` ledger keys. Consumer privileges cover only its queue, subscription table,
`telegram#`/`webhook#` ledger keys and log streams. Provision the log group separately.
Neither role requires PowerUserAccess, Gold writes, Glue DDL or infrastructure
administration. Khuê must confirm bucket encryption/KMS, Lake Formation grants,
permissions boundaries/SCPs and role assumptions before granting these proposals.
Successful metadata reads under the operator role do not establish runtime write
permissions. No IAM policy was installed or simulated as an effective grant.

At-least-once delivery remains: remote Telegram acceptance followed by a lost
SENT write can duplicate after lease expiry. Corrections create content revisions;
snapshot-only changes deduplicate. No latest-revision ordering, retractions or
cooldown policy is introduced. Partial failures preserve retryable messages;
there is no atomic rollback spanning Athena, SNS, SQS, DynamoDB and Telegram.

## Local package and verification

Use Python 3.12 and the existing `uv.lock`. Supply its seven universal SDK wheels
in an external directory; hashes must match the lock. The builder does not download,
install, resolve dependencies or contact AWS. It includes application modules,
SDK dependencies, metadata and licenses, and excludes optional wheel CLI scripts.
It rejects checksum/path errors and output overwrite, and creates no ZIP in Git.

```sh
python -B scripts/build_alert_lambda.py --wheels /path/to/verified-wheels --output /tmp/airlake-alert-build-UNUSED
python -B -m unittest discover -s tests -p 'test_*.py' -v
ruff check --no-cache real_producer.py scripts tests alerts bot_lambda
ruff format --check --no-cache real_producer.py scripts tests alerts bot_lambda
uv lock --check --offline
```

This sprint fetched only the lock-pinned public wheels, built the ZIP, imported the
extracted handler/SDK with sockets blocked, and validated Athena SDK parameter
shapes without sending requests. See the [sanitized runtime record](../tests/evidence/athena_alert_runtime_2026-11-07.json).
The prior 48 Gold Spark tests are retained evidence, not a fresh execution in this
sprint. Gold, its P0 tests and historical evidence remain byte-identical to main;
no Spark regression was needed for these alert-only changes.

## Smallest next cloud test and rollback — NOT EXECUTED

After code review and separate approval with Khuê:

1. Confirm one test recipient and test bot credential privately. Approve the
   historical routing rule/reference or provide a fresh approved Gold observation.
   Verify the proposed names are unused and the two retained snapshots still exist.
2. Provision only the staging resources above. Keep delivery disabled and mapping
   disabled until the full ZIP, environment names/values, IAM, DLQ, TTL and exact
   mapping response settings have been reviewed. Publish an immutable version and
   invoke a staging alias; pin its artifact hash and retain configuration securely.
3. Separately approve one bounded Athena SELECT over one station/hour, pinned to
   both retained snapshots. Verify the observed result against retained `21.7875`
   PM2.5 and AQI `44`; retain query ID, schema, window, snapshots and exact result
   object keys. Do not recompute Gold, write its tables or query unrelated data.
4. Separately approve one SNS publish through the existing publisher/ledger, with
   only the approved eligible recipient in the isolated subscription table. Verify
   consumer ledger SENT, no other recipient attempts, queue/DLQ state and an
   owner-confirmed Telegram message. An unchanged publisher rerun should be
   suppressed before SNS; authorize any additional network-send test separately.
5. On failure, stop without resubmission/cleanup. Record query/message IDs and
   sanitized state. Preserve the ledger, queued messages and artifacts for inspection.

Rollback: disable only the **staging** mapping and publisher first. Restore the
staging alias to its previously recorded immutable version and configuration;
if it is the first deployment, leave it disabled instead of inventing a previous
version. Restore the mapping's saved settings before re-enabling. Failed-message
redrive requires separate approval. Existing shared Lambda, API and subscriptions
remain untouched. A future shared-bot migration must first publish/record its old
version, alias/mapping configuration and secure environment backup; the privately
retained old ZIP alone cannot restore its secret environment or consume-state.

Cleanup is manual and separately approved: inventory staging query/result objects,
SNS subscription/topic, mapping, queue/DLQ messages, Lambda versions/alias/log group,
and both tables first. Stop producers and mappings; preserve required evidence
and ledger retention before deleting anything. Delete only listed staging result
keys, never the bucket, accepted Gold prefix, shared resources or historical data.
No cleanup commands were run.

Cost exposure is limited to one small Athena read, result objects, SNS/SQS,
Lambda, DynamoDB on-demand and logs. Workgroup cutoff, batch/fanout bounds and
absence of schedulers constrain it; retries, lingering messages and logs can still
incur charges. There is no new EC2 or continuously running compute in this plan.

**Deployment decision: NO-GO for the shared bot; isolated staging is required.**
Offline implementation is ready for a focused PR. Cloud execution still requires
owner approval, staging provisioning/configuration, recipient/rule confirmation,
runtime IAM review and the first live joined-query/delivery verification.
