# Bounded current-data pilot

The initial Scheduler target failure was corrected through separately approved
recovery. The scheduler exists and remains DISABLED. One real manual poll stored
**45 observations (22 / 22 / 1)**, updated all three latest records and completed
all cursors. The invocation then failed while rendering freshness metrics:
DynamoDB returned numeric cadence as Decimal, which the strict validator rejected.
The poller is stopped at concurrency zero; one attempt is recorded. No schedule
enablement, new Glue run, Athena SELECT or API HTTP smoke has occurred.

The stored-cadence conversion is corrected locally and passes 36 focused tests.
The small replacement ZIP changes only `live_data/store.py`; existing records,
watermarks, counters, secret, roles, API code and frozen evidence are preserved.
See [accepted-ingestion evidence](../tests/evidence/live_openaq_ingestion_stop_2026-11-14.json)
and [the exact repair plan](plans/live_openaq_number_fix_2026-11-14.json).
Applying the poller code update and continuing requires new explicit approval;
no automatic retry has occurred. Historical PR #13/#14 acceptance remains unchanged.

## Implemented data path

`live_data.poller` reuses the approved OpenAQ v3 client, pilot identities,
metadata validation and nine-field Bronze record adapter. Lambda writes each
accepted observation to immutable S3 before updating DynamoDB latest state and
then the cursor. `observed_at` is the source period end, not ingestion time.
`ingested_at` is the poll invocation UTC time assigned to the first accepted
raw object and retained on replay; it is not an S3 commit timestamp.
The source's measurement period is retained separately: a one-hour averaging
period does not imply hourly reporting.

A new DynamoDB table contains `RUN/LEASE`, `CURSOR/<sensor>`,
`LATEST/<location>`, `POLL/<location>` and `EXPORT/CURRENT` items. There is no TTL
or automatic ledger/cursor deletion. The poller has reserved concurrency one,
a conditional lease, a durable maximum of six attempts, a thirty-minute
wall-clock test window and no SDK write retry. Any caught poll failure sets a
stop flag; future scheduled attempts fail closed. A timeout leaves the lease
for inspection. Do not reset the cursor, stop flag, counter or raw objects.

Each invocation requests one page of at most 100 measurements per pilot: at
most 300 records. Initial lookback and overlap are two hours; later arrivals
inside that overlap can be accepted without regressing latest state. A frozen
page window persists across incomplete pages; the watermark advances only after
the page/window completes. This does not guarantee recovery of arbitrarily late
records or API pagination shifts outside the bounded overlap. Duplicate source
identity is `(location_id, sensors_id, period.datetimeTo.utc)`. A different
value/metadata for an existing identity is a correction conflict and stops the
batch; it does not silently change the existing first-wins Silver contract.
Invalid units, timestamps, identifiers, finite values or quality flags are
rejected. Empty/stale/unavailable sources never manufacture a reading.

OpenAQ GETs have at most three bounded attempts, including 429/5xx backoff.
Worst case is 108 HTTP attempts across six polls. No Kafka, EC2 or notification
resources are involved in this isolated S3 ingestion adapter. The accepted
nine-field record remains compatible with the existing Kafka/Bronze contract.

## Actual source inspection

See [source and local evidence](../tests/evidence/live_openaq_preparation_2026-11-14.json).
A bounded survey inspected the three pilots and five recent candidate locations
within 20 km of central Hanoi. This is not an administrative city-wide census.
All pilots had genuine recent observations and seven-day samples. Two additional
network locations last reported on November 13; older candidates were stale.
No new Gold station-type mapping is invented for those candidates.

Six genuine recent sample records passed the unchanged Bronze validator. The
two Hanoi network samples were five minutes apart, with one-hour measurement
periods; Dragon samples were one hour apart. These samples establish availability
and observed spacing, not a source SLA or seven-day completeness. OpenAQ's
aggregate coverage percentages were not treated as validated uptime. Dragon
reported CC BY 4.0; the two network stations supplied no license metadata in this
survey. Preserve attribution; public redistribution remains uncleared.

## API and latest view

Deploy a new isolated API Lambda reusing the existing historical handler; do
not modify the accepted `airlake-final-api-20261114_34207148` function or URL.
The new AWS_IAM URL provides:

- `/api/v1/health`, `/api/v1/gold/hourly`, `/api/v1/stations`: unchanged historical
  behavior and immutable November 7–8 provenance; JSON/CSV and filtering remain.
- `/api/v1/latest[?location_id=4946812]`: strongly consistent accepted DynamoDB
  readings for the three pilots. Returns PM2.5, unit, station, sensor,
  `observed_at`, `ingested_at`, source attribution, cadence estimate and poll
  status. **AQI is null** for instantaneous readings.
- `/api/v1/current/hourly`: bounded JSON/CSV, location/time filters and pagination
  over the new hash-verified export, initially unavailable until independent
  Athena acceptance. It does not replace the historical endpoint.

Freshness is based on observation age: `recent` means within
`max(15 minutes, twice the estimated positive observation spacing)`. If cadence
is unknown, the explicit fallback is three hours. It is not a health or safety
classification. `stale`, `no_data` and `invalid_future` are distinct; polling
success does not turn missing data green. Last successful poll time/status is
shown separately, so an old poll does not masquerade as active operation.

The existing five Superset historical panels are untouched. The HTTP smoke
command renders the same real `/latest` response into a small escaped HTML
view, with UTC timestamps and no auto-refresh or extra requests. Before cloud
approval only rendering tests use synthetic data. Do not present a fixture
screen as a live dashboard. The browser's static snapshot has its generation
time printed; reloading the HTML does not poll AWS.

## Frozen Glue update

Disable polling and verify a released lease before freezing an exact S3 object
manifest, including every object SHA-256. One Glue 5.0 job uses the same verified
Iceberg 1.12.0 JARs and original Silver/Gold source bytes. It creates only the new
database's `bronze_measurements`, `aq_measurements`, `aqi_breakpoints`,
`dim_location` and `pm25_aqi_hourly` tables. The original historical tables are
not inputs or targets. The original finite/unit/dimension safeguards apply.

The adapter checks raw identity uniqueness, reconciles UTC keys and means,
validates Gold integrity and repeats the unchanged transforms once inside the
job to prove five-snapshot idempotency. Python driver timezone and Spark session
are both UTC. Glue `MaxConcurrentRuns=1`, `MaxRetries=0`, two G.1X workers and a
ten-minute timeout prevent overlapping/retried jobs in this bounded namespace.

The job writes an immutable export version. Only a successful independent
Athena SELECT comparing every exported key/value/name permits a conditional
DynamoDB pointer promotion. The API reads the pointer consistently and verifies
the export hash per request. No multi-service atomic rollback is claimed. A
failed job/query leaves artifacts and does not promote partial results.

This first pilot deliberately rejects an existing output database or another
job receipt. **An hourly Glue schedule and continuously refreshed hourly API
are not implemented or enabled.** After the bounded run passes, a separate owner
review must define new batch versions, cross-invocation idempotency and repeated
conditional pointer replacement before scheduling further batches.

## Scope and setup

Account `701364614078`, region `ap-southeast-2`, operator profile `airlake`.
New scope `20261114_9e2c740b`:

| Resource | Exact proposed name |
|---|---|
| Poller | `airlake-live-20261114_9e2c740b-poller` |
| API | `airlake-live-20261114_9e2c740b-api` |
| State table | `airlake-live-20261114_9e2c740b-state` |
| Scheduler | `airlake-live-20261114_9e2c740b-5m`, initially DISABLED |
| Secret | `airlake/live-demo/20261114_9e2c740b/openaq` |
| Glue job | `airlake-live-20261114_9e2c740b-gold` |
| Glue database | `airlake_live_20261114_9e2c740b` |
| S3 prefix | `s3://airlake-701364614078-lake/live-demo/20261114_9e2c740b/` |

Four dedicated IAM roles use the resource stem plus `-poller`, `-api`,
`-scheduler`, `-glue`. Policies are scoped to these exact resources. Poller
reads only the newly created secret's exact ARN. API cannot read secrets or
query Athena; its DynamoDB reads are limited by leading keys. Scheduler can
invoke only the poller. Glue accesses only this run prefix and database.
The API alone also reads the already verified historical export, read-only.
No KMS policy is added for the default AWS-managed secret key. No broad managed
workload policy or existing-role change is used. Operator provisioning and
`iam:PassRole` permissions remain necessary; reads cannot establish write/SCP
permissions. DynamoDB transaction authorization uses the underlying PutItem
and ConditionCheckItem actions, not an invented TransactWriteItems IAM action.

Select an existing Python and cached SDK environment. `--offline` never downloads:

```sh
# Set DEMO_PYTHON to the existing project Python 3.12 interpreter.
PROJECT_SITE=$("$DEMO_PYTHON" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
live_sdk() {
  PYTHONPATH="$PROJECT_SITE${PYTHONPATH:+:$PYTHONPATH}" \
  uv run --offline --no-project --python "$DEMO_PYTHON" \
    --with 'boto3==1.43.115' --with 'botocore[crt]==1.43.115' python -B "$@"
}
# Choose a NEW private packet outside Git; never publish its binaries/receipts.
live_sdk -m live_data.deploy prepare --packet "$PACKET" \
  --runtime-jar "$AIRLAKE_ICEBERG_RUNTIME_JAR" \
  --aws-bundle "$AIRLAKE_ICEBERG_AWS_BUNDLE_JAR"
PLAN_SHA=$(cat "$PACKET/plan.sha256")
live_sdk -m live_data.deploy preflight --packet "$PACKET"
```

The new OpenAQ secret does not currently exist. After separate write approval,
the deploy command copies the existing explicitly selected local OpenAQ key
into plaintext SecretString internally. No token CLI value, log or private
packet is created. It does not read or change existing Telegram secrets.

## Execution commands — do not repeat the partial deployment

The initial deploy command partially completed; approved recovery then created
the disabled schedule. A manual poll accepted 45 records but failed in metrics.
Do not repeat deployment or overwrite the failed manual receipt. Preserve the
original packet; use the separately approved number-repair packet before any
remaining test steps. One of six source attempts has already been consumed.
All commands below remain gated by explicit scope approval.

```sh
live_sdk -m live_data.deploy deploy --packet "$PACKET" \
  --env-file "$OPENAQ_ENV_FILE" --execute --confirm-plan-sha256 "$PLAN_SHA"
# Schedule remains disabled. Prove one real poll first; stop on failure/uncertainty.
live_sdk -m live_data.deploy poll-once --packet "$PACKET" \
  --execute --confirm-plan-sha256 "$PLAN_SHA"
live_sdk -m live_data.deploy status --packet "$PACKET" > "$PACKET/status-after-manual.json"
# Only after genuine accepted observations and approved bounded scheduling:
live_sdk -m live_data.deploy enable --packet "$PACKET" \
  --execute --confirm-plan-sha256 "$PLAN_SHA"
# Observe without extra ingestion. EndDate and durable attempt cap are mandatory.
live_sdk -m live_data.deploy status --packet "$PACKET" > "$PACKET/status-during-window.json"
# Stop within the approved 30-minute window; no extension or automatic restart.
live_sdk -m live_data.deploy disable --packet "$PACKET" \
  --execute --confirm-plan-sha256 "$PLAN_SHA"
live_sdk -m live_data.deploy status --packet "$PACKET" > "$PACKET/status-disabled.json"
# Requires Disabled schedule, zero poller concurrency and a released lease.
live_sdk -m live_data.deploy freeze-glue --packet "$PACKET" \
  --execute --confirm-plan-sha256 "$PLAN_SHA"
# Read job status until SUCCEEDED; do not start another run.
aws --profile airlake --region ap-southeast-2 glue get-job-run \
  --job-name airlake-live-20261114_9e2c740b-gold \
  --run-id "$JOB_RUN_ID" --query 'JobRun.{State:JobRunState,Seconds:ExecutionTime,DPUSeconds:DPUSeconds}'
# Exactly one Athena SELECT, then atomic export-pointer promotion on full match:
live_sdk -m live_data.deploy verify-glue --packet "$PACKET" \
  --execute --confirm-plan-sha256 "$PLAN_SHA"
# At most eight API HTTP requests, no Athena calls, then render real latest HTML:
live_sdk -m live_data.smoke --packet "$PACKET" \
  --execute --confirm-plan-sha256 "$PLAN_SHA"
open "$PACKET/latest-measurements.html"  # macOS; no further AWS request
```

The thirty-minute deadline starts during deployment; it is not silently
extended if preparation consumes time. Six attempts include the manual call.
Schedule is enabled only after manual acceptance, with asynchronous Event delivery,
zero Scheduler retries and zero Lambda asynchronous retries. AWS asynchronous delivery can still duplicate an
invocation; the durable attempt gate and observation identities bound source
polling and prevent duplicate raw/latest updates. If the window
expires, stop and seek a newly reviewed execution window. A timeout can leave
an in-flight invocation for at most the configured Lambda timeout; freezing
requires its lease to be explicitly released, not merely elapsed.

The HTTP checks cover unsigned denial, signed historical health, latest rows
and three station filters, current hourly and preserved historical CSV. At
least one genuinely recent reading is required for the current-data smoke PASS.
The generated latest HTML uses that exact real response without extra reads.
Screenshots may be taken from this local static view after real verification;
no screenshot or cloud PASS is claimed by offline fixture tests.

## Stop, evidence and costs

Emergency stop, after approval, affects only the new poller:

```sh
live_sdk -m live_data.deploy disable --packet "$PACKET" \
  --execute --confirm-plan-sha256 "$PLAN_SHA"
# If the local packet cannot be used, stop new invocation independently:
aws --profile airlake --region ap-southeast-2 lambda put-function-concurrency \
  --function-name airlake-live-20261114_9e2c740b-poller \
  --reserved-concurrent-executions 0
# Emergency stop only this Glue JobRunId if necessary:
aws --profile airlake --region ap-southeast-2 glue batch-stop-job-run \
  --job-name airlake-live-20261114_9e2c740b-gold --job-run-ids "$JOB_RUN_ID"
```

Disable still sets concurrency zero if Scheduler update fails. It does not
require a fresh source secret or matching artifact bytes to block the fixed
poller; the fixed scope and plan confirmation remain required. Setting
concurrency zero cannot cancel already-running work. Preserve all receipts,
state, raw observations and incomplete outputs after uncertainty. No automatic
cloud cleanup is implemented. The old API and historical Superset remain usable
if this isolated pilot fails; its API can separately be disabled with concurrency
zero. A separate cleanup approval must inventory this run's S3 objects, query
result objects, five Glue tables/database/job, four roles/policies, two functions
and URL, schedule, table, secret and log groups. Never delete old acceptance data.

Lambda groups have three-day retention; capture sanitized evidence promptly.
Poll logs contain counts/status only. EMF observation-age metrics use actual
accepted DynamoDB timestamps; missing data has FreshnessKnown=0 and no invented
age gauge. New metric namespace: `AirLake/LiveDemo`; dimensions Run/Location.

Cost scope: at most six poll attempts, each 128 MB/180 seconds; one API at
128 MB/15 seconds; at most 1,800 immutable raw objects; one Glue job with two
G.1X workers/ten-minute timeout; one Athena SELECT and eight HTTP reads. Additional
cost drivers are Secrets Manager retention, DynamoDB on-demand operations,
S3 storage/requests/results, Glue catalog, CloudWatch logs/custom metrics and
Lambda duration. The nominal Glue compute scope is 20 DPU-minutes plus service
billing/startup granularity, not a guaranteed dollar cap. Billing has not been
measured. Retained resources can continue incurring storage/secret costs after
the thirty-minute poll window. No always-on compute or new ALB is created.

## Telegram boundary — no staging changes

Existing alerts consume **closed Gold hours**, not latest raw values. Preserve
station `location_id`, UTC `hour_start`, finite `pm25_avg`, units `µg/m³`,
`aqi`, `simplified_hourly_mean_mvp`, source table and Iceberg snapshot. Configure
the reader's new approved identifier/snapshot allowlist separately after this
batch passes. Existing subscriptions use string `station_ids`, `minimum_aqi`
and recipient routing; existing deterministic event IDs cover station/hour,
value and rule/version, while delivery idempotency includes each recipient.
Stale or unclosed hours are rejected by the evaluator. Instantaneous PM2.5
thresholds would require an explicit separate rule/schema; do not fabricate AQI
or send latest records directly to the current handler. No subscriber identities,
SNS publication, staging resources or Telegram messages are touched here.
Historical demonstrations retain their explicit historical/test classification.

## Local verification

New focused tests exercise source validity, cursor failure recovery, replay,
out-of-order records, source corrections, budgets, 429 responses, API isolation,
versioned exports, exact IAM scope, emergency stopping and SDK request shapes.
Existing relevant ingestion/API/configuration/Gold orchestration tests run offline.
The opt-in `tests/live_glue_integration.py` uses a fresh local Spark session,
verified runtime JAR and disposable external warehouse. Its synthetic 9-record
fixture yielded 9 Silver rows, 3 dimensions and 6 facts, with five unchanged
snapshots. It is not live AWS evidence. Production Gold/Silver source bytes and
all previous acceptance evidence are unchanged.

Primary references: [OpenAQ locations](https://docs.openaq.org/resources/locations),
[measurements](https://docs.openaq.org/resources/measurements),
[Scheduler universal targets](https://docs.aws.amazon.com/scheduler/latest/UserGuide/managing-targets-universal.html),
[DynamoDB transaction IAM](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis-iam.html).
