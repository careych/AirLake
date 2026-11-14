# Bounded automatic polling and Gold/API refresh

Status: **CODE READY, NOT AWS DEPLOYED OR LIVE VERIFIED**. PR #16's 70-record AWS
acceptance remains historical evidence. This change has not repeated it.

The existing five-minute schedule targets one new controller Lambda. Each tick can
invoke the existing poller once, then advance one persisted refresh state machine.
There is no second schedule, always-on Spark cluster, notification publisher or
new ingestion format. The three original stations and their source cadence remain
unchanged. Five-minute dispatch does not promise five-minute source observations.

## Boundaries and guarantees

- Preserve all existing raw objects, source cursors and the cumulative poll counter.
  The inspected counter is 4/6; the first operations window can use only its two
  remaining attempts (at most 600 additional observations, 670 cumulative).
  This is a supervised, bounded demonstration, not indefinite production operation.
  Once six attempts are consumed, another polling budget needs a separately reviewed
  extension; these commands never reset it. The absolute raw manifest cap is 1,800.
- A durable controller lock and Lambda concurrency 1 serialize ticks. A timeout
  leaves the lock active and requires reconciliation, rather than automatic lease
  takeover. A pending failed poll/window is never silently discarded. A watermark
  older than the 24-hour lookback stops for approved backfill instead of skipping a gap.
- After polling releases its lease, list immutable raw objects, verify hashes and
  freeze a sorted manifest. A full cumulative rebuild preserves late observations
  inside the bounded overlap. Source corrections remain explicit conflicts, not
  permission to overwrite the original observation. Arrivals outside the two-hour
  overlap and changes in upstream page ordering are not guaranteed recoverable.
- Identical input skips Glue and Athena. New input can start **one** run per window
  on the existing Glue job, with concurrency 1, two G.1X workers, ten-minute timeout,
  no retries or queuing. Processing begins on the first changed boundary; this is
  not a promised hourly production service.
- Each changed input gets a new database:
  `airlake_live_20261114_9e2c740b_r<first-16-hex-of-input-manifest-SHA256>`.
  Five tables are rebuilt there: `bronze_measurements`, `aq_measurements`,
  `aqi_breakpoints`, `dim_location`, `pm25_aqi_hourly`. Existing databases and
  historical snapshots are not targets. The existing verified Iceberg JARs stay
  in place; no dependency resolution or upload of the JARs is needed.
- Original Silver and Gold transformations are reused without source changes.
  Reconcile input count, finite values, UTC hourly keys/means, station joins,
  duplicates and five unchanged-rerun snapshots. Store the source watermark,
  manifest hash, job run, snapshots, phase and result in `OPS/<plan-hash>` in the
  existing state table. Save uncertain phases **before** writes.
- Glue success alone never promotes the API. One new, bounded Athena SELECT reads
  pinned fact/dimension snapshots and compares every row, AQI and station join.
  Use a deterministic request token, no result reuse, no automatic resubmission.
  Token behavior and result-bucket access follow the [Athena API contract](https://docs.aws.amazon.com/athena/latest/APIReference/API_StartQueryExecution.html).
- Promote `EXPORT/CURRENT` with a conditional compare of the prior key and hash.
  Store the previous verified pointer, keep immutable exports and all OPS history.
  A concurrent pointer change, failed validation or uncertain write stops the run.
  There is no atomic multi-service rollback. A crash after promotion may require
  reconciling the pointer even if the OPS record still says `PROMOTING`.

## Existing resource changes and new resources

Account `701364614078`, region `ap-southeast-2`, operator profile `airlake`.
Existing names start `airlake-live-20261114_9e2c740b`:

- New Lambda/role: `airlake-live-20261114_9e2c740b-refresh`, Python 3.12 arm64,
  256 MB, 240-second timeout, concurrency initially zero, async retries zero,
  maximum event age 60 seconds. Its log group has seven-day retention.
- Retarget existing `-5m` schedule to async `InvocationType=Event` on the controller.
  Installation keeps it **DISABLED**. The scheduler role receives an additional
  exact-controller invocation policy, not broader wildcard invocation.
- Update existing `-gold` job script/payload and allow per-run manifest/database
  arguments. Preserve JARs, workers and other configuration. Frozen manifest
  arguments must leave `NonOverridableArguments` because those take precedence over
  run arguments ([Glue parameter precedence](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html)).
  Add only the versioned database scope to the existing Glue role.
- Update only existing `-api` code for the versioned export reader. Preserve its
  IAM-only URL, role, concurrency, environment and historical endpoints. Keep a
  hash-verified copy of the previous deployed ZIP for rollback.
- At enable time only: preserve poller/controller environment except the two deadline
  values; use revision checks; concurrency 1; one schedule window of at most 45
  minutes. No source cursor, record, secret, counter or Telegram resource changes.

Controller IAM permits only this job, schedule, functions, state table, versioned
Glue database family and S3 live-demo prefix. DynamoDB writes cannot alter RUN or
CURSOR records. Athena is limited to workgroup `airlake`; its enforced results bucket
needs read/write access. This necessarily allows access to existing objects in that
results bucket, not just a pre-known query key. The role cannot retrieve secrets or
publish notifications. Existing poller/Glue permissions are retained, not broadened
outside this demo scope. Effective IAM/SCP permissions still require deployment verification.

## Offline preparation and read-only checks

Use the project Python 3.12 environment and cached dependencies, including the
pinned CRT extra for the operator's AWS login profile. Nothing below creates cloud
resources. Never copy private packets, ZIPs or inventory into Git.

```sh
# From the new worktree; dependencies must already be cached (--offline).
ops() { uv run --frozen --offline --with 'botocore[crt]==1.43.115' python -B -m live_data.operations "$@"; }
OPS_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/airlake-operations.XXXXXX")
chmod 700 "$OPS_ROOT"
ops inventory --inventory "$OPS_ROOT/inventory.json"
# Set this to the retained original API payload ZIP matching deployed CodeSha256.
: "${PREVIOUS_API_ZIP:?Set the verified original API ZIP path}"
ops prepare --packet "$OPS_ROOT/packet" --inventory "$OPS_ROOT/inventory.json" \
  --previous-api-zip "$PREVIOUS_API_ZIP"
ops preflight --packet "$OPS_ROOT/packet"
```

The packet contains exact artifact hashes, policies and rollback configuration.
Fresh preflight requires the same pointer, code, job, disabled schedule, unused
controller name, inactive locks and unchanged attempt counter. Access errors do
not mean absence. Never substitute a new plan hash into an old approval.

## Deployment and one demonstration — NOT EXECUTED

Requires separate approval of the packet SHA-256, named resources, 45-minute
window, one Glue run, one Athena SELECT and three signed API GETs. No automatic
cleanup. New data are not guaranteed; **NO_NEW_DATA is a successful skip, not proof
of a new Gold refresh**.

```sh
: "${AIRLAKE_OPS_PACKET:?Set the prepared private packet}"
: "${AIRLAKE_OPS_PLAN_SHA256:?Set the separately approved exact plan hash}"
ops deploy --packet "$AIRLAKE_OPS_PACKET" --execute \
  --confirm-plan-sha256 "$AIRLAKE_OPS_PLAN_SHA256"
# Safe default: checks installed disabled configuration only.
AIRLAKE_OPS_APPROVED=no sh scripts/live_operations_demo.sh
# Only after the exact window has been approved:
AIRLAKE_OPS_APPROVED=yes sh scripts/live_operations_demo.sh
```

Export the two AIRLAKE_OPS variables before invoking the shell script. It enables
once, watches the bounded window and always attempts both function stops and
schedule disabling on exit. A second enable is rejected by its retained receipt.
It can stop early after promotion, or after two no-new-data polls. It does not
start a Glue job with fewer than fifteen minutes left. An in-flight Lambda may
finish after the deadline; admission controls cannot cancel it. EndDate, source
runtime deadline and cumulative counters bound work even if the operator shell dies.
The watcher is a supervised demo safety control, not a deployed independent watchdog.

## Evidence, monitoring and presentation

```sh
ops status --packet "$AIRLAKE_OPS_PACKET"  # read-only, safe to repeat
aws --profile airlake --region ap-southeast-2 logs filter-log-events \
  --log-group-name /aws/lambda/airlake-live-20261114_9e2c740b-refresh \
  --filter-pattern '"AirLakeRefresh"' --limit 30
```

Existing poller logs retain accepted/replayed/rejected counts; EMF reports actual
observation age. Latest-state records retain `observed_at` and `ingested_at`.
OPS records retain last poll, controller success, watermark, job/query IDs and
promotion state. The controller emits RefreshComplete to the existing EMF
namespace. Lambda's standard Errors/Throttles and Glue's run status remain useful;
no paid alarms, dashboards or new monitoring service are created. Khuê can collect
these with her existing CloudWatch work without touching Telegram staging.

The three HTTP checks verify latest JSON plus current-hourly JSON/CSV against the
promoted pointer. They create private `latest-api.json` and `latest-api.html`; open
that HTML beside the **unchanged five-panel historical Superset dashboard**.
It labels timestamps, age, stale/missing data and no instantaneous AQI. No new
Superset/Athena chart queries are submitted. This separate API-backed snapshot
view is manually refreshed, not a claim that Superset SQL follows dynamic databases.
Historical JSON/CSV, stations, station/time filters and IAM/SigV4 remain supported.
Hourly AQI is simplified MVP AQI, **not official VN_AQI**. No city-wide coverage or
redistribution clearance is claimed. Telegram compatibility stays at the existing
Gold event boundary; no SNS, subscription or notification changes occur.

Acceptance: two scheduled polling opportunities; source/count/dedup evidence;
one Glue job only if the immutable boundary changes; five new scoped tables;
independent pinned Athena row agreement; API version agrees with the accepted
pointer; truthful latest timestamps; final disabled schedule, both concurrencies
zero and no active/uncertain leases. If there is no new input, retain the prior
export and report the skipped Glue/Athena steps explicitly.

## Stop, recovery and rollback — NOT EXECUTED

```sh
ops stop --packet "$AIRLAKE_OPS_PACKET" --execute \
  --confirm-plan-sha256 "$AIRLAKE_OPS_PLAN_SHA256"
ops status --packet "$AIRLAKE_OPS_PACKET"
# Only after checking no active/uncertain lease or Glue run, with approval:
ops rollback --packet "$AIRLAKE_OPS_PACKET" --execute \
  --confirm-plan-sha256 "$AIRLAKE_OPS_PLAN_SHA256"
```

Stop attempts schedule disabling and both concurrency-zero writes independently.
`DISABLED_RECONCILIATION_REQUIRED` means admission is stopped but an invocation or
uncertain lease still needs inspection. Wait for bounded execution to finish and
read state; never clear locks, delete evidence or reset counters automatically.
For a still-running scoped Glue job, use separately approved
`aws glue batch-stop-job-run --job-name airlake-live-20261114_9e2c740b-gold --job-run-ids <recorded-run-id>`.
Stopping is best effort; retain partial output.

For `STARTING_GLUE`, inspect job runs/arguments before any retry. For
`STARTING_QUERY`, reconcile the request token/query evidence; absent query ID is
not proof of no query. For `PROMOTING`, compare current pointer hash and window.
If a recorded run/query is pending, subsequent normal ticks advance it rather than
start another. A crashed controller lock requires an explicit, evidence-backed
operator recovery approval. Nothing here deletes or replays ambiguous work.

Rollback conditionally restores the previous validated pointer, original job
configuration and hash-verified API ZIP. It leaves polling/controller disabled,
keeps the schedule targeted at the stopped controller and retains all resources,
tables, exports and records. It cannot undo charges or provide multi-service
atomic rollback. Do not use the packet against later operator changes.

Costs: up to twelve admitted controller ticks, two remaining polls, one two-G.1X
Glue run with ten-minute timeout (nominally twenty DPU-minutes plus billing/startup
overhead), one Athena query, three API reads, S3/DynamoDB operations and CloudWatch
logs. Existing secret/storage charges continue. No measured bill or guaranteed
dollar cap is claimed. Do not leave indefinite scheduling enabled.

## Local verification

```sh
uv run --frozen --offline --with 'botocore[crt]==1.43.115' python -B -m unittest discover -s tests -p 'test_live_operations.py'
ruff check live_data tests/test_live_operations.py tests/live_glue_integration.py
ruff format --check live_data tests/test_live_operations.py tests/live_glue_integration.py
sh -n scripts/live_operations_demo.sh
# Optional NEW versioned namespace adapter test, entirely local/synthetic:
export PYSPARK_PYTHON="$(command -v python)" PYSPARK_DRIVER_PYTHON="$(command -v python)"
python -B -m tests.live_glue_integration --runtime-jar "$AIRLAKE_ICEBERG_RUNTIME_JAR" \
  --refresh-digest bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
```

Spark must use Java 17, a fresh process and an external temporary warehouse. Clear
inherited AWS/Spark overrides for that local test. No AWS integration PASS is
inferred from fixtures. Original Gold source and historical evidence stay unchanged.
