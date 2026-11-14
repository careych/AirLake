# Bounded AWS-hosted final demo

## Verified result — 2026-11-14

**PASS:** one real AWS Glue 5.0 job computed Gold from 559 pinned Silver rows:
7 breakpoints, 3 locations and 72 hourly facts. All facts matched the retained
reference within absolute tolerance `1e-12` (maximum mean error
`2.6645352591003757e-15`). The unchanged rerun inside that job preserved all
three output snapshots and row digests. The five original table snapshots
were independently checked unchanged.

One separately approved Athena SELECT returned the same 72 facts, with result
reuse disabled and 1,598 bytes scanned. The new IAM-authenticated Lambda API
passed eight HTTP checks: anonymous access rejected, signed health, full JSON,
CSV, stations and each station filter accepted. No additional Athena queries
were made by the API. See the [sanitized execution evidence](../tests/evidence/aws_final_demo_2026-11-14.json)
for exact JobRunId, query ID, snapshots, schemas, code hashes and cleanup inventory.
The [preparation record](../tests/evidence/aws_final_demo_preparation_2026-11-14.json)
is a historical checkpoint from before write approval, not the current status.

Function URL: `https://whgdwrmccvrmwipegu3jkvweii0vwaiy.lambda-url.ap-southeast-2.on.aws/`.
Use SigV4 service `lambda`, region `ap-southeast-2`, with scoped IAM invocation
permissions. Opening it unsigned in a browser intentionally returns 403.
Paths are `/api/v1/health`, `/api/v1/gold/hourly` (JSON/CSV, station/time filters,
bounded pagination), and `/api/v1/stations`. Observation times are UTC; the
bounded data cover November 7–8, 2026. They are not a live feed.

The code and commands below document the approved execution procedure.
**Do not repeat them against the retained run**: its resources already exist.
The preparer and receipts reject reuse. A future run needs a fresh reviewed
namespace, packet and approval. No automatic cleanup was performed.

## One computation, one isolated scope

The job reads the retained Bronze snapshot `82685161818144704` for station
metadata and **Silver snapshot `5024076747683534255`** for 559 cleaned PM2.5
measurements. Both are from the accepted `20261108_37179997ad2b42959f4387637e003ab8`
databases. It does not read the old Gold table to produce output.

`final_demo/glue_job.py` loads the original `scripts/gold_batch.py` and
`scripts/silver_batch.py` bytes, verifies their hashes, checks Silver key
uniqueness, and reuses Gold dimension construction, finite/unit validation,
UTC hourly averaging, AQI interpolation, preflight and unchanged MERGEs.
Silver is the already-cleaned input; this is **Gold ETL on AWS**, not a new
Bronze/Silver ingestion run. The reference's 72 Gold facts are assertion data,
never the Spark transformation's input.

Expected output: one new Glue database, three Iceberg v2/Parquet tables
(`aqi_breakpoints`: 7, `dim_location`: 3, `pm25_aqi_hourly`: 72), a verified JSON
export and evidence under one new `final-demo/<run>/` prefix. The job compares
all values and dimension joins before target DDL, then validates actual target
rows, duplicates/orphans and one unchanged rerun's digests/snapshots. Float
tolerance is absolute `1e-12`. AQI remains simplified hourly MVP, not VN_AQI.

Glue 5.0 supplies Spark 3.5.4, Scala 2.12, Java 17 and Python 3.11. The local
baseline uses Spark 3.5.9; this bounded run demonstrated numerical equivalence
on the documented cloud runtime, not compatibility for every future dataset.
Use verified Iceberg 1.12.0 runtime and AWS-bundle JARs with `--extra-jars` and
`--user-jars-first=true`. Do **not** enable built-in `--datalake-formats=iceberg`
(Glue 5.0 bundles Iceberg 1.7.1). No Maven/PyPI resolution happens in the job.
See [AWS custom Iceberg guidance](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)
and [Glue 5.0 runtime](https://docs.aws.amazon.com/glue/latest/dg/migrating-version-50.html).

## Prepare (local writes only)

From this branch, set `DEMO_PYTHON` to the existing project Python, `EVIDENCE` to
the retained cloud acceptance JSON, and the two JAR variables to their verified
local paths. The source JSON SHA-256 is enforced by the merged Clean Data API.
Choose a new private `PACKET` directory outside any Git checkout. Never commit
the packet, raw reference, JARs, runtime receipts or exports.

```sh
"$DEMO_PYTHON" -B -m final_demo.prepare \
  --output "$PACKET" --evidence "$EVIDENCE" \
  --runtime-jar "$AIRLAKE_ICEBERG_RUNTIME_JAR" \
  --aws-bundle "$AIRLAKE_ICEBERG_AWS_BUNDLE_JAR"
PLAN_SHA=$(cat "$PACKET/plan.sha256")
demo_sdk() {
  uv run --offline --no-project --python "$DEMO_PYTHON" \
    --with 'boto3==1.43.115' --with 'botocore[crt]==1.43.115' python -B "$@"
}
demo_sdk -m final_demo.control preflight --packet "$PACKET"
```

Preparation creates local IAM/trust templates, fixed Glue job definition,
source ZIPs and hashed plan. Profile is `airlake`, account `701364614078`, region
`ap-southeast-2`. Preflight treats AccessDenied as failure, verifies source
snapshots/prefixes and bucket public-access blocking, and requires unused
database/job/role/function names plus a completely empty S3 run prefix.
Successful reads do not establish effective write permissions or SCP allowance.

Glue role permissions: read only the two source table prefixes and new
artifacts; write only new warehouse/export/evidence/temp paths; delete only new
warehouse/temp objects; catalog writes only the new database; log writes only
the new job log prefix. No access to original Gold is needed. No KMS permissions
are added for the observed AES256 sources; a KMS source fails this proposal.
Lake Formation registration also stops preflight pending separately reviewed
grants. The operator needs scoped `iam:PassRole` and creation permissions.

## First approval: create and run ONE Glue job — executed once

Approve the exact packet SHA, one Glue execution role/inline policy, six S3
artifacts, one Glue job, its isolated database/three tables/export/evidence and
job log groups. Settings are **two G.1X workers, STANDARD, ten-minute timeout,
zero retries, one concurrent run, bookmarks disabled**. No scheduler is created.
This approval does not cover Lambda deployment or Athena SELECTs.

```sh
demo_sdk -m final_demo.control deploy-glue --packet "$PACKET" \
  --execute --confirm-plan-sha256 "$PLAN_SHA"
demo_sdk -m final_demo.control run-glue --packet "$PACKET" \
  --execute --confirm-plan-sha256 "$PLAN_SHA"
demo_sdk -m final_demo.control watch --packet "$PACKET"
```

Every write is recorded as uncertain before submission; no failed write is
retried. After deployment, `run-glue` repeats source/empty-output checks and
compares the actual job definition before allowing the initial run. Existing
job attempts or receipts prevent another run. IAM propagation can still cause
failure; stop and inspect instead of rerunning creation commands.

`job-result.json` retains JobRunId/state, worker/version/time metadata, exact job
parameters, validation, snapshots and CloudWatch group/stream references. Full
logs remain in CloudWatch; no credential-bearing log dump is published.
An unchanged rerun inside the same job is not multi-table atomic rollback.
Failures can leave partial tables/files: preserve them for investigation.

Emergency stop of this job only, after approval — NOT EXECUTED:

```sh
aws --profile airlake --region ap-southeast-2 glue batch-stop-job-run \
  --job-name "$FINAL_GLUE_JOB" --job-run-ids "$FINAL_JOB_RUN_ID"
```

## Separate one-SELECT approval — executed once

```sh
demo_sdk -m final_demo.athena_check --packet "$PACKET" --execute \
  --confirm-plan-sha256 "$PLAN_SHA" --approve-selects 1
```

This SELECT joins only the new fact/dimension snapshots recorded by the
successful Glue run, bounds the three locations and historical 25-hour window,
and limits results to 73. All 72 UTC keys, names, AQIs and means must match the
reference. The receipt retains query ID, scan bytes and result-object location.
No resubmission, cancellation or automatic cleanup occurs after uncertainty.

## Separate private API approval — deployed and verified

Only after Glue passes, create a new Python 3.12/arm64 Lambda (128 MB, timeout
10 seconds, reserved concurrency 1), its dedicated role and an **AWS_IAM**
Function URL. The role can read exactly the new JSON export and write its own
logs. It cannot query Athena, write S3, or touch existing APIs/bots. The URL is
internet-routable but not anonymously readable; no public permission or CORS
configuration is created. Provider-specific redistribution remains uncleared.

```sh
demo_sdk -m final_demo.control deploy-api --packet "$PACKET" \
  --execute --confirm-plan-sha256 "$PLAN_SHA"
demo_sdk -m final_demo.http_smoke --packet "$PACKET" --execute \
  --confirm-plan-sha256 "$PLAN_SHA"
```

The adapter reuses the merged Dataset validation/filter/pagination. It returns
JSON/CSV and actual station metadata, distinguishes API health from Athena,
and labels historical data. It verifies S3 export SHA-256 at cold start, then
serves that immutable AWS-computed export. It is **not continuously refreshing**.
Malformed input rejects; missing/corrupt export returns 503 without SDK details.

The prepared `api-invoker-policy.json` scopes both `lambda:InvokeFunctionUrl`
and `lambda:InvokeFunction` to the new function with URL/IAM conditions. Do not
attach broad policies to workloads or change the existing operator role
automatically. [AWS IAM URL authorization](https://docs.aws.amazon.com/lambda/latest/dg/urls-auth.html)
requires both actions. The operator must already have them or request a scoped
grant separately.

HTTP smoke performs eight requests, within a hard ceiling of ten: unsigned
health must return 403; signed health, complete JSON/CSV, stations and three
individual filters must pass. SigV4 credentials stay in process; no token or
authorization header is logged. Evidence retains function URL, requests and
snapshot provenance, not source data or credentials. No Athena queries occur.

To disable this new API after approval, set its reserved concurrency to zero.
There is no existing deployment to roll back: retain the failed new resources.
Cleanup requires a separate reviewed inventory of this database's three tables,
the exact S3 run prefix, new job/roles/policies/log groups and new Lambda URL.
Never expire source snapshots or delete old acceptance resources.

## Cost and next boundary

The configured compute scope is nominally 20 DPU-minutes (2 workers × 10 min)
at the Sydney regional rate, plus S3 artifact/data storage/requests, Glue
catalog operations, CloudWatch Lambda requests/duration and one Athena SELECT. The observed job used
287 DPU-seconds and 143 execution seconds; the query scanned 1,598 bytes. Startup/finalization and billing granularity mean this
is not a guaranteed dollar cap. No bill has been measured. See
[Glue pricing](https://aws.amazon.com/glue/pricing/). No EC2/ASG/ALB or recurring
schedule is added. Retained logs/artifacts can continue to incur storage cost.

Keep the working local Superset UI unless the AWS owner separately validates
the existing host's authenticated charts, drivers, metadata and Athena role.
An ALB healthy target proves neither those charts nor their AWS credentials.

After this manual job passes, reuse the existing incremental producer/outbox
on an approved host to ingest a bounded batch; freeze the resulting Silver
snapshot for the next job. Only after cross-invocation idempotency and owner
approval should an existing-host cron trigger a batch-aware Glue workflow.
**Do not schedule this namespace-bound one-shot job**: it deliberately rejects
an existing output database. Kafka HA and Trino scaling remain deferred.

## Local regression verification

Run without AWS credentials or cloud operations. The tests require the pinned
SDK environment used by `demo_sdk`; the base project environment alone may not
contain boto3/botocore. Use the existing project site-packages for its other
test dependencies:

```sh
PROJECT_SITE=$("$DEMO_PYTHON" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
PYTHONPATH="$PROJECT_SITE" demo_sdk -m unittest discover -s tests -p 'test_*.py' -q
ruff check final_demo tests/test_final_demo.py
ruff format --check final_demo tests/test_final_demo.py
git diff --check
```

The actual run passed 341 Python tests, including 17 focused tests. The historical
48-case Gold Spark suite was not rerun for this change: original Gold/Silver
source bytes are unchanged and hashed in the packet. Cloud runtime validation
is separately evidenced by the successful real Glue execution and Athena read.
The local Superset dashboard remains the demo UI; the new API is AWS-hosted.
Telegram delivery and any scheduler still require their own acceptance.
