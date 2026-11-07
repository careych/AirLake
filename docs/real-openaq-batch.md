# Bounded real OpenAQ integration

The 2026-11-06 local acceptance moved 200 existing historical OpenAQ PM2.5
measurements through the real Kafka producer, Bronze stream, Silver batch and
Gold batch. The result is 200 Bronze rows, 200 Silver rows, one dimension and
23 UTC hourly facts. The unchanged rerun preserved all five tables' rows and
snapshot-ID sets. This is simplified hourly MVP AQI, not official VN_AQI.

[Acceptance evidence](../tests/evidence/real_openaq_2026-11-06.json) retains source
checksums, CSV row provenance, observed counts, schemas, aggregates, snapshots,
code hashes and the separate 48-case Gold regression result. Historical Gold
and three-row cloud acceptance records are unchanged.
The later [cloud wiring checks](../tests/evidence/cloud_wiring_readiness.json)
cover the Kafka classpath and retained local checkpoint changes separately;
the November 6 real-data record is not rewritten to describe those changes.

## Source and local execution

The selector walks station `4946812` filenames in lexicographic order, then
physical CSV data-row order. It accepts the first compatible PM2.5 rows, up to
the requested cap (1–1,000). It validates the existing nine-field Bronze schema,
actual sensor `13502151`, nonblank station name, finite coordinates, explicit
`µg/m³`, finite nonnegative concentration and a timezone-bearing timestamp.
It rejects incompatible PM2.5 rows and records reasons. Other parameters are
counted separately as exclusions. It does not manufacture or normalize values.

The accepted 200 rows came from July 3/4, 2025 files: 491 source rows scanned,
291 non-PM2.5 exclusions, zero rejected PM2.5 rows. Their UTC range is
`2025-07-03T15:40:00Z` through `2025-07-04T13:50:00Z`; concentration sum is
`5746.71`. The selection hash and exact physical row numbers are in the evidence.
The selection hash covers canonical newline-joined JSON row content, not the
generated gzip bytes; its exact encoding is recorded in the JSON. Original
source-file checksums cover compressed file bytes.

The frozen gzip fixture lives outside the repository under the producer's
existing `data/raw/*/*/*/*.csv.gz` layout. The original `real_producer.py` is
executed from that directory with an explicit new topic and row cap. An
independent consumer checks offsets, keys and exact payloads. Bronze uses its
existing transformation with an available-now trigger and a private checkpoint.
SQL epoch milliseconds verify Kafka timestamps; the isolated process also uses
UTC so Python timestamp collection cannot inherit the Mac's timezone.

Prerequisites are the existing project environment, Java 17, cached Iceberg and
Spark Kafka connector JARs, and the single broker at `localhost:19092`. No Maven
downloads occur. Inspect Docker containers and port ownership before starting
the project's Compose service; do not replace another broker or delete volumes.

```sh
# Offline selection only: no Kafka, Spark or AWS calls.
.venv/bin/python -B scripts/real_batch.py --limit 200

# Deliberate LOCAL writes: one new topic and an external temporary warehouse.
source scripts/use-java17.sh
.venv/bin/python -B scripts/real_batch.py --execute-local --limit 200
```

Each invocation prints its retained run directory. A new execution creates a new
topic; do not republish into an earlier topic to test an unchanged rerun. The
runner restarts its Bronze query with the same checkpoint, then reruns Silver
and Gold without publishing again. This checks query restart within one Spark
session; process-crash recovery, long-running streaming and Kafka HA remain
unverified. Failed attempts, topics, logs, checkpoints and warehouses are retained
for inspection, never automatically deleted. The ten-minute deadline terminates
only the launcher's own worker process group.

## Catalog configuration

`scripts/pipeline_config.py` supplies explicit catalog, warehouse, namespace and
table identifiers. Default HadoopCatalog behavior and table names remain local.
Only Gold namespace DDL, session creation and entrypoint binding changed; Gold's
numeric validation, metadata repair, preflight, AQI and MERGE functions remain
identical to the published baseline. Silver rejection/key policies are unchanged.

All three entrypoints accept `--plan` to print configuration without Spark/AWS
execution. Glue mode requires the reviewed account, Sydney region, disposable
`airlake_it2_` namespaces, an `integration-real/` warehouse, explicit write opt-in
and matching account confirmation. Verified Iceberg 1.12.0 runtime/AWS-bundle
checksums and required classes are checked before Spark. The same accepted
Spark 3.5.9/Scala 2.12 Kafka connector, token provider, Kafka client 3.4.1,
commons-pool2 and JSR-305 bytes are verified as well. JSR-305 is already bundled
identically in Spark and is loaded once. No Hadoop S3A JAR is introduced.
Remote Spark overrides,
static credential variables and redirected endpoints are rejected. A fresh local
session verifies Java STS identity before pipeline SQL. These configuration paths passed the bounded cloud run described below;
they do not establish production cloud fault tolerance.

## Accepted AWS run: 2026-11-07

[Cloud evidence](../tests/evidence/real_openaq_cloud_2026-11-07.json) records one
approved execution and one unchanged rerun: Bronze 200, Silver 200, breakpoints 7,
dimension 1, hourly facts 23. Source rows/metadata, all five schemas and every Gold
hourly value matched local acceptance. Both reviewed Athena queries succeeded;
the raw sum was `5746.709999999996`, within `1e-9` of `5746.71`, and all 23
hourly records matched exactly. This raw sum is not a sum of hourly averages.
Rows, snapshot-ID sets, metadata pointers and checkpoint files were unchanged
on rerun. Iceberg stores hourly timestamps as `timestamptz`; Athena returns
`timestamp with time zone` and explicit UTC values.

One read-only S3 timeout and temporary verifier timezone assumptions were
resolved and retained as evidence. No cloud write failed or was retried, and
neither Athena query was repeated. The record includes all five snapshots,
25 scoped lake-object keys (94,856 bytes), Glue version IDs and four actual
Athena result objects. Cleanup remains separately approval-gated.

## Historical command template — resources already occupied

[The exact saved plan](plans/real_openaq_2026-11-06.json) fixes account
`701364614078`, region `ap-southeast-2`, three new Glue databases, five tables,
the new S3 prefix, frozen dataset hash and two Athena SQL statements.

Before requesting write approval, perform a separately authorized fresh inventory:
verify CLI/Java identity, bucket ownership/region, all three namespaces absent,
the complete run prefix empty, and workgroup `airlake` still enforcing engine 3
and its result bucket. Only Glue `EntityNotFoundException` proves absence;
AccessDenied or incomplete listing is a blocker. Verify the topic still has
one partition with offsets `[0,200)` and the source selection hash is unchanged.
That preflight passed before the accepted write. This is historical evidence,
not a fresh reservation or permission to reuse the occupied names.

The following template describes the accepted execution. **Do not execute it
against these occupied resources.** For a new test, select a fresh topic, run ID,
three namespaces and S3 prefix, then save/review a new plan and obtain approval.
A new machine must have the authorized source archive and verify its checksums;
this repository does not distribute or download that archive. Execution stops after the first command failure;
never automatically retry an ambiguous write. Set `AIRLAKE_REPO` to the reviewed
repository first. Existing AWS profiles must remain unchanged.

```bash
(
  set -euo pipefail
  : "${AIRLAKE_REPO:?Set the reviewed repository path}"
  source "$AIRLAKE_REPO/scripts/use-java17.sh"
  export AWS_CONFIG_FILE="$HOME/.aws/config"
  export AWS_SHARED_CREDENTIALS_FILE="$HOME/.aws/credentials"
  export AWS_LOGIN_CACHE_DIRECTORY="$HOME/.aws/login/cache"
  export AIRLAKE_ICEBERG_RUNTIME_JAR="$HOME/.ivy2/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.12.0.jar"
  export AIRLAKE_ICEBERG_AWS_BUNDLE_JAR="$HOME/.local/share/airlake/dependencies/iceberg/1.12.0/iceberg-aws-bundle-1.12.0.jar"
  AIRLAKE_CLOUD_WORKDIR="$HOME/.local/share/airlake/runs/airlake-it2-cloud-20261106_f3b3b3e0677d4a6790e4a54faf68e4aa"
  mkdir -p -m 700 "$AIRLAKE_CLOUD_WORKDIR"
  cd -P "$AIRLAKE_CLOUD_WORKDIR"
  AIRLAKE_CATALOG_ARGS=(
    --mode glue --catalog cloudreal
    --warehouse s3://airlake-701364614078-lake/integration-real/20261106_f3b3b3e0677d4a6790e4a54faf68e4aa/warehouse/
    --bronze-namespace airlake_it2_20261106_f3b3b3e0677d4a6790e4a54faf68e4aa_bronze
    --silver-namespace airlake_it2_20261106_f3b3b3e0677d4a6790e4a54faf68e4aa_silver
    --gold-namespace airlake_it2_20261106_f3b3b3e0677d4a6790e4a54faf68e4aa_gold
    --account 701364614078 --region ap-southeast-2 --profile airlake-spark
    --allow-cloud-write --confirm-account 701364614078
  )
  "$AIRLAKE_REPO/.venv/bin/python" -B "$AIRLAKE_REPO/scripts/bronze_local.py" "${AIRLAKE_CATALOG_ARGS[@]}" --bootstrap-servers localhost:19092 --topic airlake_it2_20261106_f3b3b3e0677d4a6790e4a54faf68e4aa --available-now --checkpoint "$AIRLAKE_CLOUD_WORKDIR/bronze-checkpoint"
  "$AIRLAKE_REPO/.venv/bin/python" -B "$AIRLAKE_REPO/scripts/silver_batch.py" "${AIRLAKE_CATALOG_ARGS[@]}"
  "$AIRLAKE_REPO/.venv/bin/python" -B "$AIRLAKE_REPO/scripts/gold_batch.py" "${AIRLAKE_CATALOG_ARGS[@]}"
)
```

The work directory is fixed and retained for this run. Its `bronze-checkpoint`
child is local, not S3: Iceberg S3FileIO does not implement Spark's checkpoint
filesystem. Before Spark starts, the guard rejects outside paths, checkpoint or
binding symlinks, nested symlinks, unrecognized existing state and incompatible
broker/topic/table/query bindings. Matching reruns preserve the same checkpoint
and binding; never delete them, reset offsets or regenerate the work directory
after a partial failure. Retain both until investigation and approved reruns
finish. This local Mac checkpoint is only for the bounded probe and provides no
production cloud fault tolerance or cross-machine recovery.

The repeatable offline wiring test reads the existing accepted Kafka topic without
publishing or running Silver/Gold again. A fresh JVM loads the exact cloud JAR
set; AWS identity is explicitly mocked and SQL uses a disposable local Hadoop
catalog. Empty private credential/config files prevent real login resolution.
It verifies 200 Bronze rows, a zero-input checkpoint restart and unchanged rows/
snapshots. The checkpoint fixture is retained outside the repository.

```sh
source scripts/use-java17.sh
export AIRLAKE_ICEBERG_RUNTIME_JAR="$HOME/.ivy2/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.12.0.jar"
export AIRLAKE_ICEBERG_AWS_BUNDLE_JAR="$HOME/.local/share/airlake/dependencies/iceberg/1.12.0/iceberg-aws-bundle-1.12.0.jar"
AIRLAKE_RUN_CLOUD_WIRING_TESTS=1 .venv/bin/python -B tests/cloud_wiring_integration.py
```

Exact runtime prerequisites are Python 3.12, PySpark 3.5.9, Scala 2.12, Java 17,
Iceberg 1.12.0 runtime plus AWS bundle, and the cached Kafka artifacts whose
SHA-256 values are fixed in the saved plan and `pipeline_config.KAFKA_JARS`.
Obtain the Kafka connector and transitives once in an approved setup environment;
cloud execution never uses `--packages`. JARs stay outside Git. See the
[AWS smoke dependency guide](aws-iceberg-smoke.md#dependencies-and-provenance)
for bundle provenance, GPG verification and the required process profile.
The selected bundle does not contain the Signin provider; installing its filename
alone is insufficient. The CLI profile is `airlake`, Java uses `airlake-spark`,
and both must resolve account `701364614078` in `ap-southeast-2` before writes.

The SDK uses the existing `airlake-spark` credential_process profile. Never print
its credential output. The configured EC2 Spark policy in teammate Terraform
omits Glue CreateDatabase; it is not proof that a namespace-creation run is
authorized. Fresh effective IAM/SCP checks are required for the actual caller.

After successful writes, validate actual rows, types, source timestamps, units,
station metadata and the 23 local reference aggregates. Compare float values
within `1e-9`, IDs/timestamps/AQI exactly, and exclude environment-specific
Silver `ingested_at` from cross-environment equality. Inspect each table's
metadata/snapshots directly; metadata record counts alone are insufficient.
If an unchanged cloud rerun is approved, preserve rows and snapshot-ID sets
within that cloud run. Local and cloud snapshot IDs need not match.

The plan contains at most two Athena SELECTs: count/sum/key reconciliation and
the exact hourly facts joined to station metadata. Use workgroup `airlake`;
the enforced output location is `s3://airlake-701364614078-athena-results/`.
Record actual query IDs, result-object keys, bytes scanned and snapshot IDs.
Do not execute the SQL or infer result paths before receiving those responses.

There is no AWS compute provisioning. S3 requests/storage, Glue operations/catalog storage and Athena scan minima/results may
incur charges. The 10-GiB per-query cutoff is not a spending cap. Current regional
prices and permissions have not been refreshed. A partially completed run may
retain namespace/table registrations, Iceberg files and checkpoints: separate
Iceberg commits and Athena operations do not roll back atomically.

Cleanup needs separate approval. Inventory only the exact three databases,
five tables, run prefix and actual new query-result keys. Delete registrations
and associated Glue table versions as applicable, then only enumerated data,
manifest and metadata objects under the run prefix. Review versioning
and delete markers first. Never clear a bucket or touch production tables or
the original `integration-smoke/` resources. No cleanup was executed.

## Gold-to-alert boundary (proposal, not implemented)

Use `location_id`, dimension `name`, UTC `hour_start`, `pm25_avg`, explicit units
and `aqi`, plus the prior hour only if an approved rule requires it. No threshold
or cooldown is approved here; the specification's AQI 150 example is not a rule.

A minimal event carries `schema_version`, stable `event_id` (station/hour/rule
version), `location_id`, `hour_start_utc`, `pm25_avg`, `units`, `aqi`,
`aqi_method=simplified_hourly_mean_mvp`, `rule_version` and
`source_gold_snapshot_id`. An update/retraction policy is still needed for
corrected hours. A publisher must persist deduplication/delivery state before
claiming exactly-once notifications.

The inspected `origin/thinh:bot_lambda/src/lambda_function.py` at `bc3d763`
reads SNS `Message` from JSON inside SQS records and broadcasts text to chat IDs.
For compatibility, publish human-readable alert text as SNS `Message` and carry
structured event metadata in message attributes, preserving the SNS envelope
through SQS. The current consumer ignores those attributes. Confirm the actual
application topic/queue/function with Thịnh; the operations-alert topic is not
proof of this route. That source still swallows delivery failures, so SQS retry,
deduplication and per-station subscription behavior need owner review. Its
deployed version was not verified, and no notification was published.
