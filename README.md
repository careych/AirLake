# AirLake

AirLake is a team air-quality lakehouse project using OpenAQ, Kafka, Spark and
Apache Iceberg. This branch contains the local pipeline and an accepted, bounded Spark →
Iceberg/S3 → Glue → Athena integration. Deployment infrastructure and serving
features remain separate team work.

## Implementation and verification

| Component | Local implementation | Verification status |
| --- | --- | --- |
| CSV replay producer | CLI broker/topic selection, row cap, delivery accounting | 2026-11-06 bounded real-data replay: 200 Kafka deliveries verified |
| Bronze | Kafka JSON to an append-only Iceberg stream | Same replay: 200 rows; checkpoint rerun consumed zero new records |
| Silver | Typed, deduplicated, insert-only batch | Same replay: 200 valid/deduplicated rows; unchanged rerun verified |
| Gold | Location dimension and simplified hourly PM2.5 AQI | Retained 2026-10-25 acceptance: 48 integration methods and 9 orchestration methods passed |
| Dependency smoke check | Opt-in Spark child with a disposable warehouse | Retained local fixture check; separate three-row AWS probe passed 2026-10-29 |

The historical Gold [acceptance record](tests/evidence/gold_dimension_name_repair_2026-10-25.json)
is preserved against the original publication commit. Catalog/namespace wiring
has since changed; Gold business functions remain unchanged and all 48 integration
cases passed again. The [real-data guide](docs/real-openaq-batch.md) records the
200-row Kafka-to-Gold acceptance and its 23 UTC hourly facts. The separately
approved cloud run passed on 2026-11-07; its original plan is retained unchanged
as a historical approval artifact, not a reusable resource reservation. Entry points retain local defaults and accept `--plan` for offline catalog
configuration inspection.
[Tests and evidence](tests/README.md) distinguish real Spark/Iceberg results from
mocked orchestration and historical claims. The bounded Kafka-to-Gold run has
separate acceptance with documented single-station and streaming limitations.

```text
Historical CSV.GZ files (local data/raw/)
  -> real_producer.py --topic aq_measurements
  -> local Kafka
  -> scripts/bronze_local.py
  -> local.bronze.aq_measurements
  -> scripts/silver_batch.py
  -> local.silver.aq_measurements
  -> scripts/gold_batch.py
  -> local.gold.dim_location + local.gold.pm25_aqi_hourly
```

## AWS result and handoff

The [2026-11-07 cloud acceptance](tests/evidence/real_openaq_cloud_2026-11-07.json)
verified 200 real OpenAQ measurements from location `4946812`, sensor `13502151`:
Bronze **200**, Silver **200**, dimension **1**, hourly facts **23**. All hourly
means, UTC timestamps and AQI values matched local acceptance; duplicates and
orphans were zero. An unchanged rerun preserved all five tables' rows, snapshots,
metadata pointers and the local checkpoint. Two Athena engine 3 SELECTs independently
confirmed the counts, raw measurement sum (`5746.71`, tolerance `1e-9`) and all
23 hourly results. This is hourly MVP AQI, not official VN_AQI.

The Mac ran Spark; no AWS compute deployment was needed. Glue/S3 configuration
is explicit and gated, while entrypoints still default to HadoopCatalog. The
accepted disposable databases and S3 prefix remain occupied: **do not rerun the
saved cloud commands or delete their resources without separate approval**.
See the [batch guide](docs/real-openaq-batch.md) for setup, scope and recovery,
and the [publication review](docs/reviews/sprint2-publication.md) for the proposed PR.

Still unfinished: production cloud checkpoint recovery, long-running ingestion,
Silver finite-value/unit/correction policies, regulatory AQI/completeness,
sensor health, alert thresholds/delivery/deduplication, dashboards and load/fault
acceptance. The existing SNS/SQS/Lambda/Telegram deployment is not validated by
this batch and was not modified.

## Gold-to-Telegram application slice

The [alert integration guide](docs/gold-to-telegram.md) describes the locally
implemented evaluator, SNS publisher and teammate-derived SQS/Telegram consumer.
Offline tests use retained Gold values, explicit fixture-only rules and mocked
services. This application slice has not been deployed or tested with live SNS,
SQS, DynamoDB or Telegram. Gold tables, calculations and prior acceptance records
are unchanged. The [historical Athena-to-Telegram MVP](docs/athena-to-telegram-mvp.md)
now provides a guarded Athena-to-SNS entrypoint, isolated deployment-command
preparation and offline E2E tests. Its default invocation makes no AWS requests.
Live deployment/query/notification execution and production alert rules still
require separate approval. Renewed read-only preflight verifies the staging
inventory and regional IAM simulation; the separate test-bot secret is still
missing. No continuously deployed alert pipeline is claimed.

## Local setup

Run commands from the repository root. Python 3.12, uv, Java 17 and Docker Compose
v2 are the local prerequisites. Raw data and API credentials are not distributed
with this repository.

```sh
uv sync --frozen
```

This is an explicit dependency installation step. Subsequent commands use the
created `.venv` directly. Python dependencies are locked in `uv.lock`; Iceberg
and Kafka connector JARs are separate JVM dependencies.

On macOS, source the Java helper from bash or zsh:

```sh
source scripts/use-java17.sh
```

The helper respects an existing Java 17 `JAVA_HOME`, otherwise tries a registered
macOS JDK and then Homebrew `openjdk@17`. It supports Intel and Apple Silicon
Homebrew paths and rejects other Java versions. On Linux or WSL, configure a Java
17 `JAVA_HOME` and its `bin` on `PATH` manually; the macOS helper is not supported
there. The retained Gold runtime evidence is from macOS, not a Linux/WSL run.

For offline Iceberg checks and Silver/Gold runs, point to an already installed JAR:

```sh
export AIRLAKE_ICEBERG_RUNTIME_JAR="/path/to/iceberg-spark-runtime-3.5_2.12-1.12.0.jar"
test -f "$AIRLAKE_ICEBERG_RUNTIME_JAR"
```

The project uses PySpark 3.5.9, Scala 2.12 runtime artifacts and Iceberg 1.12.0.
Provision the JARs separately on a new machine. The opt-in smoke/P0 launchers never
resolve Maven packages or download missing dependencies.

## Kafka and topic prerequisites

The following commands start the local development broker and create its topics;
they are operational steps, not audit or test commands. The Compose file binds
Kafka to `127.0.0.1:19092`, with one broker and a named Docker volume. It is not a
three-broker AWS deployment or a fault-tolerance test.

```sh
docker compose -f infra/local/docker-compose.yml up -d
docker compose -f infra/local/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --if-not-exists --topic aq_measurements --partitions 3 --replication-factor 1
docker compose -f infra/local/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --if-not-exists --topic aq_measurements_test1 --partitions 3 --replication-factor 1
```

The producer requires its chosen topic to exist and rejects broker/topic metadata
errors. Broker startup alone does not satisfy that prerequisite. If the broker
is still starting, wait for its metadata command to succeed before replaying data.

## Replay producer

`real_producer.py` reads sorted `data/raw/*/*/*/*.csv.gz` files. Each CSV row is
published unchanged as JSON with key `location_id:sensors_id`.

| Option | Default / behavior |
| --- | --- |
| `--bootstrap-servers` | `localhost:19092` |
| `--topic` | `aq_measurements_test1`; this test topic is not consumed by Bronze |
| `--max-rows` | `2000`; explicit `0` sends all matching source rows |
| positional `speed` | Finite, nonnegative seconds to pause after each 2,000 rows |

Configuration is CLI-only: explicit flags override these defaults.
`KAFKA_BOOTSTRAP_SERVERS` is not read by this local producer. The environment-based
variant on `nam` needs an owner-reviewed integration decision; this branch does
not silently inherit remote broker endpoints.

The default replay goes to the test topic. To feed Bronze deliberately:

```sh
.venv/bin/python real_producer.py --bootstrap-servers localhost:19092 --topic aq_measurements --max-rows 2000
```

The cap bounds submitted rows, not total wall-clock time. A full producer queue is
retried, delivery callbacks are counted, and incomplete delivery returns an error.
Replay starts at the first CSV row each time; no producer resume offset is kept.

## Local pipeline jobs

These commands write the existing local lakehouse. Run them deliberately after
setup; tests use different disposable resources.

1. Start Kafka and create `aq_measurements`.
2. Start Bronze, then replay rows into `aq_measurements`.
3. Run Silver after Bronze has committed the required input.
4. Run Gold after Silver is available. Stop concurrent writers when checking a
   repeatable local result; separate table commits do not provide batch atomicity.

### Bronze streaming

Bronze needs the Spark Kafka connector and its dependencies as well as Iceberg.
This invocation resolves pinned Maven dependencies if they are not cached, so it
requires an approved setup environment with network access (cloud execution instead
uses the exact verified cached JAR set; see the batch guide):

```sh
.venv/bin/spark-submit --master 'local[2]' --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0 scripts/bronze_local.py
```

For an offline run, use `--jars` with preinstalled Iceberg, the Kafka connector and
all of its transitive JARs instead of `--packages`.

Bronze defaults to the local broker/topic above, `./warehouse` and
`./checkpoints/bronze_aq`. `startingOffsets=earliest` applies to a new query; an
existing checkpoint controls restart progress. Keep the checkpoint on ordinary
restarts. Replacing it changes replay behavior and is not a cleanup operation.

Current behavior drops rows whose parsed `location_id` is null, including
malformed JSON. Other fields remain strings and Kafka receive time is added.
It does not retain the original Kafka payload, partition or offset. Lossless raw
preservation/rejection storage is a pending contract, not an implemented guarantee.

### Silver batch

```sh
.venv/bin/spark-submit --master 'local[2]' --jars "$AIRLAKE_ICEBERG_RUNTIME_JAR" scripts/silver_batch.py
```

Silver requires the Bronze table. It parses offset-bearing timestamps with
second precision in a UTC Spark session, rejects missing/invalid IDs, timestamps,
null/non-numeric values and negative values, and preserves nullable parameter/units.
It does not yet uniformly reject non-finite doubles or normalize units. Gold
separately rejects invalid PM2.5 input; stricter Silver rules require dedicated
contract tests before implementation.

The key is `(location_id, sensors_id, measured_at)`. The latest `kafka_ts` wins
source duplicates; ties use a deterministic complete-row ordering. Existing keys
are not overwritten. A correction/update policy is deferred. No new keys means no
MERGE. Reported valid/rejected counts are derived metrics, not independent proof
of source reconciliation. Persistent rejection output and `sensor_health` are not
implemented by this batch.

### Gold batch

```sh
.venv/bin/spark-submit --master 'local[2]' --jars "$AIRLAKE_ICEBERG_RUNTIME_JAR" scripts/gold_batch.py
```

Gold requires Silver and required station metadata in Bronze. It validates inputs,
existing targets and the complete name-repair plan before breakpoint initialization,
table creation or either MERGE. The project owner approved repairing only null
names from uniquely valid metadata, including legacy keys absent from Silver.
Identical valid metadata repeats are accepted. Missing, blank, null, conflicting
or invalid required metadata rejects the batch. Valid names and unrelated
historical fields are preserved; blank target names are rejected rather than repaired.

`pm25_aqi_hourly` has one row per location and UTC hour. The MVP uses the arithmetic
mean of available PM2.5 observations in `µg/m³`, finite/nonnegative validation,
linear interpolation, HALF_UP rounding and an AQI cap of 500. No hourly completeness
threshold is defined. This is **simplified hourly AQI**, not official VN_AQI or
NowCast. The project's breakpoint seed is labeled `QD1459-TCMT-2019`; legal source,
effective-date and regulatory completeness claims still need primary-source review.

Gold initializes the approved seed only after preflight, uses keyed MERGE, and
skips unchanged merges. It does not evaluate alerts, delete historical facts,
convert units or provide atomic multi-table rollback.

## Safe checks

Default unittest discovery runs pure-Python tests only. The producer tests use
inert clients and temporary CSVs; no Kafka messages are sent. Smoke safety tests
exercise import and launch guards without starting Spark.

```sh
.venv/bin/ruff check --no-cache real_producer.py scripts tests
.venv/bin/ruff format --check --no-cache real_producer.py scripts tests
.venv/bin/python -B -m unittest discover -s tests -p 'test_*.py' -v
```

For the optional disposable dependency smoke check, set Java/JAR prerequisites,
then deliberately opt in:

```sh
.venv/bin/python -B scripts/iceberg_smoke_test.py --run
```

It starts a fresh local Spark child, writes/reads one fixture row in a temporary
catalog outside this repository, and removes its temporary warehouse on exit.
Importing the module never starts Spark. It does not check production-local tables.

The full Gold P0 integration suite is separately gated:

```sh
AIRLAKE_RUN_GOLD_P0_INTEGRATION=1 .venv/bin/python -B tests/gold_p0_integration.py
```

Before each Spark test run, review fresh-process/configuration isolation and ensure
all catalogs, warehouses and temporary paths remain disposable. See
[tests/README.md](tests/README.md) for evidence interpretation and prerequisites.

## Manual OpenAQ diagnostics

Only these network diagnostics require an API key:

```sh
if [ ! -e .env ]; then cp .env.example .env; fi
# Edit the local .env, then export its variables in the current shell.
set -a
source .env
set +a
```

`check_openaq.py`, `check_latest.py`, `check_license.py`, `freshness_report.py` and
`probe_locations.py` are manual scripts, not import-safe libraries. Run a chosen
script explicitly with `.venv/bin/python`. Their output is a live observation,
not a stored acceptance result; HTTP/result handling and freshness labels need
separate diagnostic cleanup. Do not commit credentials or downloaded datasets.

## Team architecture and handoff

The [technical specification](AirLake_DacTa_KyThuat.md) contains target AWS/serving
requirements as well as local data contracts. The [project context](AirLake_PROJECT_CONTEXT.md)
records ownership, verified status, historical surveys and pending decisions.

Terraform/monitoring work is on `khue` and `nam`; bot work is on `thinh`.
Their source is not merged into this branch, and their compute/serving deployments were not verified
by this bounded cloud data test. Coordinate shared dependency/README/producer changes
through separate reviews. The bot owner, Nguyễn Phúc Thịnh, reports revoking the old Telegram token via
BotFather and configuring a new token on AWS. This is owner-reported rotation,
not independent verification. Historical Git exposure remains a separate security
follow-up; no history purge is claimed. This PR includes neither `thinh` nor its
credential template.

Runtime directories, private configuration, logs and caches are ignored. Curated
`tests/evidence/` records and dependency lockfiles remain publishable. The PDF,
learning notes and image exports are outside this PR's selected candidates pending
owner/archive decisions; preserve their attribution and source material.

## Monitoring and Kafka Cluster Producer (Nam)

### Freshness Poller & CloudWatch
`freshness_poller.py` polls latest PM2.5 measurements for active Hanoi stations, computes sensor age in hours, and publishes `SensorAgeHours` metrics to AWS CloudWatch (Namespace `AirLake`). Supports standalone CLI (`--dry-run`), periodic cron, and AWS Lambda handler:

```sh
uv run python freshness_poller.py --dry-run
```

### Kafka Producer to AWS Cluster
`real_producer.py` reads compressed historical CSV files from `data/raw/` and publishes measurements to Apache Kafka.

To publish to the AWS EC2 Kafka cluster, set:
```sh
export KAFKA_BOOTSTRAP_SERVERS="10.0.0.11:9092,10.0.1.12:9092,10.0.0.13:9092"
```

The producer writes to `aq_measurements`:
```sh
uv run python real_producer.py --topic aq_measurements
```
