# AirLake project context and handoff

Status reviewed against local source, the GitHub hygiene audit and retained Gold
acceptance evidence. Use [README.md](README.md) for commands and
[the technical specification](AirLake_DacTa_KyThuat.md) for target requirements and
local data contracts. Embedded starter scripts were replaced with links to their
maintained source files.

## Team and provenance

AirLake is a four-person, seven-week cloud-computing course project. The original
context credited A (Mai Anh) with most of the initial OpenAQ survey, archive/schema
checks and starter scripts. That attribution is retained; it does not assign later
changes or deployment work to someone without evidence.

| Role | Owner named in the original plan | Responsibility |
| --- | --- | --- |
| A | Mai Anh | Data model, Silver/Gold, data quality, technical review |
| B | Minh Khuê | Terraform, IAM, CI/CD, resource lifecycle and cost control |
| C | Khánh Nam | Kafka, producer/poller, Trino/ASG support |
| D | Phúc Thịnh | Superset, Telegram bot, load/fault tests, report and demo |

The planned phases remain core pipeline (weeks 1–3), serving/scale-out (weeks 3–5),
measurement/fault experiments (week 6), then reporting/demo (week 7). These are
planning milestones, not statements that each phase has passed acceptance.

## Scope and status

| Area | Confirmed local implementation | Evidence / boundary |
| --- | --- | --- |
| Dependencies | Python 3.12, PySpark 3.5.9, locked Python packages | pyproject.toml and uv.lock; no dependency download during PR cleanup |
| Producer | CLI-only broker configuration, test-topic default, 2,000-row cap, delivery accounting | Inert-client unit tests; no live producer run during cleanup |
| Bronze | Kafka JSON -> Iceberg append stream | Source reviewed; drops null location IDs and malformed JSON; no lossless raw guarantee or new streaming/restart test |
| Silver | UTC parsing, typed values, deterministic dedup, insert-only MERGE | Source reviewed; finite-value filtering, unit conversion, persistent rejects and sensor_health remain pending |
| Gold | Location dimension, name repair, UTC hourly PM2.5 mean/AQI, preflight and no-op MERGE | 48 integration methods and 9 orchestration methods passed in retained 2026-10-25 evidence |
| Smoke check | Deliberate dependency check with fresh child/temp catalog | Import/launcher checks and an isolated one-row write/read passed during PR preparation |
| AWS/serving | Separate branch work | No AWS deployment acceptance established by this branch |

The exact executed Gold sources are identified by hashes in
[the acceptance record](tests/evidence/gold_dimension_name_repair_2026-10-25.json).
The Git HEAD in that record is the base of a dirty working tree, not a commit
containing those unpublished Gold files. See [tests/README.md](tests/README.md)
for test scope, earlier failures and evidence interpretation. No temporary log is
required to understand the retained record.

## Current local contracts

- Kafka for local development is localhost:19092. Bronze consumes aq_measurements;
  the producer defaults to aq_measurements_test1 unless explicitly directed there.
  The local producer ignores KAFKA_BOOTSTRAP_SERVERS; nam's environment-based
  variant must be reconciled in an owner-reviewed integration change.
- Run pipeline jobs from the repository root. They use ./warehouse and
  ./checkpoints/bronze_aq. Preserve existing checkpoints on normal restarts.
- Bronze keeps parsed measurement fields as strings and adds Kafka receive time,
  but currently drops null location IDs. Raw payload/partition/offset retention
  is not implemented. Do not describe it as preserving every source record.
- Silver key: (location_id, sensors_id, measured_at). Latest kafka_ts wins source
  duplicates; complete-row ordering breaks ties. Existing keys are not overwritten.
  Reprocessing a corrected measurement needs a separate correction policy.
- Silver does not uniformly reject non-finite doubles; Gold independently rejects
  invalid PM2.5 measurements and aggregates. This cleanup does not introduce new
  Silver rejection rules, unit conversions or timestamp formats.
- The project owner approved null-name repair from uniquely valid Bronze metadata,
  including legacy keys absent from Silver and empty Silver input. Identical valid
  repeats are accepted. Invalid/missing/conflicting required metadata or invalid
  target state rejects before initialization/table creation/MERGE. Valid names and
  historical coordinates/type are preserved; blank target names are not auto-repaired.
  No approval date or group-wide approval is asserted.
- Gold uses arithmetic means of available hourly PM2.5 observations, linear interpolation and
  HALF_UP rounding with a finite-value AQI cap of 500. This is an MVP, not official
  VN_AQI/NowCast. Regulatory source/effective dates and completeness need review.
- Breakpoint history and multi-table commits are not a concurrency/atomic rollback
  guarantee. Existing orphan facts are rejected, not repaired by this name policy.

## AWS branch ownership and integration

The 2026-10-24 audit inspected these source revisions in the same GitHub repository:

| Branch | Audited revision | Source present, not proof of deployment |
| --- | --- | --- |
| khue | ed761df | Terraform bootstrap/foundation/compute and Terraform CI |
| nam | 7d4669c | khue ancestry plus monitoring, Kafka/Trino bootstrap, poller |
| thinh | 3ee5975 | Bot/DynamoDB/Lambda source |

The PR-preparation read-only branch-head check still found khue and nam at those
heads, but thinh had advanced to 0cbac093. Its new content was not imported or
reviewed here. Neither branch existence nor commit subjects verify live AWS state.

Keep the local implementation PR separate from those owners' infrastructure/bot
PRs. nam includes khue's history; do not duplicate the Terraform files. Shared
README, .gitignore, producer and dependency changes need deliberate reconciliation.
In particular, nam's audited pyproject adds boto3 while its lock omits it.

The target architecture still includes S3/Glue, self-managed Kafka, Spark/Trino,
Superset, scheduled ingestion/batches and Telegram alerts/subscriptions. Actual
reviewed source differs from the target specification: ap-southeast-2 defaults
rather than the older us-east-1 suggestion, SSM SecureString rather than Secrets
Manager, AES256 S3 configuration rather than explicit KMS, HTTP/plaintext listeners,
and a chat_id-only bot table rather than station_id/chat_id subscriptions. Those
choices and outstanding deployment, permissions, scaling and schema tests belong
with the relevant owners. They are not silently resolved by local cleanup.

## Historical survey, not current health or licensing evidence

The original context reported 37 nearby OpenAQ locations, three active candidates,
API limits of 60 requests/minute, daily archive files through 2026-09-30, and an
estimated 1–1.5 million historical measurements. Those are historical observations
or estimates, not fresh API checks or independently queried table row counts.

| Project station | location_id | PM2.5 sensors_id | Project station_type |
| --- | --- | --- | --- |
| Công viên Nhân Chính | 4946812 | 13502151 | reference |
| Số 1 đường Giải Phóng | 4946813 | 13502154 | reference |
| OceanPark | 6123215 | 14581375 | low_cost |

The survey reported long-stale CO sensors and a roughly 51-hour NO2 age at Giải
Phóng. The original text called that NO2 intermittent, which conflicts with the
shared >24-hour CHET rule. Current health must be calculated from current data per
(location_id, sensors_id, parameter), not copied from this survey. The target
thresholds are <=1 hour TUOI, <=24 hours GIAN_DOAN, otherwise CHET.

Historical metadata reported CC BY 4.0 for AirGradient/OceanPark and no license
value for the two government sources. Keep required attribution, including
AirGradient and Hanoi Air Quality Monitoring Network/OpenAQ, and verify licenses
before redistribution. A missing license value does not grant display or export
permission. No dataset is published in this PR.

## Retained design decisions and pending decisions

The original team chose one cloud (AWS), EventBridge Scheduler instead of core
Airflow, self-managed Kafka, and a Trino-versus-Athena comparison. Kubernetes/EKS,
MSK/Kinesis/Firehose and NAT Gateway were outside the initial scope. These are
project choices, not assertions about current service eligibility or pricing.
Windows developers were directed to WSL; that remains guidance, not runtime-tested
portability. Read the current setup instead of using the former latest-image/9092
starter snippets.

Historical planning proposed free credits, budget alerts at $20/$50/$80, and a
cutback order of WAF, then one query engine, then one Kafka broker. Account
eligibility, actual costs, organization restrictions, region and resource lifecycle
must be confirmed by B; old free-tier/price claims are not a current cost plan.

Outstanding work includes a lossless Bronze/reject contract; isolated Bronze,
producer-to-Kafka and Silver runtime tests; Silver finite-value/unit/correction
rules; sensor_health; regulatory AQI/completeness; breakpoint history policy;
alert thresholds and consecutive-hour semantics; cache/dashboard/subscription
interfaces; infrastructure security/deployment/scaling; and load/fault evidence.

## Publication boundary

A potentially exposed Telegram token was committed in thinh's audited environment
template. Revocation/rotation is unconfirmed until its owner provides confirmation,
even if later source changes remove the value. This is an external security blocker.
Do not copy the template or merge bot work as part of this local PR.

Publish source, contracts, locked dependencies, local configuration and curated
acceptance evidence. Keep credentials, source datasets, warehouses, checkpoints,
Derby state and generated logs private/ignored. Learning notes, the submitted PDF
and image exports remain outside this PR pending owner/archive decisions; retain
their attribution and editable source where available. Nothing is staged or
published automatically by these instructions.
