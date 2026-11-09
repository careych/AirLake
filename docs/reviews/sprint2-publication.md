# Sprint 2 publication review

Proposed PR: `feat/local-lakehouse` → `main`.

Read-only GitHub head verification found `main` at
`f334ba533a3768de5fe8748d121ed5b013d51c44` and the feature branch at
`e54c8e0bfec675dc91ece02334e70c686fc10b02`. The main merge already contains the
original local lakehouse tree; this proposal covers the current uncommitted diff
and explicitly listed new files. No commit, staging, push, merge or PR creation
was performed. Recheck the base before eventual publication.

## Proposed commits and exact paths

Use these commits in order. Checks apply to the combined reviewed working tree;
individual staged commit trees have not been constructed or tested. After approval,
stage only the explicitly reviewed paths for each commit and review the staged
diff. Do not use broad staging commands.

### 1. Add a guarded AWS Iceberg interoperability probe

Publish the standalone opt-in probe, offline safety tests and unchanged three-row acceptance; ignore local JARs.

- `.gitignore`
- `scripts/aws_iceberg_smoke.py`
- `tests/test_aws_iceberg_smoke.py`
- `tests/evidence/aws_iceberg_smoke_2026-10-29.json`

Required checks: Ruff, pure safety tests, import/dry-run isolation and evidence secret review.

### 2. Support bounded OpenAQ replay with local and Glue catalogs

Publish configurable pipeline entrypoints, real-source replay, checkpoint/classpath guards, focused tests and frozen local evidence/plan.

- `scripts/pipeline_config.py`
- `scripts/real_batch.py`
- `scripts/bronze_local.py`
- `scripts/silver_batch.py`
- `scripts/gold_batch.py`
- `tests/test_pipeline_config.py`
- `tests/test_cloud_wiring.py`
- `tests/cloud_wiring_integration.py`
- `tests/test_acceptance_evidence.py`
- `tests/evidence/cloud_wiring_readiness.json`
- `tests/evidence/real_openaq_2026-11-06.json`
- `docs/plans/real_openaq_2026-11-06.json`

Depends on commit 1. Required checks: configuration/checkpoint/source-selection tests, unchanged Gold business ASTs, 48 isolated Gold integration cases, source/evidence hashes and no runtime changes. The Kafka wiring test additionally needs the retained 200-record local topic and is not a default unit test.

### 3. Document real-data cloud acceptance and team handoff

Publish sanitized AWS results and their offline checks, fresh publication validation, setup/recovery guidance and the proposed PR handoff.

- `README.md`
- `AirLake_DacTa_KyThuat.md`
- `AirLake_PROJECT_CONTEXT.md`
- `tests/README.md`
- `docs/aws-iceberg-smoke.md`
- `docs/real-openaq-batch.md`
- `tests/evidence/real_openaq_cloud_2026-11-07.json`
- `tests/test_cloud_acceptance_evidence.py`
- `tests/evidence/publication_validation_2026-11-07.json`
- `docs/reviews/sprint2-publication.md`

Depends on commits 1–2. Required checks: cloud evidence consistency, Markdown links/fences and shell syntax, provenance/secret review, full diff and whitespace checks. Keep every historical evidence file and the approved plan byte-identical.

## Publication decisions and remaining risks

All 26 paths above are proposed publication candidates after the checks below.
The five runtime source files retain the executed cloud hashes; publication
preparation changes documentation, ignore rules and evidence checks only.
The AWS and local execution records are historical results, not claims that a
new checkout has rerun them. The approved plan's old NOT_EXECUTED fields are
intentionally preserved; its existing resources cannot be reused for a new test.

Preserve these local-only files outside the PR; ownership/archive decisions
remain with their authors:

| Path | Decision and reason |
| --- | --- |
| `AirLake_LyThuyet_VaiTroA.md` | Learning notes; preserve attribution, no runtime or acceptance role |
| `AirLake_progress_report1.pdf` | Submitted report; preserve as an owner-managed document |
| `deploy_archi.png` | Diagram export; source/ownership and current accuracy need review |
| `func_archi.jpg` | Diagram export; source/ownership and current accuracy need review |
| `user_use.jpg` | Diagram export; source/ownership and current accuracy need review |

Existing diagnostics, producer, dependency lock, Compose and Gold P0 test files
already on main are reviewed context, not new publication changes. Credentials,
Terraform state/plans/private variables, JARs, private archives, warehouses,
checkpoints, Derby databases, logs and caches remain excluded. No deployment
infrastructure or teammate branch is part of this PR.

The bot owner, Nguyễn Phúc Thịnh, reports that the old token was revoked via
BotFather and a replacement configured on AWS. This is owner-reported revocation/rotation,
not independent verification. Residual historical Git exposure remains a separate
security follow-up; no Git-history purge is claimed. This PR includes neither
Thịnh's branch nor the token, and that separate follow-up does not block this
secret-scanned Data Lakehouse publication.

Source archives are not distributed. Reproducing the same real-data selection
requires authorized copies of the two exact gzip files and matching hashes;
provider licensing/redistribution permissions still need owner confirmation.
Mock tests use synthetic temporary fixtures. Historical Git hash/AST tests
require full clone history. Kafka wiring requires an existing isolated topic;
it is environment-specific and is not recreated by unit discovery.

Gold remains simplified hourly AQI. Production checkpoint recovery, continuous
streaming, broad Silver quality/correction rules, regulatory completeness,
alerts, subscriptions, dashboards and load/fault acceptance are unfinished.
The bounded data test does not validate teammate compute/serving deployments.

## Validation in this preparation

- Pure-Python discovery: 118 passed, including six new cloud-record checks.
- Ruff lint and format: passed after formatting the new evidence test.
- Gold isolated regression: 48 passed, zero failures/errors/skips (124.638 seconds).
- No AWS API calls, replay producer, Docker, Kafka or Terraform execution in this preparation.

Fresh sanitized results belong in
`tests/evidence/publication_validation_2026-11-07.json`. Existing historical JSON
records are unchanged. Raw Spark logs and the full review patch remain outside Git.

## Proposed PR title

Validate a bounded real OpenAQ pipeline on S3, Glue and Athena

## Proposed PR description

Make the existing Bronze/Silver/Gold transformations selectable between the
local Hadoop catalog and an explicitly gated Glue/S3 catalog. Add a deterministic,
bounded replay of real OpenAQ measurements, verified Kafka dependencies and a
retained local checkpoint binding. Preserve Gold numeric, dimension-repair,
pre-write integrity and unchanged-rerun contracts.

The separately approved 200-record cloud run produced 200 Bronze rows, 200 Silver
rows, one dimension and 23 hourly facts. Independent Spark readback and two Athena
SELECTs matched the accepted local results; an unchanged rerun preserved all five
tables' rows, snapshots and metadata pointers. Publish sanitized observations,
source hashes and cleanup inventory alongside setup and recovery instructions.

Validation:

- 118 pure-Python tests passed, including offline cloud-evidence reconciliation.
- 48 isolated Gold integration cases passed with zero failures/errors/skips.
- Ruff lint/format, 18-file AST parsing, Markdown link/fence and shell syntax checks, and tracked/untracked whitespace checks passed.

Boundaries: local Mac execution, one historical station, simplified hourly AQI,
retained local checkpoints and separately approved cleanup. No Terraform,
compute/serving deployment, credentials, datasets or notifications are published.
No production-scale, regulatory AQI or reliable alert-delivery claim is made.
The owner reports token revocation/rotation; residual historical Git exposure
remains a separate follow-up, without a claim that history was purged.
