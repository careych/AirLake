# One historical Gold-to-Telegram test

The identity helper and command preparation are local tools. No deployment,
Telegram authentication, recipient verification or notification is implied by
passing mocked tests. The commands below are NOT EXECUTED. A separate exact-scope
AWS write/query/publication approval is required before the deployment section.

## Khuê: private identity setup

Use a trusted, unrecorded Terminal with Python 3.12 and locked dependencies, from
the reviewed repository root. Do not enable shell tracing, HTTP debug logging,
screen recording or shared terminal logging. Never paste token/chat identifiers
into chat, Git, CLI arguments or a support ticket. No AWS secret is retrieved by
this helper: the owner supplies the already-held staging token through hidden
input. Confirm privately that it is the token stored in the approved secret.

Choose a NEW private identity directory outside every Git checkout. This command
explicitly approves that location; there is no default location or overwrite.
The parent must be owned by the operator and not writable by other users.

```sh
umask 077
PRIVATE_ROOT="$HOME/.local/share/airlake-staging-owner"
mkdir -p "$PRIVATE_ROOT"
chmod 700 "$PRIVATE_ROOT"
python -B -m scripts.verify_telegram_staging_identity \
  --private-dir "$PRIVATE_ROOT/identity-v1" --approve-private-dir
```

The CLI requires an interactive Terminal. It prompts for the token without echo,
calls `getMe` once, requires `is_bot=true`, username `tmkhuebot`, and numeric ID
equal to the raw token prefix. It calls `getWebhookInfo` once and stops unless
the webhook URL is empty. It never removes a webhook or sends a message.

It displays a fresh `/start <nonce>` as the next action. Ask exactly ONE consenting
person to send that command in a **private DM to @tmkhuebot**, then type `READY`.
The helper makes one `getUpdates(limit=100, timeout=0)` request, without `offset`,
`allowed_updates`, acknowledgement or retry. No response body/identifier/token is
printed. Exactly one matching message from a human private chat is required;
duplicates, groups, bots, mismatched sender, absent/wrong nonce or several matches
fail. If the message is not in that bounded page, stop and investigate privately;
do not advance/drop updates or start another poller automatically.

`identity-v1/identity.json` stores only bot ID, username, chat ID, verification
time and schema/consent metadata: directory 0700, file 0600. The token and nonce
are not saved. Do not display that file. The preparer accepts it for 24 hours;
this is owner-private local evidence, not a cryptographic identity attestation.
After failure, do not share a traceback or API body. A partially written private
directory may remain; do not reuse or delete it automatically.

Telegram method semantics: [getMe](https://core.telegram.org/bots/api#getme),
[getWebhookInfo](https://core.telegram.org/bots/api#getwebhookinfo),
[getUpdates](https://core.telegram.org/bots/api#getupdates).

## Operator identity and trust

The existing deployment operator is NOT a newly created workload role. Use its
existing `airlake` CLI session; do not change AWS credentials or profiles.
Read-only identity discovery:

```sh
aws --profile airlake --region ap-southeast-2 --no-cli-pager \
  sts get-caller-identity --query '{Account:Account,SessionArn:Arn}'
python -B -m alerts.preflight --profile airlake --aws-cli aws \
  > "$PRIVATE_ROOT/fresh-preflight.json"
```

Account must be 701364614078. For an STS `assumed-role/<role>/<session>` identity,
the checker resolves the role's **actual IAM ARN using `iam:GetRole`**, preserving
any IAM path. It fails if that identity cannot be verified. Never put the STS
session ARN in a role trust policy. This read-only procedure requires GetRole;
AccessDenied is a blocker, not permission to invent a root-path role ARN.

The report's `publisher_principal_role_arn` is the operator IAM role proposed for
the reader trust. Khuê must approve it. The reader's trust permits only that
principal to call `sts:AssumeRole`; the operator must also be authorized for
`sts:AssumeRole` on
`arn:aws:iam::701364614078:role/airlake-alert-staging-v1-reader`. The runtime
requests a 900-second session. Boundaries/SCPs and any required trust conditions
must allow that session; do not weaken those controls. No new PowerUserAccess.
See [AWS role switching](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-cli.html).

The proposed consumer role is
`arn:aws:iam::701364614078:role/airlake-alert-staging-v1-consumer`, trusting only
`lambda.amazonaws.com` for `sts:AssumeRole`. The operator needs `iam:PassRole`
only for this exact role with `iam:PassedToService=lambda.amazonaws.com`, plus
reviewed create/configuration permissions for the staging resources. The reader
is not passed to Lambda and has no secret access.
See [scoped PassRole](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_passrole.html).

## Frozen source, secret and resources

The unchanged plan is `docs/plans/athena-alert-staging.json`, SHA-256
`6b2de90f767672515c5f57e06246edc374bddcae111b34937ce7999b711defa1`.

- Gold DB: `airlake_it2_20261106_f3b3b3e0677d4a6790e4a54faf68e4aa_gold`.
- `pm25_aqi_hourly` snapshot `1174385021871906125`;
  `dim_location` snapshot `4323462633891640540`.
- Station 4946812, `[2025-07-03T15:00:00Z, 2025-07-03T16:00:00Z)`.
- Expected PM2.5 average 21.7875 µg/m³; simplified MVP AQI 44.
- TEST-ONLY `fixture-only/test-v1`, threshold 40, age 7200 seconds, reference
  `2025-07-03T16:30:00Z`. No production health threshold/current-alert claim.
- One Athena SELECT, one SNS publish, one consenting private recipient.
  SNS subject and Telegram text explicitly say HISTORICAL DEMONSTRATION.

Reuse the existing plaintext SecretString, NOT JSON, at the metadata-verified ARN:
`arn:aws:secretsmanager:ap-southeast-2:701364614078:secret:airlake/alert-staging-v1/telegram-Hzz53s`.
The owner's earlier `:secret/alert-staging-v1/...` spelling is not a valid ARN.
Do not recreate, overwrite or retrieve the secret in preparation. The consumer's
generated policy uses exactly this ARN. Its runtime alone will need GetSecretValue
after separately approved execution. No customer-managed KMS key was indicated by
the inspected metadata; add no speculative KMS grant. Recheck encryption metadata.

Create only these resources, after fresh absence checks and separate approval:

| Resource | Exact scope |
| --- | --- |
| SNS / SQS / Lambda | `airlake-alert-staging-v1` |
| DLQ | `airlake-alert-staging-v1-dlq` |
| Subscriptions table | `airlake-alert-staging-v1-subscriptions` |
| Ledger table | `airlake-alert-staging-v1-ledger` |
| Roles | `airlake-alert-staging-v1-consumer`, `airlake-alert-staging-v1-reader` |
| Log group | `/aws/lambda/airlake-alert-staging-v1` |
| Athena workgroup | `airlake_alert_staging_v1` |
| Result objects | `s3://airlake-701364614078-athena-results/alert-staging/v1/` |

No production webhook, SNS/SQS/Lambda/subscriber, Gold table or previous acceptance
resource is changed. No API Gateway, EC2, Terraform or scheduled publisher.

## Local preparation — no cloud operations

Set `VERIFIED_WHEELS` to the existing external directory of lock-verified universal
wheels. No downloads are performed by the builder. Choose unused output paths.

```sh
python -B scripts/build_alert_lambda.py \
  --wheels "$VERIFIED_WHEELS" --output "$PRIVATE_ROOT/build-v1"
APPROVED_OPERATOR_ROLE_ARN="$(python -B -c \
  'import json,sys; p=json.load(open(sys.argv[1])); sys.exit("Preflight blocked") if not p["status"].startswith("READ_ONLY_CHECKS_PASS") else print(p["publisher_principal_role_arn"])' \
  "$PRIVATE_ROOT/fresh-preflight.json")"
# Khuê approves this existing IAM role as the reader's trust principal.
python -B -m scripts.prepare_alert_staging \
  --artifact "$PRIVATE_ROOT/build-v1/alert-lambda.zip" \
  --manifest "$PRIVATE_ROOT/build-v1/alert-lambda-manifest.json" \
  --secret-arn arn:aws:secretsmanager:ap-southeast-2:701364614078:secret:airlake/alert-staging-v1/telegram-Hzz53s \
  --identity-file "$PRIVATE_ROOT/identity-v1/identity.json" \
  --publisher-principal "$APPROVED_OPERATOR_ROLE_ARN" \
  --output "$PRIVATE_ROOT/deploy-v1"
```

The numeric identifiers stay inside private files, not CLI arguments. The consumer
policy pins subscription GetItem to the one approved DynamoDB LeadingKey, plus
secret/queue/ledger/log resources. No Scan, shared subscription reads or raw token
environment variable. Generated files and scripts are 0600 under a 0700 directory.
Review them privately; never commit or paste them. Do not run a whole-directory
secret scan over the private identity/deployment directory or print matching lines.

The locally built, lock-verified ZIP has SHA-256
`f76e44f7a039b696dda3f9a2969bb714f7d89930926dae52aa67ae655a00563b`.
This is an offline artifact hash, not a deployed Lambda CodeSha256 observation.
Rebuilding the same reviewed source/lock should reproduce it; stop on a mismatch.

## Exact cloud sequence — NOT EXECUTED; separate approval required

Approval must cover staging creation/configuration, runtime secret access, one
SELECT, one publication, delivery and the final disable actions. Freeze the new
package SHA, generated policy/configuration hashes and approved operator principal
before requesting it. A plan hash alone does not bind the code/private artifacts.
The saved resources may no longer be absent; fresh checks are mandatory.

```sh
export AIRLAKE_APPROVED_PLAN_SHA256=6b2de90f767672515c5f57e06246edc374bddcae111b34937ce7999b711defa1
sh "$PRIVATE_ROOT/deploy-v1/deploy-disabled.sh"
```

This repeats identity, absence, result-prefix emptiness, secret metadata, historical
snapshot and simulator checks immediately before the first write. It creates the
dedicated resources, an immutable delivery-disabled Lambda version/alias and a
disabled mapping. No notification is sent. Stop after any failure; never rerun the
creation script on partial state. Simulation does not prove effective permissions.

Before enabling, independently verify with read-only APIs and private comparisons:

- Actual Lambda CodeSha256 matches the built manifest, Python 3.12, expected
  consumer role, timeout 120s, memory 256 MiB, reserved concurrency 2; alias points
  to the recorded disabled version. Inspect environment values only privately.
- Required test mode, bot ID and recipient match the verified private identity;
  exact staging topic/queue/tables/secret; delivery disabled; partial batches on;
  no raw token or webhook secret, production target or webhook trigger.
- Mapping disabled, batch 1, window 0, `ReportBatchItemFailures`, maximum
  concurrency 2. SNS subscription RawMessageDelivery=false and exact queue policy.
- Queue visibility 720s, dedicated DLQ/maxReceiveCount=5, queue retention 4 days,
  DLQ 14 days; ledger PK delivery_id, TTL expires_at, lease 180s, retention 60 days.
- Exactly one private subscription, correct station and enabled=true. Inspect it
  without printing its key. No production subscriber copying.
- Actual roles/trust/inline and attached policies match reviewed private policies;
  no extra broad grants. Reader workgroup engine 3, enforced result prefix, cutoff
  100 MiB. Gold sources and pinned snapshots unchanged.

Only after those checks and the separately granted exact-scope approval:

```sh
sh "$PRIVATE_ROOT/deploy-v1/enable-test.sh"
# Confirm the enabled immutable alias and mapping settings before publishing.
python -B -m alerts.runtime --execute --confirm-account 701364614078 \
  --plan-sha256 "$AIRLAKE_APPROVED_PLAN_SHA256" --profile airlake \
  --evidence-dir "$PRIVATE_ROOT/historical-test-v1"
```

Run the runtime ONCE. Require query ID/output object, exact retained Gold values
and snapshots, deterministic event ID and SNS message ID. Read only the staging
queue/DLQ counts, mapping state, scoped logs/metrics and specific ledger entries
privately; never consume SQS for inspection or expose chat-bearing ledger keys.
Recipient confirmation plus consumer SENT evidence is required; Lambda success
alone is insufficient. No automatic republish/redrive after ambiguous delivery.

Disable the path after confirmed delivery or immediately on failed/uncertain test
outcome, within the approved scope. Do not retry the runtime. These are intentional
safety-disable actions, not deletion or another send attempt:

```sh
sh "$PRIVATE_ROOT/deploy-v1/disable-test.sh"
```

The script disables only the recorded mapping, restores the recorded immutable
disabled alias version and disables delivery in $LATEST. Verify mapping reaches
Disabled and alias/configuration match; an already in-flight invocation may still
finish. SQS retries are bounded by redrive, not exactly-once delivery. A Telegram
acceptance followed by a failed ledger update can duplicate to the same recipient;
stop/investigate rather than manually redrive. Keep staging disabled afterward.

## Rollback, costs and publication

Rollback is the scoped disable sequence above. If deployment stopped before the
mapping/version files were captured, inspect actual resource IDs privately before
any manual action; do not guess or rerun creation. No automatic cleanup. Retain
queues, ledger, logs, versions and result objects for investigation. Secret,
production bot/webhook and Gold resources are never rollback targets.

Potential costs: one Athena scan capped at 100 MiB (minimum billing can apply),
result storage, SNS/SQS requests and bounded queue retries, Lambda execution,
DynamoDB on-demand operations, CloudWatch logs and ongoing secret storage. No
billing measurement or exact charge claim; no running compute deployment. Cleanup
and query cancellation after timeout require their own approved recovery scope.

Proposed focused PR: **Add private Telegram staging identity setup and one-test
deployment controls**, from `feat/staging-identity-setup` to current main. Include
the helper, preparer/preflight corrections, focused tests and this guide. Do not
change Gold logic, the frozen plan or historical evidence. No publication or
deployment is authorized merely by preparing this PR.

Local verification: 281 Python tests passed, including 12 synthetic identity
cases, 16 preflight/preparation cases and 25 existing runtime cases. Four package
tests passed (included in 281), as did a fresh isolated import of the actual
lock-verified Lambda ZIP with sockets blocked. Ruff lint/format, offline lock,
AST, source-only secret scanning and diff checks passed. No live identity check,
AWS API, secret retrieval, Athena query, SNS publication or Telegram send occurred.
Gold code, frozen plan and historical acceptance evidence were unchanged; the
historical 48-case Spark regression was not rerun for these setup-only changes.
