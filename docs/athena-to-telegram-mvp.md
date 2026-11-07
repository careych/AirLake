# One historical Gold observation to a staging Telegram recipient

The callable MVP is implemented and verified offline. It is **not deployed**.
The retained real-data lakehouse acceptance is unchanged. This module does not
modify Gold or establish current air-quality/official VN_AQI compliance.

## Runnable path

From the repository root with Python 3.12 and the locked dependencies:

```sh
python -B -m alerts.runtime
```

The default prints SQL, the plan hash and staging topic without constructing SDK
clients, looking up credentials, starting a query or publishing. The guarded
execution path connects the real Athena adapter to the existing evaluator and
SNS publisher. It requires the exact plan hash, account confirmation and a fresh
external evidence directory. It assumes the dedicated reader/publisher role using
temporary credentials kept only in memory. No static keys/profile changes are made.

[The plan](plans/athena-alert-staging.json) fixes:

- Account `701364614078`, region `ap-southeast-2`, catalog `AwsDataCatalog`.
- Gold database `airlake_it2_20261106_f3b3b3e0677d4a6790e4a54faf68e4aa_gold`.
- Fact `pm25_aqi_hourly`, snapshot `1174385021871906125`.
- Dimension `dim_location`, snapshot `4323462633891640540`.
- Station `4946812`, UTC hour `[2025-07-03T15:00:00Z, 2025-07-03T16:00:00Z)`.
- One expected observation, PM2.5 hourly mean `21.7875 µg/m³`, MVP AQI `44`.
- Explicit routing fixture rule `fixture-only/test-v1`, minimum AQI 40,
  maximum age 7200s, reference time `2025-07-03T16:30:00Z`.

The rule is **TEST-ONLY**, not a health threshold. Both Athena tables are pinned.
The reader also verifies Athena's reported query, context, workgroup and output
prefix before consuming results. A wrong snapshot/query, identity, value, name,
schema or window rejects before publication. Exactly one eligible result is
required. Historical mode never changes ledger lease/TTL time.

SNS uses the existing version-1 deterministic event schema. The subject explicitly
says HISTORICAL DEMONSTRATION. The staging consumer prefixes the delivered text
with `HISTORICAL DEMONSTRATION — NOT A CURRENT AIR-QUALITY WARNING`. There is no
production freshness bypass: the historical reference is allowed only with
explicit one-recipient test configuration and isolated resource names.

The ledger deduplicates unchanged events. Corrections retain the existing content
revision semantics. Remote-send/state-write ambiguity can still duplicate a send;
there is no atomic exactly-once guarantee. A failure exits without automatic
resubmission, retry or cleanup. The execution record preserves query ID/output
object, snapshots, event ID, observation and outcome without recipient IDs or secrets.

## Fresh preflight — currently BLOCKED

```sh
python -B -m alerts.preflight --profile airlake
```

This command intentionally performs **read-only AWS calls**. It checks identity,
exact staging-name absence, complete paginated result-prefix inventory, approved
Gold metadata/snapshot availability, existing workgroup settings, secret-reference
metadata and operator IAM simulation. It does not read secret values, run SQL,
consume SQS, send messages or create resources. Denials/uncertain inventory fail
closed; an arbitrary `InvalidRequestException` is not proof of absence.

The 2026-11-07 Terminal receipt stopped at `ExpiredSession` with account null,
empty absence results and no snapshot results. Agent STS attempts also expired.
Therefore **current resource absence, live snapshot availability and deployment
permissions remain unverified**. Earlier Sprint 3A.2 inventory is historical.
The preserved receipt summary is in the [MVP evidence](../tests/evidence/athena_alert_mvp_2026-11-07.json).
Renew the existing login in Terminal, then run the checker immediately. Do not
request credentials or repeat failed cloud operations automatically.

## Exact proposed staging resources

| Kind | Name/scope |
| --- | --- |
| SNS topic / SQS source queue / Lambda | `airlake-alert-staging-v1` |
| SQS DLQ | `airlake-alert-staging-v1-dlq` |
| Subscription table | `airlake-alert-staging-v1-subscriptions` |
| Delivery ledger | `airlake-alert-staging-v1-ledger` |
| Consumer role | `airlake-alert-staging-v1-consumer` |
| Reader/publisher role | `airlake-alert-staging-v1-reader` |
| Log group | `/aws/lambda/airlake-alert-staging-v1` (7-day retention) |
| Athena workgroup | `airlake_alert_staging_v1` (engine 3, 100 MiB cutoff) |
| Query results | `s3://airlake-701364614078-athena-results/alert-staging/v1/` |
| Owner-provided test bot secret | `airlake/alert-staging-v1/telegram` |

No shared topic, queue, function, subscription table, webhook or Gold object is
modified. There is no staging API Gateway. Use a separate test bot credential;
never copy the deployed token or alter the existing webhook. The secret is a raw
`SecretString` token. An owner supplies its ARN after securely provisioning it;
only the exact name plus Secrets Manager's six-character suffix is accepted.
Do not paste the token into chat, command arguments, source or environment files.

The Lambda receives only the secret ARN and fetches it at invocation. Its role
has `GetSecretValue` for that exact name/suffix scope. Test mode rejects a raw
`TELEGRAM_TOKEN`, shared-resource names and non-staging invocation ARNs. Subscription
selection uses one consistent `GetItem`, not a scan; consumer IAM grants only
`GetItem` on the dedicated subscription table. The owner/deployer conditionally
creates exactly one subscription. The approved recipient must remain eligible.

Keep the reviewed 120s Lambda timeout, 720s visibility and 180s delivery lease.
The existing pre-send guard needs 60s remaining, and SDK/HTTP calls add time;
batch size 1 alone does not verify a shorter deadline. Billing is for actual
execution, not the configured timeout. Reduce these only after live timing and
failure evidence. Mapping: batch 1, window 0, maximum concurrency 2, partial batch
responses enabled; Lambda reserved concurrency 2. Queue retention 4 days, DLQ
14 days, redrive after five receives. Ledger TTL is 60 days, manual redrive horizon
30 days. Expiry does not guarantee immediate deletion. Failed records remain
retryable; offline tests do not simulate the SQS service moving them to its DLQ.

The plan separates reader/publisher and consumer IAM policies, with no
PowerUserAccess. Reader/publisher: exact workgroup, two Glue tables, read-only Gold
prefixes, result-prefix reads/writes, staging SNS publish and `sns#` ledger keys.
Consumer: its queue, one subscription lookup, `telegram#` ledger keys, test secret
and log streams. Deployment additionally needs scoped IAM role/policy/PassRole,
resource creation/configuration and conditional subscription PutItem permissions.
Khuê must confirm the operator principal, role propagation, account quotas, bucket
policies, encryption/KMS, Lake Formation, boundaries and SCPs. Simulation is not
effective service authorization. Customer-managed keys require a separately
reviewed, key-scoped decrypt grant; none is invented here.

## Local package and private deployment preparation

Run the [lock-verifying builder](../scripts/build_alert_lambda.py) with externally
stored verified wheels. Never publish the resulting ZIP or preparation directory.
The dependency set is boto3/botocore 1.43.115 and their locked universal wheels.

```sh
python -B scripts/build_alert_lambda.py \
  --wheels /path/to/verified-wheels --output /tmp/airlake-alert-build-UNUSED
```

The following prepares commands/files **locally**, without executing AWS calls.
The recipient input is a private external JSON file containing only `chat_id` as
a canonical numeric string. Supply the nonsecret bot-secret ARN and exact approved
existing operator role ARN privately; no token value is accepted.

```sh
python -B -m scripts.prepare_alert_staging \
  --artifact /tmp/airlake-alert-build-UNUSED/alert-lambda.zip \
  --manifest /tmp/airlake-alert-build-UNUSED/alert-lambda-manifest.json \
  --secret-arn "$APPROVED_TEST_BOT_SECRET_ARN" \
  --recipient-file "$PRIVATE_RECIPIENT_FILE" \
  --publisher-principal "$APPROVED_OPERATOR_ROLE_ARN" \
  --output /tmp/airlake-alert-deploy-UNUSED
```

Output is private (directory 0700, files 0600). It contains exact resource JSON,
policies, `deploy-disabled.sh`, `enable-test.sh` and an inventory. Generation checks
the artifact/source hashes and refuses overwrite. **Do not run these scripts before
separate scope approval.** They stop on failure, disable automatic CLI write retries,
and never clean up. IAM propagation or another service failure may leave partial
resources; inspect before requesting another attempt.

## Cloud gate and one-test sequence — NOT EXECUTED

After fresh preflight and explicit owner/Khuê approval:

1. Confirm the private test recipient, distinct test bot secret and trusted operator
   role. Review the generated resource files/policies. A secret must already exist;
   secure secret provisioning is a separate owner action if absent.
2. Set `AIRLAKE_APPROVED_PLAN_SHA256` to the reviewed hash and run only
   `sh /tmp/airlake-alert-deploy-UNUSED/deploy-disabled.sh`. The script repeats
   read-only absence/snapshot checks and requires the exact secret ARN. It creates
   only the listed staging resources, a disabled Lambda version and disabled
   SQS mapping. Save returned resource IDs and private configuration. It sends no alert.
3. Inspect mapping/configuration/DLQ/TTL/role grants and artifact `CodeSha256`.
   Only after verification run `enable-test.sh`: publish an enabled immutable
   version, point the staging alias to it, and enable only the new mapping.
4. Run this command **once**, substituting the approved plan hash:

   ```sh
   python -B -m alerts.runtime --execute --confirm-account 701364614078 \
     --plan-sha256 "$AIRLAKE_APPROVED_PLAN_SHA256" --profile airlake-spark \
     --evidence-dir /tmp/airlake-historical-alert-FIRST-UNUSED
   ```

   It submits **one SELECT**, reads one station/hour from the two pinned snapshots,
   and publishes **one SNS message** after validation. No scheduled producer or
   continuous query is created. Do not rerun automatically, even if a response is
   uncertain. Duplicate/retry network tests require their own approved scope.
5. Verify the exact query/output object and event ID from sanitized evidence;
   compare observed PM2.5/AQI to retained values. Inspect the new mapping's state
   and queue/DLQ counts with `GetQueueAttributes` (never ReceiveMessage for audit).
   Inspect the specific publisher/consumer ledger records privately and require
   an owner-confirmed Telegram message to the sole approved recipient. Ledger
   keys contain recipient identifiers: do not publish raw items.
6. Review only `/aws/lambda/airlake-alert-staging-v1` logs and Lambda/SQS metrics
   for invocation errors, duration/timeouts, throttles and DLQ arrivals. CloudWatch
   invocation success is not proof of Telegram delivery; use ledger plus recipient
   confirmation. Retain request IDs and sanitized outcomes, not raw messages,
   secret values or subscriber data. Stop on any failed/ambiguous operation.

Enforced workgroup results cannot be redirected by a client output argument.
Inventory the actual query CSV/metadata objects under the staging result prefix
for later cleanup. A query timeout may leave the query running and billing.

### Rollback and manual cleanup — NOT EXECUTED

Stop the ad-hoc publisher and disable only the newly created mapping. Restore the
staging alias to its recorded initial disabled version (normally `1` for a fresh
function). Retain messages, ledger and logs for investigation; do not retry or
redrive automatically. The generated files retain mapping UUID/version references.
Shared functions and webhooks are not a rollback target.

After a separate cleanup approval, inventory and remove the staging SNS
subscription/topic, mapping, Lambda alias/versions/function, SQS/DLQ, two tables,
two inline policies/roles, workgroup, log group and exact query result objects.
Resolve retention/redrive needs before removing the ledger. Handle the owner bot
secret separately. Never delete buckets, existing Gold metadata/data or old smoke
resources. Multi-service deployment is not an atomic transaction.

Cost exposure: one bounded Athena SELECT and result objects, one SNS publish,
SQS delivery/retries, one Lambda delivery attempt plus configured retries,
on-demand DynamoDB operations, short CloudWatch logs, and the retained test-bot
secret. Secret storage can create an ongoing charge; so can forgotten logs or
retries. IAM/workgroup configuration adds no running compute. No EC2 or always-on
service is proposed. The scan cutoff limits an individual query, not all account
spend. Confirm regional prices/free-tier eligibility with Khuê before approval.

## Verification and publication

```sh
python -B -m unittest discover -s tests -p 'test_*.py' -v
ruff check --no-cache real_producer.py scripts tests alerts bot_lambda
ruff format --check --no-cache real_producer.py scripts tests alerts bot_lambda
uv lock --check --offline
git diff --check
```

223 offline Python cases passed, including full callable wiring, wrong snapshots,
historical labels, secret references, single-recipient isolation, retries, partial
failures and command preparation. Package imports/SDK model checks are offline,
not deployment proof. Earlier 200-test and 48-Gold results remain historical;
Gold source/P0 tests and all prior acceptance JSON files are unchanged.

**Code publication can proceed after local checks. Cloud execution remains NO-GO
until login/preflight succeeds and the exact staging/test scope is approved.**
