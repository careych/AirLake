# Telegram staging contract for the AWS owner

VERIFIED means inspected in main `d2c7ac44edd00d5dc4e717c6e1eccc0305ad58c7`,
confirmed to match GitHub main by `git ls-remote`. PROPOSED means a resource or
local correction that is not verified deployed. UNKNOWN means no AWS/Telegram
inspection was performed in this task. No secret, private recipient or credential
file was opened; all test identities are synthetic.

## Exact secret contract

- VERIFIED name: `airlake/alert-staging-v1/telegram`, account 701364614078,
  region ap-southeast-2.
- VERIFIED format: **plaintext SecretString**, containing only the raw token.
  There are **no JSON keys**. JSON objects, JSON-quoted strings, whitespace and
  newline wrappers are not supported. SecretBinary is not supported.
- Structural placeholder only: `<STAGING_BOT_ID>:<STAGING_BOT_TOKEN_SUFFIX>`.
  Replace privately; the angle-bracket example is not a literal valid token.
- VERIFIED consumer: `application_from_environment` reads
  `get_secret_value(SecretId=secret_arn)["SecretString"]` and passes it directly
  to `TelegramSender`. No JSON token parser exists.
- VERIFIED environment reference: `TELEGRAM_TOKEN_SECRET_ARN`; use the actual full
  ARN returned by Secrets Manager, including its six-character suffix. No actual
  secret ARN is known from this read-only-code task. The public plan's `??????`
  is an IAM wildcard proposal, not a provisioned ARN.
- VERIFIED main transport validation: string, 1–200 characters from
  `[A-Za-z0-9:_-]`; it does not authenticate the bot's identity.
- PROPOSED local corrective code additionally requires
  `AIRLAKE_ALERT_TEST_BOT_ID`, a positive canonical numeric string, and exactly
  one colon with that ID as the token prefix and a nonempty suffix. General
  transport validation still applies. The owner must confirm the pinned ID is a
  **different staging bot**, not production; the pin cannot independently prove
  that human assertion. Never copy the production token into a staging-named secret.

Merged source references: `bot_lambda/src/lambda_function.py:317–360`,
`alerts/delivery.py:40–49`, `docs/plans/athena-alert-staging.json:354–357`.
Current corrective source references: `bot_lambda/src/lambda_function.py:367–394`.

## Roles and least-privilege resources

Both identities below are **PROPOSED**, derived from the plan and deployment
Role template. Existence and effective policies are UNKNOWN.

| Component | Proposed role ARN | Secret access |
| --- | --- | --- |
| SQS → Telegram Lambda | `arn:aws:iam::701364614078:role/airlake-alert-staging-v1-consumer` | GetSecretValue on the actual full staging secret ARN only |
| Athena → SNS CLI publisher | `arn:aws:iam::701364614078:role/airlake-alert-staging-v1-reader` | None |

The reader role is assumed by the approved existing operator role, whose actual
ARN must be supplied separately. It is not the Telegram Lambda execution role.
The consumer trust policy is:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
```

The consumer's secret permission is:

```json
{
  "Effect": "Allow",
  "Action": "secretsmanager:GetSecretValue",
  "Resource": "<ACTUAL_FULL_SECRET_ARN>"
}
```

No ListSecrets, PutSecretValue, rotation or production-secret access is needed.
Add `kms:Decrypt` on the **actual key ARN only if a customer-managed KMS key is
used**, with an allowing key policy. No extra KMS statement is proposed for the
AWS-managed `aws/secretsmanager` key. Actual encryption/key ARN is UNKNOWN.
[Official GetSecretValue permission requirements](https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html).

Other consumer grants are restricted to:

| Actions | Exact resource |
| --- | --- |
| `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes` | `arn:aws:sqs:ap-southeast-2:701364614078:airlake-alert-staging-v1` |
| `dynamodb:GetItem` | `arn:aws:dynamodb:ap-southeast-2:701364614078:table/airlake-alert-staging-v1-subscriptions` |
| `dynamodb:GetItem`, `dynamodb:PutItem`, `dynamodb:UpdateItem` | `arn:aws:dynamodb:ap-southeast-2:701364614078:table/airlake-alert-staging-v1-ledger`, LeadingKeys `telegram#*` |
| `logs:CreateLogStream`, `logs:PutLogEvents` | `arn:aws:logs:ap-southeast-2:701364614078:log-group:/aws/lambda/airlake-alert-staging-v1:log-stream:*` |

The corrective preparer pins subscription GetItem to exactly the approved
`dynamodb:LeadingKeys` value in the **private** generated policy and replaces the
secret wildcard with the supplied actual secret ARN. The deployer precreates the
log group, so the consumer needs no CreateLogGroup grant. No DynamoDB Scan or
subscription-write grant is needed for this consumer.

The reader/publisher needs only the plan's exact Athena workgroup query/read APIs,
the two allowlisted Glue Gold tables/catalog/database, GetObject on their Gold
prefixes, bucket location/scoped listing, query-result reads/writes under
`alert-staging/v1/`, Publish on staging SNS, and `sns#*` ledger key access. It has
no secret access, production SNS access, Gold writes or queue-consumption grant.
The full proposed policy is in `docs/plans/athena-alert-staging.json`.
No PowerUserAccess or new account-wide policy is required.

## One recipient and no production fanout

VERIFIED environment contract, with placeholders only:

```text
AIRLAKE_ALERT_STAGING_NAME=airlake-alert-staging-v1
AIRLAKE_ALERT_TEST_MODE=1
AIRLAKE_ALERT_TEST_RECIPIENT=<APPROVED_CHAT_ID>
AIRLAKE_SUBSCRIPTION_TABLE=airlake-alert-staging-v1-subscriptions
AIRLAKE_DELIVERY_TABLE=airlake-alert-staging-v1-ledger
AIRLAKE_ALERT_TOPIC_ARN=arn:aws:sns:ap-southeast-2:701364614078:airlake-alert-staging-v1
AIRLAKE_ALERT_QUEUE_ARN=arn:aws:sqs:ap-southeast-2:701364614078:airlake-alert-staging-v1
AIRLAKE_PARTIAL_BATCH_FAILURES=1
TELEGRAM_TOKEN_SECRET_ARN=<ACTUAL_FULL_SECRET_ARN>
AIRLAKE_ALERT_DELIVERY_ENABLED=0
```

PROPOSED corrective addition: `AIRLAKE_ALERT_TEST_BOT_ID=<APPROVED_STAGING_BOT_ID>`.
Do not configure TELEGRAM_TOKEN or TELEGRAM_WEBHOOK_SECRET for staging. Keep
DELIVERY_ENABLED=0 and the new SQS mapping disabled until a separate live-test
approval; only then deploy/enable the reviewed immutable staging version/alias.

The private preparer input has exactly one key:

```json
{"chat_id":"<APPROVED_CHAT_ID>"}
```

The dedicated DynamoDB table has partition key `chat_id` (String), no sort key,
PAY_PER_REQUEST. Provision exactly one eligible item, conditionally with
`attribute_not_exists(chat_id)`; do not copy production subscribers:

```json
{
  "chat_id": {"S":"<APPROVED_CHAT_ID>"},
  "station_ids": {"L":[{"S":"4946812"}]},
  "enabled": {"BOOL":true}
}
```

The runtime accepts a canonical nonzero signed 64-bit numeric chat ID encoded as
a string. For the one-person test, the owner should explicitly approve a private
DM, not a group. The code checks one chat identity, not the number of humans in a
group. `minimum_aqi` is optional numeric integer 0–500; omitting it uses only the
explicit event rule, not an invented health threshold. Legacy runtime defaults
for missing fields exist, but the staging item above supplies explicit scope.

Recipient identity occurs in the private input file, Lambda environment, single
subscription row, private generated IAM condition, and per-recipient delivery
ledger key. It is **not** in the bot secret or SNS alert payload. Do not print it
in chat/logs or commit generated files. Directory/file permissions are 0700/0600.

DynamoSubscriptions uses one consistent GetItem for that key, never Scan. A wrong
returned key is rejected; disabled/unsubscribed/wrong-station records receive
nothing. Application.deliver independently filters recipients to the configured
identity. The original SNS/SQS source ARN and approved rule checks remain intact.
The corrective factory also prevents disabling test mode/partial batches or
enabling a webhook when staging function/resources are selected.

Existing production resources `AirLake_Subscriptions`, `AirLake_Alerts`,
`AirLake_Bot_Queue`, `AirLake_Bot_API` and its mapping are outside both code guards
and proposed grants. The deployment generator targets only the new names; no
shared Lambda/webhook update or production-table read is part of the plan.
Effective deployment isolation remains UNKNOWN until actual role attachments,
queue policy, SNS subscription and mapping are checked after provisioning.

## Required resources and retry limits

Provision only after owner approval: the separate bot secret; staging SNS topic,
source SQS queue and Lambda all named `airlake-alert-staging-v1`; DLQ
`airlake-alert-staging-v1-dlq`; subscriptions/ledger tables above; the two roles;
log group `/aws/lambda/airlake-alert-staging-v1`; Athena workgroup
`airlake_alert_staging_v1`, results prefix `alert-staging/v1/`. Do not create Gold
copies or change existing Gold resources.

Ledger partition key `delivery_id` (String), TTL `expires_at` (Number epoch
seconds), lease 180s, retention 5,184,000s (60 days). Conditional leases and SENT
state suppress ordinary duplicate deliveries. This is **at-least-once**, not an
atomic Telegram/DynamoDB transaction: remote acceptance followed by a failed SENT
write can still duplicate a message to the **same** recipient. Do not automatically
republish a test or manually redrive an ambiguous send; investigate first.

Verify the actual deployment has batch=1, batching window=0,
ReportBatchItemFailures, mapping maximum concurrency=2 and reserved concurrency=2;
Lambda timeout=120s, SQS visibility=720s, maxReceiveCount=5 with the dedicated DLQ.
The consumer performs no unbounded HTTP retries. Queue retries are bounded by the
proposed redrive policy and do not broaden recipient scope. AWS-enforced retry/DLQ
behavior has not been live-tested here.

The currently saved reader plan remains the explicitly historical July 2025
one-hour observation, fixture-only/test-v1 rule and original 200-row cloud Gold
snapshots. It is **not** retargeted to the accepted November 559-row run. To use
that new data, separately review/update the exact reader allowlist, station name,
snapshots, hour, expected values, test rule/reference time and matching reader IAM
prefixes before query/publish approval. Secret provisioning is independent of that
choice. Do not present the historical test as a current warning.

After provisioning, the owner must confirm actual role ARNs/trust and absence of
broad/production grants, actual secret ARN/encryption metadata, separate staging
bot identity, private one-recipient eligibility, queue/SNS/mapping/DLQ/TTL settings,
artifact CodeSha256 and immutable alias/version. Share references/status only,
not values. Secret retrieval/Telegram delivery still require separate approval.

## Local verification and corrective PR

This corrective change is separate from merged PR #6 and is not deployed to AWS.
The SecretString format is unchanged. Merge the reviewed corrective PR and rebuild
the lock-verified Lambda ZIP before deployment. No Gold logic or historical
acceptance evidence was modified.

Focused tests use mocks and synthetic IDs with network blocked: 25 runtime cases
and 12 preflight/preparation cases passed. Full Python regression: 265 passed,
zero failures/errors/skips. Ruff lint/format and offline lock checks passed.
No AWS calls, Athena queries, secret retrieval, SNS publish or Telegram requests
were executed in this task. Secret scanning inspects source/doc/test candidates
only; it never opens actual secret/private-recipient files.
