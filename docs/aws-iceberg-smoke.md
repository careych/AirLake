# AWS Iceberg interoperability probe

`scripts/aws_iceberg_smoke.py` defaults to a local, offline plan. Only
`--execute --confirm-account 701364614078` can launch its cloud-writing child.
No cloud execution or dependency installation was performed while preparing it.
Offline tests use mocks; they do not establish AWS interoperability.

The probe uses three rows: `(1, alpha, 10.0)`, `(2, beta, 0.0)`,
`(3, gamma, 25.0)`. Expected count is 3 and PM2.5 sum is 35.0.
It does not import or migrate the Bronze/Silver/Gold scripts. Their local Gold
acceptance remains the separate retained 48-integration/9-orchestration baseline.

## Verified cloud run: 2026-10-29

The approved three-row run passed Spark/S3/Glue verification and two independent
Athena SELECTs. Athena returned the exact records, `COUNT(*) = 3`,
`SUM(value) = 35.0` (`pm25 AS value`), and the same append snapshot as Spark:
`238687515668187493`. Both queries used Athena engine 3 and the workgroup's
enforced result bucket. No automatic cleanup occurred.

[Sanitized acceptance evidence](../tests/evidence/aws_iceberg_smoke_2026-10-29.json)
records the exact namespace, S3 prefix, query IDs, CSV result locations, source
hashes and actual results. It is self-contained and does not require temporary
logs. All 82 pure-Python tests passed: 47 smoke safety tests, 9 Gold orchestration
tests and 26 other existing tests. The 48-test Gold integration baseline was
not rerun; its production source/test hashes and 1,499 protected runtime files
remained unchanged.

The auxiliary cleanup-inventory helper raised `KeyError` while processing S3
listing responses after interoperability verification. Further AWS activity
stopped. The complete ancillary-object inventory remains unverified; inspect
the exact recorded scopes before any separately approved cleanup. The existing
run prefix and namespace must not be reused. Commands below describe future
operations and are not authorization to repeat this run or create more resources.

## Sprint 2 follow-up

The later [real OpenAQ batch](real-openaq-batch.md) added configurable catalogs
with local defaults preserved. Its separately approved 200-record cloud run and
two Athena SELECTs passed on 2026-11-07. See its [own evidence](../tests/evidence/real_openaq_cloud_2026-11-07.json);
this three-row record is unchanged and does not itself validate Bronze/Silver/Gold.
Silver finite-value/unit/correction policy, production recovery and alerts remain
unfinished. No dataset, warehouse, checkpoint or bot deployment is migrated by
running this standalone probe.

For notification integration, the inspected `origin/thinh` Lambda consumes an
SNS `Message` inside each SQS record's JSON body and broadcasts that text to
stored chat IDs. The minimum compatible boundary is a Gold-derived alert text
through the owner's confirmed pollution-alert topic with the SNS envelope
preserved. Include station, UTC hour, PM2.5 average/units, AQI and the simplified
hourly MVP label. Agree on an event ID, source snapshot and threshold/cooldown
policy before publishing; the current consumer does not provide deduplication
or per-station targeting. Its swallowed delivery errors also need review before
claiming reliable delivery. The Terraform operations-alert topic does not prove
this application route exists. No alert was published in this sprint.

## Dependencies and provenance

Use Python 3.12, PySpark 3.5.9, Scala 2.12.18 and Java 17. Python dependencies
already belong to the project lockfile. JVM artifacts are separate:

| Artifact | Version | Purpose |
| --- | --- | --- |
| `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12` | 1.12.0 | Spark integration, GlueCatalog, S3FileIO |
| `org.apache.iceberg:iceberg-aws-bundle` | 1.12.0 | AWS SDK clients, authentication and HTTP dependencies |
| AWS SDK modules inside that bundle | 2.54.17 BOM | Includes Glue, S3, STS, KMS, auth and `apache5-client` |
| `software.amazon.awssdk:signin` | 2.54.17, if pursuing direct login | Not explicitly listed by the bundle build; requires separate classpath review |

Sources: [Iceberg releases](https://iceberg.apache.org/releases/),
[1.12.0 bundle build](https://raw.githubusercontent.com/apache/iceberg/apache-iceberg-1.12.0/aws-bundle/build.gradle),
[pinned versions](https://raw.githubusercontent.com/apache/iceberg/apache-iceberg-1.12.0/gradle/libs.versions.toml).
The bundle includes its HTTP dependencies; do not add an older Apache HTTP client
or Hadoop AWS SDK v1 JAR to this two-JAR configuration.

Local inspection found the existing Spark runtime, containing GlueCatalog and
S3FileIO but no AWS SDK classes. Integration Sprint 1 verified the downloaded
AWS bundle's signature and archive integrity: its SDK modules are 2.54.17 and
the required Glue/S3/profile/process/Apache HTTP client 5 classes are present.
No Signin client or login credential provider is present. No conflicting SDK or
Iceberg classes were found in Spark's bundled JARs. The approved cloud run above verified interoperability with these bytes;
filenames alone remain insufficient for a new installation.

For this isolated university smoke test, artifact acceptance requires the expected
SHA-256, a valid detached signature, the official Apache KEYS entry, official
release publication and independent corroboration of the complete signer
fingerprint in the archived release-vote discussion. The
[owner-supplied archive](https://www.mail-archive.com/search?f=1&l=dev%40iceberg.apache.org&o=newest&q=date%3A20260518)
records `7180E16D410C476E010F37E180763BA3EDCCE745` for Amogh Jahagirdar.
This is corroborated release provenance, not LDAP certification or a GPG owner-trust
assignment. GPG owner trust remains unchanged. The accepted bundle SHA-256 is
`dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957`.
Its persistent user-local location is
`$HOME/.local/share/airlake/dependencies/iceberg/1.12.0/`; no binary is stored in Git.

The runner pins the existing Gold runtime bytes to SHA-256
`8dd8c90e54ed1a20f2461309d047a255ee56382eb7c0a46162b27a4936fd86dc`.
This is the retained local baseline hash, not a new assertion of release-signature
verification. The AWS bundle requires an operator-supplied, trusted SHA-256.
Both JARs must contain the required classes. Before cloud writes, the child also
checks loaded class origins, Iceberg/SDK/Scala/Java versions and its Java identity.
Those runtime checks passed in the retained three-row run. Unexpected results
on a new machine still stop the probe.

### Future dependency preparation — NOT EXECUTED

After separate approval, use the Maven Central artifact linked by the Apache
release page. The following downloads only the AWS bundle, signature and public
release keys into a new directory outside the repository:

```sh
DEPENDENCY_DIR=$(mktemp -d "${TMPDIR:-/tmp}/airlake-aws-dependencies.XXXXXXXX")
DEPENDENCY_BASE=https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.12.0
curl --fail --location --proto '=https' "$DEPENDENCY_BASE/iceberg-aws-bundle-1.12.0.jar" --output "$DEPENDENCY_DIR/iceberg-aws-bundle-1.12.0.jar"
curl --fail --location --proto '=https' "$DEPENDENCY_BASE/iceberg-aws-bundle-1.12.0.jar.asc" --output "$DEPENDENCY_DIR/iceberg-aws-bundle-1.12.0.jar.asc"
curl --fail --location --proto '=https' https://downloads.apache.org/iceberg/KEYS --output "$DEPENDENCY_DIR/KEYS"
mkdir -m 700 "$DEPENDENCY_DIR/gnupg"
gpg --homedir "$DEPENDENCY_DIR/gnupg" --import "$DEPENDENCY_DIR/KEYS"
gpg --homedir "$DEPENDENCY_DIR/gnupg" --verify "$DEPENDENCY_DIR/iceberg-aws-bundle-1.12.0.jar.asc" "$DEPENDENCY_DIR/iceberg-aws-bundle-1.12.0.jar"
```

Require an existing GPG installation and verify the signing-key fingerprint
through the project's trusted release process. Stop on a missing sidecar,
untrusted signer or failed verification; do not bypass it. Availability of these
artifact sidecars was not independently established in this offline task.
The source tarball's `.sha512` is not the checksum of a runtime JAR.

Only after provenance verification, compute the local integrity pin:

```sh
export AIRLAKE_ICEBERG_AWS_BUNDLE_JAR="$DEPENDENCY_DIR/iceberg-aws-bundle-1.12.0.jar"
export AIRLAKE_ICEBERG_AWS_BUNDLE_SHA256=$(shasum -a 256 "$AIRLAKE_ICEBERG_AWS_BUNDLE_JAR" | awk '{print $1}')
```

A self-computed hash alone does not authenticate a download. The runner checks
these bytes on both sides of the process boundary. It does not install artifacts,
resolve Maven packages, or accept an arbitrary additional classpath.

## Temporary credentials

Existing profile `airlake` must contain only `region = ap-southeast-2` and its
existing `login_session` setting. Never copy session-cache contents. Static
credentials in either selected profile are rejected.
Enabled AWS CLI command history is also rejected to avoid retaining credential
process responses. No existing history files are read or altered.

Current [Java SDK v2 supports login sessions](https://docs.aws.amazon.com/sdkref/latest/guide/feature-login-credentials.html),
but that requires the Signin provider to be present and usable. The default
`--spark-profile airlake` is blocked if the verified bundle lacks that class.
Do not guess that direct login works or add unreviewed JARs to bypass the check.

The supported alternative is a manually reviewed process profile. After separate
approval, add this non-secret configuration to the existing AWS config, replacing
the executable with the absolute result of `command -v aws`:

```ini
[profile airlake-spark]
region = ap-southeast-2
credential_process = /absolute/path/to/aws configure export-credentials --profile airlake --format process
```

The runner accepts only this exact command/argument sequence and region. The
SDK consumes its JSON output internally. Never invoke it standalone, log its
output, redirect it into a file, or export credential values into environment
variables. See [AWS process credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-process.html).
The CLI inventory always uses `airlake`; only Spark selects `airlake-spark`.
Normal future execution may refresh the existing login cache. No credentials or
profiles are changed by dry-run. Java authentication through `airlake-spark` passed in the accepted run; a new
execution must repeat its read-only Java STS check.

## Offline plan and validation

From the repository root:

```sh
.venv/bin/python -B scripts/aws_iceberg_smoke.py
.venv/bin/ruff check --no-cache scripts/aws_iceberg_smoke.py tests/test_aws_iceberg_smoke.py
.venv/bin/ruff format --check --no-cache scripts/aws_iceberg_smoke.py tests/test_aws_iceberg_smoke.py
.venv/bin/python -B -m unittest discover -s tests -p 'test_aws_iceberg_smoke.py' -v
```

Dry-run reads local JAR bytes and package metadata only. It starts no JVM,
subprocess or AWS operation, creates no manifest/directory, and reports missing
dependencies without turning the preview into an execution. Each preview gets
a new proposed run ID; it does not reserve that ID for execution.
For explicit write approval, save the dry-run JSON's `target` object outside the
repository and use `--plan-file /path/to/reviewed-plan.json` for both the review
and subsequent execution. This binds execution to the exact approved UUID,
namespace and prefix. Target validation still rejects production namespaces,
arbitrary locations and modified fixture expectations.

Set `JAVA_HOME` to the existing Java 17 JDK and
`AIRLAKE_ICEBERG_RUNTIME_JAR` to the existing runtime if its Ivy default is absent.
No dependency download is part of either mode.

## Future guarded execution — NOT EXECUTED

After dependency/provenance review, authentication review and explicit approval
for cloud writes:

```sh
.venv/bin/python -B scripts/aws_iceberg_smoke.py --execute --confirm-account 701364614078 --region ap-southeast-2 --spark-profile airlake-spark
```

Use a clean shell without inherited Spark/Hadoop/gateway/classpath overrides,
custom AWS endpoints, credential variables or proxy settings. Unsafe settings
are rejected rather than silently reused. No custom account, bucket, database,
table, prefix or run ID can be supplied.

Execution generates `YYYYMMDD_<uuid>` and uses only:

- Glue database `airlake_smoke_<run_id>` and table `probe`.
- `s3://airlake-701364614078-lake/integration-smoke/<run_id>/`.
- Explicit Iceberg v2, Parquet, one append of the three fixture rows.

Read-only preflight checks CLI identity, bucket owner/region/unversioned state,
the exact workgroup settings, Glue absence and complete S3-prefix emptiness.
Only Glue `EntityNotFoundException` means absence. Denial, malformed responses,
truncated inventories, versioning or changed workgroup settings stop execution.
Absence is checked again immediately before DDL. CREATE never uses IF NOT EXISTS.
Unique IDs and checks reduce races; they do not create an atomic reservation.
Keep this scope exclusive to one writer.

The child has a fresh local Spark session, empty Spark config directory, private
temporary storage and a disposable `local` fallback catalog outside the repo.
There is no Hive metastore configuration or production checkpoint. Runtime
configuration and loaded JAR origins are checked before writes. Credential files
remain at their normal locations for profile authentication. Only this child's
process group is terminated on the five-minute deadline.

The macOS child allowlist preserves `__CF_USER_TEXT_ENCODING`, which the OS can
inject at process startup. Spark's generated module-access options and its known
IPv6 preference may precede the probe's JVM options. The runner accepts only
those generated defaults plus the exact probe options, and separately verifies
the effective temporary-directory, Derby-directory and AWS-attempt properties.
Arbitrary JVM options or changed effective properties still cause rejection.

Before the successful Integration Sprint 1 run, two approved attempts stopped before DDL:
first on the macOS locale setting, then on Spark's generated JVM prefix. A third
attempt stopped at expired-login STS resolution before Spark startup. All manifests
were retained without cleanup. The corrected startup guards passed against actual
offline Spark/JARs; 82 pure-Python tests passed. Those results are startup and
safety evidence; the separate cloud acceptance record above establishes the
three-row interoperability result.

The runner disables task/speculation, S3 and Iceberg commit retries and requests
one SDK attempt. It never retries the job or repeats an ambiguous append. These
settings were exercised by the accepted run; they are not a guarantee of atomic rollback. Runtime/library
failures remain normal failures; do not weaken guards to get a pass.

## Resources, IAM and costs

Expect one disposable Glue database/table, small Parquet/manifest/metadata files
and one append snapshot. No AWS compute is provisioned. Target less than 10 MB
of artifacts; at most two manually approved Athena queries follow.

Required access includes lake-prefix List/Get/Put/AbortMultipartUpload; Glue
Get/CreateDatabase and Get/Create/UpdateTable; workgroup Get/Start/GetExecution/
GetResults; and appropriate result-bucket access. Cleanup additionally needs
scoped S3 Delete and Glue DeleteTable/DeleteDatabase. KMS, bucket policies,
Lake Formation, boundaries and SCPs may impose additional requirements. The
runner does not grant permissions or assume that a role's name establishes them.

S3 requests/storage, Glue catalog usage, internet transfer to the Mac and Athena
scanning/results can incur costs. Do not assume free-tier coverage. The existing
10-GiB per-query cutoff is not a small total-spend cap. The fixture and fixed
query scope bound normal data volume; stop on unexpected inventory or failures.
See [Athena pricing](https://aws.amazon.com/athena/pricing/).
Reported scan bytes are not billed charges. Query minima and regional rates
can affect the bill; no current-price or total-cost guarantee is asserted here.

## Athena plan — NOT EXECUTED

The runner does not execute Athena SQL. Its CLI adapter permits only
`get-work-group`. After a successful Spark/Glue result and separate approval,
use the manifest's database in the following two queries, maximum:

```sql
SELECT id, label, pm25, count(*) OVER () AS row_count
FROM "<manifest_database>"."probe"
ORDER BY id;

SELECT snapshot_id, operation
FROM "<manifest_database>"."probe$snapshots";
```

Submit through `aws --profile airlake --region ap-southeast-2 athena
start-query-execution --work-group airlake --query-string '<reviewed SQL>'`.
Do not resubmit on an ambiguous response; retain the request token/query ID.
Use a distinct stable `--client-request-token` of at least 32 characters per query
to make an intentional retry of the same request identifiable.

For example, after setting `SMOKE_DATABASE` and `SMOKE_RUN` from the retained
manifest, the first query's future command is **NOT EXECUTED**:

```sh
QUERY_ID=$(aws --profile airlake --region ap-southeast-2 athena start-query-execution --work-group airlake --client-request-token "${SMOKE_RUN:?}-rows" --query-string "SELECT id,label,pm25,count(*) OVER () AS row_count FROM \"${SMOKE_DATABASE:?}\".\"probe\" ORDER BY id" --query QueryExecutionId --output text)
aws --profile airlake --region ap-southeast-2 athena get-query-execution --query-execution-id "${QUERY_ID:?}"
```

Poll that ID to a terminal state; do not launch another query to check status.
After SUCCEEDED, use `get-query-results --query-execution-id "$QUERY_ID"`.
If approved, submit only the second SQL above with token `${SMOKE_RUN}-snapshot`.

The workgroup enforces `s3://airlake-701364614078-athena-results/`; a supplied
client result location does not override it. From `get-query-execution`, retain
the query ID, terminal state, scan statistics, actual OutputLocation and any
DataManifestLocation. Fetch results only after SUCCEEDED. Record actual exact
output-object keys and related result metadata keys in the manifest's
`athena_queries` list before cleanup. Do not invent keys solely from a query ID.

Require the three exact rows with row_count=3; inspect ResultSetMetadata for
`integer`, `varchar`, `double`, and `bigint` respectively. Sum the returned pm25
values to 35.0. Require the snapshot ID and append operation to match Spark's
manifest. Metadata record counts alone do not prove actual row counts.
Athena supports Glue-backed Iceberg v2; do not infer full feature compatibility
from a matching table-format number. See [Athena limitations](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html).

## Manifest, partial failures and cleanup — NOT EXECUTED

The parent prints a retained external temporary directory containing
`manifest.json`. It records the generated resource plan, verified JAR hashes,
operation attempts/results, actual rows/schema/snapshots and any verified metadata
paths. No credentials, user/session ARNs, personal paths or raw SDK errors are
retained. Raw child stdout/stderr is suppressed; failures retain a fixed code.
An attempted operation can have succeeded remotely despite a local error.

There is no automatic cloud cleanup, rollback, snapshot expiration or overwrite.
An I/O error can leave a namespace, registered table, orphan data/metadata or
multipart uploads. Iceberg may itself clean up files from an aborted write;
do not infer final resources from the exception. Inspect the manifest's exact
scope and live inventory before deciding how to recover. A rerun allocates a new
scope and does not repair or erase the old one.

For manual cleanup after separate approval:

1. Confirm the manifest's account, generated database and exact S3 location.
   Verify the table's current metadata still belongs to that scope; stop any writer.
2. Delete only that Glue table, then the empty disposable database.
3. List and review the entire exact run prefix, then delete only its objects and
   any prefix-specific multipart uploads. Never delete `integration-smoke/` itself.
4. Inspect versioning again. If enabled/suspended since execution, ordinary deletion
   can retain versions/delete markers; obtain a separate version-specific plan.
5. Delete only recorded Athena result objects and associated metadata/manifests,
   never the shared result-bucket prefix. Verify both scoped inventories afterward.

Do not assume Glue deletion removes S3 files, or that a failure across Spark,
S3, Glue and Athena rolls back atomically. Preserve sanitized evidence before
removing temporary local files. Audit logs may remain independently.

Before and after an approved run, compare existing production Glue metadata
pointers, scoped S3 inventories and local runtime fingerprints. Unexpected changes
require investigation; concurrent unrelated writers prevent simple attribution.
This probe does not validate Kafka ingestion, Bronze restarts, Silver quality,
Gold cloud MERGE/idempotency, regulatory AQI, or full pipeline reconciliation.
