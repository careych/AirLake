# Gold P0 regression tests

## Local publication checks

```sh
.venv/bin/ruff check --no-cache real_producer.py scripts tests
.venv/bin/ruff format --check --no-cache real_producer.py scripts tests
.venv/bin/python -B -m unittest discover -s tests -p 'test_*.py' -v
```

Default discovery includes the nine Gold orchestration tests plus producer,
smoke-launcher and acceptance-record checks. These are pure-Python checks: producer
clients are inert stand-ins, CSV/JAR fixtures use external temporary directories,
and smoke safety checks prohibit PySpark imports and real sockets. They do not
demonstrate Kafka delivery or Spark runtime behavior.

`gold_p0_integration.py` remains excluded from default discovery and is executed
only through its opt-in launcher. The modified smoke script also requires explicit
`--run`, a preinstalled JAR and Java 17; it writes only a disposable external
warehouse. Source review and a successful fixture check do not validate existing
production-local tables.

PR preparation ran 35 pure-Python tests (9 Gold orchestration, 13 producer,
7 smoke safety and 6 acceptance-record checks), all passing. The redesigned
smoke command also completed one real isolated Spark/Iceberg fixture write/read
with Java 17 and the preinstalled Iceberg JAR; its temporary directory was removed.
Ruff, shell syntax, Compose parsing and the offline Python lock check passed.
No producer/Kafka streaming or existing-warehouse job was run. The full 48-method
Gold integration suite was not rerun for this cleanup: its production and two P0
test source hashes remain unchanged and the acceptance-record checks still pass.

## Reading the retained acceptance record

The evidence JSON is a self-contained record of the 2026-10-25 Gold run. Raw logs
and full file manifests were captured outside the repository and may be temporary;
understanding or validating the committed record does not require those files.

- `source_sha256` identifies the exact executed source bytes. `git_head` records
  the base of an uncommitted working tree, not a commit containing the Gold code.
- `verification_date` labels the run; it is not an invented policy approval date.
  Environment versions and timings describe that run. Integration `seconds` is
  command wall time including launcher/JVM startup; pure `seconds` is the unittest
  duration. They are not performance benchmarks or fresh results on every checkout.
- `states` holds actual collected fixture row strings and snapshot-ID arrays per
  disposable table. Row strings omit null fields under Spark's JSON defaults.
  IDs are opaque integers, not timestamps, row counts or chronological order.
  Python's JSON parser preserves their integer precision.
- State references are SHA-256 of UTF-8 `json.dumps(state, sort_keys=True,
  separators=(",", ":"))` using the default ASCII escaping. Matching references
  mean the recorded rows and snapshot arrays are identical.
- `protected_runtime` reports file counts, sizes, content hashes and modification
  times/fingerprint hashes. It does not independently verify production table row
  counts. Filesystem `mtime_ns` values are not snapshot timestamps.
- The temporary-path placeholders and fixture station names are intentional;
  no local user directory, credentials, real user subscription, raw warehouse
  metadata or production dataset is included.

`test_acceptance_evidence.py` checks references, recorded outcomes, name-only repair,
rejection/rerun equality, source hashes and publication-sensitive patterns without
running Spark. Re-execute the isolated suite after behavioral Gold changes; do not
edit recorded counts, hashes or snapshots merely to make evidence checks pass.

## Verification on 2026-10-25

- Ruff lint and formatting checks on the modified production/test Python files: passed.
- Pure-Python orchestration: **9 passed, 0 failed, 0 errors**.
- Full isolated Spark/Iceberg suite: **48 passed, 0 failed, 0 errors, 0 skipped**.
  This includes 27 unchanged non-policy methods, the replacement positive repair
  case, and 20 additional methods. All six original pure tests are retained, with
  three additional orchestration checks.
- All **85** rejected-case comparisons preserved actual dimension/fact/breakpoint
  rows and snapshot IDs. Four successful repair scenarios changed only the null
  name, added exactly one dimension snapshot, preserved fact/breakpoint state,
  and produced no further changes or snapshots on an unchanged rerun.

The first sandbox attempt could not bind the Java gateway socket and ran no tests.
The first execution outside that restriction had 46 passing methods and two
failing methods (three subtest failures): safe rejections used a generic source-null
message before the specific target-coordinate/mapping guard. Moving target
preflight before the derived-source null check resolved those failures without
changing their assertions. The complete suite then passed in 130.311 seconds.

The sanitized [acceptance evidence](evidence/gold_dimension_name_repair_2026-10-25.json)
retains source/JAR hashes, environment versions, all method results, collected
temporary rows and snapshot IDs, and protected-runtime fingerprint summaries.
Rejection records refer to content-addressed states to avoid duplicating identical
before/after payloads. Raw logs and full file manifests were retained separately
outside the repository. No Spark fixture writes to repository runtime directories.
These results cover the approved P0 contracts; they do not establish official
VN_AQI calculation, multi-table atomicity, or AWS deployment acceptance.

## Historical results before the approved-policy tests

After the P0 fixes on 2026-10-19:

- Pure-Python orchestration: **6 passed, 0 failed, 0 errors**.
- Original Spark/Iceberg baseline: **20 passed, 1 failed, 0 errors** (21 methods).
- Seven added write-order/valid-behavior checks: **7 passed**.
- Complete integration run: **27 passed, 1 failed, 0 errors** (28 methods).
- Unittest reports `FAILED (failures=3)` because the remaining method has three
  failing subtests. Exit status 1 is intentionally retained and is not all-green.

The remaining method in that run was
`test_invalid_existing_dimension_payload_is_rejected_before_writes`. It required
rejecting an existing null name without writes. Production repaired the name
from valid Bronze metadata, as explicitly requested. No assertion was weakened,
skipped, or marked expectedFailure/xfail. That unconditional rejection expectation
has now been replaced under the project owner's approved policy below. The old
failed run remains historical evidence; it is not a result for the revised suite.

The earlier baseline had 9 passes and 12 failing integration methods, and 3 passes
and 3 failures in the pure suite. The recorded post-fix run passed all 11 methods
covering confirmed production defects. Its handoff recorded unchanged production
warehouse/checkpoint/data fingerprints. The 2026-10-24 audit found that the earlier
temporary result files were no longer retained, so those recorded results are
historical claims rather than freshly reverified runtime evidence.

## Approved dimension-name policy

The project owner approved null-name repair; no approval date or group-wide
approval is asserted here. Required Bronze metadata covers every current Silver
location and every existing null-name dimension key, including legacy keys absent
from Silver and an empty Silver input. Identical valid metadata repeats are
accepted. Missing metadata, null/blank names, conflicting names/coordinates, or
null/unparseable/non-finite coordinates reject before any write.

Existing valid names are preserved exactly. A repair changes only the null name;
existing coordinates and station type are retained. Blank-but-non-null target
names are rejected, not automatically repaired. Invalid target keys, coordinates,
station mappings and orphan facts also reject before writes. No unrelated
historical metadata correction is authorized by this policy. Postflight requires
all target names to be non-null and nonblank.

The revised integration suite tests unique and repeated-metadata repairs; missing,
null, Unicode-whitespace-only and conflicting metadata; malformed/non-finite
coordinates; legacy keys outside Silver; empty Silver; invalid target payloads;
unsupported station mappings; unchanged valid names; and repair reruns. Successful
repair checks that only the name changes and exactly one dimension snapshot is
added, while fact/breakpoint rows and snapshot IDs stay unchanged. A second run
must preserve all three tables' rows and snapshots.

Rejected cases compare actual collected temporary rows and snapshot IDs for
dimension, hourly and breakpoint tables, and assert no CREATE or write SQL.
`P0_NAME_REPAIR=` and `P0_WRITE_SAFETY=` JSON lines provide fixture-only evidence.
All 27 pre-existing non-policy integration method bodies are preserved.

## Pure-Python orchestration tests

```sh
.venv/bin/python -B -m unittest discover -s tests -p 'test_gold_p0_orchestration.py' -v
```

`test_gold_p0_orchestration.py` reads production source and compiles only the real
`run_gold_batch`, `merge_if_changed`, `assert_gold_integrity`, `_invalid_numeric`,
`_blank_name`, and `_invalid_station_mapping` function bodies. It never imports the production module or
PySpark, starts a process, or writes a table. Remaining dependencies and SQL are
inert stand-ins. The harness bindings and valid-frame counts were extended for
the preflight helpers; rejection/write-order assertions were preserved and now
also forbid breakpoint initialization/table creation. Added cases check legacy
repair planning, uncovered repair rejection, and strict null-name postflight.
These tests establish ordering, not real numeric or Iceberg semantics.

## Opt-in Spark/Iceberg integration tests

`gold_p0_integration.py` is excluded from default `test*.py` discovery. Direct
invocation without explicit opt-in refuses before importing PySpark.

Prerequisites: project PySpark 3.5.9, Java 17, and a preinstalled local
`iceberg-spark-runtime-3.5_2.12-1.12.0.jar`. No dependencies are downloaded.
Set `JAVA_HOME` and `AIRLAKE_ICEBERG_RUNTIME_JAR` to those installed resources, then:

```sh
AIRLAKE_RUN_GOLD_P0_INTEGRATION=1 .venv/bin/python -B tests/gold_p0_integration.py
```

The launcher creates a fresh child process and external TemporaryDirectory as its
CWD. It rejects session/context reuse and configures a random Hadoop catalog plus
an explicitly isolated `local` fallback catalog, both with temporary file://
warehouses. Session metadata is in-memory; temporary Spark/Hadoop paths are also
under that directory. Cloud credentials, external config paths, remote gateways,
and inherited Spark/JVM overrides are excluded from the child environment.
All five production table identifiers are patched to disposable per-test names.
The production session factory is forbidden. No Kafka, Docker, producer, AWS,
existing checkpoint, or production warehouse is used.

The earlier P0 extension added three methods that drop only
fixture breakpoint tables to prove invalid numeric input, missing metadata, and
an invalid fact target cannot initialize breakpoints before rejection. Four other
methods verify first-run table creation, an unchanged rerun preserving rows and
snapshots, the largest finite single measurement, and all three mapped stations.

The write-safety helper still requires a domain RuntimeError, zero actual MERGE
statements, and identical collected target rows and snapshot lists before/after.
It includes breakpoint state (including absent tables) and prints
`P0_WRITE_SAFETY=` JSON evidence. A MERGE helper may validate before issuing SQL;
the revised suite also rejects CREATE SQL before a validation failure, even if
CREATE IF NOT EXISTS would not itself change an existing table.
Unexpected Spark/configuration exceptions are errors, not successful rejection.

| Integration coverage | Historical 2026-10-19 result |
| --- | --- |
| Finite positives and zero | Pass |
| Negative, null, NaN and positive/negative infinity | Pass: rejected before writes |
| Finite inputs overflowing AVG | Pass: rejected before writes |
| Existing NaN/infinite/negative hourly averages | Pass: rejected before writes |
| Valid Bronze metadata coverage | Pass |
| Missing metadata rejected by builder and batch | Pass |
| Complete metadata creates an absent target dimension key safely | Pass |
| Null/duplicate dimension and fact keys | Pass: rejected before writes |
| Existing orphan fact | Pass: rejected before writes |
| Existing null dimension name must be rejected | Fail: repair behavior preserved |
| Invalid input/target with missing breakpoint table (3 methods) | Pass: table remains absent |
| First run with Gold/breakpoint tables absent | Pass |
| Unchanged rerun | Pass: no new/changed rows or snapshots |
| Largest finite single measurement | Pass: finite mean retained, AQI capped at 500 |
| All three mapped stations | Pass |

## Production corrections and limits

Gold now checks measurement and aggregate finiteness, explicit Silver-to-Bronze
coverage, and existing target keys/numeric values/orphans before writes.
`ensure_breakpoint_table(initialize=False)` validates existing rows or the planned
in-memory seed without creating/appending a table. Only after source and target
preflight succeed does initialization or either Gold MERGE occur. Post-write
integrity checks remain. Only proven null-name repairs are allowed; existing valid
names and unrelated historical dimension fields are preserved.

Schemas, table identifiers, partitioning, station mappings, UTC bucketing,
observation-weighted means, breakpoint values, HALF_UP rounding, finite-value AQI
cap, and MERGE/no-op behavior remain unchanged. No finite concentration ceiling,
NowCast, alerts, unit conversion, SCD policy, or deletion policy was added.

Preflight validates a complete name-repair plan before the write phase and
protects against the tested predictable validation failures. Separate
Iceberg commits are not an atomic multi-table transaction; concurrent writers,
external changes after preflight, and arbitrary runtime/I/O failures can still
leave partial work. An abruptly killed worker may leave temporary files/JVMs.
The AST harness boundary must be reviewed if helper signatures/dependencies change.
