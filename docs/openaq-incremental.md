# Bounded incremental OpenAQ ingestion

This command retrieves genuine OpenAQ v3 PM2.5 measurements and publishes the
existing nine-field Bronze contract. It is a bounded poll, not an installed daemon.
No AWS service is called. Bronze/Silver/Gold production files are unchanged.

## Run

Use Python 3.12 and the repository's locked environment (`uv sync --frozen`).
Activate the environment with `. .venv/bin/activate` (or use `uv run --frozen python`).
An existing single-broker instance must be reachable at `localhost:19092`.
Credentials come from `OPENAQ_API_KEY`, then `OPENAQ_KEY`, then an explicitly
selected local dotenv file. No credential is accepted as a CLI value, logged,
loaded on import or retrieved from SSM. Never commit the dotenv file.

```sh
python -B -m ingestion.openaq \
  --root /tmp/airlake-live-demo-UNUSED --topic airlake_live_demo_20261108 \
  --env-file /path/to/private.env --max-records 300 --create-topic
```

The root must be outside every Git checkout. The topic must start `airlake_live_`;
only the loopback broker is supported. Existing topics without a verified cursor
ownership marker reject. Existing cursor bindings must match broker, topic,
pilot IDs, schema and timestamp convention. Never reset/delete the cursor.

The configured pairs come from Nam's `freshness_poller.py` at
`7d4669c2604927eed3b6fb8875004d9d260e2694`:
`4946812/13502151`, `4946813/13502154`, `6123215/14581375`.
Actual names/coordinates come from the API, including its current `Dragon` name
for 6123215, not the poller's display aliases. No station metadata is invented.
The implementation reuses the existing confluent producer and its
`location_id:sensors_id` key convention, without importing Nam's CloudWatch
publishing, import-time dotenv loading or synthetic fallback.

[OpenAQ measurements](https://docs.openaq.org/api/operations/sensor_measurements_get_v3_sensors__sensors_id__measurements_get)
are queried with explicit UTC `datetime_from`, `datetime_to`, stable limit 100 and
page number. `--max-records 300` is a cap on newly acknowledged messages across
one CLI invocation and all stations, not per station or per API request. It can
fetch/stage more rows than it publishes; pending rows remain in the outbox. The
current budget divides remaining capacity among remaining pilots. Use
`--start`/`--end` to freeze a reproducible window (at most one day).
Default is the previous 24 hours ending five minutes before the command starts.
The source measurement time is `period.datetimeTo.utc`, matching the accepted
historical CSV convention. Only raw, unflagged, finite/nonnegative PM2.5 in exact
`µg/m³` units is accepted. Fractional-second/naive/non-UTC timestamps reject because
the unchanged Silver parser requires whole-second UTC values.

Kafka messages contain exactly these STRING fields:
`location_id`, `sensors_id`, `location`, `datetime`, `lat`, `lon`, `parameter`,
`units`, `value`. Provenance is retained separately in the private SQLite outbox:
endpoint, page, retrieval time and canonical payload hash. API response source IDs,
when supplied, must match the configured pair. Entire pages validate before writes.
An unavailable pilot (404) is reported and its cursor is retained; authentication,
malformed metadata and incompatible measurements fail instead of becoming mock data.

## Resume and safety

Repeat the same command/root/topic without `--create-topic`. Choose
`--max-records` in 3–1000. An invocation publishes zero to that configured limit
in total, shares the budget across available pilots and limits pages/retries. SQLite uses FULL synchronization and a single-writer POSIX lock.
Pending outbox rows are delivered before new pages. A delivered marker is committed
only after Kafka acknowledgement; a failed/ambiguous acknowledgement does not
advance that row. The stable record hash suppresses ordinary unchanged reruns.
Changed values/metadata become distinct source events, not silent overwrites.

A process crash after broker acceptance but before the SQLite commit can duplicate
a message. This is at-least-once, not an atomic Kafka/SQLite transaction. Silver's
existing `(location_id, sensors_id, measured_at)` deduplication remains authoritative.
The producer's idempotence helps only within its producer session. Page ordering
can change upstream; a fixed query window and one-hour overlap mitigate late
arrivals, but do not guarantee recovery of arbitrary late data beyond that overlap.
Historical backfill and metadata-correction policy remain separate decisions.

Cursor pages/window bounds survive record limits and restarts. Completed windows
revisit an hour of overlap; catch-up windows stay bounded to one day. Requested end
times cannot rewind a retained cursor. Retry-After integer delays up to 30s and
transient HTTP failures have at most three attempts; longer/date-based delays
stop for a later run. No endless retry or invented missing measurement occurs.
The outbox retains audit identities and grows over time; retention/compaction is
deferred and must not discard deduplication history without a deliberate policy.

## Isolated pipeline execution

After ingestion completes, select Java 17 and run the unchanged production
functions through the isolated launcher:

```sh
. scripts/use-java17.sh
python -B -m ingestion.local_pipeline --root /tmp/airlake-live-demo-UNUSED --execute
```

It uses only the existing verified Iceberg/Kafka JAR cache, with no package
resolution. A fresh child has a sanitized environment and `TZ=UTC`. Warehouses,
fallback catalog, Derby, spill and checkpoint paths stay below the external root.
A query/source binding rejects incompatible checkpoint reuse. The parent holds the
same writer lock to prevent concurrent ingestion during acceptance. Rows and all
five snapshot-ID sets must remain identical on the unchanged rerun.

## Actual acceptance: 2026-11-08

[Sanitized evidence](../tests/evidence/openaq_incremental_2026-11-08.json) records:

- Fixed source window `2026-11-07T02:45:37Z` through `2026-11-08T02:45:37Z`.
- API resume publication counts **224, 200, 135, 0**: **559** genuine measurements.
  Acceptance reused the same root/topic with `--max-records 300`,
  `--start 2026-11-07T02:45:37Z --end 2026-11-08T02:45:37Z` on each resume;
  `--create-topic` was used only on the first invocation.
- Pilot counts **288 / 247 / 24**. Gaps are not filled synthetically.
- Kafka independently consumed **559** messages and matched the acknowledged outbox;
  partition offsets `[0,559)`, with no audit-group offset commits.
- Bronze **559**, Silver **559**, zero rejected/deduplicated source rows;
  Gold **3 dimensions / 72 hourly facts**, seven breakpoint rows.
- Independent UTC grouping/mean comparisons passed. Unchanged pipeline reruns
  consumed zero new messages and preserved rows/snapshots for all five tables.
- 182 Python tests and the fresh 48-case isolated Gold suite passed.

API freshness is recorded from actual measurement timestamps, not only the
location's latest timestamp (which can describe another pollutant). The final
verification had latest PM2.5 `02:45Z / 02:45Z / 02:00Z`; age is labeled with the
verification instant. This is one bounded local acceptance, not a continuous feed
or cloud migration. Hourly values describe available observations in the bounded
batch, not guaranteed complete hourly coverage or official VN_AQI.

The first report serialized Python-collected timestamps using the host timezone.
Reporting was corrected to Spark UTC JSON and verified independently; the actual
Iceberg data/calculations and earlier warehouse snapshots were not rewritten.

```sh
python -B -m unittest discover -s tests -p 'test_*.py' -v
ruff check --no-cache real_producer.py scripts tests alerts bot_lambda ingestion
ruff format --check --no-cache real_producer.py scripts tests alerts bot_lambda ingestion
```

Runtime cursor, Kafka messages, source rows, logs and warehouse remain local-only.
No existing production warehouse/checkpoint or accepted AWS table was modified.
The original acceptance ended before Git staging or publication. Its evidence
remains historical; the later publication review is recorded separately.

## Reviewed source-request reconstruction

The 559 acknowledged unique records are traceable to these successful API pages.
All measurement requests used `limit=100`, the fixed UTC bounds above and the
listed `page`. Each pilot also used `GET /v3/locations/{location_id}` to validate
its name, coordinates, PM2.5 parameter and sensor membership before publication.

| Location / sensor | Measurement endpoint | Pages / unique acknowledged rows |
| --- | --- | --- |
| 4946812 / 13502151 | `GET /v3/sensors/13502151/measurements` | 1: 100; 2: 100; 3: 88 |
| 4946813 / 13502154 | `GET /v3/sensors/13502154/measurements` | 1: 100; 2: 100; 3: 47 |
| 6123215 / 14581375 | `GET /v3/sensors/14581375/measurements` | 1: 24 |

These counts were independently regrouped from the retained, read-only outbox
provenance during publication review. Later overlap/metadata requests created no
additional unique publication on the unchanged replay. The outbox does not retain
a complete HTTP attempt log, so these are the requests responsible for the accepted
rows, not an invented total request/retry count. The review did not refetch API data.

## Publication review on merged PR #6

Both module branches were safely fast-forwarded to main `3e25394` after preserving
all uncommitted source bytes. The complete Python suite now passes 241 cases:
227 main/alert tests plus the same 14 ingestion tests. The earlier 182-case record
remains unchanged. No ingestion or production transformation code needed a fix.
All five retained table snapshot sets match their current local metadata, and the
retained actual Spark count report matches the acceptance JSON. Metadata counts
were not substituted for independently collected row counts. The 48-case Gold
run remains the earlier actual execution; it was not repeated for documentation
changes. See [publication evidence](../tests/evidence/openaq_publication_2026-11-08.json).
