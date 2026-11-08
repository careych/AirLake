# Gold dashboard MVP

A working local Superset 6.1.0 dashboard renders retained, real July 2025 Gold
acceptance rows. It is explicitly **HISTORICAL / NOT LIVE**. No Athena query or
AWS deployment has been executed in this sprint. The fixture contains 23 hourly
facts and one accepted dimension; it does not invent the other two pilot stations.

## Start a fresh development instance

Use Python 3.12 with project dependencies and Docker Desktop. Port 18088 and the
chosen container name must be unused. Existing containers are never stopped or
removed by the helper. Runtime must be outside every Git checkout.

```sh
python -B -m dashboard.dev --root /tmp/airlake-superset-demo-UNUSED
# After http://127.0.0.1:18088/health returns OK:
python -B -m dashboard.install --root /tmp/airlake-superset-demo-UNUSED
```

The official image is pinned by digest in `dashboard/dev.py`. Superset metadata,
SQLite fixture and generated development credentials are private local files in
the external root. Username is `airlake_dev`; the generated password stays in
`private.json` for the local owner to inspect. Never paste it into chat or commit
that file. Installation authenticates internally and refuses duplicate setup.

The actual running instance from acceptance is:
`http://127.0.0.1:18088/superset/dashboard/airlake-historical-gold/`.
The acceptance runtime is a disposable external directory listed in the delivery
report, not a deployment dependency. Use a fresh root on another machine. Do not run a second instance against the same occupied port.

This development configuration allows a local SQLite data connection and disables
Talisman for loopback HTTP. It is not a public deployment template. No AWS config,
login cache, credential file or host data directory is mounted into the container.
Private runtime is mounted only at `/app/dev`; the host UID/GID owns it.

## Dashboard contents and reusable assets

- PM2.5 hourly trend, labeled `µg/m³` and observation hour UTC.
- Simplified MVP AQI hourly trend; no official VN_AQI claim.
- Station comparison of historical mean PM2.5 and mean hourly MVP AQI, explicitly
  labeled as presentation aggregates rather than a regulatory AQI calculation.
- Latest available observation by station, retaining measurement time in UTC.
- Age since the end of the observation hour and freshness status. `STALE (>24h)`
  is a display-age rule, not a health threshold.
- Native station filter across the compatible datasets. The fixture has one
  station; comparison/filtering supports more when real Gold dimensions exist.

`dashboard/assets/` contains ten actual Superset-exported YAML files (dashboard,
five charts, two datasets, one database, metadata). They contain no credentials.
Build the portable import archive, then use Superset's dashboard import API/UI:

```sh
python -B -m dashboard.assets --output /tmp/airlake-dashboard-UNUSED.zip
```

The generated bundle was actually re-imported into the isolated instance with
HTTP 200, and its five panels and filter were browser-tested after import. The
fixture database URI intentionally uses the portable `/app/dev/gold.sqlite` path.
No runtime metadata database or ZIP is checked into Git.

The exact table shapes match accepted Gold:
`pm25_aqi_hourly(location_id, hour_start, pm25_avg, aqi)` and
`dim_location(location_id, name, lat, lon, station_type)`.
`dashboard.prepare` builds fixture rows from retained acceptance JSON, records
source hashes and creates joined/latest-per-station views. It generates no
synthetic measurements. Tests use a clearly synthetic second station only to
verify per-station latest semantics; that row is absent from the demo database.

## Athena backend preparation — NOT EXECUTED

[Hourly SQL](../dashboard/sql/athena_gold.sql) and
[latest-by-station SQL](../dashboard/sql/athena_latest.sql) are read-only virtual
SELECTs over the accepted Glue Gold database. Both fact and dimension snapshots
are pinned, and the window is explicitly bounded to July 3–4, 2025 UTC. No Glue
view/table creation is needed. Replacing the SQLite views with these virtual
datasets requires explicit query approval and authenticated Athena access.

The optional driver image is built and its SQLAlchemy dialect was imported:

```sh
docker build -f dashboard/Dockerfile.athena -t airlake-superset-athena:6.1.0-dev .
```

It pins PyAthena 3.14.1, SQLAlchemy 1.4.54 and the tested SDK/dependency closure.
Building/importing the driver does not query Athena. A source configuration uses
the `awsathena+rest` dialect, region `ap-southeast-2`, catalog `AwsDataCatalog`, the
accepted Gold database and workgroup `airlake`. Its enforced result location is
`s3://airlake-701364614078-athena-results/`; a client URI cannot override that.
Do not put access keys or session tokens into the URI, checked-in assets or image.

Connection/query settings are now executable configuration, not instructions to
edit SQL by hand. The offline renderer accepts validated account, region, catalog,
database, table names, snapshot IDs, UTC bounds, workgroup, result prefix and an
optional runtime profile. It emits a credential-free SQLAlchemy URI and both
virtual SELECTs. It never connects or queries; enforced workgroup settings still
win over the proposed result URI.

```sh
python -B -m dashboard.athena_config
python -B -m dashboard.athena_config --workgroup approved-workgroup \
  --results-location s3://approved-results-bucket/reviewed-prefix/ \
  --profile approved-runtime-profile
```

The default SQL stays byte-identical to the reviewed static files. New windows
receive a window-specific label rather than incorrectly retaining July 2025.
Live connection registration/query approval remains separate.


Khuê must supply an approved runtime credential arrangement. The Mac's
`airlake-spark` credential_process references a macOS AWS executable and is not
portable into this Linux container. Prefer the existing AWS Superset execution
role once its runtime and least-privilege grants are verified; no EC2 is started
or deployed here. Merely mounting a host profile is not a verified solution.

Required scopes: Athena query APIs on the selected workgroup, Glue GetDatabase/
GetTable for the two Gold tables, S3 reads of their data/metadata and result
reads/writes at the enforced query-output scope. KMS/Lake Formation and shared
result-bucket constraints need owner confirmation. A live query can incur scan,
result-storage and request charges; a time predicate/row limit is not a spend cap.
No write permission is inferred from the historical cloud acceptance.

Teammate references inspected without merging:

- Khuê `b5a188d`: Superset EC2/profile definitions and `userdata/base.sh` Docker
  bootstrap. These files do not contain a complete Superset/chart bootstrap or
  prove a current working deployment. That IAM file attaches SSM/CloudWatch to
  the Superset role; Athena/data access still needs explicit verification.
- Thịnh `8af4f39`: existing Superset load-test script expects an already running
  service; it is not an importable dashboard. No credentials from it were copied.

## Actual verification: 2026-11-08

[Sanitized evidence](../tests/evidence/gold_dashboard_2026-11-08.json) records:

- Actual Superset health, authenticated dataset/chart query responses, export and
  successful import round-trip against the historical SQLite fixture.
- Five panels rendered in Chrome with all non-loopback requests blocked.
- Native station selection succeeded. A browser in `Asia/Ho_Chi_Minh` retained
  UTC chart-axis values; historical/stale labels remained visible.
- 174 Python tests, Ruff lint/format and dependency checks passed.
- Optional Athena-driver import passed, but **zero Athena queries were executed**.

Optional reproducible browser smoke test (an installed Chrome/Chromium executable
is required; the test blocks all non-loopback HTTP requests):

```sh
uv venv /tmp/airlake-dashboard-ui-env --python 3.12
uv pip install --python /tmp/airlake-dashboard-ui-env/bin/python -r dashboard/requirements-ui.txt
/tmp/airlake-dashboard-ui-env/bin/python -B -m dashboard.browser_smoke \
  --root /tmp/airlake-superset-demo-UNUSED --chrome /path/to/chrome
```

A first browser check caught a removed legacy `dist_bar` renderer. The final
installer/assets use the supported aggregate table instead; the final browser
run has no visualization errors. REST query success alone was not treated as
proof of a working rendered dashboard.

```sh
python -B -m unittest discover -s tests -p 'test_*.py' -v
ruff check --no-cache real_producer.py scripts tests alerts bot_lambda dashboard
ruff format --check --no-cache real_producer.py scripts tests alerts bot_lambda dashboard
```

The shortest cloud continuation is approval for the bounded SELECTs, a verified
Superset Athena credential/role configuration, then replacing the two virtual
datasets and independently checking authenticated dashboard queries. That is not
claimed complete. For today's local demo, show the actual API-to-local-Gold run
beside this clearly historical dashboard. No Git publication occurs before review.

## Publication review on merged PR #6

The branch was safely fast-forwarded to main `3e25394` with its original files
preserved first. Fresh validation passed 241 Python tests: 227 main/alert cases,
six existing dashboard cases and eight new offline Athena configuration cases.
The original 174-case acceptance record is historical and unchanged. Actual browser
rendering/filter/UTC checks were repeated successfully against the local fixture.
The new URI was parsed by the installed PyAthena dialect with networking disabled;
no Athena connection/query was made. See
[publication evidence](../tests/evidence/dashboard_publication_2026-11-08.json).
