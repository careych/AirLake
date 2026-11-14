# Clean Data API: bounded historical Gold

This localhost API serves the **72 real hourly Gold facts and three station
dimensions** retained in the accepted November 7–8, 2026 AWS batch. It reads a
SHA-256-verified copy of that cloud acceptance record once at startup. It does
not query Athena per HTTP request, refresh ingestion, or represent current air
quality. No synthetic fixture is used by the demo server.

The server uses Python 3.12 standard-library modules. No framework, new AWS
service, database, static access key, or dependency-lock change is required.
Gold calculations and Iceberg tables are unchanged.

## Start and smoke-test

From this branch, activate the existing project Python environment. Set
`EVIDENCE` to the owner's retained `openaq_mvp_cloud_2026-11-08.json`, outside Git.
The required SHA-256 is
`21d76b6bd19d906bce9601d6ae4b3e6724a9cab62c23cb5d2f94b10b5815a50a`.
The full dataset is not included in this PR. An altered or missing artifact
stops startup; there is no synthetic fallback or automatic download.

```sh
python -B -m clean_api serve --evidence "$EVIDENCE" --port 18090
```

The server binds **127.0.0.1 only**. It refuses an occupied port and does not stop
other services. Leave it running in its Terminal. In another Terminal:

```sh
python -B -m clean_api smoke --evidence "$EVIDENCE"
```

The smoke command checks actual health, JSON, CSV, stations, station/time
filtering and pagination. It submits zero Athena queries.

```sh
curl --fail --silent --show-error http://127.0.0.1:18090/api/v1/health
curl --fail --silent --show-error \
  'http://127.0.0.1:18090/api/v1/gold/hourly?location_id=4946812&limit=2'
curl --fail --silent --show-error --get \
  --data-urlencode 'start=2026-11-07T03:00:00Z' \
  --data-urlencode 'end=2026-11-07T04:00:00Z' \
  --data-urlencode 'location_id=4946812' \
  http://127.0.0.1:18090/api/v1/gold/hourly
curl --fail --silent --show-error \
  'http://127.0.0.1:18090/api/v1/gold/hourly?format=csv&limit=100'
curl --fail --silent --show-error http://127.0.0.1:18090/api/v1/stations
```

## HTTP contract

| Endpoint | Response |
| --- | --- |
| `GET /api/v1/health` | API/dataset health and counts; `athena_health: "not_checked"` |
| `GET /api/v1/gold/hourly` | `data`, `count`, filtered `total`, relative `next` link, `metadata` |
| `GET /api/v1/stations` | Actual dimension fields: `location_id`, `name`, `lat`, `lon`, `station_type`, with metadata |

Hourly records have these fields; `station_name` is joined from `dim_location.name`:

| Field | Type / meaning |
| --- | --- |
| `location_id` | Integer Gold location key |
| `station_name` | String from the pinned Gold dimension |
| `hour_start` | ISO-8601 UTC string ending in `Z`; start of the hourly bucket |
| `pm25_avg` | Finite nonnegative hourly PM2.5 mean, in µg/m³ |
| `aqi` | Integer simplified hourly MVP AQI; **not official VN_AQI** |

Optional parameters: `location_id` (alias `station_id`), `start`, `end`, `limit`,
`offset`, `format`. Use only one station parameter. Accepted location IDs are
4946812, 4946813 and 6123215. Times must be explicit UTC (`Z` or `+00:00`) and stay
inside `[2026-11-07T02:00:00Z, 2026-11-08T03:00:00Z)`. `start` is inclusive;
`end` is exclusive. Defaults cover that entire historical window.

`limit` defaults to 50, maximum 100; `offset` is 0–1000. Stable ordering is by
`location_id, hour_start`. Follow the relative JSON `next` URL or CSV `Link`
header. Offset pagination is stable because the source is immutable and pinned.
An empty result is `200` with an empty list. Invalid/unknown/repeated parameters
return `400`; unknown routes `404`; write methods `405`; foreign Host/Origin
headers `403`. No unrestricted CORS is enabled.

Use `format=csv` or `Accept: text/csv`. CSV retains the five field names, with
provenance/unit/historical headers and pagination headers. Spreadsheet-formula
prefixes in station names are escaped in CSV only; JSON preserves the name.
JSON metadata includes units, historical scope, OpenAQ attribution, license
status, source account/catalog/database and **both Iceberg snapshot IDs**.
No source file paths, credentials, or tokens are returned.

## Separate live Athena verification: explicit approval required

The dashboard query budget does not authorize API queries. `serve` and `smoke`
never construct AWS clients. Only the following explicit verifier submits SQL,
and only after the project owner approves it. **Do not run without approval.**

```sh
API_VERIFY_ROOT=$(mktemp -d)
chmod 700 "$API_VERIFY_ROOT"
uv run --offline --no-project --with 'boto3==1.43.115' \
  --with 'botocore[crt]==1.43.115' python -B -m clean_api verify \
  --evidence "$EVIDENCE" --base-url http://127.0.0.1:18090 \
  --approve-selects 2 --report "$API_VERIFY_ROOT/verification.json"
```

`boto3` is already a project dependency. The `crt` extra is needed by the
existing `aws login` provider; it is used in an isolated environment and does
not alter the project lockfile. With an empty package cache, prepare that
environment first using public PyPI, then use `--offline` for verification.
No credentials are exported, logged, or written by this application.

The verifier uses profile `airlake`, account `701364614078`, region
`ap-southeast-2`, enforced engine-3 workgroup `airlake`, and only database
`airlake_it2_20261108_37179997ad2b42959f4387637e003ab8_gold`:

1. Hourly facts joined to dimension names, pinned to fact snapshot
   `2020624562194805942` and dimension snapshot `5513456602871200404`, three
   locations, the 25-hour UTC window above, and `LIMIT 73`.
2. Five dimension columns at the same dimension snapshot, three locations,
   `LIMIT 4`.

The exact fixed SQL is in `clean_api/verify.py`; client filters never enter SQL.
The verifier checks account, region, workgroup/output configuration, schemas,
query context, pagination and values against the actual running JSON/CSV and
stations endpoints. Float comparisons use absolute tolerance `1e-12`, zero
relative tolerance. Health remains API-only; a past verification is not a
continuous AWS health check.

Each submission is recorded before the request, with explicit `query_id: null`
until returned. A failed/ambiguous submission stops; there is no automatic
resubmission, cancellation or cleanup. A timeout can leave a query running.
Evidence is new, mode 0600, outside Git in a 0700 directory. Keep the query IDs
for separately authorized inspection. At most two SELECTs are attempted.
Athena result objects use the workgroup-enforced existing results bucket and
can incur Athena/S3 costs; Gold data and infrastructure are never modified.

## Attribution and distribution boundary

[OpenAQ's terms](https://docs.openaq.org/about/terms) require compliance with
upstream provider terms. Its [license metadata](https://docs.openaq.org/resources/licenses)
describes provider permissions and restrictions. Those per-provider licenses
were **not retained in this acceptance artifact**, so this implementation does
not claim redistribution clearance or assign a license to the observations.
It preserves OpenAQ/station/source provenance and stays localhost-only. Confirm
the three providers' attribution and redistribution requirements before
public hosting or distributing the data. The repository contains no new real
observation export; unit tests use explicitly synthetic schema fixtures.

## Tests and limitations

```sh
python -B -m unittest discover -s tests -p test_clean_api.py -v
ruff check clean_api tests/test_clean_api.py
ruff format --check clean_api tests/test_clean_api.py
```

Mocked AWS tests prove orchestration and safety, not live AWS integration.
Retained-data smoke tests prove the HTTP service serves the accepted real copy.
Only a successful separately approved two-query report establishes that the
running endpoints match the actual cloud snapshots. New data windows/snapshots,
public authentication/rate limiting, a Silver endpoint and continuous refresh
remain outside this bounded MVP. The standard-library demo server is not a
public production deployment.
