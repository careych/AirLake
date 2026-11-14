# Historical Hanoi pilot: actual coverage and release

The hosted site still serves the first sparse bounded snapshot. This release
implements a larger September 2026 product and passed local real-data acceptance.
**The historical AWS job, API extension and replacement website have not deployed.**
PR #17 is merged at `36f454b64508ea3637be5dceb2512feb95256996`.
PR #12 remains excluded. The two cosmetic-only release plans were cancelled.

## Real data

The read-only scan selected September in Asia/Ho_Chi_Minh:
`2026-08-31T17:00:00Z <= observation < 2026-09-30T17:00:00Z`.
It read 96 existing OpenAQ archive gzip files, including boundary days, and
82,083 rows across parameters. Exactly 17,181 unique PM2.5 records passed the
supported station/sensor/unit and finite-value contract. No PM2.5 rejects,
duplicate identities or invented observations entered this selection.

| Location / sensor | PM2.5 rows | Hour buckets / 720 | Missing hours | Complete days / 30 | Rights |
| --- | ---: | ---: | ---: | ---: | --- |
| Nhân Chính 4946812 / 13502151 | 8,620 | 720 | 0 | 30 | Unresolved; internal only |
| Giải Phóng 4946813 / 13502154 | 7,841 | 693 | 27 | 18 | Unresolved; internal only |
| Dragon 6123215 / 14581375 | 720 | 720 | 0 | 30 | CC BY 4.0, license 41 |

The frozen adapted JSONL SHA-256 is
`31fcd9ee61d9989c22db2fe95b08e27c370f67cc91b7976d9ccefaa7bd666b46`.
The private manifest records every relative source filename, hash and row
provenance. Source files remain unchanged; actual data are excluded from Git.
Nine bounded OpenAQ GETs returned HTTP 200 for 1/7/30-day windows ending
13 November 17:00 UTC, two samples each. Those 18 samples prove availability,
not recent-month completeness. Three official archive HEADs matched cached
sample file lengths and single-part ETags, independently checking three files,
not all 96. No bulk source download ran. Earlier cached periods exist but have
not received a full coverage acceptance in this release.

Verified coordinates are 21.0031/105.7947, 21.0052/105.8418 and
20.9933/105.9441 respectively. Historical source names are retained separately;
Dragon's same sensor was formerly named OceanPark. Nhân Chính has 5,176 numeric
coordinate serialization differences within 1e-10 degrees of verified metadata.
Only that serialization noise is normalized, preserving the original strings;
larger differences reject. Gold metadata safeguards are unchanged.

Dragon's license permits redistribution/modification under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), with attribution to
AirGradient/srgillo and OpenAQ. Only Dragon is in public projections. Unknown
licenses block downloads even in the protected internal preview. Password
protection is not redistribution permission. No citywide coverage is claimed.

## Computation and interpretation

The new adapter loads the original nine-field schema into isolated Bronze, then
calls unchanged production Silver/Gold functions. The legacy `kafka_ts` stores
adapter ingestion time: this archive path uses S3, not Kafka transport.
Source observation time and ingestion/processing times remain distinct.

The actual local Spark run produced 17,181 Silver rows, seven breakpoints,
three dimensions, 2,133 hourly facts and 90 daily rows. Every hourly mean matched
an independent source calculation. An unchanged Silver/Gold rerun preserved all
six Iceberg snapshot sets. This is local Spark/Iceberg acceptance, not AWS
acceptance. Earlier 48-case Gold results remain historical evidence, not a new run.

Daily values are equal-weight means of available Gold hour buckets by Vietnam
calendar date. Incomplete days retain actual hour counts and coverage.
[OpenAQ timestamps](https://docs.openaq.org/using-the-api/dates-datetimes) describe
time-ending measurement intervals; this adapter preserves source times and the
approved Gold bucketing contract. Daily summaries are descriptive, not a new
regulatory exposure calculation.

Monthly extrema require a full selected calendar month and at least 80% complete
days, and compare observed complete days only. Giải Phóng fails that guard.
Hour-of-day patterns need seven distinct days per bin; weekday/weekend comparison
needs three complete weekdays and two complete weekend days. Missing cells stay
empty. No forecast, safe badge, city average or spatial interpolation is produced.
The simplified hourly index is explicitly not official VN_AQI.

The [WHO PM2.5 guideline](https://www.who.int/teams/environment-climate-change-and-health/air-quality-and-health/health-impacts/types-of-pollutants)
is 15 µg/m³ for a 24-hour mean, with its annual percentile context. It is never
applied to one hourly value or presented as personal medical advice.

## Implemented citizen and research features

Six Vietnamese pages cover latest accepted values, historical trends, station
comparison, sensor status, research exports and honest Telegram availability.
Latest bounded November values remain separate from September history. Source
metadata timestamps never substitute newly ingested values.

Views include 24-hour/7-day/30-day filters anchored to the dataset end, daily bars,
heatmap, measured extrema, hour patterns and weekday/weekend comparison. Public
research exports contain 720 hourly or 30 daily rows, with station/sensor/date/
granularity selection, table preview, JSON/CSV, actual counts and attribution.
The additive private `/api/v1/history` route supports hourly/daily JSON/CSV,
limit 1–1,000, offset at most 3,000 and strict UTC filters. Daily filters align
to Vietnam midnight. Existing routes and IAM authentication are preserved.

The website is a versioned static projection, not an automatic feed. Observed,
accepted, processed and dataset-verification times are separate. Deployment time
belongs in its receipt, not an invented measurement timestamp. Identical inputs
build identical JSON and ZIP bytes. Khuê owns the separate Telegram E2E;
registration remains disabled and no real delivery is claimed here.

## Local commands

Use the locked Python 3.12 environment, Java 17 and the already verified
Iceberg 1.12.0 Spark 3.5/Scala 2.12 JAR. All data/evidence paths are private and
outside Git; no automatic download occurs.

```sh
python -B -m historical.prepare --help
python -B -m unittest discover -s tests -p 'test_historical*.py' -v
python -B tests/historical_local_integration.py --source "$FROZEN_SOURCE" \
  --runtime-jar "$AIRLAKE_ICEBERG_RUNTIME_JAR" --execute-local
python -B -m historical.build --output "$PUBLIC_PREVIEW" \
  --export "$LOCAL_EXPORT" --audit "$SOURCE_AUDIT" --metadata "$LICENSE_PROOF" \
  --latest "$ACCEPTED_LATEST" --source-metadata "$SOURCE_METADATA" \
  --evidence "$LOCAL_EVIDENCE" --mode public --allow-local-preview
# Build instructor mode into a different private directory, then:
python -B tests/historical_browser.py --public "$PUBLIC_PREVIEW" \
  --demo "$PROTECTED_PREVIEW" --reference "$LOCAL_EXPORT" --evidence "$NEW_BROWSER_EVIDENCE"
```

Ten actual browser checks passed on desktop, tablet and 390px phone widths:
all download values versus Spark, filtering, gaps, keyboard controls, readable
axes, no page overflow, research preview and friendly rate-limit errors.
Synthetic failure fixtures are labelled as tests, never real observations.

## Consolidated AWS release — NOT EXECUTED

Scope: account 701364614078, region ap-southeast-2, profile airlake;
new run `20261114_7a6e91c2`, role/job `airlake-history-20261114-7a6e91c2`,
database `airlake_history_20261114_7a6e91c2`. Six tables:
`bronze_measurements`, `aq_measurements`, `aqi_breakpoints`, `dim_location`,
`pm25_aqi_hourly`, `pm25_daily`.

All new lake writes are under
`s3://airlake-701364614078-lake/historical-demo/20261114_7a6e91c2/`.
Four uploads contain frozen adapted records (~8.3 MB), manifest, code ZIP and
job script; reuse the two existing verified JARs. One Glue 5.0 run: two G.1X,
ten minutes, zero retries, concurrency one. Exactly two pinned Athena SELECTs
compare all 2,133 joined hourly rows and all 90 daily rows, without result reuse.

Only after both pass: immutable proof, one existing API-role inline policy
allowing GetObject for exactly export/proof, patched code and three pinned history
environment fields. Five signed GETs verify new history plus original routes.
Only then derive the two website ZIPs from verified output and frozen assets:
public 720+30 rows, protected 2,133+90; combined ZIP cap 1 MB. Two manual
deployments to existing Amplify branches preserve credentials and security.
A NEW 40-GET website budget must be included in approval.

```sh
# Offline preparation and read-only preflight:
python -B -m historical.cloud prepare --packet "$HISTORY_PACKET" --source "$FROZEN_SOURCE" \
  --metadata "$LICENSE_PROOF" --latest "$ACCEPTED_LATEST" \
  --source-metadata "$SOURCE_METADATA" --credentials "$PREVIEW_LOGIN_FILE" \
  --previous-hosting-receipt "$HOSTING_RECEIPT" --previous-api-zip "$API_ROLLBACK_ZIP"
python -B -m historical.cloud preflight --packet "$HISTORY_PACKET"
# NOT EXECUTED: every following command needs exact new scope approval:
python -B -m historical.cloud run-glue --packet "$HISTORY_PACKET" --confirm-plan-sha256 "$APPROVED_HISTORY_SHA"
python -B -m historical.cloud status --packet "$HISTORY_PACKET"  # read-only
python -B -m historical.cloud verify-athena --packet "$HISTORY_PACKET" --confirm-plan-sha256 "$APPROVED_HISTORY_SHA"
python -B -m historical.cloud publish-api --packet "$HISTORY_PACKET" --confirm-plan-sha256 "$APPROVED_HISTORY_SHA"
python -B -m historical.cloud smoke-api --packet "$HISTORY_PACKET" --confirm-plan-sha256 "$APPROVED_HISTORY_SHA"
python -B -m historical.cloud publish-site --packet "$HISTORY_PACKET" --confirm-plan-sha256 "$APPROVED_HISTORY_SHA"
# Within the same approved 40-GET budget; exact HTTPS bytes and six viewports:
python -B tests/historical_hosted_browser.py --packet "$HISTORY_PACKET" --confirm-plan-sha256 "$APPROVED_HISTORY_SHA"
```

Plan pins source, code, rollback ZIP and private publication inputs. No polling,
scheduler, cursors, Telegram, old table or credential changes. Read checks do not
prove write permission. Failed/ambiguous writes retain a receipt and stop; never
erase receipts or resubmit. Any stop-job or recovery write needs approval.
No automatic cleanup runs. Retain all snapshots and job-1 website/API rollback
ZIPs. Rollback is separately approved restoration of retained code, previous
environment or website bytes, preserving IAM URL and API pointer. No deletion.
Compute scope is nominally 20 DPU-minutes plus startup/billing overhead, with
Athena, S3, Lambda, logs and Amplify traffic/storage costs. Retained storage costs
continue; no actual bill is asserted.

## Five-minute demonstration after cloud acceptance

0:00–0:45 public HTTPS on a phone: Dragon's genuine measurement time, stale
label and station context. 0:45–1:45 select 7/30 days, show September trends,
heatmap, 720 observed hours and descriptive high/low periods. 1:45–2:30 station
status and authorised protected comparison of 693 common hours; explain gaps.
2:30–3:15 actual Telegram availability or Khuê's separately verified evidence.
3:15–4:00 filter/download 720 hourly or 30 daily licensed rows, explain units,
UTC versus Vietnam dates and attribution. 4:00–5:00 recorded Glue JobRunId,
six snapshots, two Athena IDs, signed API results and Amplify jobs. Network
fallback recordings/screenshots must be labelled as past executions.
