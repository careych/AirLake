# AirLake Hanoi citizen demo

The new Vietnamese application is implemented under `citizen/web/`. Superset stays
an internal BI client. This release has no public query endpoint: Amplify serves a
small immutable, hash-verified projection of an already accepted AWS export. A
browser never receives AWS credentials or calls OpenAQ, the private API or Athena.

Current status: **private promotion PASS; both AWS Amplify branches deployed;
real HTTPS desktop/mobile-viewport and authenticated preview browser checks PASS**.
Public: https://public.dhyvgpdsnj4zi.amplifyapp.com/
Protected preview: https://demo.dhyvgpdsnj4zi.amplifyapp.com/
The user confirmed external access on 14 November 2026 in response to the
phone/separate-network check: Dragon, 40.5 µg/m³ and the stale label displayed
without login. This is user-reported browser evidence; the exact device/network
was not separately specified. The independent remote fetch tool could not retrieve
these new URLs and is not counted as a successful check. PR #17 is open, not merged, at `01464b7`; this worktree
starts at latest main `9354d21`. Only the necessary refresh/export source is reused
from PR #17; no teammate branch or PR #12 is merged.

## What the application does

- **Không khí quanh bạn:** station selection, real accepted PM2.5, observed time
  in Vietnam time, estimated-cadence freshness, and clear limited-coverage text.
  Missing, future, late and stale observations never become a healthy AQI badge.
- **Xu hướng & trạm quan trắc:** a source-aligned 24-hour window, explicit missing
  hours, station comparison where multiple authorized sources exist, source names
  and verified coordinates. Lines stop at missing hours. Hourly averages remain
  distinct from latest accepted source measurements.
- **Nhận cảnh báo Telegram:** explains controlled testing, with public signup
  disabled. No public bot link, message sending or successful-delivery claim is
  invented. Khuê must supply sanitized staging E2E evidence separately. The code
  contains subscription commands; deployment verification is a separate fact.
- **Dữ liệu nghiên cứu:** station and Vietnam-time filtering, licensed JSON/CSV
  with UTC timestamps, units and attribution. Unknown-rights sources cannot be
  downloaded. The separately authenticated internal API is documented below.

The view does not auto-refresh data. Its age labels update locally; no timer polls
an upstream source. Network failure retains already loaded real observations in
memory and displays an offline/error notice. The build and observed timestamps
are separate. Current accepted data span 2026-11-14 06:05–10:50 UTC; the hourly
chart displays 24 hours ending 11:00 UTC, with missing hours visible. Newer source
metadata does not become a fabricated new PM2.5 reading.

## Public versus instructor data

The bounded metadata review used eight GETs, no measurement ingestion or cursor
changes. Source details are retained privately in `source-coverage.json`.

| Station / sensor | Provider | Redistribution evidence | Publication |
| --- | --- | --- | --- |
| 4946812 / 13502151 | Hanoi Air Quality Monitoring Network | license metadata null | authenticated preview only |
| 4946813 / 13502154 | Hanoi Air Quality Monitoring Network | license metadata null | authenticated preview only |
| 6123215 / 14581375, Dragon | AirGradient | license 41, CC BY 4.0, redistribution/modification allowed, attribution srgillo | approved public branch |

The public bundle contains only Dragon: four verified hourly records and its last
accepted observation. Attribution names AirGradient/srgillo and OpenAQ, links to
CC BY 4.0, and explains AirLake's hourly aggregation. The protected instructor
bundle contains three stations, fourteen hourly facts, and all 120 accepted source
measurements represented in the retained provenance; raw measurements are not
embedded as a public dataset. It is never copied into the public branch.

OpenAQ [requires compliance with provider terms](https://docs.openaq.org/about/terms).
The [license endpoint](https://docs.openaq.org/resources/licenses) was checked;
absence of a license is not permission. The station license period includes the
observation dates. Unknown sources are excluded before public ZIP creation, not
merely hidden by JavaScript. No seven-day completeness or city-wide coverage is
claimed: retained records cover only a bounded window, and source coverage
percentages above 100% are rejected as evidence of uptime.

## Private promotion repair

`live_data/json_types.py` recursively converts integral Decimal to exact Python
integers and fractional Decimal to decimal strings. It preserves nulls and
normalizes timezone-aware processing timestamps to UTC without losing fractional
precision; non-finite numbers and naive datetimes fail. Only progress reports use
this normalization; Gold calculations, PM2.5 types, observation timestamp rules and
historical evidence are unchanged. `refresh.handler` normalizes both logged and
returned reports, covering the failure missed by tick-only tests.

The repair reuses query `e476332e-79e9-4b8a-8e59-c36b68437a77`, with fourteen verified
facts and 781 scanned bytes. `citizen.promotion` checks its complete stored results,
then permits exactly one controller invocation, a conditional pointer update and
three signed API GETs. It submits no SELECT and starts no polling/Glue job. The
controller stays scoped to its original OPS ledger and ten-minute deadline. The
previous pointer is retained. Stop is attempted in `finally`.

## Reproducible local build

Use Python 3.12 and the existing project environment. No Node package installation
or framework build is required. Inputs are private files, not repository fixtures:

```sh
python -B -m citizen.build --output "$NEW_BUILD/public" \
  --export "$PRIVATE_ROOT/verified-gold.json" \
  --inventory "$PRIVATE_ROOT/aws-inventory.json" \
  --coverage "$PRIVATE_ROOT/source-coverage.json" \
  --evidence "$PRIVATE_ROOT/final-readonly-evidence.json" --mode public
# The instructor build uses the same inputs and --mode instructor, a separate output.
python -B -m http.server 18119 --bind 127.0.0.1 --directory "$NEW_BUILD/public"
```

The evidence must prove the exact original S3 bytes and independent Athena result
comparison. Existing output directories are rejected to preserve prior versions.
A public build has an immutable `data/<SHA256>.json`; the browser verifies its hash
before displaying numbers. No private evidence, ZIP or password belongs in Git.

## AWS execution — completed once under exact approval; do not repeat

```sh
# Use the cached SDK/CRT environment if the AWS login profile requires it.
python -B -m citizen.promotion preflight --packet "$PROMOTION_PACKET"
python -B -m citizen.hosting preflight --packet "$HOSTING_PACKET"
# Historical execution commands, NOT instructions to rerun; new approval required:
python -B -m citizen.promotion execute --packet "$PROMOTION_PACKET" \
  --confirm-plan-sha256 "$APPROVED_PROMOTION_SHA256"
python -B -m citizen.hosting deploy --packet "$HOSTING_PACKET" \
  --confirm-plan-sha256 "$APPROVED_HOSTING_SHA256"
```

Resources: one Amplify `WEB` app `airlake-hanoi-citizen-20261114`, manual branches
`public` and `demo`, no repository connection, auto-build, service role or public
API. Two static deployments upload about 30 KB total to Amplify-managed URLs;
existing S3 lake policies never change. Native branch Basic Auth protects every
instructor resource. A generated random viewer password is saved only in private
`preview-login.json` (0600 under 0700 directory); it is not an AWS credential. Read
it only in a trusted local password manager/editor, not in chat or shared logs.

The deployer verifies anonymous access to the demo HTML, script and data returns
401, and checks real public/private asset hashes. On failure it attempts app-wide
password protection, retains job IDs and stops; there is no automatic deployment
retry or cleanup. Agent verification is capped at 40 website GETs, separate from
three private API checks. Browser asset loads count against that website allowance.

Costs are managed static-hosting storage/data transfer plus short existing Lambda,
S3/DynamoDB operations. No new Glue execution, SELECT, scheduled task or always-on
compute is authorized. Public traffic is not a guaranteed dollar cap. The safety
pause is password protection of this app; deletion is not part of the procedure.

External acceptance must include the actual HTTPS URL from `hosting-receipt.json`,
independent remote HTTP retrieval, and an incognito/phone browser without AWS CLI,
credentials, VPN or tunnel. Local screenshots alone never establish AWS hosting.
Actual hosted browser checks passed against the HTTPS branches. The user
subsequently confirmed the external-access check; this evidence is recorded
separately from the agent’s local Chrome results.

## 429 investigation

Eight bounded OpenAQ metadata/license calls all returned 200. Safe headers showed
60 requests per minute and remaining allowance 59 down to 52. Six-hour CloudWatch
Lambda Throttles totals were zero for the live-demo API and poller. These checks
do not identify a historical 429 without its request URL/time; no claim attributes
it to OpenAQ or AWS without that evidence. The client now handles case-insensitive
Retry-After seconds and HTTP-date forms with at most three attempts and a 30-second
maximum delay; longer delays stop the bounded run. This source change is not
silently deployed to the disabled poller. Citizen browsers never poll OpenAQ.

## Five-minute instructor walkthrough

1. **0:00–0:45:** open the actual public HTTPS URL on a phone/incognito. Show Dragon,
   its genuine value, Vietnam observation time, and stale label. Explain the
   restricted pilot; source metadata may be newer than accepted observations.
2. **0:45–1:45:** open trends, point out twenty missing hours versus four licensed
   hourly values. In the authenticated instructor preview, compare all three
   stations/fourteen facts. Explain that simplified MVP AQI is not official VN_AQI.
3. **1:45–2:30:** Telegram page: public signup is closed. Show only Khuê's independently
   verified, sanitized staging evidence, if provided. Otherwise state it is pending.
4. **2:30–3:15:** research: filter time/station and download licensed JSON/CSV with
   attribution. Unknown-rights downloads stay disabled. For the internal API, show
   retained signed HTTP evidence from the approved promotion, not browser AWS keys.
5. **3:15–5:00:** AWS Console: show the two scheduled polls, original successful Glue
   JobRunId, the existing successful Athena query, schema/snapshots and CloudWatch
   logs. End on the disabled scheduler/concurrency evidence and immutable provenance.

Fallback: the deployed immutable, clearly dated snapshot and its gaps remain
available if the upstream stops reporting. If the network is unavailable, use the
retained local screenshots explicitly as offline evidence, not as proof of current
external availability. No replay, source collection or new SELECT is needed.

## Tests and handoff

Focused Python tests cover actual handler serialization, precision, licensing,
field whitelists, time filters, backoff, mutation confirmation and branch-auth
configuration. Opt-in local Chrome test `tests/citizen_browser.py` uses temporary
loopback servers and real private build inputs; it does not contact AWS. Screenshots
and licensed downloads remain outside Git. Run Ruff/format and `git diff --check`.
No Gold source or historical acceptance file changed; the 48-case historical Spark
baseline is not presented as newly rerun.

Remaining ownership: Khuê supplies Telegram E2E and operations screenshots. The
citizen site's Telegram feature remains partial until that is verified. Further
station publication, full seven-day coverage and automatic web refresh need new
source/rights validation and bounded execution approval.


## Completed acceptance and operational handoff

The approved promotion plan was
`15b69f98004246be31393cc73b4ffdb5d666470f456e4d8ee3541d6901f65c41`;
the hosting plan was
`cda55682f3d3de79ed56892a468caa7d371c3c348d7d301e368b9e37327ece38`.
The selected API export is now
`d3226cf36a89bf6d543d4eb2f075da3c912a2f0ea8e12db48cd4fcb0dbc8b803`.
Its predecessor `fd3ae933ebb13c3ef989eee809c2aebc586b4a18007a8d6c54977a05233b99d9`
is preserved. Three signed GETs returned 200: latest observations for three
stations, fourteen hourly JSON rows and fourteen equivalent CSV rows. There were
no new polling attempts, Glue runs or Athena SELECTs in this promotion.

Amplify app `dhyvgpdsnj4zi` has two successful manual deployment jobs, both ID `1`
within their respective branches. Auto-build is disabled. Anonymous demo requests
to HTML, app.js, config.js, styles.css, icon.svg and the exact data JSON all returned
401. Authenticated Chrome rendered all three station selections and fourteen
comparison points; unlicensed downloads remained disabled. Public Chrome verified
Dragon, 40.5 micrograms per cubic metre at 10:00 UTC / 17:00 Vietnam time on
14 November 2026, the stale label, four hourly points, twenty explicit missing hours,
and licensed JSON/CSV downloads. A 390-pixel mobile viewport had no overflow.
These are bounded immutable observations, not a currently updating public feed.

The local evidence directory contains promotion/hosting receipts, browser results
and screenshots. It also holds the private viewer credential, so the directory
must not be uploaded to Git or shared as a whole. The curated public acceptance
record is `tests/evidence/hanoi_citizen_2026-11-14.json`. The full private HTTP
responses and instructor screenshots are deliberately excluded from publication.

Final read-only checks must show schedule DISABLED, poller/refresh reserved
concurrency zero, no active ingestion/controller lease, and AWS_IAM on the private
API URL. No Telegram staging or historical table was changed. No measured bill
is claimed; Amplify storage/egress and retained AWS data/log/secret costs continue.

To pause the website later, use Amplify Console, app `dhyvgpdsnj4zi`, Hosting >
Access control, and enable app-wide password protection using the private viewer
credential. This preserves both deployments. Do not delete branches or data. This
is a future state change, not an instruction executed by this delivery. The
protected demo password stays in the owner's local private packet and can be
opened only in a trusted local editor; never paste it into chat or a command line.

Rolling back the private API requires a separately reviewed conditional pointer
update from the current SHA to the retained predecessor; do not overwrite an
unknown newer version. A frontend rollback would be a new manual deployment and
requires another approval. No rollback, redeployment or cleanup is automatic.
Keep the scheduler disabled and both worker concurrency limits zero for this demo.
