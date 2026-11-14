> Historical first-release and cancelled-UX record. For the current implementation and new approval gate, see [Historical product release](historical-product.md).

# Hanoi citizen application: release and final demonstration

The existing AWS site is https://public.dhyvgpdsnj4zi.amplifyapp.com/.
The restricted preview is https://demo.dhyvgpdsnj4zi.amplifyapp.com/.
The first release passed external user access. The cancelled cosmetic revision added five-page UX
improvements that passed local browser tests; its AWS update is separately gated.
Superset remains the working internal BI client. No new hosting stack is needed.

## Owner login

`public` needs no login. `demo` uses branch-level Basic Auth, username
`airlake-demo`. Superset has a different admin login; the private research API uses
signed IAM requests. None of those credentials belong in browser JavaScript.

The successful first deployment's `hosting/preview-login.json` has exactly
`username` and `password`, both strings. The file must remain 0600 inside its
private directory. Set `PREVIEW_LOGIN_FILE` to that retained file in a trusted Mac
Terminal, then copy its password without printing it:

```sh
python3 -B - <<'PY'
import json, os, stat, subprocess
from pathlib import Path
p = Path(os.environ['PREVIEW_LOGIN_FILE'])
assert stat.S_IMODE(p.stat().st_mode) == 0o600
v = json.loads(p.read_text())
assert set(v) == {'username', 'password'} and v['username'] == 'airlake-demo'
subprocess.run(['/usr/bin/pbcopy'], input=v['password'].encode(), check=True)
print('Password copied; paste into the protected demo login.')
PY
```

Amplify returns a service-encrypted password component in GetBranch. Comparing
that component with the original plaintext password is invalid. The retained
password passed a real authenticated HTTP 200 check against the original HTML
hash; no password reset or website redeployment was required. The new release
also verifies anonymous 401 and authenticated 200 before deployment writes.

## UX defects and corrections

| Defect | Correction and regression |
| --- | --- |
| Localized hour plus appended `h` produced `18 giờh` | Extract the hour part and render `18:00`; explicit UTC+7 axis |
| A 940-unit SVG scaled text to unreadable mobile size | Measure actual chart width; readable axis font and mobile ticks |
| Tiny navigation tap targets | Five visible navigation items, at least 44px tall |
| Measurement, receipt and website build time were conflated | Separate observed/ingested/processed/prepared timestamps |
| Freshness hidden across pages | Dedicated Tình trạng page, station cards, source cadence and gap counts |
| Invalid/empty time filters left downloads enabled | Disable downloads with a visible reason; real licensed JSON example |
| Chart points required mouse hover | Focusable points with keyboard and pointer readout |
| Header-fetch timeout did not cover response-body reading | One bounded abort deadline through response decoding |
| An unchanged input could create another prepared version | Content fingerprint, lock, versioned artifacts and unchanged skip |

No health category is inferred from instantaneous PM2.5. The optional experimental
hourly index is explicitly not official VN_AQI. Old or missing readings never
become green/safe classifications. Unknown redistribution rights remain a reason
to block downloads even inside the password-protected preview.

## Actual coverage and timestamps

| Station | Coordinates | Accepted source records | Hourly facts / 24 | Public rights |
| --- | --- | --- | --- | --- |
| 4946812, Nhân Chính | 21.0031, 105.7947 | 58 | 5 / 24 | unresolved; protected only |
| 4946813, Giải Phóng | 21.0052, 105.8418 | 58 | 5 / 24 | unresolved; protected only |
| 6123215, Dragon | 20.9933, 105.9441 | 4 | 4 / 24 | CC BY 4.0; AirGradient/srgillo and OpenAQ attribution |

The frozen 120 accepted identities cover 14 November 2026, 06:05–10:50 UTC.
Fourteen station/hour keys are unique; missing cells are left empty. The served
24-hour comparison ends at 11:00 UTC. This is a sparse pilot, not citywide coverage
or a complete daily/weekly history. The public bundle contains only Dragon, four
hourly facts and its last accepted value, 40.5 µg/m³ at 10:00 UTC / 17:00 Vietnam.
It does not include either restricted station's dataset.

Three read-only sensor metadata GETs at 14:54 UTC returned HTTP 200. Upstream
reported last observations at 14:35 UTC for the two network sensors and 14:00 UTC
for Dragon. These are source metadata, not new values ingested into AirLake.
Website values retain their earlier observation times and stale status.

The retained earlier discovery sampled seven-day availability: two sample rows
were available for each pilot; that proves some availability, not seven-day
completeness. Five other nearby candidates had unresolved licenses, and several
were long stale in that survey. This task does not add them or imply current
administrative-city coverage. Provider coverage figures above 100% are not uptime.

## Current update state and safe refresh

Fresh read-only state on 14 November: scheduler DISABLED; poller/refresh reserved
concurrency zero; cumulative admitted poll attempts six; no active lease. The
private API remains AWS_IAM, selected export SHA
`d3226cf36a89bf6d543d4eb2f075da3c912a2f0ea8e12db48cd4fcb0dbc8b803`.
The predecessor is preserved. The accepted Glue run and Athena query are reused;
this UX task has not run another poll, Glue job, SELECT or private API smoke.

The website is a cached immutable data projection, not an automatic live feed.
Its preparation timestamp is not a measurement timestamp or proof of source
freshness. The status page labels the time of its last operational verification.
The existing bounded controller can process changed input and conditionally
promote an independently verified export, but its six-poll budget is exhausted.
Never reset that counter or enable recurring spending through these commands.

`citizen.refresh` makes repeatable local publication preparation possible. It
requires accepted export bytes, independent Athena reconciliation evidence,
station metadata and license proof. Identical content skips; changed content
creates a new private version and preserves its predecessor. Locks prevent
concurrent preparation. It never calls AWS, polls, deploys or refreshes a browser.
Use private paths outside Git and runtime data:

```sh
python -B -m citizen.refresh --workdir "$PRIVATE_ROOT/site-preparation" \
  --export "$VERIFIED_EXPORT" --inventory "$VERIFIED_INVENTORY" \
  --coverage "$LICENSE_PROOF" --evidence "$ATHENA_COMPARISON"
# Explicit local build only; NOT an AWS operation:
python -B -m citizen.refresh --workdir "$PRIVATE_ROOT/site-preparation" \
  --export "$VERIFIED_EXPORT" --inventory "$VERIFIED_INVENTORY" \
  --coverage "$LICENSE_PROOF" --evidence "$ATHENA_COMPARISON" --execute-local
```

New source collection, processing/query budgets and unattended website publication
need another bounded approval. Static age labels update locally without polling
OpenAQ. Source 429 handling respects Retry-After seconds/date, at most three
attempts and 30 seconds delay; longer waits stop the bounded run. All three new
metadata requests succeeded. No URL/time for the earlier reported 429 is available,
so its origin remains unverified. The UI never displays raw service errors.

## Cancelled cosmetic release — do not execute

The owner cancelled both plans
`8d6f5b1c8cab38529ba6d1e01abc1588b2b57de49693fc7c19241b0f63682f80`
and `220dee56ee588110cd8b58e583bfa5d27a4cbce36a8125e118c174a5c7b7441d`.
Neither deployment ran. The 36,670-byte cosmetic packet must not execute.
Private artifacts and tests remain retained as historical evidence. AWS job-1
sites are unchanged. The replacement is the larger data release in
[Historical product release](historical-product.md), with 720 licensed public
hourly rows and 30 daily summaries, not another four-row cosmetic deployment.

All table and source-state counts above describe the first hosted November
snapshot, not the expanded September implementation. The new 2,133-hour backfill
has passed local Spark and browser acceptance but has not run on AWS.
No new hosting allowance is granted by the cancelled plans.

## Tests and source integration

The focused run passed 113 Python tests. The controller/operations run passed 36
including the added timestamp/Decimal regressions; suites overlap. New release
safety tests cover canonical header validation, immutable scope, auth failure,
ambiguous upload, no recreation, idempotent local preparation and artifact tamper.
Twelve local browser checks passed on accepted data at desktop, tablet and 390px,
including navigation history, error/empty/offline states and licensing guards.
JavaScript is buildless; Node syntax and browser runtime validation are used,
not a claimed TypeScript compilation. There are no new runtime dependencies.

```sh
uv run --frozen --offline --with 'botocore[crt]==1.43.115' python -B -m unittest \
  discover -s tests -p 'test_citizen*.py'
# Requires an already installed Playwright/Chromium; never downloads a browser:
python -B tests/citizen_browser.py --public "$PUBLIC_BUILD" \
  --instructor "$PROTECTED_BUILD" --evidence "$NEW_BROWSER_EVIDENCE"
# Linux/WSL can specify --browser-executable /path/to/existing/chromium.
```

PR #17 is now merged at `36f454b`. The citizen branch starts at its corrected
commit `165834c`; latest main has the same base tree. Review the larger historical
product PR against main and retain the deployed controller fixes. Do not merge PR #12 or any mixed teammate
branch. Source publication does not authorize cloud operations or automatic merge.

## Telegram contract for Khuê

The current code supports `/subscribe [location_id ...]` and `/unsubscribe`.
It does not implement a per-subscriber threshold-setting command. Production
registration/delivery is not established by a local handler test. The UI therefore
has no public bot link and no active signup button. Test-only thresholds, exactly
one approved recipient, deterministic event IDs, conditional delivery ledger,
stale-data rejection and partial-batch retries remain the staging contract.

Latest-source records are not interchangeable with Gold alert observations.
The evaluator requires the hourly station key, UTC hour, finite PM2.5 in µg/m³,
MVP AQI provenance, source snapshot and an explicit versioned rule. A corrected
hour changes its content/event identity according to that contract; duplicate
replays must retain delivery outcomes. Do not synthesize AQI from latest state.
See `docs/athena-to-telegram-mvp.md` and `docs/telegram-staging-one-test.md`.
Khuê supplies sanitized real E2E evidence and decides when staging is ready;
this release never reads her secret, subscribers, private identity or ledger.

## Five-minute Vietnamese walkthrough

1. **0:00–0:45:** open public HTTPS on a phone. “Đây là thí điểm có một nguồn được
   phép công bố, không phải bản đồ chất lượng không khí toàn Hà Nội.”
2. **0:45–1:40:** choose Dragon. “40,5 µg/m³ được đo lúc 17:00, AirLake nhận lúc
   18:05. Nhãn dữ liệu đã cũ nói về thời gian, không phải mức an toàn.”
3. **1:40–2:25:** Xu hướng: point to readable timestamps, four measured hours and
   twenty missing hours. In authorized demo, compare three stations/fourteen facts.
4. **2:25–3:10:** Telegram: explain closed registration and the controlled historical
   demonstration; show only actual sanitized delivery evidence supplied by Khuê.
5. **3:10–3:55:** Dữ liệu: filter dates, open JSON example, download licensed CSV.
   Tình trạng: separate sensor age, ingestion time and fixed website build time.
6. **3:55–5:00:** AWS Console: existing EventBridge/Lambda logs, immutable S3/Iceberg
   data, successful Glue run, retained Athena result and Amplify jobs. Show the
   disabled end state. Do not trigger a new query, poll, job or notification.

For network failure, use the retained screenshots and label them “Ảnh đã lưu của
bản trình diễn”, with their capture date. They are not evidence of a new live run.
