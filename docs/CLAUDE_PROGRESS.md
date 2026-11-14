# Claude progress — last verified state

Updated 2026-11-15 (Asia/Ho_Chi_Minh). Labels: VERIFIED (this session), REPORTED, UNKNOWN.

## Git
- Worktree `/Users/carey/Projects/AirLake-aqi-fix`, branch `fix/aqi-half-tie` from `origin/main` `6452341`.
  Commits: `960b54b` exact AQI + run-2 prep, `4fb1648` 24h label fix, `d3639fd` IAM wait. NOT pushed.
- Main checkout `feat/local-lakehouse` has untracked kit files; left untouched.
- PR #19 (Khuê, commit `2950107`, merge `6452341`): Athena catalog case-insensitive
  match + staging-control failure diagnostics (phase/operation/exit), CLI JSON via 0600
  temp file. MERGED, not deployed (VERIFIED: staging Lambda last modified before merge;
  mapping Disabled; alias staging -> v1). Telegram E2E: NOT VERIFIED.

## AQI half-tie (fixed locally)
- Row: Nhân Chính 4946812, 2026-09-07T12:00Z, exact mean 38.75 -> AQI 77.5.
  Glue run 1 DOUBLE avg 38.74999999999999 -> 77; correct HALF_UP = 78.
- Policy (`scripts/gold_batch.py`): quantize readings HALF_UP to 1e-6 µg/m³, BIGINT sums,
  exact band + HALF_UP via integer division; `exact_hourly_aqi` pure reference;
  in-Glue gate in `historical/glue_job.py`.
- VERIFIED: 477 unit tests OK (needs botocore on path); Spark/Iceberg P0 suite 50/50;
  full 17,181-record local isolated run PASS; Spark == exact reference for 2,133/2,133
  hours (AQI and bit-exact mean) under 1/2/7/64 partitions; 0 AQI diffs vs prior local accepted export.
- Browser (local preview, Chrome/Playwright): 10/10 historical checks PASS; public bundle
  has 0 restricted-station strings; 390px no overflow.

## AWS (VERIFIED read-only, account 701364614078 ap-southeast-2)
- Run 1 job SUCCEEDED (`jr_e638…`), DB `airlake_history_20261114_7a6e91c2` kept as evidence.
- Private API `airlake-live-20261114_9e2c740b-api` (AWS_IAM) currently points at run-1
  export (contains the wrong row); 0 signed HTTP checks ever ran on history route.
- Amplify `dhyvgpdsnj4zi`: public + demo still job 1 (old 4/14-row release).
- Scheduler `airlake-live-20261114_9e2c740b-5m` DISABLED.
- Run-2 packet `/private/tmp/airlake-history-run2-20261115`, FROZEN plan sha
  `aaed01a18972f82a5c7c4869edc9047bd3d192c70586d588280bd3ecb3a726bd` (commit `a9eabf6`);
  read-only preflight PASS. Demo Dragon-only (owner decision), stop-only website failure.
- Current demo (job 1): 3 stations / 14 hourly rows, restricted downloads off; anonymous
  `/`, data JSON, config.js, app.js all 401; public 404 for demo data (VERIFIED).
- AWS python needs `botocore[crt]` (aws login provider): scratch venv used.

## Next action
Awaiting approval: run-2 Glue + 2 Athena -> API repoint + 5 signed GETs -> Amplify public/demo.
Then: push branch + PR, phone check of live URLs, demo script.
