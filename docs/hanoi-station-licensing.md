# Hanoi PM2.5 stations on OpenAQ: licence inventory

Snapshot: 2026-11-15, OpenAQ v3 `/locations` (PM2.5, 25 km around
21.0285, 105.8542 and the Hanoi bounding box 105.28,20.56,106.02,21.39),
with `/licenses/{id}` for terms. 43 locations; 4 API calls.

| Location | Provider | Last PM2.5 (UTC) | Licence on OpenAQ | Use in AirLake |
|---|---|---|---|---|
| 6123215 Dragon | AirGradient | 2026-11-15 | CC BY 4.0 (redistribution, commercial allowed) | Public, with attribution |
| 7441 Hanoi | AirNow (US Embassy) | 2025-04-09 | US Public Domain | Historical only; ended |
| 4946812 Nhân Chính | Hanoi Air Quality Monitoring Network | 2026-11-15 | none listed | Not published |
| 4946813 Giải Phóng | Hanoi Air Quality Monitoring Network | 2026-11-15 | none listed | Not published |
| 2161292 Lưu Quang Vũ, 2161306 Minh Khai | Hanoi Air Quality Monitoring Network | 2026-11-13 | none listed | Not published |
| 37 other locations | Hanoi network, StateAir, SPARTAN, HabitatMap | 2016-11 to 2026-07 | none listed | Not published |

Findings:

- Dragon is the only currently reporting Hanoi PM2.5 location with a licence
  that permits redistribution. One sensor does not describe the city.
- AirNow 7441 is public domain but stopped reporting in April 2025. It can add
  multi-year historical context for monthly/seasonal views, labelled as ended.
- The Hanoi network stations list no licence on OpenAQ. Publishing them needs
  written permission from the network operator; password protection is not
  permission.

Re-run before any publication change; licence metadata can change.
