# Documentation index

## Current system

| Document | Content |
|---|---|
| [architecture.md](architecture.md) | Deployed AWS components, data flows, status of every planned component, security boundaries |
| [public-api.md](public-api.md) | Public read-only API: routes, parameters, fields, licences, errors, examples |
| [telegram-bot.md](telegram-bot.md) | @AirLakeHanoiBot triggers, commands, alert policy, status |
| [rolling-history-and-freshness.md](rolling-history-and-freshness.md) | Rolling history sources and rules, hour-label correction, freshness thresholds, location data |
| [hanoi-coverage-and-forecast.md](hanoi-coverage-and-forecast.md) | Source inventory, CAMS model and its validation, refresh and serving design, costs |
| [hanoi-station-licensing.md](hanoi-station-licensing.md) | Licence inventory of Hanoi stations on OpenAQ and other networks |
| [final-product-and-architecture.md](final-product-and-architecture.md) | Public coverage decisions, UX patterns, deviations from the original report |
| [historical-product.md](historical-product.md) | September 2026 Glue/Iceberg/Athena backfill and private IAM API (still live) |
| [final-evidence-package.md](final-evidence-package.md) | Verified results of the final release |

Release receipts for every AWS change are in [`tests/evidence/`](../tests/evidence/).

## Historical records

Kept as project history; each file starts with a status note.

| Document | Phase |
|---|---|
| [history/local-lakehouse-pipeline.md](history/local-lakehouse-pipeline.md) | Former README: local Kafka → Spark → Iceberg pipeline |
| [../AirLake_DacTa_KyThuat.md](../AirLake_DacTa_KyThuat.md) | Original technical specification (targets) |
| [../AirLake_PROJECT_CONTEXT.md](../AirLake_PROJECT_CONTEXT.md) | Original project context and planned roles |
| [real-openaq-batch.md](real-openaq-batch.md), [openaq-incremental.md](openaq-incremental.md), [aws-iceberg-smoke.md](aws-iceberg-smoke.md) | Lakehouse phase: real-data batch, incremental ingestion, Iceberg probe |
| [reviews/sprint2-publication.md](reviews/sprint2-publication.md) | Sprint 2 pull-request review |
| [gold-dashboard.md](gold-dashboard.md), [clean-data-api.md](clean-data-api.md) | Local Superset dashboard and localhost API |
| [live-openaq-demo.md](live-openaq-demo.md), [live-operations.md](live-operations.md), [aws-final-demo.md](aws-final-demo.md) | Bounded AWS trials of 2026-11-14 |
| [hanoi-citizen-demo.md](hanoi-citizen-demo.md), [hanoi-citizen-release.md](hanoi-citizen-release.md) | First citizen website versions |
| [gold-to-telegram.md](gold-to-telegram.md), [athena-to-telegram-mvp.md](athena-to-telegram-mvp.md), [athena-alert-runtime.md](athena-alert-runtime.md) | Alert prototypes |
| [telegram-staging-contract.md](telegram-staging-contract.md), [telegram-staging-one-test.md](telegram-staging-one-test.md) | Khuê's staging alert pipeline |
| [telegram-nowcast-contract.md](telegram-nowcast-contract.md) | Alert integration proposal before the product bot |
