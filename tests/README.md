# Tests

```sh
uv run python -m unittest discover -s tests
```

runs 590 offline tests. They use synthetic data, never call AWS and need no
Kafka broker or Spark.

| Area | Files |
|---|---|
| Refresh, projection and public API | `test_nowcast.py`, `test_nowcast_deploy.py`, `test_nowcast_resume.py` |
| Rolling history and freshness | `test_rolling_history.py`, `test_release_rolling_history.py` |
| Per-area model estimates | `test_model_areas.py` |
| Telegram bot and its release | `test_telegram_bot.py`, `test_telegram_deploy.py` |
| Website data and release | `test_citizen_*.py` |
| September lakehouse | `test_historical*.py`, `test_aqi_exact_policy.py` |
| Release scripts (order of writes, stop conditions) | `test_release_*.py`, `test_*_deploy.py` |
| Earlier stages (local pipeline, staging alerts, AWS trials) | the remaining `test_*.py` files |

## Opt-in integration tests

These files are not picked up by default discovery and refuse to run without
their environment flag.

- `gold_p0_integration.py`: 48 Spark/Iceberg integration cases for the local
  Gold layer. Needs PySpark 3.5, Java 17 and a local
  `iceberg-spark-runtime-3.5_2.12-1.12.0.jar`:

  ```sh
  export JAVA_HOME=/path/to/java-17
  export AIRLAKE_ICEBERG_RUNTIME_JAR=/path/to/iceberg-spark-runtime-3.5_2.12-1.12.0.jar
  AIRLAKE_RUN_GOLD_P0_INTEGRATION=1 uv run python -B tests/gold_p0_integration.py
  ```

  Each case runs in a fresh Spark process with a temporary warehouse.
- `cloud_wiring_integration.py` (`AIRLAKE_RUN_CLOUD_WIRING_TESTS=1`): checks the
  Spark catalog wiring for Glue/S3.

## Release receipts

`evidence/` holds one JSON receipt per AWS release and data run: plan hash,
each write and its result, query IDs, row counts and checks. Secrets, personal
paths and raw measurements are not stored. Some tests read these receipts to
check that recorded results stay consistent.
