"""One historical Gold hour → staging SNS. Default invocation is offline."""

import argparse
import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from alerts.athena import AthenaConfig, AthenaGoldSource
from alerts.delivery import DynamoLedger
from alerts.gold import InvalidAlert, Rule, canonical, evaluate, resource_arn, utc

ROOT = Path(__file__).resolve().parents[1]


def load_plan(path):
    raw = path.read_bytes()
    plan = json.loads(raw)
    validate_plan(plan)
    return plan, hashlib.sha256(raw).hexdigest()


def validate_plan(plan):
    if plan["account"] != "701364614078" or plan["region"] != "ap-southeast-2":
        raise InvalidAlert("Unexpected staging account/region")
    names = plan["names"]
    scope = names["sns_topic"]
    if scope != "airlake-alert-staging-v1" or any(
        names[k] != scope + suffix
        for k, suffix in [
            ("queue", ""),
            ("dlq", "-dlq"),
            ("lambda", ""),
            ("subscriptions", "-subscriptions"),
            ("delivery_ledger", "-ledger"),
            ("consumer_role", "-consumer"),
            ("reader_role", "-reader"),
        ]
    ):
        raise InvalidAlert("Only the reviewed isolated staging names are supported")
    if (
        names["workgroup"] != "airlake_alert_staging_v1"
        or names["log_group"] != "/aws/lambda/" + scope
    ):
        raise InvalidAlert("Unexpected staging workgroup/log scope")
    if plan["secrets"]["name"] != "airlake/alert-staging-v1/telegram":
        raise InvalidAlert("Only the dedicated staging secret name is supported")
    settings = dict(plan["reader"])
    settings["allowed_tables"] = tuple(tuple(v) for v in settings["allowed_tables"])
    settings["location_ids"] = tuple(settings["location_ids"])
    config = AthenaConfig(**settings).validate()
    if (
        config.account != plan["account"]
        or config.region != plan["region"]
        or config.workgroup != names["workgroup"]
        or config.max_rows != 1
        or len(config.location_ids) != 1
        or utc(config.end) - utc(config.start) != timedelta(hours=1)
        or not config.historical_test_mode
    ):
        raise InvalidAlert("Expected one historical station/hour with one result")
    if (
        config.results_bucket != "airlake-701364614078-athena-results"
        or config.results_prefix != "alert-staging/v1/"
    ):
        raise InvalidAlert("Unexpected staging result scope")
    rule = Rule(**plan["historical_test"]["rule"]).validate()
    if rule.rule_id != "fixture-only" or not rule.version.startswith("test-"):
        raise InvalidAlert("Only explicit test-only rules are supported")
    reference = utc(plan["historical_test"]["reference_time"])
    stations = {int(k): v for k, v in plan["stations"].items()}
    if set(stations) != set(config.location_ids) or any(
        not isinstance(v, str) or not v.strip() for v in stations.values()
    ):
        raise InvalidAlert("Exact verified station identity is required")
    return config, rule, stations, reference


class HistoricalSNS:
    """Label transport subject without changing the existing event schema/ID."""

    def __init__(self, client):
        self.client = client
        self.message_id = None

    def publish(self, **kwargs):
        kwargs["Subject"] = "AirLake HISTORICAL DEMONSTRATION - not a current warning"
        result = self.client.publish(**kwargs)
        self.message_id = result.get("MessageId")
        return result


def execute(plan, executor, publisher, record, persist):
    """Injected orchestration; caller must explicitly authorize both side effects."""
    config, rule, stations, reference = validate_plan(plan)
    if executor.config != config:
        raise InvalidAlert("Executor SQL/snapshots differ from approved plan")
    expected_topic = (
        f"arn:aws:sns:{plan['region']}:{plan['account']}:{plan['names']['sns_topic']}"
    )
    if publisher.topic_arn != expected_topic:
        raise InvalidAlert("Publisher topic differs from approved staging topic")
    record.update(status="STARTED", phase="before_query", query_id=None, event_id=None)
    persist(record)
    try:
        observations = evaluate(AthenaGoldSource(executor), rule, stations, reference)
        if len(observations) != 1:
            raise InvalidAlert("Expected exactly one eligible historical observation")
        alert = observations[0]
        if (
            alert.observation.source_snapshot_id != config.hourly_snapshot
            or alert.observation.source_table != config.source_table
        ):
            raise InvalidAlert("Gold result provenance differs from approved snapshots")
        expected = plan["historical_test"]
        if (
            alert.observation.pm25_avg != expected["retained_expected_pm25_avg"]
            or alert.observation.aqi != expected["retained_expected_aqi"]
        ):
            raise InvalidAlert("Gold values differ from retained acceptance")
        record.update(
            query_id=executor.query_id,
            result_object=executor.output_location,
            phase="before_publish",
            event_id=alert.event_id,
            observation=alert.observation.payload(),
        )
        persist(record)
        record["publication"] = publisher.publish(alert)
        record.update(
            status="PUBLISHED_OR_DUPLICATE; TELEGRAM NOT YET VERIFIED", phase="finished"
        )
        persist(record)
        return record
    except Exception:  # noqa: BLE001 -- no credentials, recipient IDs or service error values
        record.update(
            status="FAILED_OR_UNCERTAIN; STOP; NO AUTOMATIC RETRY",
            query_id=executor.query_id,
            phase=record["phase"],
        )
        persist(record)
        raise InvalidAlert(
            "Historical alert execution failed; inspect sanitized evidence"
        ) from None


def construct(plan, profile):
    from boto3 import Session
    from botocore.config import Config

    from alerts.gold import SNSPublisher

    config, rule, stations, reference = validate_plan(plan)
    if any(k.startswith("AWS_ENDPOINT_URL") for k in os.environ):
        raise InvalidAlert("Custom AWS endpoints are unsupported")
    settings = Config(
        retries={"total_max_attempts": 1},
        connect_timeout=5,
        read_timeout=10,
        ignore_configured_endpoint_urls=True,
    )
    bootstrap = Session(profile_name=profile, region_name=config.region)
    sts = bootstrap.client("sts", config=settings)
    if sts.get_caller_identity()["Account"] != config.account:
        raise InvalidAlert("Incorrect AWS account")
    role = f"arn:aws:iam::{config.account}:role/{plan['names']['sns_topic']}-reader"
    # Temporary credentials stay in memory; never emit or persist the STS response.
    credentials = sts.assume_role(
        RoleArn=role, RoleSessionName="airlake-historical-test", DurationSeconds=900
    )["Credentials"]
    session = Session(
        region_name=config.region,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )
    # Use only assumed-role clients for both the query and publication.
    from alerts.athena import AthenaQueryExecutor

    executor = AthenaQueryExecutor(
        session.client("athena", config=settings),
        session.client("sts", config=settings),
        config,
        stations=stations,
        rule=rule,
        clock=lambda: datetime.now(UTC),
        execute_queries=True,
        test_now=reference,
    )
    ledger = DynamoLedger(
        session.resource("dynamodb", config=settings).Table(
            plan["names"]["delivery_ledger"]
        ),
        lambda: datetime.now(UTC),
        lease_seconds=plan["ledger"]["lease_seconds"],
        retention_seconds=plan["ledger"]["retention_seconds"],
    )
    transport = HistoricalSNS(session.client("sns", config=settings))
    publisher = SNSPublisher(
        transport,
        resource_arn(
            f"arn:aws:sns:{config.region}:{config.account}:{plan['names']['sns_topic']}",
            "sns",
            config.account,
            config.region,
        ),
        ledger,
        account=config.account,
        region=config.region,
        stations=stations,
        clock=lambda: reference,
    )
    return executor, publisher, transport


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--plan", type=Path, default=ROOT / "docs/plans/athena-alert-staging.json"
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm-account")
    parser.add_argument("--plan-sha256")
    parser.add_argument("--profile", default="airlake-spark")
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args(argv)
    plan, digest = load_plan(args.plan)
    config, _, _, _ = validate_plan(plan)
    if not args.execute:
        print(
            json.dumps(
                {
                    "mode": "OFFLINE PLAN ONLY; NO AWS REQUESTS",
                    "plan_sha256": digest,
                    "sql": config.sql(),
                    "topic": plan["names"]["sns_topic"],
                    "expected_rows": 1,
                    "historical_demonstration": True,
                },
                indent=2,
            )
        )
        return 0
    if (
        args.confirm_account != plan["account"]
        or args.plan_sha256 != digest
        or args.evidence_dir is None
    ):
        parser.error(
            "Execution requires account confirmation, exact plan SHA-256 and a fresh external evidence directory"
        )
    directory = args.evidence_dir.resolve()
    if directory == ROOT or ROOT in directory.parents:
        parser.error("Evidence must be outside the repository")
    directory.mkdir(parents=True, exist_ok=False)
    directory.chmod(0o700)
    record = {
        "plan_sha256": digest,
        "account": plan["account"],
        "region": plan["region"],
        "hourly_snapshot": config.hourly_snapshot,
        "dimension_snapshot": config.dimension_snapshot,
        "status": "INITIALIZING",
        "query_id": None,
    }
    target = directory / "historical-alert.json"

    def persist(value):
        target.write_text(canonical(value) + "\n")
        target.chmod(0o600)

    persist(record)
    try:
        executor, publisher, transport = construct(plan, args.profile)
        execute(plan, executor, publisher, record, persist)
        record["sns_message_id"] = transport.message_id
        persist(record)
    except Exception:  # noqa: BLE001 -- never log credential-provider or SDK exception details
        record["status"] = "FAILED_OR_UNCERTAIN; STOP; NO RETRY"
        persist(record)
        print(
            "Execution stopped; inspect private sanitized evidence. Do not retry automatically."
        )
        return 1
    print(
        "Historical event published or suppressed; verify consumer/Telegram separately."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
