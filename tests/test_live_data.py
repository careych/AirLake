"""Offline tests: synthetic source responses, no AWS or Telegram calls."""

import copy
import io
import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

from ingestion.openaq import API, PILOTS
from live_data import api, deploy, poller
from live_data.contract import (
    ACCOUNT,
    DATABASE,
    NAME,
    PREFIX,
    RUN,
    SECRET_NAME,
    encode,
    freshness,
    sha,
)
from live_data.export import handle, load
from live_data.store import AWSStore
from live_data.view import render

NOW = datetime(2026, 11, 14, 5, tzinfo=UTC)


def measurement(value=10, time="2026-11-14T04:35:00Z"):
    dt = datetime.fromisoformat(time)
    return {
        "value": value,
        "parameter": {"name": "pm25", "units": "µg/m³"},
        "flagInfo": {"hasFlags": False},
        "period": {
            "label": "raw",
            "datetimeTo": {"utc": time},
            "datetimeFrom": {"utc": (dt - timedelta(minutes=5)).isoformat()},
        },
    }


def metadata(location, sensor):
    return {
        "id": location,
        "name": "SYNTHETIC station " + str(location),
        "sensors": [{"id": sensor, "parameter": {"name": "pm25", "units": "µg/m³"}}],
        "isMobile": False,
        "coordinates": {"latitude": 21.0, "longitude": 105.8},
        "licenses": [{"name": "TEST FIXTURE ONLY"}],
    }


class Source:
    def __init__(self, items=None):
        self.items = items if items is not None else [measurement()]
        self.calls = []

    def metadata(self, location, sensor):
        return metadata(location, sensor)

    def get(self, path, params):
        self.calls.append((path, params))
        return {"results": self.items if params["page"] == 1 else []}


class Memory:
    def __init__(self):
        self.cursors = {}
        self.objects = {}
        self.rows = {}
        self.polls = {}
        self.attempts = 0
        self.halted = False

    def acquire(self, now):
        if self.halted or self.attempts >= 6:
            raise ValueError("Bounded")
        self.attempts += 1
        return "lease"

    def release(self, owner):
        pass

    def halt(self, owner):
        self.halted = True

    def cursor(self, sensor):
        return copy.deepcopy(self.cursors.get(sensor, {}))

    def save_cursor(self, sensor, value, owner):
        self.cursors[sensor] = copy.deepcopy(value)

    def raw(self, key, value):
        if key in self.objects:
            if self.objects[key]["record"] != value["record"]:
                raise ValueError("Correction")
            return False, self.objects[key]
        self.objects[key] = copy.deepcopy(value)
        return True, value

    def latest(self, loc, value, key):
        r = value["record"]
        old = self.rows.get(loc)
        if old and old["observed_at"] >= value["observed_at"]:
            return
        self.rows[loc] = {
            "location_id": loc,
            "sensor_id": int(r["sensors_id"]),
            "station": r["location"],
            "pm25": float(r["value"]),
            "units": r["units"],
            "observed_at": value["observed_at"],
            "ingested_at": value["ingested_at"],
            "source": value["source"],
            "record_sha256": value["record_sha256"],
            "cadence_seconds": value["source"].get("observed_spacing_seconds"),
        }

    def poll_status(self, loc, now, status):
        self.polls[loc] = {"last_poll_at": now.isoformat(), "status": status}

    def read(self, pk, sk):
        return (
            self.rows.get(int(sk), {})
            if pk == "LATEST"
            else self.polls.get(int(sk), {})
        )


def request(path="/api/v1/latest", query="", account=ACCOUNT):
    return {
        "rawPath": path,
        "rawQueryString": query,
        "requestContext": {
            "authorizer": {"iam": {"accountId": account}},
            "http": {"method": "GET"},
        },
    }


class PollTests(unittest.TestCase):
    def test_real_contract_shape_and_duplicate_replay(self):
        s = Memory()
        first = poller.poll(Source(), s, NOW, NOW + timedelta(minutes=30))
        second = poller.poll(Source(), s, NOW, NOW + timedelta(minutes=30))
        self.assertEqual(first["accepted_new"], 3)
        self.assertEqual(second["accepted_new"], 0)
        self.assertEqual(second["replayed"], 3)
        self.assertEqual(len(s.objects), 3)
        for obj in s.objects.values():
            self.assertEqual(
                set(obj["record"]),
                {
                    "location_id",
                    "sensors_id",
                    "location",
                    "datetime",
                    "lat",
                    "lon",
                    "parameter",
                    "units",
                    "value",
                },
            )
            self.assertNotEqual(obj["observed_at"], obj["ingested_at"])
            self.assertEqual(obj["source"]["measurement_period_seconds"], 300)

    def test_out_of_order_never_regresses_latest(self):
        s = Memory()
        poller.poll(
            Source(
                [
                    measurement(time="2026-11-14T04:40:00Z"),
                    measurement(time="2026-11-14T04:20:00Z"),
                ]
            ),
            s,
            NOW,
            NOW + timedelta(minutes=30),
        )
        self.assertEqual(
            {r["observed_at"] for r in s.rows.values()}, {"2026-11-14T04:40:00Z"}
        )
        self.assertEqual(len(s.objects), 6)

    def test_invalid_finite_units_identity_time_flagged_rejected(self):
        bad = []
        for value in [None, -1, float("nan"), float("inf"), float("-inf")]:
            bad.append(measurement(value))
        r = measurement()
        r["parameter"]["units"] = "ppm"
        bad.append(r)
        r = measurement()
        r["sensorsId"] = 99
        bad.append(r)
        r = measurement()
        r["flagInfo"]["hasFlags"] = True
        bad.append(r)
        bad.append(measurement(time="2025-07-03T15:00:00Z"))
        s = Memory()
        report = poller.poll(Source(bad), s, NOW, NOW + timedelta(minutes=30))
        self.assertFalse(s.objects)
        self.assertEqual(report["rejected"], 27)

    def test_missing_metadata_is_not_invented(self):
        source = Source()
        source.metadata = Mock(side_effect=ValueError("Invalid metadata"))
        s = Memory()
        with self.assertRaises(ValueError):
            poller.poll(source, s, NOW, NOW + timedelta(minutes=30))
        self.assertTrue(s.halted)
        self.assertFalse(s.objects)

    def test_page_cursor_is_retained_until_window_completes(self):
        s = Memory()
        source = Source(
            [
                measurement(
                    time=(
                        NOW - timedelta(minutes=110) + timedelta(seconds=i)
                    ).isoformat()
                )
                for i in range(100)
            ]
        )
        r = poller.poll(source, s, NOW, NOW + timedelta(minutes=30))
        self.assertEqual(r["accepted_new"], 300)
        self.assertTrue(
            all(c["page"] == 2 and "watermark" not in c for c in s.cursors.values())
        )
        poller.poll(Source([]), s, NOW, NOW + timedelta(minutes=30))
        self.assertTrue(
            all("pending_end" not in c and "watermark" in c for c in s.cursors.values())
        )

    def test_latest_failure_keeps_raw_and_prevents_cursor_advance(self):
        s = Memory()
        s.latest = Mock(side_effect=TimeoutError())
        with self.assertRaises(TimeoutError):
            poller.poll(Source(), s, NOW, NOW + timedelta(minutes=30))
        self.assertEqual(len(s.objects), 1)
        self.assertTrue(s.halted)
        self.assertNotIn("watermark", s.cursors[next(iter(PILOTS.values()))])

    def test_correction_does_not_overwrite(self):
        s = Memory()
        poller.poll(Source(), s, NOW, NOW + timedelta(minutes=30))
        before = copy.deepcopy(s.objects)
        with self.assertRaises(ValueError):
            poller.poll(Source([measurement(99)]), s, NOW, NOW + timedelta(minutes=30))
        self.assertEqual(s.objects, before)

    def test_deadline_and_invocation_cap(self):
        s = Memory()
        with self.assertRaises(ValueError):
            poller.poll(Source(), s, NOW, NOW)
        self.assertEqual(s.attempts, 0)
        for _ in range(6):
            poller.poll(Source([]), s, NOW, NOW + timedelta(minutes=30))
        with self.assertRaises(ValueError):
            poller.poll(Source(), s, NOW, NOW + timedelta(minutes=30))

    def test_empty_batch_has_no_fabricated_latest(self):
        s = Memory()
        r = poller.poll(Source([]), s, NOW, NOW + timedelta(minutes=30))
        self.assertEqual(r["accepted_new"], 0)
        self.assertFalse(s.rows)
        self.assertTrue(
            all(
                r["freshness"]["status"] == "no_data"
                for r in api.latest(s, NOW)["data"]
            )
        )

    def test_rate_limit_bounded_and_not_logged(self):
        response = Mock(status_code=429, headers={"Retry-After": "1"})
        session = Mock()
        session.get.return_value = response
        sleep = Mock()
        upstream = API("TEST-ONLY", session=session, sleep=sleep)
        with self.assertRaises(RuntimeError):
            upstream.get("/locations/1")
        self.assertEqual(session.get.call_count, 3)
        self.assertEqual(sleep.call_count, 2)


class StoreTests(unittest.TestCase):
    def test_acquire_is_conditioned_on_budget_and_failed_stop(self):
        ddb = Mock()
        store = AWSStore({"dynamodb": ddb, "s3": Mock()})
        store.acquire(NOW)
        kw = ddb.update_item.call_args.kwargs
        self.assertIn("attribute_not_exists(halted)", kw["ConditionExpression"])
        self.assertIn("attempts < :max", kw["ConditionExpression"])
        self.assertEqual(kw["ExpressionAttributeValues"][":max"], {"N": "6"})

    def test_latest_conditional_failure_does_not_regress(self):
        ddb = Mock()
        ddb.put_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
        )
        store = AWSStore({"dynamodb": ddb, "s3": Mock()})
        r = metadata(4946812, 13502151)
        record, key = poller.source_record(
            measurement(), r, 13502151, NOW - timedelta(hours=2), NOW
        )
        store.latest(
            4946812,
            {
                "record": record,
                "observed_at": record["datetime"],
                "ingested_at": NOW.isoformat(),
                "source": {},
                "record_sha256": "x",
            },
            key,
        )
        self.assertIn(
            "observed_at < :observed",
            ddb.put_item.call_args.kwargs["ConditionExpression"],
        )

    def test_ambiguous_s3_write_is_not_retried_or_read_as_absence(self):
        s3 = Mock()
        s3.put_object.side_effect = TimeoutError()
        store = AWSStore({"dynamodb": Mock(), "s3": s3})
        with self.assertRaises(TimeoutError):
            store.raw(PREFIX + "bronze/x.json", {})
        self.assertEqual(s3.put_object.call_count, 1)
        s3.get_object.assert_not_called()

    def test_cursor_and_owner_check_are_one_transaction(self):
        ddb = Mock()
        store = AWSStore({"dynamodb": ddb, "s3": Mock()})
        store.save_cursor(1, {"page": 2}, "owner")
        items = ddb.transact_write_items.call_args.kwargs["TransactItems"]
        self.assertIn("ConditionCheck", items[0])
        self.assertIn("Put", items[1])


class APITests(unittest.TestCase):
    def test_latest_readonly_endpoint_and_no_instant_aqi(self):
        s = Memory()
        poller.poll(Source(), s, NOW, NOW + timedelta(minutes=30))
        response = api.handler(request(), None, store=s, now=NOW)
        self.assertEqual(response["statusCode"], 200)
        value = json.loads(response["body"])
        self.assertEqual(value["count"], 3)
        self.assertTrue(
            all(
                r["aqi"] is None and r["freshness"]["status"] == "recent"
                for r in value["data"]
            )
        )
        filtered = api.handler(
            request(query="location_id=4946812"), None, store=s, now=NOW
        )
        self.assertEqual(json.loads(filtered["body"])["count"], 1)

    def test_invalid_or_unapproved_filters_reject_before_aws(self):
        for q in [
            "location_id=1",
            "location_id=4946812&location_id=4946813",
            "limit=all",
            "location_id=1%20OR%201=1",
        ]:
            store = Mock()
            r = api.handler(request(query=q), None, store=store, now=NOW)
            self.assertEqual(r["statusCode"], 400)
            store.read.assert_not_called()
        self.assertEqual(
            api.handler(request(account="other"), None, store=Mock(), now=NOW)[
                "statusCode"
            ],
            403,
        )
        req = request()
        req["requestContext"]["http"]["method"] = "POST"
        self.assertEqual(api.handler(req, None, store=Mock())["statusCode"], 405)

    def test_stale_no_data_and_historical_routes_are_preserved(self):
        self.assertEqual(freshness(None, NOW)["status"], "no_data")
        self.assertEqual(freshness("2025-07-04T00:00:00Z", NOW)["status"], "stale")
        self.assertEqual(
            freshness("2026-11-14T06:00:00Z", NOW)["status"], "invalid_future"
        )
        with patch(
            "live_data.api.historical_handler", return_value={"statusCode": 200}
        ) as old:
            api.handler(request("/api/v1/gold/hourly"), None)
            old.assert_called_once()

    def test_missing_ddb_is_unavailable_not_healthy(self):
        s = Mock()
        s.read.side_effect = RuntimeError("PRIVATE must not leak")
        result = api.handler(request(), None, store=s, now=NOW)
        self.assertEqual(result["statusCode"], 503)
        self.assertNotIn("PRIVATE", result["body"])

    def test_view_is_escaped_has_no_green_missing_data_or_auto_refresh(self):
        value = api.latest(Memory(), NOW)
        value["data"][0]["station"] = "<script>bad()</script>"
        text = render(value)
        self.assertIn("NO DATA", text)
        self.assertNotIn("<script>", text)
        self.assertNotIn("setInterval", text)
        self.assertNotIn("green", text)

    def test_export_not_visible_before_independent_athena_acceptance(self):
        store = Mock()
        store.read.return_value = {}
        with self.assertRaises(RuntimeError):
            load(store)
        store.s3.get_object.assert_not_called()

    def test_versioned_export_json_csv_and_hash_validation(self):
        value = {
            "facts": [
                {
                    "location_id": 4946812,
                    "hour_start": "2026-11-14T04:00:00Z",
                    "pm25_avg": 10.0,
                    "aqi": 20,
                }
            ],
            "dimensions": [{"location_id": 4946812, "name": "SYNTHETIC"}],
            "provenance": {
                "database": DATABASE,
                "aqi_method": "simplified_hourly_mean_mvp; NOT official VN_AQI",
            },
        }
        raw = encode(value)
        store = Mock()
        store.read.return_value = {
            "key": PREFIX + "exports/" + "a" * 64 + "/gold.json",
            "sha256": sha(raw),
            "athena_query_id": "test-query",
        }
        store.s3.get_object.side_effect = lambda **_: {"Body": io.BytesIO(raw)}
        response = handle(request("/api/v1/current/hourly"), store)
        self.assertEqual(json.loads(response["body"])["total"], 1)
        self.assertIn(
            "SYNTHETIC",
            handle(request("/api/v1/current/hourly", "format=csv"), store)["body"],
        )
        store.read.return_value["sha256"] = "wrong"
        with self.assertRaises(RuntimeError):
            load(store)


class DeployTests(unittest.TestCase):
    def test_prepare_hash_mismatch_stops_before_packet_creation(self):
        with tempfile.TemporaryDirectory() as root:
            jar = Path(root) / "bad.jar"
            jar.write_bytes(b"bad")
            output = Path(root) / "packet"
            with self.assertRaises(ValueError):
                deploy.prepare(output, jar, jar)
            self.assertFalse(output.exists())

    def test_policy_scopes_and_no_production_alert_or_athena_permissions(self):
        arn = f"arn:aws:secretsmanager:ap-southeast-2:{ACCOUNT}:secret:{SECRET_NAME}-Ab1234"
        policies = deploy.policies(arn)
        text = json.dumps(policies)
        self.assertNotIn("AirLake_Subscriptions", text)
        self.assertNotIn("sns:", text)
        self.assertNotIn("PowerUserAccess", text)
        self.assertNotIn("athena:", text)
        self.assertEqual(policies["poller"]["Statement"][1]["Resource"], arn)
        self.assertNotIn("secretsmanager:", json.dumps(policies["api"]))
        self.assertNotIn("dynamodb:TransactWriteItems", text)

    def test_write_command_requires_explicit_approval_before_clients(self):
        with (
            patch("live_data.deploy.load_packet", return_value=({}, "hash")),
            patch("live_data.deploy.clients") as clients,
            patch("sys.argv", ["deploy", "deploy", "--packet", "/tmp/test"]),
        ):
            with self.assertRaises(SystemExit):
                deploy.main()
            clients.assert_not_called()

    def test_disabled_schedule_and_bounded_configuration_in_source(self):
        # Inspect generated AWS arguments by running deploy against mock clients.
        with tempfile.TemporaryDirectory() as root:
            p = Path(root)
            (p / "payload.zip").write_bytes(b"unit fixture")
            c = {n: Mock() for n in deploy.SERVICES}
            c["secretsmanager"].create_secret.return_value = {
                "ARN": f"arn:aws:secretsmanager:ap-southeast-2:{ACCOUNT}:secret:{SECRET_NAME}-Ab1234"
            }
            c["lambda"].create_function.return_value = {}
            c["lambda"].create_function_url_config.return_value = {
                "FunctionUrl": "https://unit.invalid"
            }
            with (
                patch("live_data.deploy.preflight"),
                patch(
                    "live_data.deploy.key_from_environment", return_value="TEST-ONLY"
                ),
                patch("live_data.deploy.time.sleep"),
            ):
                deploy.deploy(c, p, "hash", None)
            schedule = c["scheduler"].create_schedule.call_args.kwargs
            self.assertEqual(schedule["State"], "DISABLED")
            self.assertEqual(
                schedule["Target"]["RetryPolicy"]["MaximumRetryAttempts"], 0
            )
            self.assertEqual(
                json.loads(schedule["Target"]["Input"])["InvocationType"],
                "Event",
            )
            self.assertLessEqual(
                (schedule["EndDate"] - schedule["StartDate"]).total_seconds(), 1800
            )
            configs = [
                call.kwargs for call in c["lambda"].create_function.call_args_list
            ]
            self.assertTrue(
                all(
                    x["Role"].startswith(f"arn:aws:iam::{ACCOUNT}:role/{NAME}-")
                    for x in configs
                )
            )
            self.assertNotIn("TEST-ONLY", (p / "deployment-receipt.json").read_text())

    def test_source_credential_is_not_retrieved_during_prepare_or_import(self):
        with (
            patch("live_data.poller.config", return_value=("arn", NOW)),
            patch("live_data.poller.now_utc", return_value=NOW),
            patch("live_data.store.clients") as clients,
        ):
            self.assertEqual(poller.handler({}, None)["status"], "STOP_DEADLINE")
            clients.assert_not_called()


class RecoveryAndCompatibilityTests(unittest.TestCase):
    def schedule(self):
        return {
            "Name": NAME + "-5m",
            "GroupName": "default",
            "ScheduleExpression": "rate(5 minutes)",
            "ScheduleExpressionTimezone": "UTC",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "State": "DISABLED",
            "StartDate": NOW,
            "EndDate": NOW + timedelta(minutes=30),
            "Target": {},
        }

    def test_emergency_disable_stops_lambda_even_when_scheduler_write_fails(self):
        with tempfile.TemporaryDirectory() as root:
            c = {"scheduler": Mock(), "lambda": Mock()}
            c["scheduler"].get_schedule.return_value = self.schedule()
            c["scheduler"].update_schedule.side_effect = TimeoutError()
            with self.assertRaises(TimeoutError):
                deploy.switch(c, Path(root), "hash", False)
            c["lambda"].put_function_concurrency.assert_called_once_with(
                FunctionName=NAME + "-poller", ReservedConcurrentExecutions=0
            )

    def test_disable_is_repeatable_and_verifies_final_states(self):
        with tempfile.TemporaryDirectory() as root:
            c = {"scheduler": Mock(), "lambda": Mock()}
            c["scheduler"].get_schedule.return_value = self.schedule()
            c["lambda"].get_function_concurrency.return_value = {
                "ReservedConcurrentExecutions": 0
            }
            for _ in range(2):
                self.assertEqual(
                    deploy.switch(c, Path(root), "hash", False)["schedule_state"],
                    "DISABLED",
                )
            self.assertEqual(len(list(Path(root).glob("disable-receipt-*.json"))), 2)

    def test_access_denied_preflight_is_not_absence(self):
        c = {n: Mock() for n in deploy.SERVICES}
        c["sts"].get_caller_identity.return_value = {"Account": ACCOUNT}
        c["s3"].head_bucket.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "HeadBucket"
        )
        with self.assertRaises(ClientError):
            deploy.preflight(c)
        c["iam"].create_role.assert_not_called()
        c["secretsmanager"].create_secret.assert_not_called()

    def test_measurement_period_is_not_assumed_to_be_reporting_cadence(self):
        s = Memory()
        source = Source(
            [
                measurement(time="2026-11-14T04:30:00Z"),
                measurement(time="2026-11-14T04:35:00Z"),
            ]
        )
        poller.poll(source, s, NOW, NOW + timedelta(minutes=30))
        self.assertEqual(s.rows[4946812]["cadence_seconds"], 300)
        self.assertEqual(api.latest(s, NOW)["data"][0]["freshness"]["status"], "stale")

    def test_current_latest_shape_cannot_accidentally_enter_gold_alert_consumer(self):
        from alerts.gold import InvalidAlert, Observation

        row = api.latest(Memory(), NOW)["data"][0]
        with self.assertRaises(InvalidAlert):
            Observation.from_row(row)

    def test_runtime_logs_do_not_reveal_source_key(self):
        from contextlib import redirect_stdout

        source = Source()
        store = Memory()
        c = {"secretsmanager": Mock()}
        c["secretsmanager"].get_secret_value.return_value = {
            "SecretString": "DO-NOT-LOG-TEST-KEY"
        }
        output = io.StringIO()
        with (
            patch(
                "live_data.poller.config",
                return_value=("approved-arn", NOW + timedelta(minutes=30)),
            ),
            patch("live_data.poller.now_utc", return_value=NOW),
            patch("live_data.store.clients", return_value=c),
            patch("live_data.store.AWSStore", return_value=store),
            patch("live_data.poller.API", return_value=source),
            redirect_stdout(output),
        ):
            self.assertEqual(poller.handler({}, None)["status"], "PASS")
        self.assertNotIn("DO-NOT-LOG-TEST-KEY", output.getvalue())
        self.assertIn("CloudWatchMetrics", output.getvalue())

    def test_deployed_aws_parameters_match_sdk_shapes_offline(self):
        from botocore.session import Session
        from botocore.validate import validate_parameters

        from final_demo.prepare import job_definition

        definition = job_definition(
            {
                "job": NAME + "-gold",
                "glue_role": NAME + "-glue",
                "run": RUN,
                "database": DATABASE,
                "prefix": PREFIX,
                "bucket": "airlake-701364614078-lake",
                "log_prefix": "/airlake/live-demo/" + RUN,
            },
            "hash",
        )
        args = definition["NonOverridableArguments"]
        args.pop("--plan_uri")
        args.pop("--plan_sha256")
        args.update(
            {
                "--manifest_key": PREFIX + "manifests/test.json",
                "--manifest_sha256": "a" * 64,
            }
        )
        args["--conf"] = args["--conf"].replace(
            "spark.sql.catalog.demo", "spark.sql.catalog.live"
        )
        validate_parameters(
            definition,
            Session()
            .get_service_model("glue")
            .operation_model("CreateJob")
            .input_shape,
        )
        self.assertNotIn("spark.sql.catalog.demo", args["--conf"])
        self.assertNotIn("--packages", str(definition))


class PacketTests(unittest.TestCase):
    def test_packet_imports_outside_checkout_and_preserves_generated_initializer(self):
        import subprocess
        import sys
        import zipfile

        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            runtime = root / "runtime.jar"
            bundle = root / "aws.jar"
            runtime.write_bytes(b"TEST RUNTIME")
            bundle.write_bytes(b"TEST AWS")
            with patch(
                "live_data.deploy.JARS",
                {
                    "runtime.jar": sha(runtime.read_bytes()),
                    "aws-bundle.jar": sha(bundle.read_bytes()),
                },
            ):
                packet = root / "packet"
                deploy.prepare(packet, runtime, bundle)
                deploy.load_packet(packet)
            with zipfile.ZipFile(packet / "payload.zip") as z:
                self.assertEqual(z.read("scripts/__init__.py"), b"")
            script = "import sys;sys.path.insert(0,sys.argv[1]);import live_data.api,live_data.poller,live_data.glue_job,importlib.util;assert importlib.util.find_spec('scripts.gold_batch') is not None;print('PASS packaged imports, no service execution')"
            result = subprocess.run(
                [sys.executable, "-I", "-B", "-c", script, str(packet / "payload.zip")],
                capture_output=True,
                check=False,
                text=True,
                cwd=root,
                timeout=15,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("PASS packaged imports", result.stdout)


if __name__ == "__main__":
    unittest.main()
