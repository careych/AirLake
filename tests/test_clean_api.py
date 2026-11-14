"""Synthetic schema fixtures and mocked AWS only; HTTP tests use localhost."""

import csv
import hashlib
import io
import json
import socket
import tempfile
import threading
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from clean_api.data import (
    DIM_SNAPSHOT,
    FACT_SNAPSHOT,
    STATIONS,
    Dataset,
    load_evidence,
)
from clean_api.server import make_server
from clean_api.verify import (
    DATABASE,
    QUERIES,
    RESULTS,
    SCHEMAS,
    fetch,
    private_report,
    read_api,
    sdk_clients,
    smoke,
    verify,
)


def fixture():
    """Values are explicitly synthetic; location IDs match the allowlisted schema."""
    dims = [
        {
            "location_id": i,
            "name": f"SYNTHETIC station {n}",
            "lat": 21.0,
            "lon": 105.0,
            "station_type": "reference",
        }
        for n, i in enumerate(STATIONS)
    ]
    facts = [
        {
            "location_id": i,
            "hour_start": f"2026-11-07T0{h}:00:00Z",
            "pm25_avg": float(h),
            "aqi": h * 2,
        }
        for i in STATIONS
        for h in (2, 3, 4)
    ]
    return facts, dims


def dataset():
    return Dataset(*fixture(), evidence_sha256="synthetic-test-fixture")


class DataTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(socket, "socket", side_effect=AssertionError("No network"))
        )

    def test_metadata_is_honest_about_history_aqi_and_licensing(self):
        m = dataset().metadata()
        self.assertIn("NOT a live feed", m["dataset"])
        self.assertIn("NOT official VN_AQI", m["aqi_method"])
        self.assertFalse(m["provenance"]["athena_called_per_request"])
        self.assertIn("not cleared", m["licensing"]["status"])

    def test_sorted_stable_pagination_has_no_duplicates_or_missing_rows(self):
        d = dataset()
        rows = []
        for offset in (0, 2, 4, 6, 8):
            p, fmt = d.page({"limit": ["2"], "offset": [str(offset)]})
            self.assertEqual("json", fmt)
            rows.extend(p["data"])
        self.assertEqual(list(d.rows), rows)
        self.assertEqual(9, len({(r["location_id"], r["hour_start"]) for r in rows}))
        self.assertIsNone(p["next"])

    def test_station_and_half_open_utc_filters(self):
        p, _ = dataset().page(
            {
                "station_id": [str(STATIONS[0])],
                "start": ["2026-11-07T03:00:00+00:00"],
                "end": ["2026-11-07T04:00:00Z"],
            }
        )
        self.assertEqual(1, p["count"])
        self.assertEqual("2026-11-07T03:00:00Z", p["data"][0]["hour_start"])

    def test_invalid_query_parameters_fail_closed(self):
        cases = [
            {"location_id": ["1 OR TRUE"]},
            {"station_id": ["999"]},
            {"location_id": [str(STATIONS[0])], "station_id": [str(STATIONS[0])]},
            {"limit": ["0"]},
            {"limit": ["101"]},
            {"limit": ["1.5"]},
            {"limit": ["2", "3"]},
            {"offset": ["-1"]},
            {"offset": ["1001"]},
            {"sql": ["SELECT 1"]},
            {"format": ["xml"]},
            {"start": [""]},
            {"start": ["2026-11-07T02:00:00"]},
            {"start": ["2026-11-07T02:00:00+07:00"]},
            {"start": ["2026-11-07T01:00:00Z"]},
            {"end": ["2026-11-08T04:00:00Z"]},
            {"end": ["2026-11-07T02:00:00Z"]},
        ]
        for params in cases:
            with self.subTest(params=params), self.assertRaises(ValueError):
                dataset().page(params)

    def test_invalid_numeric_measurements_reject(self):
        for v in (None, True, -1.0, float("nan"), float("inf"), -float("inf")):
            facts, dims = fixture()
            facts[0]["pm25_avg"] = v
            with self.subTest(value=v), self.assertRaises(ValueError):
                Dataset(facts, dims, evidence_sha256="synthetic")

    def test_zero_is_valid(self):
        facts, dims = fixture()
        facts[0]["pm25_avg"] = 0.0
        self.assertEqual(
            0.0, Dataset(facts, dims, evidence_sha256="synthetic").rows[0]["pm25_avg"]
        )

    def test_duplicate_or_orphan_facts_and_bad_hour_fail(self):
        for kind in ("duplicate", "orphan", "hour", "aqi", "schema"):
            facts, dims = fixture()
            if kind == "duplicate":
                facts.append(dict(facts[0]))
            elif kind == "orphan":
                dims = dims[1:]
            elif kind == "hour":
                facts[0]["hour_start"] = "2026-11-07T02:01:00Z"
            elif kind == "aqi":
                facts[0]["aqi"] = 501
            else:
                facts[0]["unexpected"] = "not Gold"
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                Dataset(facts, dims, evidence_sha256="synthetic")

    def test_invalid_dimension_names_coordinates_and_duplicate_keys_fail(self):
        for key, value in (
            ("name", None),
            ("name", " "),
            ("lat", float("nan")),
            ("lat", 91),
            ("lon", -181),
            ("location_id", 999),
        ):
            facts, dims = fixture()
            dims[0][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                Dataset(facts, dims, evidence_sha256="synthetic")
        facts, dims = fixture()
        dims.append(dict(dims[0]))
        with self.assertRaises(ValueError):
            Dataset(facts, dims, evidence_sha256="synthetic")

    def test_altered_evidence_rejects_without_sdk_or_network(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "changed.json"
            p.write_text('{"hourly_rows": []}')
            with self.assertRaises(ValueError):
                load_evidence(p)

    def test_verified_evidence_loader_with_synthetic_72_row_artifact(self):
        facts, dims = fixture()
        start = datetime(2026, 11, 7, 2, tzinfo=UTC)
        facts = [
            {
                "location_id": i,
                "hour_start": (start + timedelta(hours=h)).isoformat(),
                "pm25_avg": float(h),
                "aqi": h,
            }
            for i in STATIONS
            for h in range(24)
        ]
        value = {
            "status": "PASS_REAL_559_RECORD_CLOUD_PIPELINE_AND_AUTHENTICATED_ATHENA_DASHBOARD",
            "hourly_rows": facts,
            "dimension_rows": dims,
            "unchanged_rerun": {
                "tables": {
                    "HOURLY_TABLE": {"current_snapshot_id": int(FACT_SNAPSHOT)},
                    "DIMENSION_TABLE": {"current_snapshot_id": int(DIM_SNAPSHOT)},
                }
            },
        }
        content = json.dumps(value).encode()
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "synthetic.json"
            p.write_bytes(content)
            with patch(
                "clean_api.data.EVIDENCE_SHA256", hashlib.sha256(content).hexdigest()
            ):
                self.assertEqual(72, len(load_evidence(p).rows))


class HTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = dataset()
        cls.server = make_server(cls.data, 0)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(3)

    def get(self, route, **kwargs):
        return urlopen(Request(self.base + route, **kwargs), timeout=5)

    def test_health_does_not_claim_athena_health_or_call_sdk(self):
        with (
            patch("clean_api.verify.sdk_clients", side_effect=AssertionError("No AWS")),
            self.get("/api/v1/health") as r,
        ):
            result = json.load(r)
            self.assertEqual("not_checked", result["athena_health"])
            self.assertEqual(9, result["hourly_rows"])

    def test_json_filter_and_next_page(self):
        with self.get(f"/api/v1/gold/hourly?location_id={STATIONS[0]}&limit=2") as r:
            result = json.load(r)
        self.assertEqual(2, result["count"])
        self.assertEqual(3, result["total"])
        with self.get(result["next"]) as r:
            last = json.load(r)
        self.assertEqual(1, last["count"])
        self.assertIsNone(last["next"])

    def test_csv_content_headers_and_accept_negotiation(self):
        for route, headers in (
            ("/api/v1/gold/hourly?format=csv", {}),
            ("/api/v1/gold/hourly", {"Accept": "text/csv"}),
        ):
            with self.get(route, headers=headers) as r:
                rows = list(csv.DictReader(io.StringIO(r.read().decode())))
                self.assertEqual(9, len(rows))
                self.assertEqual(
                    "historical-bounded-batch", r.headers["X-Data-Classification"]
                )
                self.assertEqual(FACT_SNAPSHOT, r.headers["X-Gold-Snapshot"])
                self.assertNotIn("Access-Control-Allow-Origin", r.headers)
                self.assertEqual("no-store", r.headers["Cache-Control"])

    def test_stations_preserve_dimension_schema(self):
        with self.get("/api/v1/stations") as r:
            result = json.load(r)
        self.assertEqual(list(self.data.stations), result["data"])

    def test_smoke_command_checks_real_http_without_aws(self):
        with patch(
            "clean_api.verify.sdk_clients", side_effect=AssertionError("No AWS")
        ):
            result = smoke(self.data, self.base)
        self.assertEqual("PASS", result["status"])
        self.assertEqual(0, result["athena_selects"])

    def test_bad_queries_paths_origins_and_methods(self):
        cases = [
            ("/missing", {}, 404),
            ("/api/v1/health?sql=x", {}, 400),
            ("/api/v1/gold/hourly?limit=101", {}, 400),
            ("/api/v1/health", {"headers": {"Origin": "https://example.invalid"}}, 403),
            ("/api/v1/health", {"headers": {"Host": "example.invalid"}}, 403),
            ("/api/v1/gold/hourly", {"method": "POST", "data": b"no-write"}, 405),
        ]
        for route, kwargs, code in cases:
            with self.subTest(code=code), self.assertRaises(HTTPError) as ctx:
                self.get(route, **kwargs)
            self.assertEqual(code, ctx.exception.code)

    def test_head_has_no_body(self):
        with self.get("/api/v1/health", method="HEAD") as r:
            self.assertEqual(b"", r.read())
            self.assertGreater(int(r.headers["Content-Length"]), 0)

    def test_safe_csv_escaping_preserves_json_station_name(self):
        facts, dims = fixture()
        dims[0]["name"] = "=SYNTHETIC()"
        data = Dataset(facts, dims, evidence_sha256="synthetic")
        server = make_server(data, 0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urlopen(
                f"http://127.0.0.1:{server.server_port}/api/v1/gold/hourly?format=csv",
                timeout=5,
            ) as r:
                rows = list(csv.DictReader(io.StringIO(r.read().decode())))
            self.assertEqual("'=SYNTHETIC()", rows[0]["station_name"])
            self.assertEqual("=SYNTHETIC()", data.rows[0]["station_name"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(3)


class VerificationTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(socket, "socket", side_effect=AssertionError("No network"))
        )
        self.data = dataset()
        self.report = {"queries": [], "athena_selects_attempted": 0}
        self.save = Mock()
        self.client = Mock(meta=SimpleNamespace(region_name="ap-southeast-2"))
        self.identity = Mock(meta=SimpleNamespace(region_name="ap-southeast-2"))
        self.identity.get_caller_identity.return_value = {"Account": "701364614078"}
        self.client.get_work_group.return_value = {
            "WorkGroup": {
                "State": "ENABLED",
                "Configuration": {
                    "EnforceWorkGroupConfiguration": True,
                    "EngineVersion": {
                        "EffectiveEngineVersion": "Athena engine version 3"
                    },
                    "ResultConfiguration": {"OutputLocation": RESULTS},
                },
            }
        }
        self.client.start_query_execution.side_effect = [
            {"QueryExecutionId": "query-1"},
            {"QueryExecutionId": "query-2"},
        ]
        self.client.get_query_execution.side_effect = self.execution
        self.client.get_query_results.side_effect = self.results
        self.enterContext(patch("clean_api.verify.read_api", side_effect=self.api))

    def api(self, base, route):
        if route == "/api/v1/stations":
            return json.dumps(
                {"data": self.data.stations, "metadata": self.data.metadata()}
            ).encode()
        if "format=csv" in route:
            s = io.StringIO()
            writer = csv.DictWriter(s, fieldnames=list(self.data.rows[0]))
            writer.writeheader()
            writer.writerows(self.data.rows)
            return s.getvalue().encode()
        return json.dumps(
            {
                "data": self.data.rows,
                "metadata": self.data.metadata(),
                "total": len(self.data.rows),
                "next": None,
            }
        ).encode()

    def execution(self, QueryExecutionId):
        index = int(QueryExecutionId[-1]) - 1
        return {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Query": QUERIES[index],
                "WorkGroup": "airlake",
                "QueryExecutionContext": {
                    "Database": DATABASE,
                    "Catalog": "awsdatacatalog",
                },
                "ResultConfiguration": {
                    "OutputLocation": RESULTS + QueryExecutionId + ".csv"
                },
                "Statistics": {
                    "DataScannedInBytes": 123,
                    "ResultReuseInformation": {"ReusedPreviousResult": False},
                },
            }
        }

    def results(self, QueryExecutionId, **kwargs):
        index = int(QueryExecutionId[-1]) - 1
        rows = self.data.rows if index == 0 else self.data.stations
        keys = [k for k, _ in SCHEMAS[index]]
        raw = [{"Data": [{"VarCharValue": k} for k in keys]}]
        raw += [
            {
                "Data": [
                    {"VarCharValue": str(r[k])} if r[k] is not None else {}
                    for k in keys
                ]
            }
            for r in rows
        ]
        return {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": k, "Type": t} for k, t in SCHEMAS[index]]
                },
                "Rows": raw,
            }
        }

    def run_verify(self, **kwargs):
        return verify(
            self.data,
            "http://127.0.0.1:1",
            self.client,
            self.identity,
            self.report,
            self.save,
            approved_selects=kwargs.pop("approved_selects", 2),
            **kwargs,
        )

    def test_complete_mock_vertical_slice_uses_exactly_two_queries(self):
        self.run_verify()
        self.assertEqual("PASS", self.report["status"])
        self.assertEqual(2, self.client.start_query_execution.call_count)
        self.assertEqual(2, self.report["athena_selects_attempted"])
        self.assertEqual(
            [QUERIES[0], QUERIES[1]],
            [
                c.kwargs["QueryString"]
                for c in self.client.start_query_execution.call_args_list
            ],
        )

    def test_approval_wrong_identity_region_and_workgroup_block_before_sql(self):
        with self.assertRaises(ValueError):
            self.run_verify(approved_selects=0)
        self.identity.get_caller_identity.return_value = {"Account": "000000000000"}
        with self.assertRaises(ValueError):
            self.run_verify()
        self.identity.get_caller_identity.return_value = {"Account": "701364614078"}
        self.client.meta.region_name = "us-east-1"
        with self.assertRaises(ValueError):
            self.run_verify()
        self.client.meta.region_name = "ap-southeast-2"
        self.client.get_work_group.return_value["WorkGroup"]["Configuration"][
            "EnforceWorkGroupConfiguration"
        ] = False
        with self.assertRaises(ValueError):
            self.run_verify()
        self.client.start_query_execution.assert_not_called()

    def test_ambiguous_submission_is_recorded_and_not_retried(self):
        self.client.start_query_execution.side_effect = TimeoutError(
            "SENSITIVE_SENTINEL"
        )
        with self.assertRaises(TimeoutError):
            self.run_verify()
        self.assertEqual(1, self.client.start_query_execution.call_count)
        self.assertIsNone(self.report["queries"][0]["query_id"])
        self.assertEqual("SUBMISSION_UNCERTAIN", self.report["queries"][0]["status"])
        self.save.assert_called()

    def test_timeout_preserves_query_id_and_does_not_cancel_or_resubmit(self):
        self.client.get_query_execution.side_effect = None
        self.client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "RUNNING"}}
        }
        with self.assertRaises(TimeoutError):
            self.run_verify(clock=Mock(side_effect=[0, 0, 61]), sleep=Mock())
        self.assertEqual("query-1", self.report["queries"][0]["query_id"])
        self.assertEqual(1, self.client.start_query_execution.call_count)
        self.client.stop_query_execution.assert_not_called()

    def test_schema_mismatch_stops_before_second_select(self):
        result = self.results("query-1")
        result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"][0]["Type"] = "varchar"
        self.client.get_query_results.side_effect = None
        self.client.get_query_results.return_value = result
        with self.assertRaises(ValueError):
            self.run_verify()
        self.assertEqual(1, self.client.start_query_execution.call_count)

    def test_failed_query_and_wrong_scope_stop(self):
        for change in ("failed", "catalog", "output", "reuse"):
            value = self.execution("query-1")
            v = value["QueryExecution"]
            if change == "failed":
                v["Status"]["State"] = "FAILED"
            elif change == "catalog":
                v["QueryExecutionContext"]["Catalog"] = "other_catalog"
            elif change == "output":
                v["ResultConfiguration"]["OutputLocation"] = (
                    "s3://other-bucket/result.csv"
                )
            else:
                v["Statistics"]["ResultReuseInformation"]["ReusedPreviousResult"] = True
            client = Mock()
            client.start_query_execution.return_value = {"QueryExecutionId": "query-1"}
            client.get_query_execution.return_value = value
            with self.subTest(change=change), self.assertRaises(ValueError):
                fetch(
                    client,
                    QUERIES[0],
                    0,
                    {"queries": [], "athena_selects_attempted": 0},
                    Mock(),
                )
            self.assertEqual(1, client.start_query_execution.call_count)

    def test_private_report_cannot_overwrite_and_preserves_initial_schema(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "report.json"
            r, save = private_report(p)
            self.assertIn("phase", r)
            self.assertEqual([], r["queries"])
            self.assertEqual(0o600, p.stat().st_mode & 0o777)
            with self.assertRaises(ValueError):
                private_report(p)
            r["status"] = "FAILED_OR_UNCERTAIN"
            save()
            self.assertEqual(r, json.loads(p.read_text()))

    def test_remote_api_origin_and_custom_aws_endpoint_reject_offline(self):
        for origin in (
            "https://example.invalid",
            "http://user:password@localhost:1",
            "http://127.0.0.1:1/unapproved",
        ):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                read_api(origin, "/api/v1/health")
        with (
            patch.dict("os.environ", {"AWS_ENDPOINT_URL": "https://example.invalid"}),
            self.assertRaises(ValueError),
        ):
            sdk_clients()


if __name__ == "__main__":
    unittest.main()
