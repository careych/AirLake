"""Synthetic fixtures for the live layers; no network or AWS."""

import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from nowcast import openmeteo, project, sensor_community, validate

NOW = datetime(2026, 11, 15, 1, 20, tzinfo=UTC)
RUN = int(datetime(2026, 11, 14, 12, tzinfo=UTC).timestamp())


def model_body(
    start=datetime(2026, 11, 14, tzinfo=UTC), hours=144, value=lambda i: 20.0 + i % 24
):
    return {
        "latitude": 21.0,
        "longitude": 105.9,
        "hourly_units": {"pm2_5": "μg/m³"},
        "hourly": {
            "time": [
                (start + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M")
                for i in range(hours)
            ],
            "pm2_5": [value(i) if i < hours - 11 else None for i in range(hours)],
        },
    }


META = {
    "last_run_initialisation_time": RUN,
    "last_run_availability_time": RUN + 36000,
    "update_interval_seconds": 43200,
}


class OpenMeteoTests(unittest.TestCase):
    def test_current_is_latest_past_hour_and_forecast_is_strictly_future(self):
        m = openmeteo.normalize(META, model_body(), NOW)
        self.assertEqual(m["current"]["valid_at"], "2026-11-15T01:00:00Z")
        self.assertEqual(m["current"]["kind"], "model_estimate")
        self.assertEqual(m["forecast"][0]["valid_at"], "2026-11-15T02:00:00Z")
        self.assertEqual(m["forecast"][0]["hours_ahead"], 1)
        self.assertEqual(m["forecast"][0]["lead_hours_from_run"], 14)
        self.assertEqual(m["model_run_at"], "2026-11-14T12:00:00Z")
        self.assertTrue(all(d["hours"] >= 20 for d in m["daily"]))
        w = m["relative_windows_24h"]
        self.assertTrue(w["distinct"])
        self.assertLess(w["lower"]["mean"], w["higher"]["mean"])
        flat = openmeteo.normalize(META, model_body(value=lambda i: 30.0), NOW)
        self.assertFalse(flat["relative_windows_24h"]["distinct"])
        self.assertIsNone(flat["relative_windows_24h"]["lower"])

    def test_rejects_wrong_cell_unit_gaps_and_bad_values(self):
        for change in (
            lambda b: b.update(latitude=10.8),
            lambda b: b["hourly_units"].update(pm2_5="ppm"),
            lambda b: b["hourly"]["time"].pop(30),
            lambda b: b["hourly"]["pm2_5"].__setitem__(5, -1.0),
            lambda b: b["hourly"]["pm2_5"].__setitem__(5, float("nan")),
        ):
            body = model_body()
            change(body)
            with self.assertRaises(ValueError):
                openmeteo.normalize(META, body, NOW)
        with self.assertRaises(ValueError):
            openmeteo.normalize(
                {**META, "update_interval_seconds": 3600}, model_body(), NOW
            )

    def test_needs_a_full_next_day(self):
        with self.assertRaises(ValueError):
            openmeteo.normalize(META, model_body(hours=40), NOW)


class SensorCommunityTests(unittest.TestCase):
    def record(self, sid, ts, p2, indoor=0):
        return {
            "sensor": {"id": sid},
            "location": {"indoor": indoor},
            "timestamp": ts,
            "sensordatavalues": [
                {"value_type": "P1", "value": "9"},
                {"value_type": "P2", "value": p2},
            ],
        }

    def test_latest_ignores_indoor_and_other_sensors(self):
        rows = [
            self.record(99385, "2026-11-15 01:10:00", "40.1"),
            self.record(99385, "2026-11-15 01:12:30", "41.5"),
            self.record(99385, "2026-11-15 01:13:00", "90", indoor=1),
            self.record(1, "2026-11-15 01:14:00", "80"),
        ]
        self.assertEqual(
            sensor_community.latest(rows, 99385),
            {"observed_at": "2026-11-15T01:12:30Z", "pm25": 41.5},
        )
        self.assertIsNone(sensor_community.latest([], 99385))

    def test_archive_hours_need_enough_readings_and_are_not_filled(self):
        head = "sensor_id;sensor_type;location;lat;lon;timestamp;P1;durP1;ratioP1;P2;durP2;ratioP2\n"
        full = "".join(
            f"99385;SDS011;1;21.07;105.824;2026-09-15T00:{i * 4:02d}:00;9;;;{10 + i};;\n"
            for i in range(15)
        )
        sparse = "".join(
            f"99385;SDS011;1;21.07;105.824;2026-09-15T01:{i * 6:02d}:00;9;;;50;;\n"
            for i in range(5)
        )
        rows = sensor_community.hourly(head + full + sparse, 99385)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["observed_hour_utc"], "2026-09-15T00:00:00Z")
        self.assertEqual((rows[0]["pm25"], rows[0]["readings"]), (17.0, 15))
        with self.assertRaises(ValueError):
            sensor_community.hourly(
                head + "99385;SDS011;1;21;105;2026-09-15T02:00:00;9;;;-3;;\n", 99385
            )


class ProjectionTests(unittest.TestCase):
    def test_layers_stay_separate_and_freshness_uses_observation_time(self):
        model = openmeteo.normalize(META, model_body(), NOW)
        value = project.build(
            NOW,
            {
                "status": "ok",
                "reading": {"observed_at": "2026-11-15T00:00:00Z", "pm25": 40.5},
                "fetched_at": "2026-11-15T01:20:00Z",
            },
            {
                "status": "fetch_failed",
                "error": "HTTP 429",
                "reading": None,
                "fetched_at": None,
            },
            model,
            {"status": "SYNTHETIC"},
        )
        dragon, sc = value["observations"]
        self.assertEqual(
            (dragon["kind"], dragon["freshness"]["status"]), ("observed", "recent")
        )
        self.assertEqual(sc["freshness"]["status"], "no_data")
        self.assertIsNone(sc["pm25"])
        self.assertEqual(
            (sc["fetch_status"], sc["fetch_error"]), ("fetch_failed", "HTTP 429")
        )
        self.assertEqual(dragon["fetched_at"], "2026-11-15T01:20:00Z")
        self.assertNotEqual(dragon["licence"], sc["licence"])
        self.assertTrue(sc["share_alike"])
        self.assertFalse(sc["exact_location"])
        self.assertEqual(value["model"]["kind"], "model")
        self.assertNotIn("pm25", value["model"])

    def test_openaq_latest_picks_pm25_sensor(self):
        body = {
            "results": [
                {
                    "sensorsId": 14581374,
                    "value": 30,
                    "datetime": {"utc": "2026-11-15T00:00:00Z"},
                },
                {
                    "sensorsId": 14581375,
                    "value": 40.5,
                    "datetime": {"utc": "2026-11-15T00:00:00Z"},
                },
            ]
        }
        self.assertEqual(project.openaq_latest(body)["pm25"], 40.5)


class ValidationTests(unittest.TestCase):
    def test_bias_mae_and_ratio_split_use_edge_mean_alignment(self):
        start = datetime(2026, 8, 31, 17, tzinfo=UTC)
        series = [
            ((start + timedelta(hours=i)).isoformat(), 30.0) for i in range(30 * 24 + 1)
        ]
        obs = [
            {
                "observed_hour_utc": (start + timedelta(hours=i)).isoformat(),
                "pm25": 20.0,
            }
            for i in range(30 * 24)
        ]
        r = validate.compare(series, obs)
        self.assertEqual(
            (r["hourly"]["n"], r["hourly"]["mae"], r["hourly"]["bias"]),
            (720, 10.0, 10.0),
        )
        self.assertEqual(r["daily_complete"]["n"], 30)
        self.assertAlmostEqual(r["calibration_test"]["ratio"], 2 / 3)
        self.assertAlmostEqual(r["calibration_test"]["test_calibrated"]["mae"], 0.0)

    def test_missing_model_edge_drops_the_hour(self):
        start = datetime(2026, 9, 1, tzinfo=UTC)
        r = validate.compare(
            [(start.isoformat(), 1.0)],
            [{"observed_hour_utc": start.isoformat(), "pm25": 1.0}],
        )
        self.assertEqual(r["hourly"]["n"], 0)


class FakeS3:
    class exceptions:
        class NoSuchKey(Exception):
            pass

    class Precondition(Exception):
        @property
        def response(self):
            return {"Error": {"Code": "PreconditionFailed"}}

    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.puts = []

    def etag(self, key):
        import hashlib

        return '"' + hashlib.md5(self.objects[key]).hexdigest() + '"'

    def get_object(self, Bucket, Key, ExpectedBucketOwner):
        if Key not in self.objects:
            raise self.exceptions.NoSuchKey()
        import io

        return {"Body": io.BytesIO(self.objects[Key]), "ETag": self.etag(Key)}

    def put_object(self, Bucket, Key, Body, Condition=None, **kw):
        c = Condition or {}
        if c.get("If-None-Match") == "*" and Key in self.objects:
            raise self.Precondition()
        if "If-Match" in c and (
            Key not in self.objects or self.etag(Key) != c["If-Match"]
        ):
            raise self.Precondition()
        self.objects[Key] = Body
        self.puts.append(Key)


class RefreshTests(unittest.TestCase):
    def http(self, calls):
        def get(url, params=None, headers=None):
            calls.append(url)
            if url == openmeteo.META:
                return META
            if url == openmeteo.API:
                return model_body()
            if "openaq" in url:
                return {
                    "results": [
                        {
                            "sensorsId": 14581375,
                            "value": 50.0,
                            "datetime": {"utc": "2026-11-15T01:00:00Z"},
                        }
                    ]
                }
            return [
                {
                    "sensor": {"id": 99385},
                    "location": {"indoor": 0},
                    "timestamp": "2026-11-15 01:15:00",
                    "sensordatavalues": [{"value_type": "P2", "value": "33"}],
                }
            ]

        return get

    def test_model_downloads_once_per_run_and_current_is_replaced(self):
        from nowcast import refresh

        s3, calls = FakeS3(), []
        first = refresh.run(
            s3, self.http(calls), "SYNTHETIC", {"status": "SYNTHETIC"}, NOW
        )
        self.assertEqual(first["status"], "PUBLISHED")
        self.assertEqual(calls.count(openmeteo.API), 1)
        later = NOW + timedelta(minutes=15)
        refresh.run(s3, self.http(calls), "SYNTHETIC", {"status": "SYNTHETIC"}, later)
        self.assertEqual(calls.count(openmeteo.API), 1)  # same run reused from S3
        self.assertEqual(
            sum(k.startswith("nowcast/v1/projections/") for k in s3.objects), 2
        )
        self.assertEqual(
            sum("/observations/" in k for k in s3.objects), 2
        )  # not duplicated
        import json

        current = json.loads(s3.objects["nowcast/v1/current.json"])
        self.assertEqual(current["published_at"], "2026-11-15T01:35:00Z")

    def failing(self, calls, fail):
        from nowcast.http import UpstreamError

        ok = self.http(calls)

        def get(url, params=None, headers=None):
            if fail(url):
                calls.append("FAIL " + url)
                raise UpstreamError("HTTP 429")
            return ok(url, params, headers)

        return get

    def test_upstream_429_keeps_last_reading_which_then_ages(self):
        import json

        from nowcast import refresh

        s3 = FakeS3()
        refresh.run(s3, self.http([]), "K", {}, NOW)
        later = NOW + timedelta(hours=5)
        r = refresh.run(
            s3, self.failing([], lambda u: "sensor.community" in u), "K", {}, later
        )
        self.assertEqual(r["sources"][99385], "fetch_failed")
        sc = json.loads(s3.objects["nowcast/v1/current.json"])["observations"][1]
        self.assertEqual(
            (sc["observed_at"], sc["pm25"], sc["fetch_error"]),
            ("2026-11-15T01:15:00Z", 33.0, "HTTP 429"),
        )
        self.assertEqual(sc["freshness"]["status"], "stale")

    def test_malformed_source_does_not_block_others(self):

        from nowcast import refresh

        calls = []
        ok = self.http(calls)
        bad = lambda u, params=None, headers=None: (
            {
                "results": [
                    {"sensorsId": 14581375, "value": "x", "datetime": {"utc": "?"}}
                ]
            }
            if "openaq" in u
            else ok(u, params, headers)
        )
        r = refresh.run(FakeS3(), bad, "K", {}, NOW)
        self.assertEqual(r["sources"], {6123215: "fetch_failed", 99385: "ok"})

    def test_model_failure_reuses_stored_run_or_publishes_nothing(self):
        from nowcast import refresh

        s3 = FakeS3()
        refresh.run(s3, self.http([]), "K", {}, NOW)
        r = refresh.run(
            s3,
            self.failing([], lambda u: "open-meteo" in u),
            "K",
            {},
            NOW + timedelta(hours=1),
        )
        self.assertEqual(r["model_fetch"], "reused_previous_run")
        empty = FakeS3()
        with self.assertRaises(RuntimeError):
            refresh.run(
                empty, self.failing([], lambda u: "open-meteo" in u), "K", {}, NOW
            )
        self.assertNotIn("nowcast/v1/current.json", empty.objects)

    def test_malformed_model_is_not_stored(self):
        from nowcast import refresh

        calls = []
        ok = self.http(calls)
        bad = lambda u, params=None, headers=None: (
            {**model_body(), "latitude": 10.0}
            if u == openmeteo.API
            else ok(u, params, headers)
        )
        with self.assertRaises(RuntimeError):
            refresh.run(FakeS3(), bad, "K", {}, NOW)

    def test_failed_write_leaves_current_unchanged(self):
        from nowcast import refresh

        s3 = FakeS3()
        refresh.run(s3, self.http([]), "K", {}, NOW)
        before = s3.objects["nowcast/v1/current.json"]
        original = s3.put_object

        def broken(Bucket, Key, Body, **kw):
            if "/projections/" in Key:
                raise OSError("synthetic S3 failure")
            return original(Bucket, Key, Body, **kw)

        s3.put_object = broken
        with self.assertRaises(OSError):
            refresh.run(s3, self.http([]), "K", {}, NOW + timedelta(minutes=15))
        self.assertEqual(s3.objects["nowcast/v1/current.json"], before)

    def test_no_new_reading_keeps_last_one_to_age(self):
        import json

        from nowcast import refresh

        s3 = FakeS3()
        refresh.run(s3, self.http([]), "K", {}, NOW)
        ok = self.http([])
        quiet = lambda u, params=None, headers=None: (
            [] if "sensor.community" in u else ok(u, params, headers)
        )
        r = refresh.run(s3, quiet, "K", {}, NOW + timedelta(minutes=15))
        self.assertEqual(r["sources"][99385], "no_new_reading")
        sc = json.loads(s3.objects["nowcast/v1/current.json"])["observations"][1]
        self.assertEqual(
            (sc["observed_at"], sc["pm25"]), ("2026-11-15T01:15:00Z", 33.0)
        )

    def test_recent_series_accumulates_distinct_real_readings_for_24_hours(self):
        import json

        from nowcast import refresh

        s3 = FakeS3()
        refresh.run(s3, self.http([]), "K", {}, NOW)
        refresh.run(s3, self.http([]), "K", {}, NOW + timedelta(minutes=15))
        cur = json.loads(s3.objects["nowcast/v1/current.json"])
        self.assertEqual(
            len(cur["recent"]["6123215"]), 1
        )  # same hourly value not repeated
        self.assertEqual(cur["recent"]["6123215"][0]["pm25"], 50.0)
        refresh.run(s3, self.http([]), "K", {}, NOW + timedelta(hours=26))
        cur = json.loads(s3.objects["nowcast/v1/current.json"])
        self.assertEqual(
            cur["recent"]["6123215"], []
        )  # older than 24 h dropped, no filling


class PublicApiTests(unittest.TestCase):
    def event(
        self,
        path,
        query="",
        method="GET",
        origin="https://public.dhyvgpdsnj4zi.amplifyapp.com",
    ):
        return {
            "rawPath": path,
            "rawQueryString": query,
            "headers": {"origin": origin},
            "requestContext": {"http": {"method": method}},
        }

    def test_history_is_pinned_to_licensed_station(self):
        from nowcast import public_api

        self.assertEqual(
            public_api.history_event(self.event("/v1/history", "granularity=daily"))[
                "rawQueryString"
            ],
            "granularity=daily&station_id=6123215",
        )
        for q in (
            "station_id=4946812",
            "location_id=4946813",
            "sensor_id=13502151",
        ):
            r = public_api.handler(self.event("/v1/history", q), None, store=object())
            self.assertEqual(r["statusCode"], 404, q)
        dup = public_api.handler(
            self.event("/v1/history", "station_id=6123215&station_id=4946812"),
            None,
            store=object(),
        )
        self.assertEqual(dup["statusCode"], 400)

    def test_read_only_cors_and_safe_errors(self):
        from nowcast import public_api

        self.assertEqual(
            public_api.handler(self.event("/v1/now", method="POST"), None)[
                "statusCode"
            ],
            405,
        )
        docs = public_api.handler(self.event("/v1"), None)
        self.assertEqual(
            docs["headers"]["access-control-allow-origin"],
            "https://public.dhyvgpdsnj4zi.amplifyapp.com",
        )
        other = public_api.handler(
            self.event("/v1", origin="https://evil.example"), None
        )
        self.assertNotIn("access-control-allow-origin", other["headers"])
        self.assertEqual(
            public_api.handler(
                self.event("/v1/history", "limit=abc"), None, store=object()
            )["statusCode"],
            400,
        )
        broken = public_api.handler(
            self.event("/v1/now"), None, store=type("S", (), {"s3": FakeS3()})()
        )
        self.assertEqual(broken["statusCode"], 503)
        self.assertNotIn("nowcast", broken["body"])

    def test_strict_paths_queries_and_methods(self):
        from nowcast import public_api

        for path, q, method, code in (
            ("/v1/now", "x=1", "GET", 400),
            ("/v1", "a=b", "GET", 400),
            ("/v1/history/../now", "", "GET", 404),
            ("/v2/history", "", "GET", 404),
            ("/v1/history", "sql=select", "GET", 400),
            ("/v1/history", "limit=1&limit=2", "GET", 400),
            ("/v1/history", "q=" + "x" * 600, "GET", 400),
            ("/v1/history", "", "DELETE", 405),
        ):
            r = public_api.handler(self.event(path, q, method), None, store=object())
            self.assertEqual(r["statusCode"], code, (path, q, method))
            self.assertEqual(
                r["headers"]["content-security-policy"],
                "default-src 'none'; frame-ancestors 'none'",
            )

    def test_cached_s3_reuses_immutable_objects(self):
        from nowcast import public_api

        public_api._cache.clear()
        s3 = FakeS3(
            {"historical-demo/x/export.json": b"E", public_api.CURRENT_KEY: b"C"}
        )
        calls = []
        original = s3.get_object
        s3.get_object = lambda **kw: calls.append(kw["Key"]) or original(**kw)
        cached = public_api.CachedS3(s3)
        for _ in range(3):
            cached.get_object(
                Bucket="b", Key="historical-demo/x/export.json", ExpectedBucketOwner="a"
            )
            cached.get_object(
                Bucket="b", Key=public_api.CURRENT_KEY, ExpectedBucketOwner="a"
            )
        self.assertEqual(calls.count("historical-demo/x/export.json"), 1)
        self.assertEqual(calls.count(public_api.CURRENT_KEY), 1)  # within the 60 s TTL
        public_api._cache.clear()

    def test_oversized_projection_is_refused(self):
        from nowcast import public_api

        big = b'{"schema_version": 2, "x": "' + b"a" * public_api.MAX_BODY + b'"}'
        store = type("S", (), {"s3": FakeS3({public_api.CURRENT_KEY: big})})()
        self.assertEqual(
            public_api.handler(self.event("/v1/now"), None, store=store)["statusCode"],
            503,
        )

    def test_now_serves_current_projection_with_short_cache(self):
        from nowcast import public_api

        store = type(
            "S", (), {"s3": FakeS3({public_api.CURRENT_KEY: b'{"schema_version": 2}'})}
        )()
        r = public_api.handler(self.event("/v1/now"), None, store=store)
        self.assertEqual(
            (r["statusCode"], r["headers"]["cache-control"]),
            (200, "public, max-age=60"),
        )
        etag = r["headers"]["etag"]
        again = self.event("/v1/now")
        again["headers"]["if-none-match"] = etag
        r304 = public_api.handler(again, None, store=store)
        self.assertEqual((r304["statusCode"], r304["body"]), (304, ""))


class PublicationSafetyTests(unittest.TestCase):
    http = RefreshTests.http

    def test_versions_have_unique_content_keys_and_pointer_only_moves_forward(self):
        from nowcast import refresh

        s3 = FakeS3()
        a = refresh.run(s3, self.http([]), "K", {}, NOW)
        b = refresh.run(s3, self.http([]), "K", {}, NOW + timedelta(seconds=1))
        self.assertNotEqual(a["projection_key"], b["projection_key"])
        self.assertRegex(
            b["projection_key"],
            r"projections/2026-11-15/2026-11-15T012001Z-[0-9a-f]{16}\.json$",
        )
        before = s3.objects["nowcast/v1/current.json"]
        with self.assertRaises(ValueError):  # same second: not newer
            refresh.run(s3, self.http([]), "K", {}, NOW + timedelta(seconds=1))
        with self.assertRaises(ValueError):  # older run cannot replace newer
            refresh.run(s3, self.http([]), "K", {}, NOW)
        self.assertEqual(s3.objects["nowcast/v1/current.json"], before)

    def test_concurrent_pointer_change_is_not_overwritten(self):
        from nowcast import refresh

        s3 = FakeS3()
        refresh.run(s3, self.http([]), "K", {}, NOW)
        store = refresh.Store(s3)
        original = store.replace_current

        def racing(body, etag):
            s3.objects["nowcast/v1/current.json"] = (
                b'{"published_at": "2026-11-15T09:00:00Z"}'
            )
            return original(body, etag)

        store.replace_current = racing
        with self.assertRaises(FakeS3.Precondition):
            refresh.run(store, self.http([]), "K", {}, NOW + timedelta(minutes=15))
        self.assertIn(b"09:00:00Z", s3.objects["nowcast/v1/current.json"])

    def test_invalid_projection_never_becomes_current(self):
        from nowcast import project, refresh

        s3 = FakeS3()
        refresh.run(s3, self.http([]), "K", {}, NOW)
        before = s3.objects["nowcast/v1/current.json"]
        built = project.build

        def leaky(*a, **k):
            v = built(*a, **k)
            v["observations"].append({**v["observations"][0], "station_id": 4946812})
            return v

        with patch.object(project, "build", leaky), self.assertRaises(ValueError):
            refresh.run(s3, self.http([]), "K", {}, NOW + timedelta(minutes=15))
        self.assertEqual(s3.objects["nowcast/v1/current.json"], before)

    def test_repeated_observation_is_idempotent_but_conflicting_bytes_stop(self):
        from nowcast import refresh

        s3 = FakeS3()
        store = refresh.Store(s3)
        store.put_new("nowcast/v1/x.json", b"A")
        store.put_new("nowcast/v1/x.json", b"A")
        with self.assertRaises(FakeS3.Precondition):
            store.put_new("nowcast/v1/x.json", b"B")

    def test_real_botocore_client_sends_conditional_headers(self):
        import boto3
        from botocore.awsrequest import AWSResponse

        from nowcast import refresh

        client = boto3.client(
            "s3",
            region_name="ap-southeast-2",
            aws_access_key_id="SYNTHETIC",
            aws_secret_access_key="SYNTHETIC",
        )
        seen = []

        class Raw:
            def stream(self, **kw):
                yield b""

        def send(request, **kw):
            seen.append(dict(request.headers))
            return AWSResponse(request.url, 200, {"ETag": '"e"'}, Raw())

        client.meta.events.register("before-send.s3.PutObject", send)
        store = refresh.Store(client)
        store.put_new("nowcast/v1/a.json", b"{}")
        store.replace_current(b"{}", '"abc"')
        store.replace_current(b"{}", None)
        self.assertEqual(seen[0].get("If-None-Match"), b"*")
        self.assertEqual(seen[1].get("If-Match"), b'"abc"')
        self.assertEqual(seen[2].get("If-None-Match"), b"*")
        self.assertNotIn("If-Match", seen[0])


if __name__ == "__main__":
    unittest.main()
