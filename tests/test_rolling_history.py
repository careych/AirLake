import csv
import io
import json
import unittest
from datetime import UTC, datetime, timedelta

from nowcast import history as h
from nowcast import project, public_api, rolling_api
from nowcast.http import UpstreamError
from tests.test_nowcast import FakeS3

NOW = datetime(2026, 11, 15, 8, 20, tzinfo=UTC)  # 15:20 Vietnam time


def row(hour, pm25, basis="openaq_hourly", n=1):
    return {"hour_start": hour, "pm25": pm25, "n": n, "basis": basis}


def hours(start, count, value=20.0, basis="openaq_archive"):
    t = datetime.fromisoformat(start)
    return [
        row((t + timedelta(hours=i)).isoformat().replace("+00:00", "Z"), value, basis)
        for i in range(count)
    ]


def openaq_hour(start, value, observed=1):
    end = (
        (datetime.fromisoformat(start) + timedelta(hours=1))
        .isoformat()
        .replace("+00:00", "Z")
    )
    return {
        "value": value,
        "parameter": {"name": "pm25"},
        "period": {
            "interval": "01:00:00",
            "datetimeFrom": {"utc": start},
            "datetimeTo": {"utc": end},
        },
        "coverage": {"observedCount": observed},
    }


class ConventionTests(unittest.TestCase):
    def test_openaq_archive_end_of_hour_becomes_hour_start(self):
        text = (
            '"location_id","sensors_id","location","datetime","lat","lon","parameter","units","value"\n'
            '6123215,14581375,"Dragon","2026-11-11T01:00:00+07:00","20.99","105.94","pm25","µg/m³","6.5"\n'
            '6123215,14581375,"Dragon","2026-11-11T01:00:00+07:00","20.99","105.94","pm10","µg/m³","9"\n'
        )
        self.assertEqual(
            h.openaq_archive_hours(text),
            [row("2026-11-10T17:00:00Z", 6.5, "openaq_archive")],
        )

    def test_local_day_boundary_follows_the_measured_interval(self):
        # value stamped 00:00 on 12/11 (Vietnam) covers 23:00-24:00 on 11/11
        text = (
            '"location_id","sensors_id","location","datetime","lat","lon","parameter","units","value"\n'
            '6123215,14581375,"D","2026-11-12T00:00:00+07:00","1","1","pm25","µg/m³","40"\n'
            '6123215,14581375,"D","2026-11-12T01:00:00+07:00","1","1","pm25","µg/m³","50"\n'
        )
        rows = h.openaq_archive_hours(text)
        self.assertEqual(
            [r["hour_start"] for r in rows],
            ["2026-11-11T16:00:00Z", "2026-11-11T17:00:00Z"],
        )
        self.assertEqual(
            [d["date_local"] for d in h.daily(rows)], ["2026-11-11", "2026-11-12"]
        )

    def test_provisional_hours_never_make_a_valid_day(self):
        rows = hours("2026-11-13T17:00:00Z", 24, 10.0, "sc_sampled")
        d = h.daily(rows)[0]
        self.assertEqual(
            (d["hours"], d["provisional_hours"], d["valid_day"], d["complete_day"]),
            (24, 24, False, False),
        )
        start = datetime(2026, 11, 13, 17, tzinfo=UTC)
        self.assertEqual(
            h.coverage(rows, start, start + timedelta(days=1))["provisional_hours"], 24
        )

    def test_openaq_hours_use_period_start_and_skip_empty_hours(self):
        body = {
            "results": [
                openaq_hour("2026-11-10T17:00:00Z", 6.5),
                openaq_hour("2026-11-10T18:00:00Z", 9.0, observed=0),
            ]
        }
        self.assertEqual(h.openaq_hours(body), [row("2026-11-10T17:00:00Z", 6.5)])
        bad = {
            "results": [
                {
                    **openaq_hour("2026-11-10T17:00:00Z", 1.0),
                    "period": {"interval": "24:00:00"},
                }
            ]
        }
        with self.assertRaises(ValueError):
            h.openaq_hours(bad)

    def test_sampled_hours_need_two_readings_and_a_finished_hour(self):
        readings = [
            {"observed_at": "2026-11-15T06:05:00Z", "pm25": 10.0},
            {"observed_at": "2026-11-15T06:50:00Z", "pm25": 20.0},
            {"observed_at": "2026-11-15T07:10:00Z", "pm25": 30.0},
            {"observed_at": "2026-11-15T08:01:00Z", "pm25": 40.0},
            {"observed_at": "2026-11-15T08:16:00Z", "pm25": 50.0},
        ]
        self.assertEqual(
            h.sc_sampled_hours(readings, NOW),
            [row("2026-11-15T06:00:00Z", 15.0, "sc_sampled", 2)],
        )


class MergeAndCoverageTests(unittest.TestCase):
    def test_better_basis_wins_and_nothing_before_september(self):
        rows = h.merge(
            [
                row("2026-11-14T00:00:00Z", 100.0),
                row("2026-11-14T01:00:00Z", 5.0, "sc_archive", 20),
            ],
            [
                row("2026-11-14T00:00:00Z", 100.4, "openaq_archive"),
                row("2026-11-14T01:00:00Z", 9.0, "sc_sampled", 3),
                row("2026-08-31T16:00:00Z", 1.0),
            ],
        )
        self.assertEqual([r["pm25"] for r in rows], [100.4, 5.0])
        again = h.merge(rows, [row("2026-11-14T00:00:00Z", 100.0)])
        self.assertEqual(
            again[0]["basis"], "openaq_archive"
        )  # rounded API value never replaces archive

    def test_daily_validity_uses_vietnam_calendar_days(self):
        rows = hours("2026-11-12T17:00:00Z", 18) + hours(
            "2026-11-13T17:00:00Z", 24, 30.0
        )
        days = h.daily(rows)
        self.assertEqual([d["date_local"] for d in days], ["2026-11-13", "2026-11-14"])
        self.assertEqual(
            [(d["hours"], d["valid_day"], d["complete_day"]) for d in days],
            [(18, True, False), (24, True, True)],
        )
        short = h.daily(hours("2026-11-12T17:00:00Z", 17))
        self.assertFalse(short[0]["valid_day"])

    def test_coverage_reports_gaps_and_whole_days_only(self):
        rows = hours("2026-11-12T17:00:00Z", 10) + hours("2026-11-13T05:00:00Z", 36)
        start = datetime(2026, 11, 12, 17, tzinfo=UTC)
        c = h.coverage(rows, start, start + timedelta(days=2))
        self.assertEqual(
            (c["hours_expected"], c["hours_present"], c["days_in_range"]), (48, 46, 2)
        )
        self.assertEqual(
            c["gaps"],
            [
                {
                    "start": "2026-11-13T03:00:00Z",
                    "end": "2026-11-13T05:00:00Z",
                    "hours": 2,
                }
            ],
        )
        self.assertEqual((c["valid_days"], c["complete_days"]), (2, 1))
        partial = h.coverage(
            rows, start + timedelta(hours=1), start + timedelta(days=2)
        )
        self.assertEqual(partial["days_in_range"], 1)  # the first day is not whole

    def test_flags_mark_but_never_change_values(self):
        rows = hours("2026-11-14T00:00:00Z", 7, 20.0)
        rows[3] = row(rows[3]["hour_start"], 120.0, "openaq_archive")
        rows[5] = row(rows[5]["hour_start"], 1.0, "openaq_archive")
        out = h.annotate(rows)
        self.assertEqual(out[3]["flags"], ["isolated_peak"])
        self.assertEqual(out[5]["flags"], ["very_low"])
        self.assertEqual([r["pm25"] for r in out], [r["pm25"] for r in rows])


class RangeTests(unittest.TestCase):
    def test_default_is_last_seven_complete_vietnam_days(self):
        p = rolling_api.params("station_id=6123215", NOW)
        self.assertEqual(
            (p["start"].isoformat(), p["end"].isoformat()),
            ("2026-11-07T17:00:00+00:00", "2026-11-14T17:00:00+00:00"),
        )

    def test_bounds(self):
        for q in (
            "station_id=6123215&start=2026-08-30",
            "station_id=6123215&start=2026-07-01&end=2026-11-14",
            "station_id=6123215&start=2026-11-20&end=2026-11-25",
            "station_id=6123215&start=2026-11-10&end=2026-11-09",
            "station_id=6123215&limit=5000",
            "station_id=6123215&start=2026-11-10T03:30:00Z",
            "station_id=6123215&sql=select",
            "",
        ):
            with self.assertRaises(ValueError, msg=q):
                rolling_api.params(q, NOW)
        with self.assertRaises(LookupError):
            rolling_api.params("station_id=4946812", NOW)
        p = rolling_api.params("station_id=99385&start=2026-11-15&end=2026-11-16", NOW)
        self.assertEqual(
            p["end"], datetime(2026, 11, 15, 9, tzinfo=UTC)
        )  # clipped to now


class ApiTests(unittest.TestCase):
    def setUp(self):
        public_api._cache.clear()
        rows = hours("2026-11-07T17:00:00Z", 168, 25.0)
        del rows[30:40]
        series = {
            sid: json.dumps(h.series(sid, rows, NOW)).encode()
            for sid in (6123215, 99385)
        }
        self.s3 = FakeS3({h.key(k): v for k, v in series.items()})

    def call(self, path, query="", headers=None, method="GET"):
        class Store:
            pass

        store = Store()
        store.s3 = self.s3
        return public_api.handler(
            {
                "rawPath": path,
                "rawQueryString": query,
                "headers": {
                    "origin": "https://public.dhyvgpdsnj4zi.amplifyapp.com",
                    **(headers or {}),
                },
                "requestContext": {"http": {"method": method}},
            },
            None,
            store=store,
        )

    def test_hourly_json_states_coverage_licence_and_convention(self):
        r = self.call("/v1/history", "station_id=99385&start=2026-11-08&end=2026-11-15")
        self.assertEqual(r["statusCode"], 200)
        v = json.loads(r["body"])
        self.assertEqual(
            (
                v["total_rows"],
                v["coverage"]["hours_expected"],
                v["coverage"]["hours_present"],
            ),
            (158, 168, 158),
        )
        self.assertEqual(v["station"]["licence"], project.COMMUNITY["licence"])
        self.assertFalse(v["station"]["exact_location"])
        self.assertIn("UTC start", v["methodology"]["hour_convention"])
        self.assertEqual(v["rows"][0]["hour_start_local"], "2026-11-08T00:00:00+07:00")
        self.assertEqual(
            r["headers"]["access-control-allow-origin"],
            "https://public.dhyvgpdsnj4zi.amplifyapp.com",
        )
        again = self.call(
            "/v1/history",
            "station_id=99385&start=2026-11-08&end=2026-11-15",
            {"if-none-match": r["headers"]["etag"]},
        )
        self.assertEqual((again["statusCode"], again["body"]), (304, ""))

    def test_daily_csv_and_pagination(self):
        r = self.call(
            "/v1/history",
            "station_id=6123215&start=2026-11-08&end=2026-11-15&granularity=daily&format=csv",
        )
        rows = list(csv.DictReader(io.StringIO(r["body"])))
        self.assertEqual(len(rows), 7)
        self.assertEqual(rows[0]["attribution"], project.DRAGON["attribution"])
        self.assertIn("attachment", r["headers"]["content-disposition"])
        page = json.loads(
            self.call(
                "/v1/history",
                "station_id=6123215&start=2026-11-08&end=2026-11-15&limit=100",
            )["body"]
        )
        self.assertEqual(len(page["rows"]), 100)
        self.assertIn("offset=100", page["next"])

    def test_each_dataset_carries_its_own_licence_with_uris(self):
        sc = self.call("/v1/history", "station_id=99385&format=csv")
        rows = list(csv.DictReader(io.StringIO(sc["body"])))
        self.assertEqual(rows[0]["licence"], "ODbL 1.0")
        self.assertEqual(rows[0]["licence_url"], rolling_api.ODBL)
        self.assertEqual(rows[0]["contents_licence_url"], rolling_api.DBCL)
        self.assertIn("This AirLake derived database", rows[0]["licence_notice"])
        self.assertEqual(sc["headers"]["link"], f'<{rolling_api.ODBL}>; rel="license"')
        dragon = self.call("/v1/history", "station_id=6123215&format=csv")["body"]
        first = next(iter(csv.DictReader(io.StringIO(dragon))))
        self.assertEqual(
            (first["licence"], first["licence_url"]), ("CC BY 4.0", rolling_api.CC_BY)
        )
        self.assertNotIn("contents_licence", first)
        self.assertNotIn("ODbL", dragon)  # never relicensed or mixed
        v = json.loads(self.call("/v1/history", "station_id=99385")["body"])["station"]
        self.assertEqual(
            (v["dataset_licence_url"], v["contents_licence_url"]),
            (rolling_api.ODBL, rolling_api.DBCL),
        )
        self.assertIn(
            "relabels",
            json.loads(self.call("/v1/history", "station_id=6123215")["body"])[
                "station"
            ]["changes"],
        )

    def test_stations_and_head(self):
        v = json.loads(self.call("/v1/stations")["body"])
        self.assertEqual([s["station_id"] for s in v["stations"]], [6123215, 99385])
        self.assertEqual(v["stations"][0]["history"]["hours"], 158)
        self.assertEqual(self.call("/v1/stations", method="HEAD")["body"], "")
        self.assertEqual(self.call("/v1/stations", "x=1")["statusCode"], 400)


class FakeStore:
    def __init__(self, objects):
        from nowcast.refresh import Store

        self.s3 = FakeS3(objects)
        self.store = Store(self.s3)

    def __getattr__(self, name):
        return getattr(self.store, name)


class UpdateTests(unittest.TestCase):
    def setUp(self):
        dragon = h.series(
            6123215, hours("2026-11-14T00:00:00Z", 24), NOW - timedelta(hours=1)
        )
        sc = h.series(
            99385,
            hours("2026-11-13T00:00:00Z", 24, 9.0, "sc_archive"),
            NOW - timedelta(hours=1),
        )
        self.store = FakeStore(
            {
                h.key(6123215): json.dumps(dragon).encode(),
                h.key(99385): json.dumps(sc).encode(),
            }
        )
        self.calls = []

    def http(self, fail=False):
        def get(url, params=None, headers=None, text=False, timeout=10):
            self.calls.append(url)
            if fail:
                raise UpstreamError("HTTP 503")
            if "openaq" in url:
                return {
                    "results": [
                        openaq_hour("2026-11-15T06:00:00Z", 44.0),
                        openaq_hour("2026-11-15T07:00:00Z", 37.0),
                    ]
                }
            if url.endswith("2026-11-14_sds011_sensor_99385.csv"):
                lines = [
                    "sensor_id;sensor_type;location;lat;lon;timestamp;P1;durP1;ratioP1;P2;durP2;ratioP2"
                ]
                lines += [
                    f"99385;SDS011;1;21.07;105.82;2026-11-14T03:{m:02d}:00;1;;;12.0;;"
                    for m in range(0, 60, 4)
                ]
                return "\n".join(lines)
            raise UpstreamError("HTTP 404")

        return get

    def projection(self):
        return {
            "recent": {
                "99385": [
                    {"observed_at": "2026-11-15T07:05:00Z", "pm25": 10.0},
                    {"observed_at": "2026-11-15T07:35:00Z", "pm25": 14.0},
                ]
            }
        }

    def test_extends_both_series_with_versioned_conditional_writes(self):
        out = h.update(
            self.store,
            self.http(),
            "SYNTHETIC",
            self.projection(),
            NOW.replace(minute=5),
        )
        self.assertEqual(out, {6123215: "updated", 99385: "updated"})
        self.assertFalse(any("2026-11-13" in u for u in self.calls))  # already archived
        dragon = json.loads(self.store.s3.objects[h.key(6123215)])
        self.assertEqual(dragon["rows"][-1], row("2026-11-15T07:00:00Z", 37.0))
        sc = json.loads(self.store.s3.objects[h.key(99385)])
        bases = {r["hour_start"]: r["basis"] for r in sc["rows"]}
        self.assertEqual(
            (bases["2026-11-14T03:00:00Z"], bases["2026-11-15T07:00:00Z"]),
            ("sc_archive", "sc_sampled"),
        )
        self.assertEqual(
            sum("/history/versions/" in k for k in self.store.s3.objects), 2
        )

    def test_latest_dragon_reading_fills_the_hour_until_better_sources_arrive(self):
        proj = {
            "recent": {},
            "observations": [
                {
                    "station_id": 6123215,
                    "fetch_status": "ok",
                    "observed_at": "2026-11-15T08:00:00Z",
                    "pm25": 33.9,
                },
                {
                    "station_id": 6123215,
                    "fetch_status": "fetch_failed",
                    "observed_at": "2026-11-15T09:00:00Z",
                    "pm25": 1.0,
                },
            ],
        }
        h.update(
            self.store, self.http(fail=True), "SYNTHETIC", proj, NOW.replace(minute=30)
        )
        rows = json.loads(self.store.s3.objects[h.key(6123215)])["rows"]
        self.assertEqual(rows[-1], row("2026-11-15T07:00:00Z", 33.9, "openaq_latest"))
        merged = h.merge(rows, [row("2026-11-15T07:00:00Z", 33.9, "openaq_archive")])
        self.assertEqual(merged[-1]["basis"], "openaq_archive")
        self.assertEqual(h.annotate(rows)[-1]["flags"], ["provisional"])
        confirmed = h.merge(rows, [row("2026-11-15T07:00:00Z", 34.0)])
        self.assertEqual(confirmed[-1]["basis"], "openaq_hourly")
        again = h.merge(
            confirmed, h.openaq_latest_hours(proj)
        )  # next refresh, same /latest
        self.assertEqual(again[-1]["basis"], "openaq_hourly")  # never downgraded
        day = h.daily(hours("2026-11-13T17:00:00Z", 24, 10.0, "openaq_latest"))[0]
        self.assertEqual((day["provisional_hours"], day["valid_day"]), (24, False))

    def test_upstream_failure_never_raises_and_writes_nothing(self):
        before = dict(self.store.s3.objects)
        out = h.update(
            self.store,
            self.http(fail=True),
            "SYNTHETIC",
            {"recent": {}},
            NOW.replace(minute=5),
        )
        self.assertEqual(out, {6123215: "source_error", 99385: "source_error"})
        self.assertEqual(self.store.s3.objects, before)

    def test_quiet_when_nothing_is_due(self):
        self.store.s3.objects[h.key(6123215)] = json.dumps(
            h.series(
                6123215, hours("2026-11-15T00:00:00Z", 8), NOW - timedelta(minutes=10)
            )
        ).encode()
        out = h.update(
            self.store, self.http(), "SYNTHETIC", {"recent": {}}, NOW
        )  # minute 20: no archive try
        self.assertEqual(out, {6123215: "unchanged", 99385: "unchanged"})
        self.assertEqual(self.calls, [])

    def test_missing_series_is_never_created_by_the_lambda(self):
        out = h.update(FakeStore({}), self.http(), "SYNTHETIC", {}, NOW)
        self.assertEqual(out, {6123215: "no_series", 99385: "no_series"})


class FreshnessTests(unittest.TestCase):
    def status(self, sid, minutes):
        at = (NOW - timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")
        return project.freshness(sid, at, NOW)["status"]

    def test_policy_follows_each_source_cadence(self):
        self.assertEqual(
            [self.status(6123215, m) for m in (20, 80, 120, 200)],
            ["fresh", "fresh", "delayed", "stale"],
        )
        self.assertEqual(
            [self.status(99385, m) for m in (5, 20, 40, 90)],
            ["fresh", "fresh", "delayed", "stale"],
        )
        self.assertEqual(self.status(99385, -30), "invalid_future")
        self.assertEqual(project.freshness(99385, None, NOW)["status"], "no_data")
        self.assertEqual(project.freshness(99385, None, NOW)["threshold_seconds"], 1500)

    def test_bot_reads_new_and_old_freshness_states(self):
        from telegram_bot import messages

        o = {"station_id": 6123215, "pm25": 40.0, "observed_at": "2026-11-15T08:00:00Z"}
        line = lambda state: messages.reading_line(
            {**o, "freshness": {"status": state}}, NOW
        )
        self.assertIn("trung bình 14:00–15:00 15/11", line("fresh"))
        self.assertNotIn("(", line("fresh").split("phút trước")[1])
        self.assertEqual(line("recent"), line("fresh"))
        self.assertIn("cập nhật chậm", line("delayed"))
        self.assertIn("đã cũ", line("stale"))

    def test_future_reading_is_not_published_as_current(self):
        from nowcast import refresh

        src = refresh.source(
            None,
            99385,
            lambda: {"observed_at": "2026-11-15T09:00:00Z", "pm25": 5.0},
            NOW,
        )
        self.assertEqual(
            (src["status"], src["error"], src["reading"]),
            ("fetch_failed", "future_timestamp", None),
        )


if __name__ == "__main__":
    unittest.main()
