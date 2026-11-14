"""Local browser acceptance using real verified historical builds; no external APIs."""

import argparse
import csv
import io
import json
import math
import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    a = argparse.ArgumentParser(description=__doc__)
    for k in ("public", "demo", "evidence", "reference"):
        a.add_argument("--" + k, type=Path, required=True)
    a.add_argument("--browser-executable", type=Path)
    x = a.parse_args()
    if x.evidence.exists():
        raise ValueError("Preserve prior browser evidence")
    x.evidence.mkdir(mode=0o700, parents=True)
    reference = json.loads(x.reference.read_text())
    expected = {
        r["hour_start"]: r for r in reference["facts"] if r["location_id"] == 6123215
    }
    expected_daily = {
        r["date_local"]: r for r in reference["daily"] if r["location_id"] == 6123215
    }
    servers = []
    checks = []
    network = []

    def serve(root):
        s = ThreadingHTTPServer(
            ("127.0.0.1", 0), partial(SimpleHTTPRequestHandler, directory=str(root))
        )
        threading.Thread(target=s.serve_forever, daemon=True).start()
        servers.append(s)
        return f"http://127.0.0.1:{s.server_port}/"

    public, demo = serve(x.public), serve(x.demo)
    try:
        with sync_playwright() as p:
            chrome = x.browser_executable
            if chrome is None:
                f = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
                chrome = f if f.exists() else None
            b = p.chromium.launch(
                headless=True, **({"executable_path": str(chrome)} if chrome else {})
            )
            c = b.new_context(
                viewport={"width": 1440, "height": 1100},
                locale="vi-VN",
                timezone_id="Asia/Ho_Chi_Minh",
                accept_downloads=True,
            )
            page = c.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.on("request", lambda r: network.append(r.url))
            page.goto(public)
            page.wait_for_function(
                "window.AirLake?.getData()?.schema_version===2 && document.querySelector('#pm-value').textContent!=='—'"
            )
            assert page.locator("#station option").count() == 1
            assert page.evaluate("AirLake.getData().hourly.length") == 720
            assert page.locator("#pm-value").inner_text() == "40,5"
            assert page.locator("#freshness").inner_text() == "Dữ liệu đã cũ"
            assert "BẢN KIỂM THỬ" in page.locator("#notice").inner_text()
            assert page.evaluate("AirLake.getData().cloud_verified") is False
            body = page.evaluate("JSON.stringify(AirLake.getData())")
            assert "4946812" not in body and "4946813" not in body
            checks.append(
                "real_720_licensed_hourly_rows_not_four_and_explicit_local_not_aws_status"
            )
            page.locator("a[data-nav=xu-huong]").click()
            page.locator("#history-range").select_option("30d")
            assert page.locator("#trend-chart [data-observation]").count() == 720
            assert page.locator("#daily-chart [data-history-value]").count() == 30
            assert page.locator("#history-heatmap button").count() == 720
            assert (
                page.locator("#hour-pattern-chart [data-history-value]").count() == 24
            )
            assert "cao nhất" in page.locator("#monthly-extremes").inner_text()
            assert (
                "ngày thường đủ giờ" in page.locator("#weekday-comparison").inner_text()
            )
            page.screenshot(
                path=str(x.evidence / "public-month-desktop.png"), full_page=True
            )
            checks.append(
                "month_daily_hourly_heatmap_hour_of_day_extremes_and_weekend_real_data"
            )
            page.locator("#history-range").select_option("7d")
            assert page.locator("#trend-chart [data-observation]").count() == 168
            assert page.locator("#daily-chart [data-history-value]").count() == 7
            assert "Chưa đủ" in page.locator("#monthly-extremes").inner_text()
            page.locator("#history-range").select_option("24h")
            assert page.locator("#trend-chart [data-observation]").count() == 24
            assert page.locator("#hour-pattern-chart .empty-chart").count() == 1
            checks.append("7day_24hour_filters_and_insufficient_month_pattern_guards")
            page.locator("a[data-nav=nghien-cuu]").click()
            assert "720 dòng" in page.locator("#research-count").inner_text()
            for fmt in ("json", "csv"):
                with page.expect_download() as d:
                    page.locator("#download-" + fmt).click()
                d.value.save_as(str(x.evidence / ("public-720." + fmt)))
            rows = json.loads((x.evidence / "public-720.json").read_text())["data"]
            assert len(rows) == 720
            for r in rows:
                e = expected[r["observed_hour_utc"]]
                assert (
                    math.isclose(r["pm25"], e["pm25_avg"], rel_tol=1e-12, abs_tol=1e-10)
                    and r["aqi_mvp"] == e["aqi"]
                )
            assert (
                len(
                    list(
                        csv.DictReader(
                            io.StringIO(
                                (x.evidence / "public-720.csv").read_text(
                                    encoding="utf-8-sig"
                                )
                            )
                        )
                    )
                )
                == 720
            )
            page.locator("#research-granularity").select_option("daily")
            assert "30 dòng" in page.locator("#research-count").inner_text()
            with page.expect_download() as d:
                page.locator("#download-json").click()
            d.value.save_as(str(x.evidence / "public-30-days.json"))
            daily = json.loads((x.evidence / "public-30-days.json").read_text())
            assert daily["timezone"] == "Asia/Ho_Chi_Minh" and len(daily["data"]) == 30
            for r in daily["data"]:
                assert (
                    math.isclose(
                        r["pm25"],
                        expected_daily[r["date_local"]]["pm25_avg"],
                        rel_tol=1e-12,
                        abs_tol=1e-10,
                    )
                    and r["hours_present"] == 24
                )
            checks.append(
                "720_hour_json_csv_and_30_daily_export_match_actual_spark_gold"
            )
            page.locator("#date-start").fill("2026-09-24T00:00")
            page.locator("#date-start").dispatch_event("change")
            assert "7 dòng" in page.locator("#research-count").inner_text()
            page.locator("#research-granularity").select_option("hourly")
            assert "168 dòng" in page.locator("#research-count").inner_text()
            assert page.locator("#research-preview tr").count() == 20
            page.screenshot(
                path=str(x.evidence / "public-research.png"), full_page=True
            )
            checks.append(
                "research_preview_station_sensor_granularity_and_exact_range_rows"
            )
            for width, height, name in ((768, 1024, "tablet"), (390, 844, "mobile")):
                page.set_viewport_size({"width": width, "height": height})
                page.locator("a[data-nav=xu-huong]").click()
                page.locator("#history-range").select_option("30d")
                page.wait_for_timeout(120)
                assert page.evaluate("document.documentElement.scrollWidth<=innerWidth")
                sizes = page.locator("#trend-chart svg text").evaluate_all(
                    '(els)=>els.map(e=>Number(e.getAttribute("font-size"))*e.getScreenCTM().a)'
                )
                assert min(sizes) >= 10
                assert not any(
                    "giờh" in t
                    for t in page.locator("#trend-chart svg text").all_text_contents()
                )
                assert page.locator("nav a").evaluate_all(
                    "(els)=>els.every(e=>e.getBoundingClientRect().height>=44)"
                )
                page.screenshot(
                    path=str(x.evidence / ("public-month-" + name + ".png")),
                    full_page=True,
                )
            checks.append(
                "tablet_phone_readable_charts_six_navigation_targets_no_page_overflow"
            )
            first = page.locator("#history-heatmap button").first
            first.focus()
            first.press("ArrowRight")
            assert "1:00" in page.locator("#heatmap-readout").inner_text()
            page.locator("a[data-nav=tinh-trang]").click()
            assert "720/720" in page.locator("#station-status").inner_text()
            page.go_back()
            assert page.locator("#xu-huong").is_visible()
            page.go_forward()
            assert page.locator("#tinh-trang").is_visible()
            checks.append("heatmap_keyboard_status_coverage_and_browser_history")
            c.set_offline(True)
            assert page.locator("#pm-value").inner_text() == "40,5"
            c.set_offline(False)
            page.set_viewport_size({"width": 1440, "height": 1100})
            page.goto(demo)
            page.wait_for_function(
                'document.querySelector("#station").options.length===3'
            )
            page.locator("a[data-nav=xu-huong]").click()
            page.locator("#history-range").select_option("30d")
            page.locator("#compare").check()
            assert page.locator("#trend-chart [data-observation]").count() == 2133
            page.locator("#trend-station").select_option("4946813")
            assert "18 ngày đủ giờ" in page.locator("#monthly-extremes").inner_text()
            page.locator("a[data-nav=khu-vuc]").click()
            assert page.locator("#area-comparison tr").count() == 3
            assert "693 giờ chung" in page.locator("#area-window").inner_text()
            page.screenshot(
                path=str(x.evidence / "protected-comparison.png"), full_page=True
            )
            page.locator("a[data-nav=nghien-cuu]").click()
            page.locator("#research-station").select_option("4946813")
            assert (
                page.locator("#download-json").is_disabled()
                and page.locator("#download-csv").is_disabled()
            )
            checks.append(
                "2133_protected_facts_common_hour_comparison_and_18day_month_guard"
            )
            page.locator("a[data-nav=telegram]").click()
            assert page.get_by_role(
                "button", name="Chưa mở đăng ký", exact=True
            ).is_disabled()
            assert page.locator('a[href*="t.me"]').count() == 0
            checks.append(
                "restricted_download_and_unverified_telegram_signup_remain_blocked"
            )
            bad = b.new_context()
            badpage = bad.new_page()
            badpage.route(
                "**/data/*.json",
                lambda r: r.fulfill(
                    status=429,
                    body="SYNTHETIC upstream details must not display",
                    headers={"Retry-After": "60"},
                ),
            )
            badpage.goto(public)
            badpage.locator("#error-panel").wait_for(state="visible")
            assert badpage.locator("#pm-value").inner_text() == "—"
            assert (
                "429" not in badpage.locator("body").inner_text()
                and "SYNTHETIC upstream" not in badpage.locator("body").inner_text()
            )
            checks.append(
                "rate_limited_source_has_friendly_error_no_fake_reading_or_raw_details"
            )
            assert not errors, errors
            assert all(url.startswith((public, demo, "blob:")) for url in network)
            b.close()
    finally:
        for s in servers:
            s.shutdown()
            s.server_close()
    result = {
        "status": "PASS_LOCAL_HISTORICAL_ANALYTICS_NOT_AWS_RELEASE",
        "passed": len(checks),
        "checks": checks,
        "real_public_hours": 720,
        "real_public_days": 30,
        "real_protected_hours": 2133,
        "real_protected_days": 90,
        "aws_calls": 0,
        "new_measurements_fabricated": 0,
    }
    (x.evidence / "browser-results.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2)
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
