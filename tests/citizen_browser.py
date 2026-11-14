"""Opt-in local Chrome checks. Uses real verified local bundles; never contacts AWS."""

import argparse
import json
import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--public", type=Path, required=True)
    a.add_argument("--instructor", type=Path, required=True)
    a.add_argument("--evidence", type=Path, required=True)
    a.add_argument("--browser-executable", type=Path)
    args = a.parse_args()
    if args.evidence.exists():
        raise ValueError("Preserve prior browser evidence")
    args.evidence.mkdir(mode=0o700, parents=True)
    servers = []

    def serve(path):
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0), partial(SimpleHTTPRequestHandler, directory=str(path))
        )
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"http://127.0.0.1:{server.server_port}/"

    public, instructor = serve(args.public), serve(args.instructor)
    checks = []
    try:
        with sync_playwright() as p:
            chrome = args.browser_executable
            if chrome is None:
                mac_chrome = Path(
                    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
                )
                chrome = mac_chrome if mac_chrome.exists() else None
            browser = p.chromium.launch(
                headless=True,
                **({"executable_path": str(chrome)} if chrome else {}),
            )
            context = browser.new_context(
                viewport={"width": 1440, "height": 1100},
                locale="vi-VN",
                timezone_id="Asia/Ho_Chi_Minh",
                accept_downloads=True,
            )
            page = context.new_page()
            requests, errors = [], []
            page.on("request", lambda r: requests.append(r.url))
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.goto(public)
            page.wait_for_function(
                "document.querySelector('#station').options.length===1 && document.querySelector('#station').value==='6123215'"
            )
            page.wait_for_function(
                "document.querySelector('#pm-value').textContent !== '—'"
            )
            assert page.locator("#freshness").inner_text() in [
                "Dữ liệu mới",
                "Cập nhật trễ",
                "Dữ liệu đã cũ",
            ]
            assert page.locator("#home-chart [data-observation]").count() == 4
            assert page.locator("#home-chart [data-missing]").count() == 20
            assert page.locator("#compare").is_disabled()
            text = page.locator("body").inner_text()
            assert all(
                x not in text
                for x in [
                    "DynamoDB",
                    "Iceberg",
                    "Lambda",
                    "Athena",
                    "snapshot",
                    "Gold",
                    "429",
                ]
            )
            page.screenshot(
                path=str(args.evidence / "public-desktop.png"), full_page=True
            )
            checks.append("public_real_four_hourly_points_and_twenty_explicit_gaps")
            labels = page.locator("#home-chart svg text").all_text_contents()
            assert not any("giờh" in t or "hh" in t for t in labels)
            assert any(t == "18:00" for t in labels)
            assert "UTC+7" in labels[-1]
            assert page.locator("#ingested-time").inner_text() != "Chưa có dữ liệu"
            point = page.locator("#home-chart [data-observation]").first
            point.focus()
            assert "µg/m³" in page.locator("#home-chart-readout").inner_text()
            checks.append(
                "single_hour_unit_meaningful_axes_keyboard_readout_separate_ingestion_time"
            )
            page.locator('a[data-nav="tinh-trang"]').click()
            assert page.locator("#tinh-trang").is_visible()
            assert page.locator("#station-status article").count() == 1
            page.screenshot(
                path=str(args.evidence / "public-status.png"), full_page=True
            )
            assert "Không đủ mới" in page.locator("#tinh-trang").inner_text()
            page.go_back()
            assert page.locator("#quanh-ban").is_visible()
            page.go_forward()
            assert page.locator("#tinh-trang").is_visible()
            checks.append("fifth_status_page_and_browser_history")

            page.locator('a[data-nav="nghien-cuu"]').click()
            with page.expect_download() as event:
                page.locator("#download-json").click()
            event.value.save_as(str(args.evidence / "licensed-download.json"))
            value = json.loads((args.evidence / "licensed-download.json").read_text())
            assert (
                len(value["data"]) == 4
                and value["license"] == "https://creativecommons.org/licenses/by/4.0/"
            )
            with page.expect_download() as event:
                page.locator("#download-csv").click()
            event.value.save_as(str(args.evidence / "licensed-download.csv"))
            assert (
                "OpenAQ / AirGradient / srgillo"
                in (args.evidence / "licensed-download.csv").read_text()
            )
            page.locator("#date-start").fill("2026-11-14T16:00")
            page.locator("#date-start").dispatch_event("change")
            assert "2 dòng" in page.locator("#research-count").inner_text()
            checks.append("licensed_json_csv_attribution_and_vietnam_time_filters")
            page.locator("#date-start").fill("2026-11-13T00:00")
            page.locator("#date-end").fill("2026-11-13T01:00")
            page.locator("#date-end").dispatch_event("change")
            assert page.locator("#download-json").is_disabled()
            assert "Không có số liệu" in page.locator("#research-count").inner_text()
            page.locator("#date-start").fill("2026-11-14T16:00")
            page.locator("#date-end").fill("2026-11-14T18:00")
            page.locator("#date-end").dispatch_event("change")
            assert not page.locator("#download-json").is_disabled()
            page.locator(".example-data summary").click()
            assert (
                '"station_id": 6123215'
                in page.locator("#research-example").inner_text()
            )
            checks.append("empty_filter_disables_download_and_real_example_is_licensed")

            page.locator('a[data-nav="telegram"]').click()
            assert page.get_by_role(
                "button", name="Chưa mở đăng ký", exact=True
            ).is_disabled()
            assert page.locator('a[href*="t.me"]').count() == 0
            checks.append("no_unverified_telegram_delivery_or_public_signup_claim")
            context.set_offline(True)
            page.wait_for_function(
                "document.querySelector('#notice').textContent.includes('ngoại tuyến')"
            )
            assert page.locator("#pm-value").inner_text() != "—"
            context.set_offline(False)
            assert all(url.startswith((public, "blob:")) for url in requests)
            checks.append(
                "offline_retains_real_data_and_no_openaq_or_aws_browser_calls"
            )
            mobile = browser.new_context(
                viewport={"width": 390, "height": 844},
                locale="vi-VN",
                timezone_id="Asia/Ho_Chi_Minh",
                is_mobile=True,
                device_scale_factor=1,
            )
            mobile_page = mobile.new_page()
            mobile_page.goto(public)
            mobile_page.wait_for_function(
                "document.querySelector('#pm-value').textContent !== '—'"
            )
            assert mobile_page.evaluate(
                "document.documentElement.scrollWidth <= window.innerWidth"
            )
            mobile_page.screenshot(
                path=str(args.evidence / "public-mobile.png"), full_page=True
            )
            checks.append("mobile_no_horizontal_overflow")
            for width, height in ((768, 1024), (390, 844)):
                mobile_page.set_viewport_size({"width": width, "height": height})
                mobile_page.wait_for_timeout(100)
                sizes = mobile_page.locator("#home-chart svg text").evaluate_all(
                    '(els)=>els.map(e=>Number(e.getAttribute("font-size"))*e.getScreenCTM().a)'
                )
                assert min(sizes) >= 10.5, sizes
                assert mobile_page.locator("nav a").evaluate_all(
                    "(els)=>els.every(e=>e.getBoundingClientRect().height>=44)"
                )
                assert mobile_page.evaluate(
                    "document.documentElement.scrollWidth<=innerWidth"
                )
                if width == 768:
                    mobile_page.screenshot(
                        path=str(args.evidence / "public-tablet.png"), full_page=True
                    )
            mobile_page.screenshot(
                path=str(args.evidence / "public-mobile-readable-chart.png"),
                full_page=True,
            )
            checks.append("tablet_mobile_readable_chart_labels_and_44px_navigation")

            page.goto(instructor)
            page.wait_for_function(
                "document.querySelector('#station').options.length===3"
            )
            for loc in ("4946812", "4946813", "6123215"):
                page.locator("#station").select_option(loc)
                assert page.locator("#trend-station").input_value() == loc
                assert page.locator("#pm-value").inner_text() != "—"
            page.locator('a[data-nav="xu-huong"]').click()
            page.locator("#compare").check()
            assert page.locator("#trend-chart [data-observation]").count() == 14
            page.locator("#metric-aqi").click()
            assert page.locator("#aqi-warning").is_visible()
            assert page.locator("#station-table tr").count() == 3
            page.screenshot(
                path=str(args.evidence / "instructor-comparison.png"), full_page=True
            )
            page.locator('a[data-nav="nghien-cuu"]').click()
            page.locator("#research-station").select_option("4946812")
            assert (
                page.locator("#download-json").is_disabled()
                and page.locator("#download-csv").is_disabled()
            )
            checks.append(
                "three_real_station_filters_fourteen_point_comparison_restricted_download_blocked"
            )
            statuses = page.evaluate(
                "[null,{pm25:0,observed_at:'2026-11-14T10:00:00Z',cadence_seconds:300},{pm25:1,observed_at:'2026-11-14T09:50:00Z',cadence_seconds:300},{pm25:1,observed_at:'2026-11-14T08:00:00Z',cadence_seconds:300}].map(x=>AirLake.freshness(x,Date.parse('2026-11-14T10:00:00Z')).label)"
            )
            assert statuses == [
                "Chưa có dữ liệu",
                "Dữ liệu mới",
                "Cập nhật trễ",
                "Dữ liệu đã cũ",
            ]
            checks.append("zero_missing_late_stale_states_use_actual_observation_time")
            broken = browser.new_context()
            bad = broken.new_page()
            bad.route(
                "**/data/*.json",
                lambda route: route.fulfill(
                    status=200, content_type="application/json", body="{}"
                ),
            )
            bad.goto(public)
            bad.locator("#error-panel").wait_for(state="visible")
            assert bad.locator("#pm-value").inner_text() == "—"
            checks.append("corrupt_data_fail_closed_friendly_error")
            assert not errors, errors
            browser.close()
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()
    result = {
        "status": "PASS_LOCAL_BROWSER_REAL_VERIFIED_DATA_NOT_AWS_HOSTING",
        "checks": checks,
        "passed": len(checks),
        "public_stations": 1,
        "public_hours": 4,
        "instructor_stations": 3,
        "instructor_hours": 14,
        "aws_calls": 0,
        "athena_queries": 0,
    }
    (args.evidence / "browser-results.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False)
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
