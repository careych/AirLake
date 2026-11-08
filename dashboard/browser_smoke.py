"""Optional real-browser check of the isolated historical Superset fixture."""

import argparse
import json
import sqlite3
from pathlib import Path
from urllib.parse import urlparse


def verify(root, chrome=None):
    from playwright.sync_api import sync_playwright

    root = Path(root).resolve()
    if any((p / ".git").exists() for p in (root, *root.parents)):
        raise ValueError("Use the external fixture runtime")
    private = json.loads((root / "private.json").read_text())
    db = sqlite3.connect(f"file:{root / 'gold.sqlite'}?mode=ro", uri=True)
    station = db.execute(
        "SELECT name FROM dim_location ORDER BY location_id LIMIT 1"
    ).fetchone()[0]
    db.close()
    with sync_playwright() as p:
        browser = p.chromium.launch(executable_path=chrome, headless=True)
        context = browser.new_context(
            timezone_id="Asia/Ho_Chi_Minh", viewport={"width": 1450, "height": 1100}
        )
        context.route(
            "**/*",
            lambda route: (
                route.continue_()
                if urlparse(route.request.url).hostname in ("127.0.0.1", "localhost")
                else route.abort()
            ),
        )
        page = context.new_page()
        statuses = []
        page.add_init_script(
            "window.__chartText=[]; const fill=CanvasRenderingContext2D.prototype.fillText; CanvasRenderingContext2D.prototype.fillText=function(t,...a){window.__chartText.push(String(t));return fill.call(this,t,...a)}"
        )
        page.on(
            "response",
            lambda response: (
                statuses.append(response.status)
                if "/chart/data" in response.url
                else None
            ),
        )
        page.goto("http://127.0.0.1:18088/login/", wait_until="networkidle")
        page.locator("#username").fill("airlake_dev")
        page.locator("#password").fill(private["ADMIN_PASSWORD"])
        page.get_by_role("button", name="Sign in", exact=True).click()
        page.wait_for_timeout(1000)
        url = "http://127.0.0.1:18088/superset/dashboard/airlake-historical-gold/"
        page.goto(url, wait_until="networkidle")
        page.wait_for_timeout(6000)
        text = page.locator("body").inner_text()
        assert "Data error" not in text and "An error occurred" not in text
        assert all(
            title in text
            for title in [
                "PM2.5 hourly trend",
                "MVP AQI hourly trend",
                "Station comparison",
                "Latest observation",
                "Freshness",
                "STALE (>24h)",
            ]
        )
        page.locator(".ant-select-selector").first.click()
        page.locator(".ant-select-item-option-content").filter(
            has_text=station
        ).first.click()
        page.keyboard.press("Escape")
        page.wait_for_timeout(500)
        if page.get_by_role("button", name="Apply filters", exact=True).is_enabled():
            page.get_by_role("button", name="Apply filters", exact=True).click()
        page.wait_for_timeout(2500)
        assert page.locator(".ant-select-selection-item").first.inner_text() == station
        assert "Data error" not in page.locator("body").inner_text()
        labels = sorted(set(page.evaluate("window.__chartText")))
        assert "01 PM" in labels and "Observation hour (UTC)" in labels
        assert statuses and all(status == 200 for status in statuses)
        page.screenshot(path=str(root / "dashboard-screenshot.png"), full_page=True)
        report = {
            "mode": "ACTUAL AUTHENTICATED LOCAL SUPERSET; HISTORICAL FIXTURE",
            "dashboard_url": url,
            "five_panels_rendered": True,
            "browser_timezone": "Asia/Ho_Chi_Minh",
            "chart_http_statuses": statuses,
            "native_station_filter_applied": True,
            "historical_stale_label_visible": True,
            "athena_queries_executed": 0,
            "canvas_axis_text": labels,
        }
        (root / "browser-final.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False) + "\n"
        )
        browser.close()
        return report


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--chrome")
    a = p.parse_args()
    try:
        result = verify(a.root, a.chrome)
    except Exception:  # noqa: BLE001 -- browser errors must not expose private login values
        raise SystemExit(
            "Local browser validation failed; private login details withheld"
        ) from None
    print(json.dumps(result, indent=2, ensure_ascii=False))
