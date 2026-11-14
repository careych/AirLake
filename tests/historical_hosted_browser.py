"""Opt-in HTTPS asset and rendered-page checks within the new 40-GET budget."""

import argparse
import io
import json
import sys
import zipfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", type=Path, required=True)
    parser.add_argument("--confirm-plan-sha256", required=True)
    parser.add_argument("--browser-executable", type=Path)
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from citizen.release import DOMAIN, private_credentials
    from citizen.release import load as release_load
    from final_demo.control import Receipt
    from historical.cloud import load

    _, digest = load(args.packet)
    if args.confirm_plan_sha256 != digest:
        parser.exit(1, "STOP: exact new hosting GET approval required\n")
    child = args.packet / "website-release"
    release_load(child)
    prior = json.loads((child / "release-receipt.json").read_text())
    if prior["status"] != "PASS_DEPLOYED_BROWSER_VERIFICATION_REQUIRED" or [
        i["status"] for i in prior["http_checks"]
    ] != [401, 200]:
        raise ValueError(
            "Successful deployment and two recorded credential checks required"
        )
    # Reserve every request before sending; never resubmit an uncertain check.
    receipt = Receipt(args.packet / "website-http.json", digest)
    receipt.value.update(gets_previously_used=2, get_limit=40, requests=[])
    receipt.save()
    _, credentials, _ = private_credentials(child / "preview-login.json")
    import requests
    from playwright.sync_api import sync_playwright

    http = requests.Session()
    http.trust_env = False
    cache = {}

    def fetch(branch, name, expected, authenticated=False):
        if len(receipt.value["requests"]) + 2 >= 40:
            raise ValueError("Website GET budget exhausted")
        path = "" if name == "index.html" else name
        url = "https://" + branch + "." + DOMAIN + "/" + path
        item = {
            "branch": branch,
            "asset": name,
            "authenticated": authenticated,
            "status": "UNCERTAIN",
        }
        receipt.value["requests"].append(item)
        receipt.save()
        response = http.get(
            url,
            auth=(credentials["username"], credentials["password"])
            if authenticated
            else None,
            timeout=25,
            allow_redirects=False,
        )
        item["status"] = response.status_code
        receipt.save()
        if expected is None:
            if response.status_code != 401:
                raise ValueError(
                    "Restricted asset accessible anonymously; stop and contain"
                )
        else:
            if response.status_code != 200 or response.content != expected:
                raise ValueError("HTTPS bytes do not match verified release; no retry")
            cache[url] = (
                response.content,
                response.headers.get("Content-Type", "application/octet-stream"),
            )

    for branch in ("public", "demo"):
        with zipfile.ZipFile(io.BytesIO((child / (branch + ".zip")).read_bytes())) as z:
            for name in sorted(z.namelist()):
                if branch == "demo":
                    fetch(branch, name, None)
                fetch(branch, name, z.read(name), authenticated=branch == "demo")

    def route(request_route):
        url = request_route.request.url
        if url not in cache:
            request_route.abort()
            return
        body, content_type = cache[url]
        request_route.fulfill(status=200, body=body, content_type=content_type)

    # Render only the exact HTTPS bytes just fetched. Reuse in-memory responses
    # at each viewport so Chrome cannot silently spend extra Athena/HTTP requests.
    with sync_playwright() as p:
        chrome = args.browser_executable
        mac = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
        if chrome is None and mac.exists():
            chrome = mac
        browser = p.chromium.launch(
            headless=True, **({"executable_path": str(chrome)} if chrome else {})
        )
        for branch, count in (("public", 1), ("demo", 3)):
            context = browser.new_context(
                locale="vi-VN", timezone_id="Asia/Ho_Chi_Minh", service_workers="block"
            )
            context.route("**/*", route)
            page = context.new_page()
            errors = []
            page.on("pageerror", lambda e, errors=errors: errors.append(str(e)))
            page.goto("https://" + branch + "." + DOMAIN + "/")
            page.wait_for_function("window.AirLake?.getData()?.cloud_verified===true")
            assert page.locator("#station option").count() == count
            assert "BẢN KIỂM THỬ" not in page.locator("#notice").inner_text()
            page.locator("a[data-nav=xu-huong]").click()
            page.locator("#history-range").select_option("30d")
            for width, height, device in (
                (1440, 1100, "desktop"),
                (768, 1024, "tablet"),
                (390, 844, "phone"),
            ):
                page.set_viewport_size({"width": width, "height": height})
                page.wait_for_timeout(120)
                assert page.evaluate("document.documentElement.scrollWidth<=innerWidth")
                assert page.locator("#daily-chart [data-history-value]").count() == 30
                assert page.locator("#trend-chart [data-observation]").count() == 720
                page.screenshot(
                    path=str(args.packet / (branch + "-" + device + ".png")),
                    full_page=True,
                )
            page.locator("#history-range").select_option("7d")
            assert page.locator("#trend-chart [data-observation]").count() == 168
            page.locator("a[data-nav=nghien-cuu]").click()
            page.locator("#research-station").select_option("6123215")
            assert "720 dòng" in page.locator("#research-count").inner_text()
            assert page.locator("#download-json").is_enabled()
            if branch == "demo":
                page.locator("#research-station").select_option("4946813")
                assert page.locator("#download-json").is_disabled()
            assert not errors
            context.close()
        browser.close()
    receipt.value.update(
        status="PASS_HTTPS_ASSETS_AUTH_AND_RENDERED_VIEWPORTS",
        total_gets=len(receipt.value["requests"]) + 2,
        separate_device="OWNER_CONFIRMATION_REQUIRED",
    )
    receipt.save()
    print(
        json.dumps(
            {
                "status": receipt.value["status"],
                "total_gets": receipt.value["total_gets"],
            }
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 -- credentials, response data and private URLs stay out of stdout
        print(
            json.dumps(
                {
                    "status": "STOP_PRESERVE_EVIDENCE",
                    "exception_type": type(exc).__name__,
                }
            )
        )
        raise SystemExit(1) from None
