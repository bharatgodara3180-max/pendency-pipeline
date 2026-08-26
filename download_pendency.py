"""
Logs into the Shadowfax ops portal and downloads the FWD and REV floor
pendency reports as fwd_pendency.csv / rev_pendency.csv.

Built to run inside GitHub Actions, not on a laptop. Every step takes a
screenshot, and a full Playwright trace is recorded the whole way through.
If a step fails, open the run in the Actions tab, download the
"playwright-trace" artifact, then drag the trace.zip file into
https://trace.playwright.dev — it replays the whole run visually,
click by click, so we can see exactly where and why it stopped.
"""

import os
import sys

from playwright.sync_api import sync_playwright

LOGIN_URL = "https://ecom.shadowfax.in/#/login"
FWD_URL = "https://ecomnew.shadowfax.in/floor-pendency-tracking"
REV_URL = "https://ecomnew.shadowfax.in/floor-pendency-tracking-rev"
USERNAME = os.environ.get("SHADOWFAX_USERNAME")
PASSWORD = os.environ.get("SHADOWFAX_PASSWORD")
SCREENSHOT_DIR = "debug_screenshots"

if not USERNAME or not PASSWORD:
    sys.exit("Missing SHADOWFAX_USERNAME or SHADOWFAX_PASSWORD environment variables.")

os.makedirs(SCREENSHOT_DIR, exist_ok=True)
_step_count = 0


def snap(page, label):
    """Save a numbered screenshot so we can see the run step by step afterward."""
    global _step_count
    _step_count += 1
    path = os.path.join(SCREENSHOT_DIR, f"{_step_count:02d}_{label}.png")
    page.screenshot(path=path, full_page=True)
    print(f"  [screenshot] {path}  (on page: {page.url})")


def login(page):
    print("Opening login page...")
    page.goto(LOGIN_URL, wait_until="networkidle")
    snap(page, "login_page")

    # Confirmed via DevTools inspection -- exact element IDs.
    page.locator("#input_ecom_username").fill(USERNAME)
    page.locator("#input_ecom_password").fill(PASSWORD)
    snap(page, "login_filled")

    page.locator("#btn_ecom_signin").click()
    page.wait_for_load_state("networkidle")
    snap(page, "after_login")


def download_report(page, report_url, save_as):
    print(f"Going to {report_url} ...")
    page.goto(report_url, wait_until="networkidle")
    snap(page, f"{save_as}_page_loaded")

    print("  clicking Apply & Download...")
    page.get_by_role("button", name="Apply & Download").click()
    snap(page, f"{save_as}_after_apply")

    print("  clicking Download...")
    with page.expect_download(timeout=60000) as download_info:
        page.get_by_role("button", name="Download").click()
    download = download_info.value
    download.save_as(save_as)
    print(f"  saved -> {save_as}")
    snap(page, f"{save_as}_downloaded")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context(accept_downloads=True)
        context.tracing.start(screenshots=True, snapshots=True, sources=True)
        page = context.new_page()
        try:
            login(page)
            download_report(page, FWD_URL, "fwd_pendency.csv")
            download_report(page, REV_URL, "rev_pendency.csv")
            print("\nBoth files downloaded successfully.")
        except Exception as e:
            snap(page, "failure")
            print(f"\nStopped early on page: {page.url}")
            print(f"Error: {e}")
            raise
        finally:
            context.tracing.stop(path="trace.zip")
            browser.close()


if __name__ == "__main__":
    main()
