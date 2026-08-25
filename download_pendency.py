"""
Logs into the Shadowfax ops portal and downloads the FWD and REV floor
pendency reports as fwd_pendency.csv / rev_pendency.csv.

Built to run inside GitHub Actions, not on a laptop. Every step takes a
screenshot, and a full Playwright trace is recorded the whole way through.
If a step fails, open the run in the Actions tab, download the
"playwright-trace" artifact, then drag the trace.zip file into
https://trace.playwright.dev — it replays the whole run visually,
click by click, so we can see exactly where and why it stopped.

NOTE: the exact selectors below (button text, link text) are my best
guess based on how the portal was described to me, not something I
could see or test directly. The first run or two will likely need a
small fix once we can actually see where it stops — that's expected,
not a sign the overall approach is wrong.
"""

import os
import sys

from playwright.sync_api import sync_playwright

LOGIN_URL = "https://ecom.shadowfax.in/#/login"
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
    print(f"  [screenshot] {path}")


def login(page):
    print("Opening login page...")
    page.goto(LOGIN_URL, wait_until="networkidle")
    snap(page, "login_page")

    page.get_by_placeholder("Username").fill(USERNAME)
    page.get_by_placeholder("Password").fill(PASSWORD)
    snap(page, "login_filled")

    page.get_by_role("button", name="Login").click()
    page.wait_for_load_state("networkidle")
    snap(page, "after_login")


def go_to_pendency_report(page):
    print("Navigating: DC Live Dashboards > Fwd/Rev Floor > Pendency & Tracking...")
    page.get_by_text("DC Live Dashboards").click()
    snap(page, "clicked_dc_live_dashboards")

    page.get_by_text("Fwd/Rev Floor").click()
    snap(page, "clicked_fwd_rev_floor")

    page.get_by_text("Pendency & Tracking").click()
    page.wait_for_load_state("networkidle")
    snap(page, "pendency_tracking_page")


def download_report(page, save_as):
    print(f"Clicking Download, saving as {save_as}...")
    with page.expect_download(timeout=60000) as download_info:
        page.get_by_role("button", name="Download").click()
    download = download_info.value
    download.save_as(save_as)
    print(f"  saved -> {save_as}")
    snap(page, f"downloaded_{save_as}")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context(accept_downloads=True)
        context.tracing.start(screenshots=True, snapshots=True, sources=True)
        page = context.new_page()
        try:
            login(page)
            go_to_pendency_report(page)
            download_report(page, "fwd_pendency.csv")

            print("Switching to REV...")
            page.get_by_text("REV", exact=True).click()
            page.wait_for_load_state("networkidle")
            snap(page, "switched_to_rev")
            download_report(page, "rev_pendency.csv")

            print("\nBoth files downloaded successfully.")
        except Exception as e:
            snap(page, "failure")
            print(f"\nStopped early: {e}")
            raise
        finally:
            context.tracing.stop(path="trace.zip")
            browser.close()


if __name__ == "__main__":
    main()
