"""
Downloads the FWD and REV floor pendency reports from the Shadowfax ops
portal as fwd_pendency.csv / rev_pendency.csv.

Built to run inside GitHub Actions, not on a laptop. Every step takes a
screenshot, and a full Playwright trace is recorded the whole way through.
If a step fails, open the run in the Actions tab, download the
"playwright-trace" artifact, then drag the trace.zip file into
https://trace.playwright.dev — it replays the whole run visually,
click by click, so we can see exactly where and why it stopped.
"""

import os
import sys

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

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


def download_report(page, save_as):
    print("  opening download panel...")
    page.locator("button.dwnld-btn").click()
    snap(page, f"{save_as}_panel_opened")

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
            # Step 1: go straight to the FWD report.
            print(f"Going to {FWD_URL} ...")
            page.goto(FWD_URL, wait_until="networkidle")
            snap(page, "fwd_first_visit")

            login_button = page.locator("button:has-text('Login')")
            try:
                login_button.wait_for(state="visible", timeout=8000)
                print("  'Please login' screen shown -- logging in...")
                login_button.click(force=True)
                page.wait_for_load_state("networkidle")
                snap(page, "clicked_login_button")

                # Step 2: fill the login form and sign in.
                page.locator("#input_ecom_username").fill(USERNAME)
                page.locator("#input_ecom_password").fill(PASSWORD)
                snap(page, "login_filled")
                page.locator("#btn_ecom_signin").click()
                page.wait_for_load_state("networkidle")
                snap(page, "after_login")

                # Go back to the FWD report now that we're logged in.
                print(f"Going to {FWD_URL} again after login...")
                page.goto(FWD_URL, wait_until="networkidle")
                snap(page, "fwd_after_login_revisit")
            except PlaywrightTimeoutError:
                print("  went straight to the report, no login screen needed")

            # Step 3: download FWD.
            download_report(page, "fwd_pendency.csv")

            # Step 4: go to REV and repeat step 3.
            print(f"Going to {REV_URL} ...")
            page.goto(REV_URL, wait_until="networkidle")
            snap(page, "rev_page_loaded")
            download_report(page, "rev_pendency.csv")

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
