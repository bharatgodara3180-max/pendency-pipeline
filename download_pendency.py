"""
Downloads the FWD and REV floor pendency reports from the Shadowfax ops
portal as fwd_pendency.csv / rev_pendency.csv.

Runs inside GitHub Actions, not on a laptop. Trimmed down now that the
flow is confirmed working -- only takes a screenshot if something actually
fails, instead of at every step (that was for debugging the original setup).
"""

import os
import sys

from playwright.sync_api import sync_playwright

FWD_URL = "https://ecomnew.shadowfax.in/floor-pendency-tracking"
REV_URL = "https://ecomnew.shadowfax.in/floor-pendency-tracking-rev"
USERNAME = os.environ.get("SHADOWFAX_USERNAME")
PASSWORD = os.environ.get("SHADOWFAX_PASSWORD")
SCREENSHOT_DIR = "debug_screenshots"

if not USERNAME or not PASSWORD:
    sys.exit("Missing SHADOWFAX_USERNAME or SHADOWFAX_PASSWORD environment variables.")


def click_login(page):
    """Try several ways to click the Login button; return True if one worked."""
    attempts = [
        "button:has-text('Login')",
        "a:has-text('Login')",
        "[role=button]:has-text('Login')",
        "text=Login",
    ]
    for selector in attempts:
        loc = page.locator(selector).first
        try:
            if loc.count() > 0:
                loc.click(force=True, timeout=10000)
                return True
        except Exception:
            pass
    return False


def download_report(page, save_as):
    page.locator("button.dwnld-btn").click()
    page.get_by_role("button", name="Apply & Download").click()

    # File is generated server-side -- poll until it's ready, hitting
    # Refresh each time, up to ~2 minutes.
    for attempt in range(20):
        page.wait_for_timeout(6000)
        if page.locator("button.download-btn").count() > 0:
            break
        refresh = page.locator("button.download-chk-btn")
        if refresh.count() > 0:
            try:
                refresh.first.click()
            except Exception:
                pass
    else:
        raise RuntimeError(f"{save_as}: file never became ready to download.")

    with page.expect_download(timeout=120000) as download_info:
        page.locator("button.download-btn").first.click()
    download_info.value.save_as(save_as)
    print(f"  saved -> {save_as}")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        try:
            print(f"Going to {FWD_URL} ...")
            page.goto(FWD_URL, wait_until="networkidle")

            if "Please login" in page.content():
                print("Logging in...")
                if not click_login(page):
                    raise RuntimeError("Could not click the Login button.")

                page.locator("#input_ecom_username").fill(USERNAME)
                page.locator("#input_ecom_password").fill(PASSWORD)
                page.locator("#btn_ecom_signin").click()
                page.locator("#input_ecom_username").wait_for(state="hidden", timeout=60000)
                page.wait_for_timeout(5000)

                print(f"Going to {FWD_URL} again after login...")
                page.goto(FWD_URL, wait_until="networkidle")

            print("Downloading FWD...")
            download_report(page, "fwd_pendency.csv")

            print(f"Going to {REV_URL} ...")
            page.goto(REV_URL, wait_until="networkidle")
            print("Downloading REV...")
            download_report(page, "rev_pendency.csv")

            print("\nBoth files downloaded successfully.")
        except Exception as e:
            os.makedirs(SCREENSHOT_DIR, exist_ok=True)
            page.screenshot(path=os.path.join(SCREENSHOT_DIR, "failure.png"), full_page=True)
            print(f"\nStopped early on page: {page.url}")
            print(f"Error: {e}")
            raise
        finally:
            browser.close()


if __name__ == "__main__":
    main()
