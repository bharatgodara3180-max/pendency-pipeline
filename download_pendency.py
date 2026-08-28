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


def wait_for_page_settle(page):
    """networkidle only means network requests stopped -- it does NOT mean
    the Angular app finished its own internal data-fetch/render cycle
    (confirmed via a screenshot showing a loading spinner still spinning
    mid-table well after networkidle fired). This gives it real time to
    finish before we try clicking anything on the page."""
    page.wait_for_timeout(5000)


def download_report(page, save_as, attempts=1):
    """Clicks Apply & Download, waits for the file to be generated, and
    downloads it. If it never becomes ready, re-clicks Apply & Download
    from scratch (up to `attempts` times) rather than giving up after one
    try -- the portal occasionally doesn't start generating the file at all,
    and a fresh click resolves it.

    All clicks use force=True: a dark modal backdrop (Angular Material's
    CDK overlay) sometimes sits on top of these buttons and blocks a
    normal click for a long time before Playwright gives up -- forcing
    skips that check and clicks through it immediately."""
    last_error = None
    for attempt_num in range(1, attempts + 1):
        try:
            page.locator("button.dwnld-btn").click(force=True)
            page.get_by_role("button", name="Apply & Download").click(force=True)

            ready = False
            for _ in range(20):  # ~2 minutes
                page.wait_for_timeout(6000)
                if page.locator("button.download-btn").count() > 0:
                    ready = True
                    break
                refresh = page.locator("button.download-chk-btn")
                if refresh.count() > 0:
                    try:
                        refresh.first.click(force=True)
                    except Exception:
                        pass

            if not ready:
                raise RuntimeError(f"{save_as}: file never became ready (attempt {attempt_num}/{attempts}).")

            with page.expect_download(timeout=120000) as download_info:
                page.locator("button.download-btn").first.click(force=True)
            download_info.value.save_as(save_as)
            print(f"  saved -> {save_as} (attempt {attempt_num})")
            return
        except Exception as e:
            last_error = e
            print(f"  attempt {attempt_num}/{attempts} for {save_as} failed: {e}")
            if attempt_num < attempts:
                print("  retrying from scratch...")

    raise RuntimeError(f"{save_as}: all {attempts} attempts failed. Last error: {last_error}")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        try:
            print(f"Going to {FWD_URL} ...")
            page.goto(FWD_URL, wait_until="networkidle")
            wait_for_page_settle(page)

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
            wait_for_page_settle(page)

            results = {"fwd": False, "rev": False}

            print("Downloading FWD...")
            try:
                download_report(page, "fwd_pendency.csv")
                results["fwd"] = True
            except Exception as e:
                print(f"FWD download failed after retries: {e}")
                os.makedirs(SCREENSHOT_DIR, exist_ok=True)
                page.screenshot(path=os.path.join(SCREENSHOT_DIR, "fwd_failure.png"), full_page=True)

            print(f"Going to {REV_URL} ...")
            try:
                page.goto(REV_URL, wait_until="networkidle")
                wait_for_page_settle(page)
                if "Please login" in page.content():
                    print("Logging in again for REV...")
                    if not click_login(page):
                        raise RuntimeError("Could not click the Login button.")
                    page.locator("#input_ecom_username").fill(USERNAME)
                    page.locator("#input_ecom_password").fill(PASSWORD)
                    page.locator("#btn_ecom_signin").click()
                    page.locator("#input_ecom_username").wait_for(state="hidden", timeout=60000)
                    page.wait_for_timeout(5000)
                    page.goto(REV_URL, wait_until="networkidle")
                wait_for_page_settle(page)
                print("Downloading REV...")
                download_report(page, "rev_pendency.csv")
                results["rev"] = True
            except Exception as e:
                print(f"REV download failed after retries: {e}")
                os.makedirs(SCREENSHOT_DIR, exist_ok=True)
                page.screenshot(path=os.path.join(SCREENSHOT_DIR, "rev_failure.png"), full_page=True)

            if not results["fwd"] and not results["rev"]:
                os.makedirs(SCREENSHOT_DIR, exist_ok=True)
                page.screenshot(path=os.path.join(SCREENSHOT_DIR, "failure.png"), full_page=True)
                raise RuntimeError("Both FWD and REV downloads failed -- nothing to push this run.")

            if not results["fwd"]:
                print("\nWARNING: FWD failed, continuing with REV only.")
            if not results["rev"]:
                print("\nWARNING: REV failed, continuing with FWD only.")
            if results["fwd"] and results["rev"]:
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
