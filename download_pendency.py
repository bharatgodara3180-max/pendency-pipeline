"""
Downloads the FWD and REV floor pendency reports from the Shadowfax ops
portal as fwd_pendency.csv / rev_pendency.csv.

Runs inside GitHub Actions, not on a laptop. Trimmed down now that the
flow is confirmed working -- only takes a screenshot if something actually
fails, instead of at every step (that was for debugging the original setup).
"""

import os
import sys

import gspread
import pandas as pd
from google.auth import default as google_auth_default
from playwright.sync_api import sync_playwright

FWD_URL = "https://ecomnew.shadowfax.in/floor-pendency-tracking"
REV_URL = "https://ecomnew.shadowfax.in/floor-pendency-tracking-rev"
USERNAME = os.environ.get("SHADOWFAX_USERNAME")
PASSWORD = os.environ.get("SHADOWFAX_PASSWORD")
SCREENSHOT_DIR = "debug_screenshots"
AUDIT_SHEET_ID = os.environ.get("AUDIT_SHEET_ID")

FWD_RAW_COLUMNS = [
    "awb_number", "category", "location_name", "aging_bucket", "manifest_code",
    "rejection_category", "shipment_type", "item_status_text", "action_user",
    "bin_level", "bin_name", "layout_name", "seal_number",
    "manifest_destination_name", "next_location", "client_name",
    "item_destination_name", "item_last_updated", "inter_intra_flag",
]

REV_RAW_COLUMNS = [
    "category", "aging_bucket", "manifest_code", "rejection_category",
    "action_user", "bin_level", "bin_name", "layout_name", "seal_number",
    "manifest_next_location_name", "client_name", "item_destination_name",
    "item_last_updated", "dsp_awb_number", "connected_awb", "inter_intra_flag",
]

RAW_WRITE_CHUNK = 5000

if not USERNAME or not PASSWORD:
    sys.exit("Missing SHADOWFAX_USERNAME or SHADOWFAX_PASSWORD environment variables.")


def _clean_for_sheet(df):
    """Return Google-Sheets-safe string values while preserving blank cells."""
    df = df.astype(object)
    df = df.where(pd.notna(df), "")
    return [[str(v) if v is not None else "" for v in row] for row in df.values.tolist()]


def _write_tab_in_chunks(ws, values):
    """Replace a raw tab with the current export using bounded requests."""
    rows = max(1, len(values))
    cols = max(1, len(values[0]) if values else 1)

    # Resize once so stale rows/columns from a previous, larger export disappear.
    ws.resize(rows=rows, cols=cols)

    if not values:
        return

    ws.update(values[:1], "A1", raw=True)
    start_row = 2
    for i in range(1, len(values), RAW_WRITE_CHUNK):
        chunk = values[i:i + RAW_WRITE_CHUNK]
        ws.update(chunk, f"A{start_row}", raw=True)
        start_row += len(chunk)


def push_raw_to_google_sheet(fwd_path=None, rev_path=None):
    """Push the filtered FWD/REV exports into the single PENDENCY MASTER workbook."""
    if not AUDIT_SHEET_ID:
        raise RuntimeError("Missing AUDIT_SHEET_ID.")

    creds, _ = google_auth_default(
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(AUDIT_SHEET_ID)

    if fwd_path and os.path.exists(fwd_path):
        fwd = pd.read_csv(
            fwd_path, dtype=str, na_values=["\\N"], keep_default_na=True
        )
        missing = [c for c in FWD_RAW_COLUMNS if c not in fwd.columns]
        if missing:
            raise RuntimeError(f"FWD export is missing columns: {missing}")
        fwd = fwd[FWD_RAW_COLUMNS].copy()
        fwd_values = [FWD_RAW_COLUMNS] + _clean_for_sheet(fwd)
        print(f"Writing FWD_RAW: {len(fwd)} rows...")
        _write_tab_in_chunks(sh.worksheet("FWD_RAW"), fwd_values)

    if rev_path and os.path.exists(rev_path):
        rev = pd.read_csv(
            rev_path, dtype=str, na_values=["\\N"], keep_default_na=True
        )
        # REV is only the Forward_Cancelled population. order_type is used
        # only as a filter and is deliberately not stored in REV_RAW.
        if "order_type" not in rev.columns:
            raise RuntimeError("REV export is missing required filter column: order_type")
        rev = rev[rev["order_type"].astype(str).str.strip().eq("Forward_Cancelled")].copy()
        missing = [c for c in REV_RAW_COLUMNS if c not in rev.columns]
        if missing:
            raise RuntimeError(f"REV export is missing columns: {missing}")
        rev = rev[REV_RAW_COLUMNS].copy()
        rev_values = [REV_RAW_COLUMNS] + _clean_for_sheet(rev)
        print(f"Writing REV_RAW: {len(rev)} rows (Forward_Cancelled only)...")
        _write_tab_in_chunks(sh.worksheet("REV_RAW"), rev_values)


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
    downloads it. Only one attempt is made."""

    last_error = None

    for attempt_num in range(1, attempts + 1):
        try:
            page.locator("button.dwnld-btn").click(force=True)
            page.get_by_role("button", name="Apply & Download").click(force=True)

            ready = False
            download_button = None

            for _ in range(20):  # ~2 minutes
                page.wait_for_timeout(6000)

                btn = page.locator("button.download-btn").first

                try:
                    btn.wait_for(state="visible", timeout=3000)

                    if btn.is_enabled():
                        download_button = btn
                        ready = True
                        break
                except Exception:
                    pass

                refresh = page.locator("button.download-chk-btn")
                if refresh.count() > 0:
                    try:
                        refresh.first.click(force=True)
                    except Exception:
                        pass

            if not ready or download_button is None:
                raise RuntimeError(
                    f"{save_as}: file never became ready "
                    f"(attempt {attempt_num}/{attempts})."
                )

            with page.expect_download(timeout=120000) as download_info:
                download_button.click(force=True)

            download_info.value.save_as(save_as)
            print(f"  saved -> {save_as} (attempt {attempt_num})")
            return

        except Exception as e:
            last_error = e
            print(
                f"  attempt {attempt_num}/{attempts} "
                f"for {save_as} failed: {e}"
            )

            if attempt_num < attempts:
                print("  retrying from scratch...")

    raise RuntimeError(
        f"{save_as}: all {attempts} attempts failed. "
        f"Last error: {last_error}"
    )


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
                page.locator("#input_ecom_username").wait_for(
                    state="hidden",
                    timeout=60000
                )
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
                page.screenshot(
                    path=os.path.join(
                        SCREENSHOT_DIR,
                        "fwd_failure.png"
                    ),
                    full_page=True
                )

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
                    page.locator("#input_ecom_username").wait_for(
                        state="hidden",
                        timeout=60000
                    )
                    page.wait_for_timeout(5000)
                    page.goto(REV_URL, wait_until="networkidle")

                wait_for_page_settle(page)

                print("Downloading REV...")
                download_report(page, "rev_pendency.csv")
                results["rev"] = True

            except Exception as e:
                print(f"REV download failed after retries: {e}")
                os.makedirs(SCREENSHOT_DIR, exist_ok=True)
                page.screenshot(
                    path=os.path.join(
                        SCREENSHOT_DIR,
                        "rev_failure.png"
                    ),
                    full_page=True
                )

            if not results["fwd"] and not results["rev"]:
                os.makedirs(SCREENSHOT_DIR, exist_ok=True)
                page.screenshot(
                    path=os.path.join(
                        SCREENSHOT_DIR,
                        "failure.png"
                    ),
                    full_page=True
                )
                raise RuntimeError(
                    "Both FWD and REV downloads failed "
                    "-- nothing to push this run."
                )

            if not results["fwd"]:
                print(
                    "\nWARNING: FWD failed, "
                    "continuing with REV only."
                )

            if not results["rev"]:
                print(
                    "\nWARNING: REV failed, "
                    "continuing with FWD only."
                )

            # Filter to the exact raw columns and push both datasets into the
            # same PENDENCY MASTER workbook. Failed side is not overwritten.
            push_raw_to_google_sheet(
                "fwd_pendency.csv" if results["fwd"] else None,
                "rev_pendency.csv" if results["rev"] else None,
            )

            if results["fwd"] and results["rev"]:
                print("\nBoth files downloaded and FWD_RAW/REV_RAW updated.")

        except Exception as e:
            os.makedirs(SCREENSHOT_DIR, exist_ok=True)
            page.screenshot(
                path=os.path.join(
                    SCREENSHOT_DIR,
                    "failure.png"
                ),
                full_page=True
            )
            print(f"\nStopped early on page: {page.url}")
            print(f"Error: {e}")
            raise

        finally:
            browser.close()


if __name__ == "__main__":
    main()
