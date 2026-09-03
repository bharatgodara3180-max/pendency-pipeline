"""
Uploads the FWD and REV floor pendency CSV exports into Cloudflare.

For now, run this by hand after downloading the two files from the portal:
    python upload_to_Cloudflare.py

Later, this becomes step 2 of the automated pipeline — the download
script will call this automatically instead of you running it manually.
"""

import os
import sys
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from cf_store import put_json, get_json

load_dotenv()

CF_API_URL = os.environ.get("CF_API_URL")
CF_API_TOKEN = os.environ.get("CF_API_TOKEN")
FWD_CSV_PATH = os.environ.get("FWD_CSV_PATH", "fwd_pendency.csv")
REV_CSV_PATH = os.environ.get("REV_CSV_PATH", "rev_pendency.csv")

if not CF_API_URL or not CF_API_TOKEN:
    sys.exit("Missing CF_API_URL or CF_API_TOKEN — check GitHub secrets.")

FWD_COLUMNS = [
    "awb_number", "category", "location_id", "location_name", "region", "aging_bucket",
    "item_received_time_ist", "manifest_received_time_ist", "manifest_id", "manifest_type",
    "manifest_code", "rejection_category", "shipment_type", "staging_area_code", "bag_source_type",
    "item_status_text", "action_user", "bin_level", "bin_name", "layout_name", "layout_type",
    "seal_number", "manifest_status_text", "manifest_origin_name", "manifest_origin_type",
    "manifest_destination_name", "manifest_destination_type", "manifest_next_location_name",
    "next_location", "manifest_previous_location_name", "client_name", "client_category",
    "next_std_time", "item_destination_name", "item_destination_type", "doh_remarks", "reason",
    "doh_flag", "item_last_updated", "received_from_client_warehouse", "inter_intra_flag",
    "order_type", "next_location_source",
]

REV_COLUMNS = [
    "awb_number", "category", "location_id", "location_name", "region", "aging_bucket",
    "item_received_time_ist", "manifest_received_time_ist", "manifest_id", "manifest_type",
    "manifest_code", "rejection_category", "shipment_type", "staging_area_code", "bag_source_type",
    "item_status_text", "action_user", "bin_level", "bin_name", "layout_name", "layout_type",
    "seal_number", "manifest_status_text", "manifest_origin_name", "manifest_origin_type",
    "manifest_destination_name", "manifest_destination_type", "manifest_next_location_name",
    "manifest_previous_location_name", "client_name", "client_category", "next_std_time",
    "item_destination_name", "item_destination_type", "item_last_updated",
    "received_from_client_warehouse", "dsp_awb_number", "connected_awb", "source_type",
    "order_type", "doh_flag", "inter_intra_flag",
]

CHUNK_SIZE = 2000  # was 500 -- fewer, larger requests to cut total run time


def normalize_aging_bucket(val):
    """The raw WMS export doesn't stop counting at 6 days -- it keeps
    going day by day (7_day, 8_day, 9_day, 10_day, 10+_day, ...), but the
    dashboard only has columns up through "6+_day". Anything past 6_day
    gets collapsed into that one bucket so it's actually counted instead
    of silently falling outside every known column."""
    if not val or pd.isna(val):
        return val
    s = str(val).strip()
    core = s[:-4] if s.lower().endswith("_day") else s
    if "+" in core:
        return "6+_day"
    try:
        days = float(core)
    except ValueError:
        return val
    return "6+_day" if days > 6 else s


def load_csv(path, columns, label):
    if not os.path.exists(path):
        sys.exit(f"Can't find the {label} file at: {path}\nCheck the path / filename.")
    df = pd.read_csv(path, dtype=str, na_values=["\\N"], keep_default_na=True)
    missing = [c for c in columns if c not in df.columns]
    if missing:
        sys.exit(
            f"{label} file is missing expected columns: {missing}\n"
            f"This usually means the portal changed its export format — send me the file."
        )
    df = df[columns].copy()
    if "doh_flag" in df.columns:
        df["doh_flag"] = df["doh_flag"].map({"True": True, "False": False})
    if "aging_bucket" in df.columns:
        df["aging_bucket"] = df["aging_bucket"].map(normalize_aging_bucket)
    return df


def to_clean_records(df):
    # Convert row by row and replace any kind of missing value with a real
    # None, so it becomes SQL NULL instead of breaking the upload.
    records = df.to_dict(orient="records")
    return [{k: (None if pd.isna(v) else v) for k, v in r.items()} for r in records]


def replace_current(_store, table, records, captured_at):
    for r in records:
        r["captured_at"] = captured_at
    print(f"  {table}: writing KV snapshot ({len(records)} rows)...")
    object_name = table + ".json.gz"
    put_json(object_name, records)


def summary_rows(df, report_type, captured_at):
    grouped = (
        df.groupby(["category", "aging_bucket"], dropna=False)
        .size()
        .reset_index(name="shipment_count")
    )
    grouped["report_type"] = report_type
    grouped["captured_at"] = captured_at
    return grouped.to_dict(orient="records")


def main():
    captured_at = datetime.now(timezone.utc).isoformat()
    print(f"Run started: {captured_at}")

    store = None

    fwd_df = None
    rev_df = None

    if os.path.exists(FWD_CSV_PATH):
        print(f"\nReading FWD file: {FWD_CSV_PATH}")
        fwd_df = load_csv(FWD_CSV_PATH, FWD_COLUMNS, "FWD")
        print(f"  {len(fwd_df)} rows")
    else:
        print(f"\nFWD file not found ({FWD_CSV_PATH}) -- skipping FWD this run.")

    if os.path.exists(REV_CSV_PATH):
        print(f"\nReading REV file: {REV_CSV_PATH}")
        rev_df = load_csv(REV_CSV_PATH, REV_COLUMNS, "REV")
        print(f"  {len(rev_df)} rows")
    else:
        print(f"\nREV file not found ({REV_CSV_PATH}) -- skipping REV this run.")

    if fwd_df is None and rev_df is None:
        sys.exit("Neither FWD nor REV file is present -- nothing to upload.")

    if fwd_df is not None:
        print("\nUploading FWD...")
        replace_current(store, "fwd_pendency_current", to_clean_records(fwd_df), captured_at)

    if rev_df is not None:
        print("\nUploading REV...")
        replace_current(store, "rev_pendency_current", to_clean_records(rev_df), captured_at)

    print("\nBuilding + uploading trend summary...")
    summary = []
    if fwd_df is not None:
        summary += summary_rows(fwd_df, "FWD", captured_at)
    if rev_df is not None:
        summary += summary_rows(rev_df, "REV", captured_at)
    if summary:
        history = get_json("pendency_snapshot_summary.json.gz", []) or []
        history.extend(summary)
        # Keep a bounded trend history. The dashboard only needs recent snapshots.
        cutoff = pd.Timestamp(captured_at) - pd.Timedelta(days=31)
        history = [r for r in history if pd.Timestamp(r.get("captured_at")) >= cutoff]
        put_json("pendency_snapshot_summary.json.gz", history)
        print(f"  stored {len(summary)} summary rows; retained {len(history)} history rows")

    print("\nDone — data is live in Cloudflare KV.")


if __name__ == "__main__":
    main()
