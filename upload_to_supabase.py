"""
Uploads the FWD and REV floor pendency CSV exports into Supabase.

For now, run this by hand after downloading the two files from the portal:
    python upload_to_supabase.py

Later, this becomes step 2 of the automated pipeline — the download
script will call this automatically instead of you running it manually.
"""

import os
import sys
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY")
FWD_CSV_PATH = os.environ.get("FWD_CSV_PATH", "fwd_pendency.csv")
REV_CSV_PATH = os.environ.get("REV_CSV_PATH", "rev_pendency.csv")

if not SUPABASE_URL or not SUPABASE_KEY:
    sys.exit("Missing SUPABASE_URL or SUPABASE_SECRET_KEY — check your .env file.")

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

CHUNK_SIZE = 500  # rows per insert request, keeps each request small and reliable


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
    return df


def to_clean_records(df):
    # Convert row by row and replace any kind of missing value with a real
    # None, so it becomes SQL NULL instead of breaking the upload.
    records = df.to_dict(orient="records")
    return [{k: (None if pd.isna(v) else v) for k, v in r.items()} for r in records]


def replace_current(supabase, table, records, captured_at):
    for r in records:
        r["captured_at"] = captured_at
    print(f"  {table}: clearing previous snapshot...")
    # Deleting row by row through the API times out on tables this size,
    # so call the TRUNCATE helper function instead -- it's instant.
    supabase.rpc("truncate_pendency_table", {"target_table": table}).execute()
    total = len(records)
    print(f"  {table}: inserting {total} rows...")
    for i in range(0, total, CHUNK_SIZE):
        supabase.table(table).insert(records[i:i + CHUNK_SIZE]).execute()
        done = min(i + CHUNK_SIZE, total)
        if done % 10000 == 0 or done == total:
            print(f"    {done}/{total} rows")

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

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print(f"\nReading FWD file: {FWD_CSV_PATH}")
    fwd_df = load_csv(FWD_CSV_PATH, FWD_COLUMNS, "FWD")
    print(f"  {len(fwd_df)} rows")

    print(f"\nReading REV file: {REV_CSV_PATH}")
    rev_df = load_csv(REV_CSV_PATH, REV_COLUMNS, "REV")
    print(f"  {len(rev_df)} rows")

    print("\nUploading FWD...")
    replace_current(supabase, "fwd_pendency_current", to_clean_records(fwd_df), captured_at)

    print("\nUploading REV...")
    replace_current(supabase, "rev_pendency_current", to_clean_records(rev_df), captured_at)

    print("\nBuilding + uploading trend summary...")
    summary = summary_rows(fwd_df, "FWD", captured_at) + summary_rows(rev_df, "REV", captured_at)
    supabase.table("pendency_snapshot_summary").insert(summary).execute()
    print(f"  inserted {len(summary)} summary rows")

    print("\nDone — data is live in Supabase.")


if __name__ == "__main__":
    main()
