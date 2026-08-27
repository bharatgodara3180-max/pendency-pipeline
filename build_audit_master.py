"""
Builds the enriched audit_master table from the freshly downloaded FWD/REV
pendency CSVs, plus the 5 reference sheets that still live in Google Sheets
(EXCEPTION, Layout Name Block Wise, MAPPING, EMP_DATA, Stagging).

This replicates the RDC AUDIT workbook's DATA-sheet formulas in Python,
computed once per pipeline run instead of by nested IFERROR/INDEX/MATCH
formulas recalculating across ~34,000 rows on every edit.

Known, deliberately-preserved quirks carried over from the original sheet
(not bugs I introduced -- see chat for the full explanation):
  - The exception override used for "blocks" reads EXCEPTION column E
    ("Pending With", e.g. a person's name), not column H ("Block").
  - REV's "last_destination" is always "TAURU_DC_FMRTS" -- the original
    formula's condition can never be true, so it never takes the other branch.
"""

import json
import os
import sys
from datetime import datetime, timezone

import gspread
import pandas as pd
from dateutil import parser as dateutil_parser
from google.oauth2.service_account import Credentials
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
AUDIT_SHEET_ID = os.environ.get("AUDIT_SHEET_ID")
FWD_CSV_PATH = os.environ.get("FWD_CSV_PATH", "fwd_pendency.csv")
REV_CSV_PATH = os.environ.get("REV_CSV_PATH", "rev_pendency.csv")
CHUNK_SIZE = 2000  # was 500 -- fewer, larger requests to cut total run time

if not all([SUPABASE_URL, SUPABASE_KEY, GOOGLE_SERVICE_ACCOUNT_JSON, AUDIT_SHEET_ID]):
    sys.exit(
        "Missing one of: SUPABASE_URL, SUPABASE_SECRET_KEY, "
        "GOOGLE_SERVICE_ACCOUNT_JSON, AUDIT_SHEET_ID"
    )

# Category -> which enrichment rules apply, confirmed against the real sheet.
RDCPFC_CATEGORIES = {"NOT IN BAG / Received at DC", "CLIENT Warehouse"}
AT_DOCKBRSNR_CATEGORIES = {"IN BAG / At Dock", "IN BAG / BRSNR"}


def load_reference_sheets():
    creds_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(AUDIT_SHEET_ID)

    def rows(tab_name):
        return sh.worksheet(tab_name).get_all_values()

    return {
        "EXCEPTION": rows("EXCEPTION"),
        "Layout Name Block Wise": rows("Layout Name Block Wise"),
        "MAPPING": rows("MAPPING"),
        "EMP_DATA": rows("EMP_DATA"),
        "Stagging": rows("Stagging"),
        "AREA BARCODE": rows("AREA BARCODE"),
    }


def sync_area_barcodes(supabase, ref):
    """Mirrors the AREA BARCODE tab (barcode -> area name) into Supabase.
    Not used by the audit_master enrichment itself -- only the scan app
    needs this one, for turning a scanned area QR into a readable name."""
    rows_ = ref.get("AREA BARCODE", [])[1:]
    records = [
        {"barcode": str(r[0]).strip().upper(), "area_name": r[1]}
        for r in rows_
        if r and len(r) > 1 and r[0]
    ]
    if not records:
        return
    print(f"Syncing {len(records)} area barcodes...")
    supabase.table("area_barcodes").upsert(records, on_conflict="barcode").execute()


def build_lookup_maps(ref):
    # EXCEPTION columns: A=AWB, B=Category, C=Moved to GA, D=POC,
    # E=Pending With, F=Status, G=Inbound Date, H=Block
    exc_rows = ref["EXCEPTION"][1:]
    exception_blocks_override = {}
    exception_block_col = {}
    exception_salvage = {}
    exception_inbound_date = {}
    for row in exc_rows:
        if not row or not row[0]:
            continue
        awb = str(row[0]).strip().upper()
        exception_blocks_override[awb] = row[4] if len(row) > 4 else ""
        exception_salvage[awb] = row[2] if len(row) > 2 else ""
        exception_inbound_date[awb] = row[6] if len(row) > 6 else ""
        exception_block_col[awb] = row[7] if len(row) > 7 else ""

    # Layout Name Block Wise: A=Block A, B=Block D, C=PURPLE, D=Block B,
    # E=Primary, F=Secondary, G=INTRA, H=ZONAL, I=AIR, J=LARGE, K=SAVANA, L=TAURU NYKAA
    lw_rows = ref["Layout Name Block Wise"][1:]

    def col_set(idx):
        return {r[idx].strip() for r in lw_rows if len(r) > idx and r[idx].strip()}

    block_layout_sets = {
        "Block A": col_set(0),
        "Block D": col_set(1),
        "PURPLE": col_set(2),
        "Block B": col_set(3),
        "SAVANA": col_set(10),
        "TAURU NYKAA": col_set(11),
    }
    dest_type_sets = {
        "INTRA": col_set(6),
        "ZONAL": col_set(7),
        "AIR": col_set(8),
        "LARGE": col_set(9),
    }

    # MAPPING: A/B = station -> primary bin. D/E/F/G/H = station(D) with
    # secondary bin name in H (5th column counting from D).
    map_rows = ref["MAPPING"][1:]
    mapping_ab = {r[0].strip(): r[1].strip() for r in map_rows if len(r) > 1 and r[0].strip()}
    mapping_d_to_h = {r[3].strip(): r[7].strip() for r in map_rows if len(r) > 7 and r[3].strip()}

    # EMP_DATA: A = action_user id, B = employee name
    emp_rows = ref["EMP_DATA"][1:]
    emp_map = {str(r[0]).strip(): r[1].strip() for r in emp_rows if len(r) > 1 and r[0]}

    # Stagging: A=Zone, B=Cage, C=Next_Destination, D=Zone+Cage key, E=category
    stag_rows = ref["Stagging"][1:]
    stagging_c_to_d = {r[2].strip(): r[3].strip() for r in stag_rows if len(r) > 3 and r[2].strip()}
    stagging_d_to_c = {r[3].strip(): r[2].strip() for r in stag_rows if len(r) > 3 and r[3].strip()}
    stagging_d_to_e = {r[3].strip(): r[4].strip() for r in stag_rows if len(r) > 4 and r[3].strip()}

    return {
        "exception_blocks_override": exception_blocks_override,
        "exception_block_col": exception_block_col,
        "exception_salvage": exception_salvage,
        "exception_inbound_date": exception_inbound_date,
        "block_layout_sets": block_layout_sets,
        "dest_type_sets": dest_type_sets,
        "mapping_ab": mapping_ab,
        "mapping_d_to_h": mapping_d_to_h,
        "emp_map": emp_map,
        "stagging_c_to_d": stagging_c_to_d,
        "stagging_d_to_c": stagging_d_to_c,
        "stagging_d_to_e": stagging_d_to_e,
    }


def parse_sheet_date(raw):
    """Google Sheets dates come back as plain text (often DD/MM/YYYY, Indian
    style), which Postgres will misread or reject outright. Parse explicitly
    with day-first assumed, rather than letting the database guess."""
    if not raw or not str(raw).strip():
        return None
    try:
        return dateutil_parser.parse(str(raw).strip(), dayfirst=True).isoformat()
    except (ValueError, OverflowError):
        return None


def resolve_block(layout_name, awb, lookups):
    override = lookups["exception_blocks_override"].get(awb)
    if override:
        return override
    for block_name, layout_set in lookups["block_layout_sets"].items():
        if layout_name in layout_set:
            return block_name
    return "Block A"  # matches the original formula's own fallback


def enrich_rdcpfc_style(row, lookups):
    dest = row.get("item_destination_name") or ""
    primary_bin = lookups["mapping_ab"].get(dest, "CFGR")
    secondary_bin = lookups["mapping_d_to_h"].get(dest, "CHECK")
    last_destination = "CFGR"
    for dest_type, bins in lookups["dest_type_sets"].items():
        if primary_bin in bins:
            last_destination = dest_type
            break
    return primary_bin, secondary_bin, last_destination


def enrich_at_dockbrsnr_style(row, lookups):
    manifest_dest = row.get("manifest_destination_name") or ""
    manifest_prev = row.get("manifest_previous_location_name") or ""
    primary_bin = (
        lookups["stagging_c_to_d"].get(manifest_dest)
        or lookups["stagging_c_to_d"].get(manifest_prev)
        or lookups["stagging_c_to_d"].get("NCR_Bilaspur_DC", "")
    )
    secondary_bin = lookups["stagging_d_to_c"].get(primary_bin, "")
    last_destination = lookups["stagging_d_to_e"].get(primary_bin, "")
    return primary_bin, secondary_bin, last_destination


def enrich_rev_style(is_at_dockbrsnr_variant):
    secondary = "Block B" if is_at_dockbrsnr_variant else "REV PROCESSING AREA"
    return "CFGR", secondary, "TAURU_DC_FMRTS"


def enrich_dataframe(df, lookups, report_type):
    records = []
    for _, row in df.iterrows():
        awb = str(row.get("awb_number") or "").strip().upper()
        category = row.get("category") or ""
        layout_name = row.get("layout_name") or ""

        if category in RDCPFC_CATEGORIES:
            if report_type == "FWD":
                primary_bin, secondary_bin, last_destination = enrich_rdcpfc_style(row, lookups)
            else:
                primary_bin, secondary_bin, last_destination = enrich_rev_style(False)
        elif category in AT_DOCKBRSNR_CATEGORIES:
            if report_type == "FWD":
                primary_bin, secondary_bin, last_destination = enrich_at_dockbrsnr_style(row, lookups)
            else:
                primary_bin, secondary_bin, last_destination = enrich_rev_style(True)
        else:
            primary_bin, secondary_bin, last_destination = "", "", ""

        action_user = row.get("action_user") or ""
        item_last_updated = row.get("item_last_updated")
        has_timestamp = item_last_updated is not None and pd.notna(item_last_updated)
        pendency_type = str(category).replace("NOT IN BAG / ", "").replace("IN BAG / ", "")

        records.append({
            "awb_number": awb,
            "aging_bucket": row.get("aging_bucket"),
            "action_user": action_user,
            "bin_level": row.get("bin_level"),
            "bin_name": row.get("bin_name"),
            "layout_name": layout_name,
            "client_name": row.get("client_name"),
            "item_destination_name": row.get("item_destination_name"),
            "item_last_updated": item_last_updated if has_timestamp else None,
            "pendency_type": pendency_type,
            "shipment_type": row.get("shipment_type") if "shipment_type" in row else None,
            "emp_name": lookups["emp_map"].get(str(action_user).strip(), ""),
            "blocks": resolve_block(layout_name, awb, lookups),
            "last_destination": last_destination,
            "primary_bin": primary_bin,
            "secondary_bin": secondary_bin,
            "hour": str(item_last_updated)[11:13] if has_timestamp else None,
            "salvage_type": lookups["exception_salvage"].get(awb, ""),
            "inbound_date": parse_sheet_date(lookups["exception_inbound_date"].get(awb)),
            "block": lookups["exception_block_col"].get(awb, ""),
            "report_type": report_type,
        })
    return records


def main():
    captured_at = datetime.now(timezone.utc).isoformat()
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("Loading reference sheets from Google Sheets...")
    ref = load_reference_sheets()
    lookups = build_lookup_maps(ref)
    sync_area_barcodes(supabase, ref)

    fwd_present = os.path.exists(FWD_CSV_PATH)
    rev_present = os.path.exists(REV_CSV_PATH)

    if not fwd_present or not rev_present:
        # audit_master is one shared table -- rebuilding it from only one
        # side would wipe out the other side's rows entirely (the truncate
        # clears everything). Safer to skip the rebuild this run and keep
        # the last complete version than to replace it with an incomplete
        # one. fwd/rev_pendency_current are unaffected by this -- those
        # already updated correctly in upload_to_supabase.py.
        missing = "FWD" if not fwd_present else "REV"
        print(f"{missing} file missing this run -- skipping audit_master rebuild "
              f"to avoid wiping the other side. Keeping the last complete version.")
        return

    records = []
    print(f"Reading {FWD_CSV_PATH}...")
    fwd_df = pd.read_csv(FWD_CSV_PATH, dtype=str, na_values=["\\N"], keep_default_na=True)
    print("Enriching FWD rows...")
    records += enrich_dataframe(fwd_df, lookups, "FWD")

    print(f"Reading {REV_CSV_PATH}...")
    rev_df = pd.read_csv(REV_CSV_PATH, dtype=str, na_values=["\\N"], keep_default_na=True)
    print("Enriching REV rows...")
    records += enrich_dataframe(rev_df, lookups, "REV")

    for r in records:
        r["captured_at"] = captured_at
    records = [{k: (None if pd.isna(v) else v) for k, v in r.items()} for r in records]

    print("Clearing previous audit_master snapshot...")
    supabase.rpc("truncate_pendency_table", {"target_table": "audit_master"}).execute()

    total = len(records)
    print(f"Inserting {total} audit_master rows...")
    for i in range(0, total, CHUNK_SIZE):
        supabase.table("audit_master").insert(records[i:i + CHUNK_SIZE]).execute()
        done = min(i + CHUNK_SIZE, total)
        if done % 5000 == 0 or done == total:
            print(f"  {done}/{total} rows")

    print("Done.")


if __name__ == "__main__":
    main()
