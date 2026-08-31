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
from datetime import datetime, timedelta, timezone

import gspread
import pandas as pd
import requests
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
NTFY_TOPIC = os.environ.get("NTFY_TOPIC")           # FWD alerts
NTFY_TOPIC_REV = os.environ.get("NTFY_TOPIC_REV")    # REV alerts -- separate channel

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

    def rows(tab_name, required=True):
        try:
            return sh.worksheet(tab_name).get_all_values()
        except gspread.exceptions.WorksheetNotFound:
            if required:
                raise
            print(f"WARNING: worksheet '{tab_name}' not found -- skipping (non-critical).")
            return []

    return {
        "EXCEPTION": rows("EXCEPTION"),
        "Layout Name Block Wise": rows("Layout Name Block Wise"),
        "MAPPING": rows("MAPPING"),
        "EMP_DATA": rows("EMP_DATA"),
        "Stagging": rows("Stagging"),
    }

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

    # EMP_DATA: A = action_user id, B = employee name, D = employee's
    # default/assigned block (used as a fallback when layout matching fails)
    emp_rows = ref["EMP_DATA"][1:]
    emp_map = {str(r[0]).strip(): r[1].strip() for r in emp_rows if len(r) > 1 and r[0]}
    emp_default_block_map = {
        str(r[0]).strip(): r[3].strip() for r in emp_rows if len(r) > 3 and r[0] and r[3]
    }

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
        "emp_default_block_map": emp_default_block_map,
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


def resolve_block(layout_name, awb, action_user, lookups):
    override = lookups["exception_blocks_override"].get(awb)
    if override:
        return override
    for block_name, layout_set in lookups["block_layout_sets"].items():
        if layout_name in layout_set:
            return block_name
    fallback = lookups["emp_default_block_map"].get(str(action_user).strip())
    if fallback:
        return fallback
    return "Unknown"


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
    # Lookup order fixed: item_destination_name first (the shipment's own
    # declared destination), THEN next_location as a fallback if that's
    # blank/unmatched, THEN the NCR_Bilaspur_DC default -- previously this
    # checked manifest_destination_name/manifest_previous_location_name,
    # which was giving wrong last_destination values.
    item_dest = row.get("item_destination_name") or ""
    next_loc = row.get("next_location") or ""
    primary_bin = (
        lookups["stagging_c_to_d"].get(item_dest)
        or lookups["stagging_c_to_d"].get(next_loc)
        or lookups["stagging_c_to_d"].get("NCR_Bilaspur_DC", "")
    )
    secondary_bin = lookups["stagging_d_to_c"].get(primary_bin, "")
    last_destination = lookups["stagging_d_to_e"].get(primary_bin, "")
    return primary_bin, secondary_bin, last_destination


def enrich_rev_style(row, is_at_dockbrsnr_variant):
    secondary = "Block B" if is_at_dockbrsnr_variant else "REV PROCESSING AREA"

    item_destination = str(row.get("item_destination_name") or "").strip()

    if item_destination == "NBP_DC_FMRTS":
        last_destination = "NBP_DC_FMRTS"
    else:
        last_destination = "TAURU_DC_FMRTS"

    return "CFGR", secondary, last_destination


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
                primary_bin, secondary_bin, last_destination = enrich_rev_style(row, False)
        elif category in AT_DOCKBRSNR_CATEGORIES:
            if report_type == "FWD":
                primary_bin, secondary_bin, last_destination = enrich_at_dockbrsnr_style(row, lookups)
            else:
                primary_bin, secondary_bin, last_destination = enrich_rev_style(row, True)
        else:
            primary_bin, secondary_bin, last_destination = "", "", ""

        action_user = row.get("action_user") or ""
        item_last_updated = row.get("item_last_updated")
        has_timestamp = item_last_updated is not None and pd.notna(item_last_updated)
        pendency_type = str(category).replace("NOT IN BAG / ", "").replace("IN BAG / ", "")

        records.append({
            "awb_number": awb,
            "manifest_code": row.get("manifest_code"),
            "seal_number": row.get("seal_number"),
            "aging_bucket": row.get("aging_bucket"),
            "action_user": action_user,
            "bin_level": row.get("bin_level"),
            "bin_name": row.get("bin_name"),
            "rejection_category": row.get("rejection_category"),
            "layout_name": layout_name,
            "client_name": row.get("client_name"),
            "item_destination_name": row.get("item_destination_name"),
            "item_last_updated": item_last_updated if has_timestamp else None,
            "pendency_type": pendency_type,
            "shipment_type": row.get("shipment_type") if "shipment_type" in row else None,
            "emp_name": lookups["emp_map"].get(str(action_user).strip(), ""),
            "blocks": resolve_block(layout_name, awb, action_user, lookups),
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


def _ageing_days(val):
    """Parses an aging_bucket string ("0_day", "3_day", "6+_day") into a
    plain number for threshold comparisons. Returns None if unparseable."""
    if not val:
        return None
    s = str(val).strip()
    if s.lower().endswith("_day"):
        s = s[:-4]
    if "+" in s:
        return 99
    try:
        return float(s)
    except ValueError:
        return None


def is_tracked(val):
    """1_day and above -- a baseline item_last_updated gets recorded
    starting here, so a comparison point already exists by the time a
    shipment reaches the alert-eligible threshold below. Being "tracked"
    does NOT by itself trigger any notification."""
    days = _ageing_days(val)
    return days is not None and days >= 1


def is_alert_eligible(val):
    """2_day and above -- only shipments at this ageing level actually
    trigger a push notification when their item_last_updated changes.
    1_day shipments are still tracked (see is_tracked) but never alert."""
    days = _ageing_days(val)
    return days is not None and days >= 2


def send_update_alert(r):
    # FWD and REV go to separate ntfy channels so the two don't mix in one
    # feed. NTFY_TOPIC is FWD's (kept as-is so the existing secret name
    # doesn't need to change); NTFY_TOPIC_REV is the new one for REV.
    topic = NTFY_TOPIC if r.get("report_type") == "FWD" else NTFY_TOPIC_REV
    if not topic:
        print(f"  no ntfy topic configured for report_type={r.get('report_type')} -- skipping alert for {r.get('awb_number')}")
        return

    def line(label, key):
        return f"{label}: {r.get(key) or '-'}"

    message = "\n".join([
        line("AWB", "awb_number"),
        line("Ageing", "aging_bucket"),
        line("Item Last Updated", "item_last_updated"),
        line("Blocks", "blocks"),
        line("Layout Name", "layout_name"),
        line("Action User", "action_user"),
        line("Emp Name", "emp_name"),
        line("Bin Level", "bin_level"),
        line("Bin Name", "bin_name"),
        line("Rejection Category", "rejection_category"),
        line("Client Name", "client_name"),
        line("Last Destination", "last_destination"),
        line("Shipment Type", "shipment_type"),
    ])
    try:
        resp = requests.post(
            f"https://ntfy.sh/{topic}",
            data=message.encode("utf-8"),
            headers={
                "Title": f"Shipment updated: {r.get('awb_number', '-')}",
                "Priority": "high",
                "Tags": "warning",
            },
            timeout=15,
        )
        print(f"  ntfy status {resp.status_code} for {r.get('awb_number')} (topic={topic})")
    except Exception as e:
        print(f"  failed to send ntfy alert for {r.get('awb_number')}: {e}")


def purge_old_rows(supabase, table, column="detected_at", days=15):
    """awb_update_alerts is an append-only log -- without this it grows
    forever. Keeps only the last `days` days of rows."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    try:
        supabase.table(table).delete().lt(column, cutoff).execute()
        print(f"Purged {table} rows older than {cutoff}.")
    except Exception as e:
        print(f"  failed to purge old {table} rows: {e}")


def check_for_updates_and_alert(supabase, records):
    """Tracks item_last_updated for every 1_day+ shipment (so a baseline
    already exists by the time it reaches 2_day), but only ever sends a
    notification once a shipment is 2_day+ AND its item_last_updated has
    genuinely changed since the last run. First sighting of any AWB
    ALWAYS seeds its baseline silently -- never alerts by itself, no
    matter its ageing bucket -- otherwise every shipment that newly
    crosses into the tracked range fires an alert storm in one run."""
    if not NTFY_TOPIC and not NTFY_TOPIC_REV:
        print("Neither NTFY_TOPIC nor NTFY_TOPIC_REV is set -- skipping update-detection alerts.")
        return

    tracked_records = [r for r in records if is_tracked(r.get("aging_bucket"))]
    if not tracked_records:
        print("No currently-ageing shipments -- nothing to check for updates.")
        return

    unique_records = {}
    for r in tracked_records:
        awb = r["awb_number"]
        if awb not in unique_records or str(r.get("item_last_updated") or "") > str(unique_records[awb].get("item_last_updated") or ""):
            unique_records[awb] = r
    tracked_records = list(unique_records.values())

    awbs = list({r["awb_number"] for r in tracked_records})
    previous = {}
    LOOKUP_CHUNK = 200
    for i in range(0, len(awbs), LOOKUP_CHUNK):
        chunk = awbs[i:i + LOOKUP_CHUNK]
        resp = (
            supabase.table("awb_last_seen")
            .select("awb_number,item_last_updated")
            .in_("awb_number", chunk)
            .execute()
        )
        for row in resp.data:
            previous[row["awb_number"]] = row["item_last_updated"]

    to_upsert = []
    alerts_sent = 0

    def record_alert(r, awb, current_val):
        nonlocal alerts_sent
        send_update_alert(r)
        alerts_sent += 1
        to_upsert.append({"awb_number": awb, "item_last_updated": current_val})
        supabase.table("awb_update_alerts").insert({
            "awb_number": awb,
            "aging_bucket": r.get("aging_bucket"),
            "pendency_type": r.get("pendency_type"),
            "report_type": r.get("report_type"),
            "last_destination": r.get("last_destination"),
        }).execute()

    for r in tracked_records:
        awb = r["awb_number"]
        current_val = r.get("item_last_updated")
        prev_val = previous.get(awb)

        if prev_val is None:
            # Always seed silently on first sighting -- never alert here.
            to_upsert.append({"awb_number": awb, "item_last_updated": current_val})
            continue

        if current_val and prev_val and str(current_val) > str(prev_val):
            if is_alert_eligible(r.get("aging_bucket")):
                record_alert(r, awb, current_val)
            else:
                # Genuinely changed, but still only 1_day -- update the
                # baseline so a repeat check later only alerts on a
                # further change past this point, without notifying now.
                to_upsert.append({"awb_number": awb, "item_last_updated": current_val})

    if to_upsert:
        for i in range(0, len(to_upsert), CHUNK_SIZE):
            supabase.table("awb_last_seen").upsert(
                to_upsert[i:i + CHUNK_SIZE], on_conflict="awb_number"
            ).execute()

    purge_old_rows(supabase, "awb_update_alerts")

    print(f"Update-detection: checked {len(tracked_records)} tracked shipments, sent {alerts_sent} alerts.")


# FWD-only: CLIENT Warehouse and BRSNR are two different categories but the
# same physical step ("PFC"); Received at DC is "RDC" (bin distinguished by
# bin_level); At Dock is the final step before a shipment clears out of
# pendency entirely. REV doesn't follow this flow, so stage-tracking is
# scoped to report_type == "FWD" only.
FWD_PFC_TYPES = {"CLIENT Warehouse", "BRSNR"}


def get_stage_key(pendency_type, bin_level):
    """Returns (stage_label, stage_key) for the PFC -> RDC (bin) -> At Dock
    flow. stage_key is what's actually compared run-to-run; stage_label is
    just for messages. bin_level is only meaningful (and only checked) at
    the RDC stage -- whatever its real values are, a change in bin_level
    while still at RDC is picked up as a bin1->bin2 style move without
    needing to know those values in advance. Categories outside this flow
    return (None, None) and are skipped entirely."""
    pt = (pendency_type or "").strip()
    if pt in FWD_PFC_TYPES:
        return "PFC", "PFC"
    if pt == "Received at DC":
        bl = (bin_level or "").strip() or "(no bin)"
        return f"RDC bin {bl}", f"RDC:{bl}"
    if pt == "At Dock":
        return "At Dock", "AT_DOCK"
    return None, None


def check_stage_transitions_and_alert(supabase, records):
    """Tracks each FWD shipment's PFC -> RDC (bin) -> At Dock movement
    across pipeline runs, purely for state-keeping -- no alert of any kind
    is sent from here anymore. Normal stage-to-stage movement AND a
    shipment disappearing from the data (whatever stage it was last at)
    are both handled silently."""
    current_by_awb = {}
    for r in records:
        if r.get("report_type") != "FWD":
            continue
        label, key = get_stage_key(r.get("pendency_type"), r.get("bin_level"))
        if key is None:
            continue
        current_by_awb[r["awb_number"]] = (label, key, r)

    previous = {}
    PAGE = 1000
    start = 0
    while True:
        resp = (
            supabase.table("awb_stage_last_seen")
            .select("awb_number, stage_key, stage_label")
            .range(start, start + PAGE - 1)
            .execute()
        )
        for row in resp.data:
            previous[row["awb_number"]] = (row["stage_label"], row["stage_key"])
        if len(resp.data) < PAGE:
            break
        start += PAGE

    to_upsert = []
    to_delete = []

    for awb, (label, key, r) in current_by_awb.items():
        previous.pop(awb, None)
        to_upsert.append({"awb_number": awb, "stage_key": key, "stage_label": label})

    # Anything still left in `previous` was tracked last run but isn't in
    # this run's FWD data at all -- it has left the flow entirely. No
    # alert, no log -- just stop tracking it.
    for awb in previous.keys():
        to_delete.append(awb)

    if to_upsert:
        for i in range(0, len(to_upsert), CHUNK_SIZE):
            supabase.table("awb_stage_last_seen").upsert(
                to_upsert[i:i + CHUNK_SIZE], on_conflict="awb_number"
            ).execute()

    if to_delete:
        for i in range(0, len(to_delete), 200):
            supabase.table("awb_stage_last_seen").delete().in_(
                "awb_number", to_delete[i:i + 200]
            ).execute()

    print(f"Stage-tracking: {len(to_upsert)} shipments tracked, {len(to_delete)} left the flow (no alerts sent).")


def main():
    captured_at = datetime.now(timezone.utc).isoformat()
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("Loading reference sheets from Google Sheets...")
    ref = load_reference_sheets()
    lookups = build_lookup_maps(ref)

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

    print("\nChecking for shipment updates...")
    check_for_updates_and_alert(supabase, records)

    print("\nChecking for stage transitions (PFC -> RDC -> At Dock)...")
    check_stage_transitions_and_alert(supabase, records)

    print("Done.")


if __name__ == "__main__":
    main()
