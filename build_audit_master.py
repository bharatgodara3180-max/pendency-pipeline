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

import os
import sys
from datetime import datetime, timedelta, timezone

import gspread
import pandas as pd
import requests
from dateutil import parser as dateutil_parser
from google.auth import default as google_auth_default
from cf_store import CFStore, put_json

CF_API_URL = os.environ.get("CF_API_URL")
CF_API_TOKEN = os.environ.get("CF_API_TOKEN")
AUDIT_SHEET_ID = os.environ.get("AUDIT_SHEET_ID")
FWD_CSV_PATH = os.environ.get("FWD_CSV_PATH", "fwd_pendency.csv")
REV_CSV_PATH = os.environ.get("REV_CSV_PATH", "rev_pendency.csv")
CHUNK_SIZE = 500  # was 500 -- fewer, larger requests to cut total run time
NTFY_TOPIC = os.environ.get("NTFY_TOPIC")           # FWD alerts
NTFY_TOPIC_REV = os.environ.get("NTFY_TOPIC_REV")    # REV alerts -- separate channel

if not all([CF_API_URL, CF_API_TOKEN, AUDIT_SHEET_ID]):
    sys.exit(
        "Missing one of: CF_API_URL, CF_API_TOKEN, AUDIT_SHEET_ID"
    )

# Category -> which enrichment rules apply, confirmed against the real sheet.
RDCPFC_CATEGORIES = {"NOT IN BAG / Received at DC", "CLIENT Warehouse"}
AT_DOCKBRSNR_CATEGORIES = {"IN BAG / At Dock", "IN BAG / BRSNR"}


# All Google-Sheet data needed by the audit build is fetched in one
# values.batchGet request per pipeline run. The download step has already
# replaced FWD_RAW/REV_RAW in this same workbook.
SHEET_VALUES_CACHE = {}


def load_reference_sheets():
    creds, _ = google_auth_default(
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(AUDIT_SHEET_ID)

    tab_names = [
        "FWD_RAW",
        "REV_RAW",
        "EXCEPTION",
        "Layout Name Block Wise",
        "MAPPING",
        "EMP_DATA",
        "Stagging",
        "SDD LOAD",
        "AIR LOAD",
        "NDD LOAD",
    ]
    ranges = [f"'{name}'!A:ZZ" for name in tab_names]

    try:
        response = sh.values_batch_get(ranges)
    except Exception as e:
        raise RuntimeError(f"Could not read PENDENCY MASTER tabs: {e}") from e

    value_ranges = response.get("valueRanges", [])
    values_by_tab = {}
    for name, vr in zip(tab_names, value_ranges):
        values_by_tab[name] = vr.get("values", [])

    SHEET_VALUES_CACHE.clear()
    SHEET_VALUES_CACHE.update(values_by_tab)

    def required_rows(tab_name):
        rows = values_by_tab.get(tab_name, [])
        if not rows:
            raise RuntimeError(f"Required worksheet '{tab_name}' is empty or missing.")
        return rows

    return {
        "EXCEPTION": required_rows("EXCEPTION"),
        "Layout Name Block Wise": required_rows("Layout Name Block Wise"),
        "MAPPING": required_rows("MAPPING"),
        "EMP_DATA": required_rows("EMP_DATA"),
        "Stagging": required_rows("Stagging"),
        "FWD_RAW": required_rows("FWD_RAW"),
        "REV_RAW": required_rows("REV_RAW"),
    }


LOAD_PENDING_SHEETS = ["SDD LOAD", "AIR LOAD", "NDD LOAD"]


def sync_load_pending_summary(store):
    """Reads the 3 Vehicle-Pending tabs (SDD/AIR/NDD LOAD) from the same
    mapping Google Sheet and mirrors their summary rows into store for
    the TV view's "Load Pending" screen. Same layout on every tab:
      row 2 = SHIPMENT COUNT, row 3 = VEHICLE COUNT, row 5 = status labels
      (Delivered / Intransit / No Load / Vehicle placed; Departure
      pending / Grand Total / ...), column A is just the "VEHICLE NO."
      label. Whatever labels/columns actually exist are read dynamically
      -- nothing about the exact column layout is hardcoded, only the
      row numbers (2/3/5) are fixed, since that's the one thing confirmed
      identical across all three tabs."""
    def to_num(v):
        try:
            return int(str(v).replace(",", "").strip())
        except (ValueError, TypeError):
            return None

    records = []
    for tab_name in LOAD_PENDING_SHEETS:
        rows = SHEET_VALUES_CACHE.get(tab_name, [])
        if not rows:
            print(f"WARNING: Load Pending worksheet '{tab_name}' missing/empty -- skipping.")
            continue
        if len(rows) < 5:
            print(f"WARNING: '{tab_name}' has fewer than 5 rows -- skipping.")
            continue

        shipment_row = rows[1]  # row 2
        vehicle_row = rows[2]   # row 3
        header_row = rows[4]    # row 5

        for col_idx in range(1, len(header_row)):  # skip column A (the "VEHICLE NO." label column)
            label = header_row[col_idx].strip() if col_idx < len(header_row) else ""
            if not label:
                continue
            shipment_val = shipment_row[col_idx] if col_idx < len(shipment_row) else ""
            vehicle_val = vehicle_row[col_idx] if col_idx < len(vehicle_row) else ""
            records.append({
                "sheet_name": tab_name,
                "status_label": label,
                "shipment_count": to_num(shipment_val),
                "vehicle_count": to_num(vehicle_val),
            })

    if not records:
        print("Load Pending: nothing read from any tab -- leaving existing data as-is.")
        return

    put_json("load_pending_summary.json.gz", records)
    print(f"Load Pending: synced {len(records)} rows across {len(LOAD_PENDING_SHEETS)} tabs to KV.")


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


def normalize_aging_bucket(val):
    """The raw WMS export doesn't stop counting at 6 days -- it keeps
    going day by day (7_day, 8_day, 9_day, 10_day, 10+_day, ...), but the
    Live Pendency dashboard only has columns through "6+_day". Anything
    past 6_day gets collapsed into that one bucket -- same normalization
    as upload_to_store.py, kept in sync so audit_master's aging_bucket
    matches what the pendency summary shows."""
    if not val:
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


def normalize_timestamp(raw):
    """Keeps item_last_updated as plain IST wall-clock time -- exactly what
    the WMS portal shows, no UTC conversion at all. Every timestamp column
    that stores this (audit_master.item_last_updated, awb_last_seen,
    primary_scan_events/secondary_scan_events.occurred_at) must be a plain `timestamp` column
    (NOT `timestamptz`), otherwise Postgres/store Studio will display
    it in UTC regardless of what's written here. This only re-formats the
    raw CSV value into one consistent string shape."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    try:
        dt = dateutil_parser.parse(str(raw).strip())
    except (ValueError, OverflowError):
        return None
    return dt.strftime("%Y-%m-%d %H:%M:%S")


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
        item_last_updated_raw = row.get("item_last_updated")
        has_timestamp = item_last_updated_raw is not None and pd.notna(item_last_updated_raw)
        item_last_updated = normalize_timestamp(item_last_updated_raw) if has_timestamp else None
        pendency_type = str(category).replace("NOT IN BAG / ", "").replace("IN BAG / ", "")

        records.append({
            "awb_number": awb,
            "manifest_code": row.get("manifest_code"),
            "seal_number": row.get("seal_number"),
            "aging_bucket": normalize_aging_bucket(row.get("aging_bucket")),
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
            "hour": str(item_last_updated_raw)[11:13] if has_timestamp else None,
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


def _parse_dt(val):
    """Parses a timestamp value regardless of exact string shape -- what
    we write is "YYYY-MM-DD HH:MM:SS" (space), but PostgREST reads a
    plain `timestamp` column back out as "YYYY-MM-DDTHH:MM:SS" (T). A raw
    string comparison between those two shapes is unreliable (a space
    character sorts before 'T', so it could compare wrong regardless of
    the actual times), so every comparison must go through this parser
    instead of str(a) > str(b)."""
    if not val:
        return None
    try:
        return dateutil_parser.parse(str(val).strip())
    except (ValueError, OverflowError):
        return None


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


def purge_old_rows(store, table, column="detected_at", days=15):
    """awb_update_alerts is an append-only log -- without this it grows
    forever. Keeps only the last `days` days of rows."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    try:
        store.table(table).delete().lt(column, cutoff).execute()
        print(f"Purged {table} rows older than {cutoff}.")
    except Exception as e:
        print(f"  failed to purge old {table} rows: {e}")


def check_for_updates_and_alert(store, records):
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
            store.table("awb_last_seen")
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
        store.table("awb_update_alerts").insert({
            "awb_number": awb,
            "aging_bucket": r.get("aging_bucket"),
            "pendency_type": r.get("pendency_type"),
            "report_type": r.get("report_type"),
            "last_destination": r.get("last_destination"),
        }).execute()
        # Findings Status tracking -- one active row per AWB, capturing the
        # stage it was AT WHEN ALERTED so findings_cleanup.py can later
        # tell whether it has since progressed (closed) or not (open).
        # upsert (not insert) so a re-alert on the same AWB just refreshes
        # this row instead of erroring on a duplicate key.
        store.table("active_findings").upsert({
            "awb_number": awb,
            "report_type": r.get("report_type"),
            "pendency_type": r.get("pendency_type"),
            "blocks": r.get("blocks"),
            "aging_bucket": r.get("aging_bucket"),
            "seal_number": r.get("seal_number"),
            "last_destination": r.get("last_destination"),
        }, on_conflict="awb_number").execute()

    for r in tracked_records:
        awb = r["awb_number"]
        current_val = r.get("item_last_updated")
        prev_val = previous.get(awb)

        if prev_val is None:
            # Always seed silently on first sighting -- never alert here.
            to_upsert.append({"awb_number": awb, "item_last_updated": current_val})
            continue

        if current_val and prev_val and _parse_dt(current_val) and _parse_dt(prev_val) and _parse_dt(current_val) > _parse_dt(prev_val):
            if is_alert_eligible(r.get("aging_bucket")):
                record_alert(r, awb, current_val)
            else:
                # Genuinely changed, but still only 1_day -- update the
                # baseline so a repeat check later only alerts on a
                # further change past this point, without notifying now.
                to_upsert.append({"awb_number": awb, "item_last_updated": current_val})

    if to_upsert:
        for i in range(0, len(to_upsert), CHUNK_SIZE):
            store.table("awb_last_seen").upsert(
                to_upsert[i:i + CHUNK_SIZE], on_conflict="awb_number"
            ).execute()

    purge_old_rows(store, "awb_update_alerts")

    print(f"Update-detection: checked {len(tracked_records)} tracked shipments, sent {alerts_sent} alerts.")


# FWD-only: CLIENT Warehouse and BRSNR are two different categories but the
# same physical step ("PFC"); Received at DC is "RDC" (bin distinguished by
# bin_level); At Dock is the final step before a shipment clears out of
# pendency entirely. REV doesn't follow this flow, so stage-tracking is
# scoped to report_type == "FWD" only.
FWD_PFC_TYPES = {"CLIENT Warehouse", "BRSNR"}


def _fetch_awb_time_map(store, table, time_column):
    """Small tables only (currently-at-RDC / currently-at-Dock shipments,
    not the full historical volume) -- cheap to fetch every run. Returns
    {awb_number: last_recorded_time} so callers can detect a genuinely
    NEW scan (item_last_updated moved forward) vs. the same snapshot
    being re-observed by this run's poll."""
    times = {}
    start = 0
    PAGE = 1000
    while True:
        resp = store.table(table).select(f"awb_number, {time_column}").range(start, start + PAGE - 1).execute()
        for row in resp.data:
            times[row["awb_number"]] = row.get(time_column)
        if len(resp.data) < PAGE:
            break
        start += PAGE
    return times


def _is_new_scan(new_time, prev_time):
    """True if this is a genuinely fresh scan -- either the AWB wasn't
    tracked before at all, or its item_last_updated has moved forward
    since we last recorded it. This is what lets a re-scan of a
    shipment that never left RDC still count (the WMS bumps
    item_last_updated on every real scan action, even when the
    category/bin doesn't change), while a repeat poll of the exact same
    unchanged snapshot does NOT get double-counted."""
    if prev_time is None:
        return True
    nt, pt = _parse_dt(new_time), _parse_dt(prev_time)
    if nt is None or pt is None:
        return False
    return nt > pt


def log_primary_secondary_events(store, records):
    """Classifies every FWD shipment as Primary or Secondary PURELY by
    bin_level -- the old "at RDC" / "At Dock" pendency_type logic has
    been removed entirely, per requirement:
      bin_level == "1"  -> Primary
      bin_level == "2"  -> Secondary (even if the shipment's pendency_type
                           is still "Received at DC" -- bin_level 2 always
                           wins, checked first)
    A Primary/Secondary event is logged the first time this is seen, or
    again whenever item_last_updated moves forward while the shipment
    keeps that same bin_level (a genuine re-scan), same freshness logic
    as before -- just keyed off bin_level now instead of pendency_type.
    rdc_last_seen / at_dock_last_seen are reused as the "currently bin
    level 1" / "currently bin level 2" tracking tables (same table
    names, redefined meaning) so no new tables are needed."""
    existing_primary = _fetch_awb_time_map(store, "rdc_last_seen", "rdc_time")
    existing_secondary = _fetch_awb_time_map(store, "at_dock_last_seen", "at_dock_time")

    current_primary = set()
    current_secondary = set()
    primary_rows = []
    secondary_rows = []
    primary_track_rows = []
    secondary_track_rows = []

    for r in records:
        if r.get("report_type") != "FWD":
            continue
        bin_level = str(r.get("bin_level") or "").strip()
        awb = r["awb_number"]
        new_time = r.get("item_last_updated")

        if bin_level == "2":
            # Checked first -- bin_level 2 always means Secondary, even if
            # pendency_type still reads "Received at DC".
            current_secondary.add(awb)
            secondary_track_rows.append({"awb_number": awb, "at_dock_time": new_time})
            if _is_new_scan(new_time, existing_secondary.get(awb)):
                secondary_rows.append({
                    "awb_number": awb,
                    "action_user": r.get("action_user"), "emp_name": r.get("emp_name"),
                    "client_name": r.get("client_name"), "layout_name": r.get("layout_name"),
                    "blocks": r.get("blocks"), "occurred_at": new_time,
                })
        elif bin_level == "1":
            current_primary.add(awb)
            primary_track_rows.append({"awb_number": awb, "rdc_time": new_time, "bin_level": bin_level})
            if _is_new_scan(new_time, existing_primary.get(awb)):
                primary_rows.append({
                    "awb_number": awb,
                    "action_user": r.get("action_user"), "emp_name": r.get("emp_name"),
                    "client_name": r.get("client_name"), "layout_name": r.get("layout_name"),
                    "blocks": r.get("blocks"), "occurred_at": new_time,
                })

    pfc_rows = [
        {"awb_number": r["awb_number"], "first_seen_at": r.get("item_last_updated"), "pendency_type": r.get("pendency_type")}
        for r in records
        if r.get("report_type") == "FWD" and (r.get("pendency_type") or "").strip() in FWD_PFC_TYPES
    ]

    rdc_to_clear = list(existing_primary.keys() - current_primary)
    at_dock_to_clear = list(existing_secondary.keys() - current_secondary)

    # Plain inserts -- genuine duplicates over time are exactly what's
    # wanted now (two real scans of the same AWB, hours apart, should
    # both count), the freshness check above is what stops a shipment
    # sitting still from being logged every single 15-minute cycle.
    if primary_rows:
        for i in range(0, len(primary_rows), CHUNK_SIZE):
            store.table("primary_scan_events").insert(primary_rows[i:i + CHUNK_SIZE]).execute()

    if secondary_rows:
        for i in range(0, len(secondary_rows), CHUNK_SIZE):
            store.table("secondary_scan_events").insert(secondary_rows[i:i + CHUNK_SIZE]).execute()

    if pfc_rows:
        for i in range(0, len(pfc_rows), CHUNK_SIZE):
            store.table("pfc_first_seen").upsert(
                pfc_rows[i:i + CHUNK_SIZE], on_conflict="awb_number", ignore_duplicates=True
            ).execute()

    if primary_track_rows:
        for i in range(0, len(primary_track_rows), CHUNK_SIZE):
            store.table("rdc_last_seen").upsert(
                primary_track_rows[i:i + CHUNK_SIZE], on_conflict="awb_number"
            ).execute()

    if secondary_track_rows:
        for i in range(0, len(secondary_track_rows), CHUNK_SIZE):
            store.table("at_dock_last_seen").upsert(
                secondary_track_rows[i:i + CHUNK_SIZE], on_conflict="awb_number"
            ).execute()

    if rdc_to_clear:
        for i in range(0, len(rdc_to_clear), 200):
            store.table("rdc_last_seen").delete().in_("awb_number", rdc_to_clear[i:i + 200]).execute()

    if at_dock_to_clear:
        for i in range(0, len(at_dock_to_clear), 200):
            store.table("at_dock_last_seen").delete().in_("awb_number", at_dock_to_clear[i:i + 200]).execute()

    purge_old_rows(store, "primary_scan_events", column="occurred_at")
    purge_old_rows(store, "secondary_scan_events", column="occurred_at")

    print(f"Scan events: {len(primary_rows)} new primary, {len(secondary_rows)} new secondary, "
          f"{len(pfc_rows)} PFC rows, {len(primary_track_rows)} Primary(bin1) rows, {len(secondary_track_rows)} Secondary(bin2) rows, "
          f"{len(rdc_to_clear)} left RDC, {len(at_dock_to_clear)} left At Dock.")



def main():
    captured_at = datetime.now(timezone.utc).isoformat()
    store = CFStore()

    print("Loading reference sheets from Google Sheets...")
    ref = load_reference_sheets()
    lookups = build_lookup_maps(ref)

    fwd_rows = ref.get("FWD_RAW", [])
    rev_rows = ref.get("REV_RAW", [])

    if len(fwd_rows) < 2 or len(rev_rows) < 2:
        print("FWD_RAW or REV_RAW has no data rows -- skipping audit_master rebuild.")
        return

    records = []
    print(f"Reading FWD_RAW from Google Sheets: {len(fwd_rows) - 1} rows...")
    fwd_df = pd.DataFrame(fwd_rows[1:], columns=fwd_rows[0], dtype=str)
    print("Enriching FWD rows...")
    records += enrich_dataframe(fwd_df, lookups, "FWD")

    print(f"Reading REV_RAW from Google Sheets: {len(rev_rows) - 1} rows...")
    rev_df = pd.DataFrame(rev_rows[1:], columns=rev_rows[0], dtype=str)
    print("Enriching REV rows...")
    records += enrich_dataframe(rev_df, lookups, "REV")

    for r in records:
        r["captured_at"] = captured_at
    records = [{k: (None if pd.isna(v) else v) for k, v in r.items()} for r in records]

    total = len(records)
    print(f"Writing {total} audit_master rows to compressed KV snapshot...")
    put_json("audit_master.json.gz", records)

    print("\nChecking for shipment updates...")
    check_for_updates_and_alert(store, records)

    print("\nLogging Primary/Secondary scan events...")
    log_primary_secondary_events(store, records)

    print("\nSyncing Load Pending summary (SDD/AIR/NDD LOAD tabs)...")
    try:
        sync_load_pending_summary(store)
    except Exception as e:
        print(f"  Load Pending sync failed (non-critical, leaving previous data): {e}")

    print("Done.")


if __name__ == "__main__":
    main()
