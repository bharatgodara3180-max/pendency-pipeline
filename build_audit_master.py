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

AUDIT_SHEET_ID = os.environ.get("AUDIT_SHEET_ID")
FWD_CSV_PATH = os.environ.get("FWD_CSV_PATH", "fwd_pendency.csv")
REV_CSV_PATH = os.environ.get("REV_CSV_PATH", "rev_pendency.csv")
CHUNK_SIZE = 5000  # Google Sheets write chunk; keeps write-request count low
NTFY_TOPIC = os.environ.get("NTFY_TOPIC")           # FWD alerts
NTFY_TOPIC_REV = os.environ.get("NTFY_TOPIC_REV")    # REV alerts -- separate channel

if not AUDIT_SHEET_ID:
    sys.exit("Missing AUDIT_SHEET_ID")

# Category -> which enrichment rules apply, confirmed against the real sheet.
RDCPFC_CATEGORIES = {"NOT IN BAG / Received at DC", "CLIENT Warehouse"}
AT_DOCKBRSNR_CATEGORIES = {"IN BAG / At Dock", "IN BAG / BRSNR"}


# All Google-Sheet data needed by the audit build is fetched in one
# values.batchGet request per pipeline run. The download step has already
# replaced FWD_RAW/REV_RAW in this same workbook.
SHEET_VALUES_CACHE = {}


def load_reference_sheets():
    """Read all required Google Sheet tabs in ONE values.batchGet call.

    IMPORTANT: this function is READ-ONLY. This script reads FWD/REV and reference tabs, then writes the enriched
    AUDIT_MASTER and tracking/state tabs back into the same Google Spreadsheet.
    No Cloudflare storage is used.
    """
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
            raise RuntimeError(
                f"Required worksheet '{tab_name}' is empty or missing. "
                "Do not create PRIMARY_SCAN_EVENTS/SECONDARY_SCAN_EVENTS tabs; "
                "only the existing pipeline/reference tabs are required."
            )
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
    mapping Google Sheet and mirrors their summary rows into for local processing only. Same layout on every tab:
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

    print(f"Load Pending: read {len(records)} summary rows. No Cloudflare write performed.")


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



def _sheet_col(n):
    s = ""
    while n:
        n, rem = divmod(n - 1, 26)
        s = chr(65 + rem) + s
    return s


def _get_spreadsheet():
    creds, _ = google_auth_default(
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(AUDIT_SHEET_ID)


def _get_or_create_worksheet(sh, title, rows=1000, cols=30):
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Creating worksheet: {title}")
        ws = sh.add_worksheet(title=title, rows=max(rows, 100), cols=max(cols, 10))
    return ws


def write_records_to_sheet(sh, title, records, chunk_size=5000):
    """Replace a worksheet in a small number of batch value writes.

    The previous implementation wrote the ~97k-row AUDIT_MASTER to Cloudflare
    and wrote other state to a database. This version keeps the complete
    enriched AUDIT_MASTER in the same PENDENCY MASTER Google Spreadsheet.
    5,000-row chunks keep the number of Sheets write requests low enough to
    avoid the 60 writes/minute quota that the old 500-row implementation hit.
    """
    if not records:
        print(f"{title}: no rows to write.")
        return

    headers = list(records[0].keys())
    matrix = [headers]
    for r in records:
        matrix.append([None if pd.isna(r.get(h)) else r.get(h) for h in headers])

    ws = _get_or_create_worksheet(sh, title, rows=len(matrix), cols=len(headers))
    ws.resize(rows=max(len(matrix), 1), cols=max(len(headers), 1))
    ws.clear()

    last_col = _sheet_col(len(headers))
    total = len(matrix)
    print(f"{title}: writing {total - 1} data rows in chunks of {chunk_size}...")
    for start in range(0, total, chunk_size):
        end = min(start + chunk_size, total)
        rng = f"A{start + 1}:{last_col}{end}"
        ws.update(rng, matrix[start:end], raw=True)
        if start == 0 or end == total or end % (chunk_size * 5) == 0:
            print(f"{title}: wrote rows {start + 1}-{end}")


def _read_tab_records(sh, title):
    try:
        rows = sh.worksheet(title).get_all_values()
    except gspread.exceptions.WorksheetNotFound:
        return []
    if len(rows) < 2:
        return []
    headers = rows[0]
    return [dict(zip(headers, r + [""] * (len(headers) - len(r)))) for r in rows[1:]]


def _replace_small_tab(sh, title, headers, rows):
    ws = _get_or_create_worksheet(sh, title, rows=max(len(rows) + 1, 100), cols=max(len(headers), 5))
    matrix = [headers] + [[r.get(h, "") for h in headers] for r in rows]
    ws.resize(rows=max(len(matrix), 1), cols=max(len(headers), 1))
    ws.clear()
    last_col = _sheet_col(len(headers))
    for start in range(0, len(matrix), 5000):
        end = min(start + 5000, len(matrix))
        ws.update(f"A{start+1}:{last_col}{end}", matrix[start:end], raw=True)


def _parse_dt(val):
    if not val:
        return None
    try:
        return dateutil_parser.parse(str(val).strip())
    except (ValueError, OverflowError):
        return None


def _is_new_scan(new_time, prev_time):
    if prev_time is None:
        return True
    nt, pt = _parse_dt(new_time), _parse_dt(prev_time)
    if nt is None or pt is None:
        return False
    return nt > pt


def check_for_updates_and_alert(sh, records):
    if not NTFY_TOPIC and not NTFY_TOPIC_REV:
        print("No NTFY topic configured -- skipping update alerts.")
        return

    tracked = [r for r in records if is_tracked(r.get("aging_bucket"))]
    if not tracked:
        print("No tracked shipments -- nothing to check for updates.")
        return

    previous_rows = _read_tab_records(sh, "ALERT_STATE")
    previous = {str(r.get("awb_number", "")).strip().upper(): r.get("item_last_updated") for r in previous_rows if r.get("awb_number")}

    unique = {}
    for r in tracked:
        awb = r["awb_number"]
        if awb not in unique or (_parse_dt(r.get("item_last_updated")) or datetime.min) > (_parse_dt(unique[awb].get("item_last_updated")) or datetime.min):
            unique[awb] = r

    state = dict(previous)
    alerts = []
    active = {str(r.get("awb_number", "")).strip().upper(): r for r in _read_tab_records(sh, "ACTIVE_FINDINGS") if r.get("awb_number")}

    for r in unique.values():
        awb = r["awb_number"]
        current = r.get("item_last_updated")
        prev = previous.get(awb)
        if prev is None:
            state[awb] = current or ""
            continue
        if current and prev and _is_new_scan(current, prev):
            if is_alert_eligible(r.get("aging_bucket")):
                send_update_alert(r)
                alerts.append({
                    "awb_number": awb,
                    "aging_bucket": r.get("aging_bucket"),
                    "pendency_type": r.get("pendency_type"),
                    "report_type": r.get("report_type"),
                    "last_destination": r.get("last_destination"),
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                })
                active[awb] = {
                    "awb_number": awb,
                    "report_type": r.get("report_type"),
                    "pendency_type": r.get("pendency_type"),
                    "blocks": r.get("blocks"),
                    "aging_bucket": r.get("aging_bucket"),
                    "seal_number": r.get("seal_number"),
                    "last_destination": r.get("last_destination"),
                }
            state[awb] = current

    _replace_small_tab(sh, "ALERT_STATE", ["awb_number", "item_last_updated"], [
        {"awb_number": k, "item_last_updated": v} for k, v in state.items()
    ])

    if alerts:
        old = _read_tab_records(sh, "AWB_UPDATE_ALERTS")
        old.extend(alerts)
        cutoff = datetime.now(timezone.utc) - timedelta(days=15)
        kept = []
        for r in old:
            dt = _parse_dt(r.get("detected_at"))
            if dt is None or dt >= cutoff:
                kept.append(r)
        headers = ["awb_number", "aging_bucket", "pendency_type", "report_type", "last_destination", "detected_at"]
        _replace_small_tab(sh, "AWB_UPDATE_ALERTS", headers, kept)

    active_rows = list(active.values())
    _replace_small_tab(sh, "ACTIVE_FINDINGS", ["awb_number", "report_type", "pendency_type", "blocks", "aging_bucket", "seal_number", "last_destination"], active_rows)
    print(f"Update-detection: checked {len(unique)} tracked shipments, sent {len(alerts)} alerts.")


def _fetch_awb_time_map(sh, title, time_column):
    rows = _read_tab_records(sh, title)
    return {str(r.get("awb_number", "")).strip().upper(): r.get(time_column) for r in rows if r.get("awb_number")}


def log_primary_secondary_events(sh, records):
    existing_primary = _fetch_awb_time_map(sh, "RDC_LAST_SEEN", "rdc_time")
    existing_secondary = _fetch_awb_time_map(sh, "AT_DOCK_LAST_SEEN", "at_dock_time")
    current_primary, current_secondary = set(), set()
    primary_rows, secondary_rows = [], []
    primary_track, secondary_track = [], []

    for r in records:
        if r.get("report_type") != "FWD":
            continue
        level = str(r.get("bin_level") or "").strip()
        awb = r["awb_number"]
        t = r.get("item_last_updated")
        if level == "2":
            current_secondary.add(awb)
            secondary_track.append({"awb_number": awb, "at_dock_time": t})
            if _is_new_scan(t, existing_secondary.get(awb)):
                secondary_rows.append({
                    "awb_number": awb, "action_user": r.get("action_user"), "emp_name": r.get("emp_name"),
                    "client_name": r.get("client_name"), "layout_name": r.get("layout_name"),
                    "blocks": r.get("blocks"), "occurred_at": t,
                })
        elif level == "1":
            current_primary.add(awb)
            primary_track.append({"awb_number": awb, "rdc_time": t, "bin_level": level})
            if _is_new_scan(t, existing_primary.get(awb)):
                primary_rows.append({
                    "awb_number": awb, "action_user": r.get("action_user"), "emp_name": r.get("emp_name"),
                    "client_name": r.get("client_name"), "layout_name": r.get("layout_name"),
                    "blocks": r.get("blocks"), "occurred_at": t,
                })

    pfc_rows = [{"awb_number": r["awb_number"], "first_seen_at": r.get("item_last_updated"), "pendency_type": r.get("pendency_type")}
                for r in records if r.get("report_type") == "FWD" and (r.get("pendency_type") or "").strip() in FWD_PFC_TYPES]

    if primary_rows:
        old = _read_tab_records(sh, "PRIMARY_SCAN_EVENTS")
        old.extend(primary_rows)
        _replace_small_tab(sh, "PRIMARY_SCAN_EVENTS", ["awb_number","action_user","emp_name","client_name","layout_name","blocks","occurred_at"], old)
    if secondary_rows:
        old = _read_tab_records(sh, "SECONDARY_SCAN_EVENTS")
        old.extend(secondary_rows)
        _replace_small_tab(sh, "SECONDARY_SCAN_EVENTS", ["awb_number","action_user","emp_name","client_name","layout_name","blocks","occurred_at"], old)
    if pfc_rows:
        old = {r.get("awb_number"): r for r in _read_tab_records(sh, "PFC_FIRST_SEEN") if r.get("awb_number")}
        for r in pfc_rows:
            old.setdefault(r["awb_number"], r)
        _replace_small_tab(sh, "PFC_FIRST_SEEN", ["awb_number","first_seen_at","pendency_type"], list(old.values()))

    # Current-state tracking sheets are replaced in one batch each.
    _replace_small_tab(sh, "RDC_LAST_SEEN", ["awb_number","rdc_time","bin_level"], [
        {"awb_number": a, "rdc_time": r.get("rdc_time"), "bin_level": r.get("bin_level")} for a, r in {x["awb_number"]: x for x in primary_track}.items()
    ])
    _replace_small_tab(sh, "AT_DOCK_LAST_SEEN", ["awb_number","at_dock_time"], [
        {"awb_number": a, "at_dock_time": r.get("at_dock_time")} for a, r in {x["awb_number"]: x for x in secondary_track}.items()
    ])

    # Retain 15 days of event history.
    cutoff = datetime.now(timezone.utc) - timedelta(days=15)
    for title in ("PRIMARY_SCAN_EVENTS", "SECONDARY_SCAN_EVENTS"):
        rows = _read_tab_records(sh, title)
        kept = []
        for r in rows:
            dt = _parse_dt(r.get("occurred_at"))
            if dt is None or dt >= cutoff:
                kept.append(r)
        _replace_small_tab(sh, title, ["awb_number","action_user","emp_name","client_name","layout_name","blocks","occurred_at"], kept)

    print(f"Scan events: {len(primary_rows)} new primary, {len(secondary_rows)} new secondary, {len(pfc_rows)} PFC observations.")

def main():
    captured_at = datetime.now(timezone.utc).isoformat()

    print("Loading PENDENCY MASTER data from Google Sheets...")
    sh = _get_spreadsheet()
    ref = load_reference_sheets()
    lookups = build_lookup_maps(ref)

    fwd_rows = ref.get("FWD_RAW", [])
    rev_rows = ref.get("REV_RAW", [])
    if len(fwd_rows) < 2 or len(rev_rows) < 2:
        print("FWD_RAW or REV_RAW has no data rows -- stopping without changing AUDIT_MASTER.")
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
    print(f"Writing {total} audit_master rows to Google Sheet...")
    write_records_to_sheet(sh, "AUDIT_MASTER", records, chunk_size=5000)

    print("Checking for shipment updates...")
    check_for_updates_and_alert(sh, records)

    print("Logging Primary/Secondary scan events...")
    log_primary_secondary_events(sh, records)

    print("Syncing Load Pending summary (read-only)...")
    sync_load_pending_summary(None)

    print("DONE - no Cloudflare used by build_audit_master.py.")


if __name__ == "__main__":
    main()
