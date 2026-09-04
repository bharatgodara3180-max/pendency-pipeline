"""Build AUDIT_MASTER entirely inside the PENDENCY MASTER Google Sheet.

NO CLOUDFLARE / D1 is used by this script.

Inputs (same workbook):
  FWD_RAW, REV_RAW, EXCEPTION, Layout Name Block Wise, MAPPING,
  EMP_DATA, Stagging, SDD LOAD, AIR LOAD, NDD LOAD

Outputs/state (same workbook):
  AUDIT_MASTER
  ALERT_STATE
  AWB_UPDATE_ALERTS
  PRIMARY_SCAN_EVENTS
  SECONDARY_SCAN_EVENTS
  PFC_FIRST_SEEN
  RDC_LAST_SEEN
  AT_DOCK_LAST_SEEN
  LOAD_PENDING_SUMMARY

NTFY alerts remain direct HTTP calls and do not use Cloudflare.
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
NTFY_TOPIC = os.environ.get("NTFY_TOPIC")
NTFY_TOPIC_REV = os.environ.get("NTFY_TOPIC_REV")

WRITE_CHUNK = 500
STATE_READ_CHUNK = 500

RDCPFC_CATEGORIES = {"NOT IN BAG / Received at DC", "CLIENT Warehouse"}
AT_DOCKBRSNR_CATEGORIES = {"IN BAG / At Dock", "IN BAG / BRSNR"}
FWD_PFC_TYPES = {"CLIENT Warehouse", "BRSNR"}
LOAD_PENDING_SHEETS = ["SDD LOAD", "AIR LOAD", "NDD LOAD"]

if not AUDIT_SHEET_ID:
    sys.exit("Missing AUDIT_SHEET_ID")


def get_google_client():
    # GitHub Actions authenticates with Workload Identity Federation.
    # google-github-actions/auth exposes those short-lived credentials as
    # Application Default Credentials (ADC), so no service-account JSON secret
    # is required.
    creds, _ = google_auth_default(
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)


def get_sheet():
    return get_google_client().open_by_key(AUDIT_SHEET_ID)


def get_or_create_worksheet(sh, title, rows=1000, cols=30):
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Creating worksheet: {title}")
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def read_all_values(sh, title):
    try:
        return sh.worksheet(title).get_all_values()
    except gspread.exceptions.WorksheetNotFound:
        return []


def load_reference_sheets(sh):
    """Read all source/reference tabs from the SAME workbook.
    FWD_RAW and REV_RAW are therefore not read from local CSV files.
    """
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

    # One Google Sheets values.batchGet call for all source tabs.
    ranges = [f"'{name}'!A:ZZ" for name in tab_names]
    try:
        response = sh.values_batch_get(ranges)
    except Exception as e:
        raise RuntimeError(f"Could not read PENDENCY MASTER source tabs: {e}") from e

    out = {}
    for name, vr in zip(tab_names, response.get("valueRanges", [])):
        out[name] = vr.get("values", [])

    for name in ["FWD_RAW", "REV_RAW", "EXCEPTION", "Layout Name Block Wise", "MAPPING", "EMP_DATA", "Stagging"]:
        if not out.get(name):
            raise RuntimeError(f"Required worksheet '{name}' is empty or missing.")

    return out


def build_lookup_maps(ref):
    # EXCEPTION: A=AWB, B=Category, C=Moved to GA, D=POC,
    # E=Pending With, F=Status, G=Inbound Date, H=Block
    exception_blocks_override = {}
    exception_salvage = {}
    exception_inbound_date = {}
    exception_block_col = {}

    for row in ref["EXCEPTION"][1:]:
        if not row or not row[0]:
            continue
        awb = str(row[0]).strip().upper()
        exception_blocks_override[awb] = row[4] if len(row) > 4 else ""
        exception_salvage[awb] = row[2] if len(row) > 2 else ""
        exception_inbound_date[awb] = row[6] if len(row) > 6 else ""
        exception_block_col[awb] = row[7] if len(row) > 7 else ""

    # Layout Name Block Wise
    lw_rows = ref["Layout Name Block Wise"][1:]

    def col_set(idx):
        return {
            str(r[idx]).strip()
            for r in lw_rows
            if len(r) > idx and str(r[idx]).strip()
        }

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

    # MAPPING
    map_rows = ref["MAPPING"][1:]
    mapping_ab = {
        str(r[0]).strip(): str(r[1]).strip()
        for r in map_rows
        if len(r) > 1 and str(r[0]).strip()
    }
    mapping_d_to_h = {
        str(r[3]).strip(): str(r[7]).strip()
        for r in map_rows
        if len(r) > 7 and str(r[3]).strip()
    }

    # EMP_DATA: A=id, B=name, D=default block
    emp_rows = ref["EMP_DATA"][1:]
    emp_map = {
        str(r[0]).strip(): str(r[1]).strip()
        for r in emp_rows
        if len(r) > 1 and str(r[0]).strip()
    }
    emp_default_block_map = {
        str(r[0]).strip(): str(r[3]).strip()
        for r in emp_rows
        if len(r) > 3 and str(r[0]).strip() and str(r[3]).strip()
    }

    # Stagging: A=Zone, B=Cage, C=Next_Destination, D=Zone+Cage key, E=category
    stag_rows = ref["Stagging"][1:]
    stagging_c_to_d = {
        str(r[2]).strip(): str(r[3]).strip()
        for r in stag_rows
        if len(r) > 3 and str(r[2]).strip()
    }
    stagging_d_to_c = {
        str(r[3]).strip(): str(r[2]).strip()
        for r in stag_rows
        if len(r) > 3 and str(r[3]).strip()
    }
    stagging_d_to_e = {
        str(r[3]).strip(): str(r[4]).strip()
        for r in stag_rows
        if len(r) > 4 and str(r[3]).strip()
    }

    return {
        "exception_blocks_override": exception_blocks_override,
        "exception_salvage": exception_salvage,
        "exception_inbound_date": exception_inbound_date,
        "exception_block_col": exception_block_col,
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
    return fallback or "Unknown"


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
    last_destination = "NBP_DC_FMRTS" if item_destination == "NBP_DC_FMRTS" else "TAURU_DC_FMRTS"
    return "CFGR", secondary, last_destination


def normalize_aging_bucket(val):
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
        raw_ts = row.get("item_last_updated")
        has_timestamp = raw_ts is not None and pd.notna(raw_ts)
        item_last_updated = normalize_timestamp(raw_ts) if has_timestamp else None
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
            "item_last_updated": item_last_updated,
            "pendency_type": pendency_type,
            "shipment_type": row.get("shipment_type") if "shipment_type" in row else None,
            "emp_name": lookups["emp_map"].get(str(action_user).strip(), ""),
            "blocks": resolve_block(layout_name, awb, action_user, lookups),
            "last_destination": last_destination,
            "primary_bin": primary_bin,
            "secondary_bin": secondary_bin,
            "hour": str(raw_ts)[11:13] if has_timestamp else None,
            "salvage_type": lookups["exception_salvage"].get(awb, ""),
            "inbound_date": parse_sheet_date(lookups["exception_inbound_date"].get(awb)),
            "block": lookups["exception_block_col"].get(awb, ""),
            "report_type": report_type,
        })
    return records


def clean_cell(v):
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def records_to_matrix(records):
    if not records:
        return [[]]
    headers = list(records[0].keys())
    matrix = [headers]
    for r in records:
        matrix.append([clean_cell(r.get(h)) for h in headers])
    return matrix


def write_full_table(sh, title, records, min_cols=30):
    """Replace a worksheet's data in one logical operation.
    Data is sent in chunks so a 100k-row audit table stays within request
    payload limits. This is Google Sheets only.
    """
    matrix = records_to_matrix(records)
    rows_needed = max(len(matrix), 100)
    cols_needed = max(len(matrix[0]), min_cols)
    ws = get_or_create_worksheet(sh, title, rows=max(rows_needed, 1000), cols=max(cols_needed, 30))

    if ws.row_count < rows_needed or ws.col_count < cols_needed:
        ws.resize(rows=max(ws.row_count, rows_needed), cols=max(ws.col_count, cols_needed))

    print(f"Clearing {title}...")
    ws.clear()

    # Header + data. Use rectangular ranges for reliable writes.
    total = len(matrix)
    for start in range(0, total, WRITE_CHUNK):
        chunk = matrix[start:start + WRITE_CHUNK]
        end = start + len(chunk)
        cell_range = f"A{start + 1}:{gspread.utils.rowcol_to_a1(end, len(matrix[0])).replace(str(end), '')}{end}"
        # rowcol_to_a1 gives e.g. Z500; build the column letter separately.
        last_col = gspread.utils.rowcol_to_a1(1, len(matrix[0])).rstrip("1")
        cell_range = f"A{start + 1}:{last_col}{end}"
        ws.update(range_name=cell_range, values=chunk, raw=True)
        print(f"  {title}: wrote rows {start + 1}-{end}")

    return ws


def write_matrix(sh, title, matrix, clear_first=False, min_rows=100, min_cols=10):
    if not matrix:
        return
    ws = get_or_create_worksheet(
        sh,
        title,
        rows=max(len(matrix), min_rows),
        cols=max(len(matrix[0]), min_cols),
    )
    if ws.row_count < len(matrix) or ws.col_count < len(matrix[0]):
        ws.resize(rows=max(ws.row_count, len(matrix)), cols=max(ws.col_count, len(matrix[0])))
    if clear_first:
        ws.clear()
    for start in range(0, len(matrix), WRITE_CHUNK):
        chunk = matrix[start:start + WRITE_CHUNK]
        end = start + len(chunk)
        last_col = gspread.utils.rowcol_to_a1(1, len(matrix[0])).rstrip("1")
        ws.update(range_name=f"A{start + 1}:{last_col}{end}", values=chunk, raw=True)
    return ws


def _ageing_days(val):
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
    days = _ageing_days(val)
    return days is not None and days >= 1


def is_alert_eligible(val):
    days = _ageing_days(val)
    return days is not None and days >= 2


def _parse_dt(val):
    if not val:
        return None
    try:
        dt = dateutil_parser.parse(str(val).strip())
        # Normalize timezone-aware values to naive UTC so comparisons never
        # fail because one timestamp has tzinfo and another does not.
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except (ValueError, OverflowError):
        return None


def _dt_key(val):
    dt = _parse_dt(val)
    return dt.timestamp() if dt is not None else float("-inf")


def send_update_alert(r):
    topic = NTFY_TOPIC if r.get("report_type") == "FWD" else NTFY_TOPIC_REV
    if not topic:
        return False

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
        return resp.ok
    except Exception as e:
        print(f"  ntfy failed for {r.get('awb_number')}: {e}")
        return False


def read_key_value_sheet(sh, title, key_col=1, value_col=2):
    rows = read_all_values(sh, title)
    if len(rows) < 2:
        return {}
    out = {}
    for row in rows[1:]:
        if len(row) < max(key_col, value_col):
            continue
        key = str(row[key_col - 1]).strip()
        if key:
            out[key] = row[value_col - 1]
    return out


def write_key_value_sheet(sh, title, mapping):
    rows = [["awb_number", "value"]]
    for k, v in mapping.items():
        rows.append([k, clean_cell(v)])
    write_matrix(sh, title, rows, clear_first=True, min_rows=max(100, len(rows)), min_cols=2)


def read_state_rows(sh, title):
    rows = read_all_values(sh, title)
    if len(rows) < 2:
        return {}
    headers = [str(x).strip() for x in rows[0]]
    if "awb_number" not in headers:
        return {}
    ai = headers.index("awb_number")
    ti = headers.index("item_last_updated") if "item_last_updated" in headers else None
    out = {}
    for row in rows[1:]:
        if len(row) <= ai:
            continue
        awb = str(row[ai]).strip().upper()
        if not awb:
            continue
        out[awb] = row[ti] if ti is not None and len(row) > ti else ""
    return out


def upsert_simple_state(sh, title, rows, headers):
    """Read current state, merge changes, rewrite the small current-state tab.
    These tabs contain one row per AWB, so Google Sheets remains manageable.
    """
    existing_rows = read_all_values(sh, title)
    existing = {}
    if len(existing_rows) >= 2:
        old_headers = existing_rows[0]
        try:
            ai = old_headers.index("awb_number")
        except ValueError:
            ai = 0
        for row in existing_rows[1:]:
            if len(row) <= ai:
                continue
            awb = str(row[ai]).strip().upper()
            if awb:
                existing[awb] = dict(zip(old_headers, row + [""] * (len(old_headers) - len(row))))

    for r in rows:
        awb = str(r.get("awb_number") or "").strip().upper()
        if awb:
            existing[awb] = {h: clean_cell(r.get(h)) for h in headers}

    matrix = [headers]
    for awb, r in existing.items():
        matrix.append([clean_cell(r.get(h)) for h in headers])
    write_matrix(sh, title, matrix, clear_first=True, min_rows=max(100, len(matrix)), min_cols=len(headers))


def append_rows(sh, title, headers, new_rows):
    if not new_rows:
        return
    ws = get_or_create_worksheet(sh, title, rows=1000, cols=max(len(headers), 10))
    existing = ws.get_all_values()
    if not existing:
        ws.update(range_name=f"A1:{gspread.utils.rowcol_to_a1(1, len(headers)).rstrip('1')}1", values=[headers], raw=True)
        existing_count = 1
    else:
        existing_count = len(existing)
        if existing[0] != headers:
            # Do not destroy an existing log with a different header order.
            print(f"WARNING: {title} header differs; appending using existing header order.")
            headers = existing[0]

    rows_out = []
    for r in new_rows:
        rows_out.append([clean_cell(r.get(h)) for h in headers])

    needed = existing_count + len(rows_out)
    if ws.row_count < needed:
        ws.resize(rows=needed, cols=max(ws.col_count, len(headers)))

    for start in range(0, len(rows_out), WRITE_CHUNK):
        chunk = rows_out[start:start + WRITE_CHUNK]
        row_start = existing_count + start + 1
        row_end = row_start + len(chunk) - 1
        last_col = gspread.utils.rowcol_to_a1(1, len(headers)).rstrip("1")
        ws.update(range_name=f"A{row_start}:{last_col}{row_end}", values=chunk, raw=True)


def check_for_updates_and_alert(sh, records):
    if not NTFY_TOPIC and not NTFY_TOPIC_REV:
        print("No NTFY topic configured -- skipping update alerts.")
        return

    tracked = [r for r in records if is_tracked(r.get("aging_bucket")) and r.get("awb_number")]
    if not tracked:
        print("No 1_day+ shipments -- no update checks.")
        return

    unique = {}
    for r in tracked:
        awb = r["awb_number"]
        old = unique.get(awb)
        if old is None or _dt_key(r.get("item_last_updated")) > _dt_key(old.get("item_last_updated")):
            unique[awb] = r
    tracked = list(unique.values())

    previous = read_state_rows(sh, "ALERT_STATE")
    state_updates = []
    alert_rows = []
    alerts_sent = 0

    for r in tracked:
        awb = r["awb_number"]
        current = r.get("item_last_updated") or ""
        prev = previous.get(awb)

        if not prev:
            state_updates.append({"awb_number": awb, "item_last_updated": current})
            continue

        cur_dt = _parse_dt(current)
        prev_dt = _parse_dt(prev)
        changed = cur_dt is not None and prev_dt is not None and cur_dt > prev_dt

        if changed and is_alert_eligible(r.get("aging_bucket")):
            if send_update_alert(r):
                alerts_sent += 1
            alert_rows.append({
                "detected_at": datetime.now(timezone.utc).isoformat(),
                "awb_number": awb,
                "aging_bucket": r.get("aging_bucket"),
                "pendency_type": r.get("pendency_type"),
                "report_type": r.get("report_type"),
                "last_destination": r.get("last_destination"),
                "item_last_updated": current,
            })
            state_updates.append({"awb_number": awb, "item_last_updated": current})
        elif changed:
            state_updates.append({"awb_number": awb, "item_last_updated": current})

    if state_updates:
        # ALERT_STATE is a current-state sheet; rewrite only one compact table.
        current = previous.copy()
        for r in state_updates:
            current[r["awb_number"]] = r["item_last_updated"]
        write_key_value_sheet(sh, "ALERT_STATE", current)

    append_rows(
        sh,
        "AWB_UPDATE_ALERTS",
        ["detected_at", "awb_number", "aging_bucket", "pendency_type", "report_type", "last_destination", "item_last_updated"],
        alert_rows,
    )

    print(f"Update detection: checked {len(tracked)}, sent {alerts_sent} alerts.")


def read_current_stage_state(sh, title, time_field):
    rows = read_all_values(sh, title)
    if len(rows) < 2:
        return {}
    headers = rows[0]
    try:
        ai = headers.index("awb_number")
        ti = headers.index(time_field)
    except ValueError:
        return {}
    out = {}
    for row in rows[1:]:
        if len(row) <= ai:
            continue
        awb = str(row[ai]).strip().upper()
        if awb:
            out[awb] = row[ti] if len(row) > ti else ""
    return out


def _is_new_scan(new_time, prev_time):
    if not prev_time:
        return True
    nt, pt = _parse_dt(new_time), _parse_dt(prev_time)
    return nt is not None and pt is not None and nt > pt


def log_primary_secondary_events(sh, records):
    existing_primary = read_current_stage_state(sh, "RDC_LAST_SEEN", "rdc_time")
    existing_secondary = read_current_stage_state(sh, "AT_DOCK_LAST_SEEN", "at_dock_time")

    current_primary = {}
    current_secondary = {}

    for r in records:
        if r.get("report_type") != "FWD":
            continue
        awb = str(r.get("awb_number") or "").strip().upper()
        if not awb:
            continue
        ts = r.get("item_last_updated")
        level = str(r.get("bin_level") or "").strip()
        if level == "2":
            old = current_secondary.get(awb)
            if old is None or (_parse_dt(ts) or datetime.min) > (_parse_dt(old.get("item_last_updated")) or datetime.min):
                current_secondary[awb] = r
        elif level == "1":
            old = current_primary.get(awb)
            if old is None or (_parse_dt(ts) or datetime.min) > (_parse_dt(old.get("item_last_updated")) or datetime.min):
                current_primary[awb] = r

    primary_events = []
    secondary_events = []
    primary_state = []
    secondary_state = []

    for awb, r in current_primary.items():
        ts = r.get("item_last_updated")
        if _is_new_scan(ts, existing_primary.get(awb)):
            primary_events.append({
                "awb_number": awb,
                "action_user": r.get("action_user"),
                "emp_name": r.get("emp_name"),
                "client_name": r.get("client_name"),
                "layout_name": r.get("layout_name"),
                "blocks": r.get("blocks"),
                "occurred_at": ts,
            })
        primary_state.append({"awb_number": awb, "rdc_time": ts, "bin_level": "1"})

    for awb, r in current_secondary.items():
        ts = r.get("item_last_updated")
        if _is_new_scan(ts, existing_secondary.get(awb)):
            secondary_events.append({
                "awb_number": awb,
                "action_user": r.get("action_user"),
                "emp_name": r.get("emp_name"),
                "client_name": r.get("client_name"),
                "layout_name": r.get("layout_name"),
                "blocks": r.get("blocks"),
                "occurred_at": ts,
            })
        secondary_state.append({"awb_number": awb, "at_dock_time": ts})

    pfc_rows = []
    existing_pfc = read_state_rows(sh, "PFC_FIRST_SEEN")
    for r in records:
        if r.get("report_type") == "FWD" and (r.get("pendency_type") or "").strip() in FWD_PFC_TYPES:
            awb = str(r.get("awb_number") or "").strip().upper()
            if awb and awb not in existing_pfc:
                pfc_rows.append({
                    "awb_number": awb,
                    "first_seen_at": r.get("item_last_updated"),
                    "pendency_type": r.get("pendency_type"),
                })

    append_rows(
        sh,
        "PRIMARY_SCAN_EVENTS",
        ["awb_number", "action_user", "emp_name", "client_name", "layout_name", "blocks", "occurred_at"],
        primary_events,
    )
    append_rows(
        sh,
        "SECONDARY_SCAN_EVENTS",
        ["awb_number", "action_user", "emp_name", "client_name", "layout_name", "blocks", "occurred_at"],
        secondary_events,
    )
    append_rows(
        sh,
        "PFC_FIRST_SEEN",
        ["awb_number", "first_seen_at", "pendency_type"],
        pfc_rows,
    )

    # Current-stage sheets are fully replaced each run, which is correct because
    # they represent CURRENT bin state, not history.
    write_matrix(
        sh,
        "RDC_LAST_SEEN",
        [["awb_number", "rdc_time", "bin_level"]] + [[r["awb_number"], r["rdc_time"], r["bin_level"]] for r in primary_state],
        clear_first=True,
        min_rows=max(100, len(primary_state) + 1),
        min_cols=3,
    )
    write_matrix(
        sh,
        "AT_DOCK_LAST_SEEN",
        [["awb_number", "at_dock_time"]] + [[r["awb_number"], r["at_dock_time"]] for r in secondary_state],
        clear_first=True,
        min_rows=max(100, len(secondary_state) + 1),
        min_cols=2,
    )

    print(f"Scan events: {len(primary_events)} new primary, {len(secondary_events)} new secondary, {len(pfc_rows)} new PFC first-seen.")


def sync_load_pending_summary(sh, ref):
    records = []

    def to_num(v):
        try:
            return int(str(v).replace(",", "").strip())
        except (ValueError, TypeError):
            return None

    for tab_name in LOAD_PENDING_SHEETS:
        rows = ref.get(tab_name, [])
        if len(rows) < 5:
            continue
        shipment_row = rows[1]
        vehicle_row = rows[2]
        header_row = rows[4]
        for col_idx in range(1, len(header_row)):
            label = str(header_row[col_idx]).strip()
            if not label:
                continue
            records.append({
                "sheet_name": tab_name,
                "status_label": label,
                "shipment_count": to_num(shipment_row[col_idx] if col_idx < len(shipment_row) else ""),
                "vehicle_count": to_num(vehicle_row[col_idx] if col_idx < len(vehicle_row) else ""),
            })

    if records:
        write_matrix(
            sh,
            "LOAD_PENDING_SUMMARY",
            records_to_matrix(records),
            clear_first=True,
            min_rows=max(100, len(records) + 1),
            min_cols=4,
        )
        print(f"Load Pending: wrote {len(records)} summary rows to Google Sheets.")


def main():
    started = datetime.now(timezone.utc).isoformat()
    print(f"Run started: {started}")
    print("MODE: GOOGLE SHEETS ONLY — NO CLOUDFLARE / D1")

    sh = get_sheet()

    print("Reading FWD_RAW + REV_RAW + reference tabs from PENDENCY MASTER...")
    ref = load_reference_sheets(sh)
    lookups = build_lookup_maps(ref)

    fwd_rows = ref["FWD_RAW"]
    rev_rows = ref["REV_RAW"]

    if len(fwd_rows) < 2 or len(rev_rows) < 2:
        raise RuntimeError("FWD_RAW or REV_RAW has no data rows; refusing to overwrite AUDIT_MASTER.")

    records = []

    print(f"Reading FWD_RAW: {len(fwd_rows) - 1} rows...")
    fwd_df = pd.DataFrame(fwd_rows[1:], columns=fwd_rows[0], dtype=str)
    print("Enriching FWD rows...")
    records.extend(enrich_dataframe(fwd_df, lookups, "FWD"))

    print(f"Reading REV_RAW: {len(rev_rows) - 1} rows...")
    rev_df = pd.DataFrame(rev_rows[1:], columns=rev_rows[0], dtype=str)
    print("Enriching REV rows...")
    records.extend(enrich_dataframe(rev_df, lookups, "REV"))

    captured_at = datetime.now(timezone.utc).isoformat()
    for r in records:
        r["captured_at"] = captured_at

    print(f"Writing {len(records)} AUDIT_MASTER rows to Google Sheets...")
    write_full_table(sh, "AUDIT_MASTER", records, min_cols=30)

    print("Checking shipment update alerts...")
    check_for_updates_and_alert(sh, records)

    print("Logging Primary/Secondary scan events...")
    log_primary_secondary_events(sh, records)

    print("Syncing Load Pending summary...")
    sync_load_pending_summary(sh, ref)

    print("DONE — no Cloudflare write performed by build_audit_master.py")


if __name__ == "__main__":
    main()
