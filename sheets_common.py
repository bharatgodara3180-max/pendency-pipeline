"""
Shared Google Sheets helpers for the pendency pipeline.

Every script in this repo talks to the SAME "PENDENCY MASTER" Google Sheet
(AUDIT_SHEET_ID) via this module -- there is no Cloudflare / D1 / Supabase
anywhere in the pipeline anymore. Auth is Application Default Credentials
(in GitHub Actions this comes from Workload Identity Federation via
google-github-actions/auth@v2), so no service-account JSON key is needed.
"""

import json
import os
import sys

import gspread
from google.auth import default as google_auth_default

AUDIT_SHEET_ID = os.environ.get("AUDIT_SHEET_ID")
WRITE_CHUNK = 5000


def require_sheet_id():
    if not AUDIT_SHEET_ID:
        sys.exit("Missing AUDIT_SHEET_ID.")
    return AUDIT_SHEET_ID


def get_google_client():
    creds, _ = google_auth_default(
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)


def get_sheet():
    require_sheet_id()
    return get_google_client().open_by_key(AUDIT_SHEET_ID)


def get_or_create_worksheet(sh, title, rows=1000, cols=30):
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Creating worksheet: {title}")
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def read_all_values(sh, title):
    """Raw grid for a tab, or [] if the tab doesn't exist yet."""
    try:
        return sh.worksheet(title).get_all_values()
    except gspread.exceptions.WorksheetNotFound:
        return []


def read_records(sh, title):
    """A tab's data rows as a list of dicts keyed by its header row.
    Missing tab, empty tab, or header-only tab -> []. Fully blank rows
    (e.g. left over from a resize) are skipped."""
    rows = read_all_values(sh, title)
    if len(rows) < 2:
        return []
    headers = rows[0]
    out = []
    for row in rows[1:]:
        if not any(str(c).strip() for c in row):
            continue
        padded = row + [""] * (len(headers) - len(row))
        out.append(dict(zip(headers, padded)))
    return out


def clean_cell(v):
    if v is None or v == "":
        return ""
    if isinstance(v, float) and v != v:  # NaN, without requiring pandas here
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def records_to_matrix(records, headers=None):
    if not records:
        return [headers] if headers else [[]]
    headers = headers or list(records[0].keys())
    matrix = [headers]
    for r in records:
        matrix.append([clean_cell(r.get(h)) for h in headers])
    return matrix


def _last_col_letter(n_cols):
    # gspread.utils.rowcol_to_a1 requires col >= 1 -- guard so a header-less
    # / empty-column situation can never crash a write (this is exactly what
    # caused the IncorrectCellLabel: (1, 0) crash before this fix).
    n_cols = max(1, n_cols)
    return gspread.utils.rowcol_to_a1(1, n_cols).rstrip("1")


def write_matrix(sh, title, matrix, clear_first=False, min_rows=100, min_cols=10):
    if not matrix or not matrix[0]:
        return None
    ws = get_or_create_worksheet(
        sh, title,
        rows=max(len(matrix), min_rows),
        cols=max(len(matrix[0]), min_cols),
    )
    if ws.row_count < len(matrix) or ws.col_count < len(matrix[0]):
        ws.resize(rows=max(ws.row_count, len(matrix)), cols=max(ws.col_count, len(matrix[0])))
    if clear_first:
        ws.clear()
    last_col = _last_col_letter(len(matrix[0]))
    for start in range(0, len(matrix), WRITE_CHUNK):
        chunk = matrix[start:start + WRITE_CHUNK]
        end = start + len(chunk)
        ws.update(range_name=f"A{start + 1}:{last_col}{end}", values=chunk, raw=True)
    return ws


def write_full_table(sh, title, records, headers=None, min_cols=30):
    """Replace a worksheet's data in one logical operation, chunked so a
    100k-row table stays within request payload limits."""
    matrix = records_to_matrix(records, headers=headers)
    rows_needed = max(len(matrix), 100)
    cols_needed = max(len(matrix[0]) if matrix[0] else 1, min_cols)
    ws = get_or_create_worksheet(sh, title, rows=max(rows_needed, 1000), cols=max(cols_needed, 30))
    if ws.row_count < rows_needed or ws.col_count < cols_needed:
        ws.resize(rows=max(ws.row_count, rows_needed), cols=max(ws.col_count, cols_needed))
    print(f"Clearing {title}...")
    ws.clear()
    last_col = _last_col_letter(len(matrix[0]) if matrix[0] else 1)
    total = len(matrix)
    for start in range(0, total, WRITE_CHUNK):
        chunk = matrix[start:start + WRITE_CHUNK]
        end = start + len(chunk)
        ws.update(range_name=f"A{start + 1}:{last_col}{end}", values=chunk, raw=True)
        print(f"  {title}: wrote rows {start + 1}-{end}")
    return ws


def append_rows(sh, title, headers, new_rows):
    """Append rows to a log-style tab, creating it (with these headers) if
    it doesn't exist yet.

    If the tab already has a *non-empty* header row that differs from
    `headers`, that existing order wins so an established log is never
    reshuffled. A previously-corrupted/blank header row (all cells empty)
    is now treated as "no header yet" and rewritten, instead of being
    carried forward as an empty list -- THIS is the fix for the
    `gspread.exceptions.IncorrectCellLabel: (1, 0)` crash, which happened
    because `len(headers)` silently became 0 and
    `rowcol_to_a1(1, 0)` is not a valid cell reference.
    """
    if not new_rows:
        return
    ws = get_or_create_worksheet(sh, title, rows=1000, cols=max(len(headers), 10))
    existing = ws.get_all_values()

    if not existing or not any(str(c).strip() for c in existing[0]):
        ws.update(range_name=f"A1:{_last_col_letter(len(headers))}1", values=[headers], raw=True)
        existing_count = 1
    else:
        existing_count = len(existing)
        if existing[0] != headers:
            print(f"WARNING: {title} header differs; appending using existing header order.")
            headers = existing[0]

    rows_out = [[clean_cell(r.get(h)) for h in headers] for r in new_rows]

    needed = existing_count + len(rows_out)
    if ws.row_count < needed:
        ws.resize(rows=needed, cols=max(ws.col_count, len(headers)))

    last_col = _last_col_letter(len(headers))
    for start in range(0, len(rows_out), WRITE_CHUNK):
        chunk = rows_out[start:start + WRITE_CHUNK]
        row_start = existing_count + start + 1
        row_end = row_start + len(chunk) - 1
        ws.update(range_name=f"A{row_start}:{last_col}{row_end}", values=chunk, raw=True)
