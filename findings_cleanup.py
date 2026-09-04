"""
Runs at 9:00 and 21:00 IST (see .github/workflows/findings-cleanup.yml,
triggered externally like the other pipelines). For every open finding in
ACTIVE_FINDINGS, checks its CURRENT stage in AUDIT_MASTER and decides:

  - alerted at PFC/RDC/BRSNR, now At Dock              -> CLOSED
  - alerted at PFC/RDC/BRSNR, still not At Dock          -> OPEN (unchanged)
  - alerted at PFC/RDC/BRSNR, gone from data entirely    -> OPEN (no explicit
                                                             close rule covers
                                                             this -- stays
                                                             flagged rather
                                                             than silently
                                                             dropped)
  - alerted At Dock, gone from data entirely (cleared)   -> CLOSED
  - alerted At Dock, still shows At Dock                 -> OPEN

Closed findings are removed from ACTIVE_FINDINGS so the next shift only
ever sees what's still open. The Findings Status page in the app computes
the SAME open/closed logic live on every view (so status is visible in
real time, not just at the two cleanup times) -- this script's only job is
the twice-daily prune.

Also purges AUDIT_SCANS rows older than 3 days -- the Audit Reports
dashboard itself only ever queries "since the last shift-day start"
(computed client-side), so this is just housekeeping to stop the tab
growing forever, not what makes the dashboard show only today's data.

Everything below lives in the same PENDENCY MASTER Google Sheet as the
rest of the pipeline -- no Cloudflare, no external database.

NOTE: this script assumes an ACTIVE_FINDINGS tab (awb_number, report_type,
pendency_type, blocks, seal_number) and an AUDIT_SCANS tab (matching what
the scanning site logs). Neither tab existed anywhere in this repo before
this rewrite -- the old Cloudflare version's `active_findings` table was
never populated by any script here either, so whatever decides "this is a
finding" needs to be built as part of the scanning-site rebuild (Phase 2)
and told to write into these same tab names/columns.
"""

from datetime import datetime, timedelta, timezone

from sheets_common import get_sheet, read_records, read_all_values, write_matrix


def parse_ts(val):
    if not val:
        return None
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except ValueError:
        pass
    try:
        return datetime.strptime(str(val).strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def purge_old_audit_scans(sh, days=3):
    rows = read_all_values(sh, "AUDIT_SCANS")
    if len(rows) < 2:
        print("AUDIT_SCANS is empty -- nothing to purge.")
        return
    headers = rows[0]
    if "scanned_at" not in headers:
        print("AUDIT_SCANS has no scanned_at column -- skipping purge.")
        return
    ti = headers.index("scanned_at")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    kept = [headers]
    dropped = 0
    for row in rows[1:]:
        ts = parse_ts(row[ti] if len(row) > ti else "")
        if ts is not None and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts is not None and ts < cutoff:
            dropped += 1
            continue
        kept.append(row)

    if dropped:
        write_matrix(sh, "AUDIT_SCANS", kept, clear_first=True, min_rows=max(100, len(kept)), min_cols=len(headers))
    print(f"Purged {dropped} AUDIT_SCANS rows older than {cutoff.isoformat()}.")


def main():
    sh = get_sheet()

    findings = read_records(sh, "ACTIVE_FINDINGS")
    if not findings:
        print("No active findings -- nothing to check.")
        purge_old_audit_scans(sh)
        return

    audit_master = read_records(sh, "AUDIT_MASTER")
    current_stage = {}
    for row in audit_master:
        awb = str(row.get("awb_number") or "").strip().upper()
        if awb:
            current_stage[awb] = row.get("pendency_type")

    to_close = set()
    for f in findings:
        awb = str(f.get("awb_number") or "").strip().upper()
        alerted_stage = f.get("pendency_type")
        now_stage = current_stage.get(awb)  # None if the AWB isn't in AUDIT_MASTER at all right now

        if alerted_stage == "At Dock":
            if now_stage is None:
                to_close.add(awb)  # cleared from pendency after being at dock -- normal completion
            # still shows At Dock -> stays open
        else:
            if now_stage == "At Dock":
                to_close.add(awb)  # progressed to At Dock -- closed
            # still not at dock (whether still present or gone) -> stays open

    if to_close:
        headers = list(findings[0].keys())
        remaining = [f for f in findings if str(f.get("awb_number") or "").strip().upper() not in to_close]
        matrix = [headers] + [[f.get(h, "") for h in headers] for f in remaining]
        write_matrix(sh, "ACTIVE_FINDINGS", matrix, clear_first=True, min_rows=max(100, len(matrix)), min_cols=len(headers))

    print(f"Checked {len(findings)} findings, closed {len(to_close)}, {len(findings) - len(to_close)} remain open.")

    purge_old_audit_scans(sh)


if __name__ == "__main__":
    main()
