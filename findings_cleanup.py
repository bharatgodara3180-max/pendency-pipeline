"""
Runs at 9:00 and 21:00 IST (see .github/workflows/findings-cleanup.yml,
triggered externally like the other pipelines). For every open finding in
active_findings, checks its CURRENT stage in audit_master and decides:

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

Closed findings are deleted from active_findings so the next shift only
ever sees what's still open. The Findings Status page in the app computes
the SAME open/closed logic live on every view (so status is visible in
real time, not just at the two cleanup times) -- this script's only job is
the twice-daily prune.

Also purges audit_scans rows older than 3 days -- the Audit Reports
dashboard itself only ever queries "since the last 9 AM" (computed
client-side), so this is just housekeeping to stop the table growing
forever, not what makes the dashboard show only today's data.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

from cf_store import CFStore

CF_API_URL = os.environ.get("CF_API_URL")
CF_API_TOKEN = os.environ.get("CF_API_TOKEN")

if not all([CF_API_URL, CF_API_TOKEN]):
    sys.exit("Missing CF_API_URL or CF_API_TOKEN")


def fetch_all(supabase, table, select, chunk=1000):
    rows = []
    start = 0
    while True:
        resp = supabase.table(table).select(select).range(start, start + chunk - 1).execute()
        rows += resp.data
        if len(resp.data) < chunk:
            break
        start += chunk
    return rows


def purge_old_audit_scans(supabase, days=3):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    try:
        supabase.table("audit_scans").delete().lt("scanned_at", cutoff).execute()
        print(f"Purged audit_scans rows older than {cutoff}.")
    except Exception as e:
        print(f"  failed to purge old audit_scans rows: {e}")



def main():
    supabase = CFStore()

    findings = fetch_all(supabase, "active_findings", "awb_number, pendency_type")
    if not findings:
        print("No active findings -- nothing to check.")
        purge_old_audit_scans(supabase)
        return

    awbs = [f["awb_number"] for f in findings]
    current_stage = {}
    CHUNK = 200
    for i in range(0, len(awbs), CHUNK):
        chunk = awbs[i:i + CHUNK]
        resp = supabase.table("audit_master").select("awb_number, pendency_type").in_("awb_number", chunk).execute()
        for row in resp.data:
            current_stage[row["awb_number"]] = row.get("pendency_type")

    to_close = []
    for f in findings:
        awb = f["awb_number"]
        alerted_stage = f.get("pendency_type")
        now_stage = current_stage.get(awb)  # None if the AWB isn't in audit_master at all right now

        if alerted_stage == "At Dock":
            if now_stage is None:
                to_close.append(awb)  # cleared from pendency after being at dock -- normal completion
            # still shows At Dock -> stays open
        else:
            if now_stage == "At Dock":
                to_close.append(awb)  # progressed to At Dock -- closed
            # still not at dock (whether still present or gone) -> stays open

    if to_close:
        for i in range(0, len(to_close), CHUNK):
            supabase.table("active_findings").delete().in_("awb_number", to_close[i:i + CHUNK]).execute()

    print(f"Checked {len(findings)} findings, closed {len(to_close)}, {len(findings) - len(to_close)} remain open.")

    purge_old_audit_scans(supabase)


if __name__ == "__main__":
    main()
