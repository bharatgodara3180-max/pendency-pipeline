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
"""

import os
import sys

from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY]):
    sys.exit("Missing SUPABASE_URL or SUPABASE_SECRET_KEY")


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


def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    findings = fetch_all(supabase, "active_findings", "awb_number, pendency_type")
    if not findings:
        print("No active findings -- nothing to check.")
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


if __name__ == "__main__":
    main()
