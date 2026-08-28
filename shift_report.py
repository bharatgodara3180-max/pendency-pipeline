"""
Twice-daily shift report: how many "+1" (2_day-6+_day) ageing shipments
were scanned during the shift, and how many have since been resolved
("closed" -- no longer showing that level of ageing in the latest data).
Sent as a push notification via ntfy, on its own topic separate from the
real-time scan alerts.

Runs at 8:05 AM IST (covers the previous 8:00 PM - 8:00 AM night shift)
and 8:05 PM IST (covers the 8:00 AM - 8:00 PM day shift) -- in both
cases, simply "the last 12 hours" relative to when this actually runs.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import requests
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY")
NTFY_SHIFT_TOPIC = os.environ.get("NTFY_SHIFT_TOPIC")

if not all([SUPABASE_URL, SUPABASE_KEY, NTFY_SHIFT_TOPIC]):
    sys.exit("Missing SUPABASE_URL, SUPABASE_SECRET_KEY, or NTFY_SHIFT_TOPIC")

AGING_ORDER = ["2_day", "3_day", "4_day", "5_day", "6_day", "6+_day"]
IST = timezone(timedelta(hours=5, minutes=30))


def is_ageing_positive(val):
    """"+1" here means 2_day and above -- same definition used for the
    update-detection alert, deliberately narrower than the scan app's
    own "Found" check (which flags anything above 0_day)."""
    if not val:
        return False
    s = str(val).strip()
    if s.lower().endswith("_day"):
        s = s[:-4]
    if "+" in s:
        return True
    try:
        return float(s) >= 2
    except ValueError:
        return False


def fetch_all(supabase, table, select, apply_filters=None, chunk=1000):
    """Paginate through a Supabase select -- a single request caps at 1000 rows."""
    rows = []
    start = 0
    while True:
        q = supabase.table(table).select(select)
        if apply_filters:
            q = apply_filters(q)
        q = q.range(start, start + chunk - 1)
        resp = q.execute()
        rows += resp.data
        if len(resp.data) < chunk:
            break
        start += chunk
    return rows


def fmt_ist(dt):
    return dt.astimezone(IST).strftime("%d-%m-%Y %H:%M")


def send_ntfy(topic, shift_label, window_start, window_end, matrix):
    lines = [f"Window: {fmt_ist(window_start)} - {fmt_ist(window_end)} IST", ""]
    total_scanned = 0
    total_closed = 0

    if not matrix:
        lines.append("No +1 ageing (2_day+) shipments scanned this shift.")
    else:
        for rt in sorted(matrix.keys()):
            lines.append(f"=== {rt} ===")
            for cat in sorted(matrix[rt].keys()):
                cat_scanned = sum(v["scanned"] for v in matrix[rt][cat].values())
                cat_closed = sum(v["closed"] for v in matrix[rt][cat].values())
                lines.append(f"{cat}: {cat_scanned} scanned, {cat_closed} closed")
                for aging in AGING_ORDER:
                    if aging in matrix[rt][cat]:
                        v = matrix[rt][cat][aging]
                        lines.append(f"    {aging}: {v['scanned']} scanned, {v['closed']} closed")
                        total_scanned += v["scanned"]
                        total_closed += v["closed"]
            lines.append("")
        lines.append(f"TOTAL: {total_scanned} scanned, {total_closed} closed")

    message = "\n".join(lines)
    try:
        resp = requests.post(
            f"https://ntfy.sh/{topic}",
            data=message.encode("utf-8"),
            headers={
                "Title": f"Shift Report: {shift_label}",
                "Priority": "default",
                "Tags": "bar_chart",
            },
            timeout=15,
        )
        print(f"  ntfy status {resp.status_code}")
    except Exception as e:
        print(f"  failed to send ntfy report: {e}")


def main():
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=12)
    ist_hour = now.astimezone(IST).hour
    shift_label = "Night Shift (8PM-8AM)" if ist_hour < 14 else "Day Shift (8AM-8PM)"

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print(f"Fetching scans between {window_start.isoformat()} and {now.isoformat()}...")
    scans = fetch_all(
        supabase, "audit_scans",
        "awb_number, aging_bucket, pendency, report_type, scanned_at",
        apply_filters=lambda q: q.gte("scanned_at", window_start.isoformat()).lte("scanned_at", now.isoformat()),
    )

    ageing_scans = [s for s in scans if is_ageing_positive(s.get("aging_bucket"))]
    print(f"  {len(scans)} total scans, {len(ageing_scans)} were +1 ageing (2_day+)")

    if not ageing_scans:
        send_ntfy(NTFY_SHIFT_TOPIC, shift_label, window_start, now, {})
        print("No +1 ageing scans this shift -- sent empty report.")
        return

    awbs = list({s["awb_number"] for s in ageing_scans})
    print(f"Checking current status of {len(awbs)} scanned AWBs against latest audit_master...")

    still_ageing = set()
    CHUNK = 200
    for i in range(0, len(awbs), CHUNK):
        chunk = awbs[i:i + CHUNK]
        resp = (
            supabase.table("audit_master")
            .select("awb_number, aging_bucket")
            .in_("awb_number", chunk)
            .execute()
        )
        for row in resp.data:
            if is_ageing_positive(row.get("aging_bucket")):
                still_ageing.add(row["awb_number"])

    # matrix[report_type][category][aging_bucket] = {"scanned": n, "closed": n}
    matrix = {}
    for s in ageing_scans:
        rt = s.get("report_type") or "UNKNOWN"
        cat = s.get("pendency") or "UNKNOWN"
        aging = s.get("aging_bucket")
        matrix.setdefault(rt, {}).setdefault(cat, {}).setdefault(aging, {"scanned": 0, "closed": 0})
        matrix[rt][cat][aging]["scanned"] += 1
        if s["awb_number"] not in still_ageing:
            matrix[rt][cat][aging]["closed"] += 1

    send_ntfy(NTFY_SHIFT_TOPIC, shift_label, window_start, now, matrix)
    print("Report sent.")


if __name__ == "__main__":
    main()
