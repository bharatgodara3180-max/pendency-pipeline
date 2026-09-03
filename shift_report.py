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
from cf_store import CFStore

CF_API_URL = os.environ.get("CF_API_URL")
CF_API_TOKEN = os.environ.get("CF_API_TOKEN")
NTFY_SHIFT_TOPIC = os.environ.get("NTFY_SHIFT_TOPIC")

if not all([CF_API_URL, CF_API_TOKEN, NTFY_SHIFT_TOPIC]):
    sys.exit("Missing CF_API_URL, CF_API_TOKEN, or NTFY_SHIFT_TOPIC")

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
    # ntfy doesn't render Markdown tables reliably (confirmed -- even the
    # Android app doesn't support them), so this uses inline "|" separated
    # counts per line instead of columns that need to actually line up.
    lines = [f"Window: {fmt_ist(window_start)} - {fmt_ist(window_end)} IST", "(each number below is scanned+auto-detected/closed)", ""]
    total_scanned = 0
    total_closed = 0

    if not matrix:
        lines.append("Nothing scanned or auto-detected this shift.")
    else:
        for rt in sorted(matrix.keys()):
            rt_scanned = sum(v["scanned"] for cat in matrix[rt].values() for v in cat.values())
            rt_closed = sum(v["closed"] for cat in matrix[rt].values() for v in cat.values())
            lines.append(f"{rt} -- {rt_scanned} total, {rt_closed} closed")
            for cat in sorted(matrix[rt].keys()):
                cat_scanned = sum(v["scanned"] for v in matrix[rt][cat].values())
                cat_closed = sum(v["closed"] for v in matrix[rt][cat].values())
                lines.append(f"• {cat} ({cat_scanned} total, {cat_closed} closed)")
                parts = []
                for aging in AGING_ORDER:
                    if aging in matrix[rt][cat]:
                        v = matrix[rt][cat][aging]
                        parts.append(f"{aging}: {v['scanned']}/{v['closed']}")
                        total_scanned += v["scanned"]
                        total_closed += v["closed"]
                if parts:
                    lines.append("   " + "  |  ".join(parts))
            lines.append("")
        lines.append(f"TOTAL: {total_scanned} shipments, {total_closed} closed")

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

    supabase = CFStore()

    print(f"Fetching scans between {window_start.isoformat()} and {now.isoformat()}...")
    scans = fetch_all(
        supabase, "audit_scans",
        "awb_number, ageing, pendency, report_type, scanned_at",
        apply_filters=lambda q: q.gte("scanned_at", window_start.isoformat()).lte("scanned_at", now.isoformat()),
    )
    ageing_scans = [
        s for s in scans
        if is_ageing_positive(s.get("ageing")) and s.get("report_type")
    ]
    print(f"  {len(scans)} total scans, {len(ageing_scans)} were +1 ageing (2_day+) with a known FWD/REV type")

    print("Fetching automated update-detection alerts for this window...")
    auto_alerts = fetch_all(
        supabase, "awb_update_alerts",
        "awb_number, aging_bucket, pendency_type, report_type, detected_at",
        apply_filters=lambda q: q.gte("detected_at", window_start.isoformat()).lte("detected_at", now.isoformat()),
    )
    print(f"  {len(auto_alerts)} automated update alerts this window")

    # Merge both sources into ONE combined set, keyed by AWB, so a shipment
    # that was both scanned AND auto-detected in the same window is counted
    # once, not twice. Scan data wins if both exist for the same AWB.
    combined = {}
    for a in auto_alerts:
        combined[a["awb_number"]] = {
            "report_type": a.get("report_type") or "UNKNOWN",
            "category": a.get("pendency_type") or "UNKNOWN",
            "aging": a.get("aging_bucket"),
        }
    for s in ageing_scans:
        combined[s["awb_number"]] = {
            "report_type": s.get("report_type") or "UNKNOWN",
            "category": s.get("pendency") or "UNKNOWN",
            "aging": s.get("ageing"),
        }

    if not combined:
        send_ntfy(NTFY_SHIFT_TOPIC, shift_label, window_start, now, {})
        print("Nothing scanned or auto-detected this shift -- sent empty report.")
        return

    print(f"Checking current status of {len(combined)} shipments against latest audit_master...")
    still_ageing = set()
    awbs = list(combined.keys())
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
    # "scanned" here means "known" -- via a human scan OR auto-detection.
    matrix = {}
    for awb, info in combined.items():
        rt, cat, aging = info["report_type"], info["category"], info["aging"]
        matrix.setdefault(rt, {}).setdefault(cat, {}).setdefault(aging, {"scanned": 0, "closed": 0})
        matrix[rt][cat][aging]["scanned"] += 1
        if awb not in still_ageing:
            matrix[rt][cat][aging]["closed"] += 1

    send_ntfy(NTFY_SHIFT_TOPIC, shift_label, window_start, now, matrix)
    print("Report sent.")


if __name__ == "__main__":
    main()
