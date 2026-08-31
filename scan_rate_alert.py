"""
Half-hourly / hourly Primary & Secondary scan-rate image alerts, for
Block A and Block D, styled like the "Block A - Primary Scan Rate" pivot
tables from the RDC audit workbook (layout_name -> action_user -> count
of awb_number).

Runs on its own schedule (see .github/workflows/scan-rate-alert.yml),
offset 15 minutes past each natural slot boundary so the underlying
awb_scan_events data has time to fully settle before being read:

  :45 past the hour  -> HALF-HOUR slot, this hour's first half
                        (e.g. the 10:45 run pushes the 10:00-10:30 slot)
  :15 past the hour  -> FULL-HOUR slot, the previous complete hour
                        (e.g. the 11:15 run pushes the 10:00-11:00 slot)

A Block/scan-type combination with zero events in the window sends no
image at all -- four empty images every 30 minutes would just be noise.
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from io import BytesIO

import requests
from PIL import Image, ImageDraw, ImageFont
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC")  # same channel as "Shipment updated" alerts

if not all([SUPABASE_URL, SUPABASE_KEY, NTFY_TOPIC]):
    sys.exit("Missing SUPABASE_URL, SUPABASE_SECRET_KEY, or NTFY_TOPIC")

IST = timezone(timedelta(hours=5, minutes=30))
BLOCKS = ["Block A", "Block D"]
SCAN_TYPES = [("primary", "PRIMARY"), ("secondary", "SECONDARY")]

FONT_REGULAR = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
FONT_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"


def load_font(bold=False, size=15):
    path = FONT_BOLD if bold else FONT_REGULAR
    try:
        return ImageFont.truetype(path, size)
    except Exception:
        return ImageFont.load_default()


def determine_window():
    """Returns (window_start, window_end, slot_label, is_full_hour) as
    naive IST wall-clock datetimes -- occurred_at is stored as plain IST
    (no timezone), so the query bounds must match that exactly, with no
    UTC conversion. Robust to the workflow firing a couple of minutes
    late: anything in the first half of the hour (0-29 min) is treated as
    the ":15" full-hour trigger, anything in the second half (30-59 min)
    as the ":45" half-hour trigger."""
    now_ist = datetime.now(IST).replace(tzinfo=None)
    minute = now_ist.minute

    if minute < 30:
        hour_start = now_ist.replace(minute=0, second=0, microsecond=0)
        window_start = hour_start - timedelta(hours=1)
        window_end = hour_start
        is_full_hour = True
    else:
        window_start = now_ist.replace(minute=0, second=0, microsecond=0)
        window_end = window_start + timedelta(minutes=30)
        is_full_hour = False

    slot_label = f"{window_start.strftime('%d-%m-%Y')} {window_start.strftime('%H:%M')}-{window_end.strftime('%H:%M')}"
    return window_start, window_end, slot_label, is_full_hour


def fetch_events(supabase, scan_type, block, window_start, window_end):
    rows = []
    start = 0
    PAGE = 1000
    while True:
        resp = (
            supabase.table("awb_scan_events")
            .select("awb_number, layout_name, action_user")
            .eq("scan_type", scan_type)
            .eq("blocks", block)
            .gte("occurred_at", window_start.strftime("%Y-%m-%d %H:%M:%S"))
            .lt("occurred_at", window_end.strftime("%Y-%m-%d %H:%M:%S"))
            .range(start, start + PAGE - 1)
            .execute()
        )
        rows += resp.data
        if len(resp.data) < PAGE:
            break
        start += PAGE
    return rows


def group_rows(rows):
    """layout_name -> action_user -> count, in first-seen order (matches
    how a pivot table naturally lists groups as they're first encountered)."""
    grouped = {}
    order = []
    for r in rows:
        layout = r.get("layout_name") or "(blank)"
        user = r.get("action_user") or "(blank)"
        if layout not in grouped:
            grouped[layout] = {}
            order.append(layout)
        grouped[layout][user] = grouped[layout].get(user, 0) + 1
    return order, grouped


def render_image(block, scan_type_label, slot_label, layout_order, grouped, grand_total):
    pad = 14
    row_h = 28
    col1_w = 260
    col2_w = 220
    col3_w = 170
    width = pad * 2 + col1_w + col2_w + col3_w

    body_rows = sum(1 + len(grouped[layout]) for layout in layout_order)
    height = pad * 2 + row_h * (3 + body_rows + 1)  # slot bar + heading + col header + body + grand total

    img = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(img)
    font_bold = load_font(bold=True, size=15)
    font = load_font(bold=False, size=14)

    y = pad

    draw.rectangle([pad, y, width - pad, y + row_h], fill="#dbe5f1", outline="#999999")
    draw.text((pad + 8, y + 6), f"SLOT   {slot_label}", font=font_bold, fill="#1f3864")
    y += row_h

    draw.rectangle([pad, y, width - pad, y + row_h], fill="#ffff00", outline="#999999")
    heading = f"BLOCK {block.replace('Block ', '')} - {scan_type_label} SCAN RATE"
    tw = draw.textlength(heading, font=font_bold)
    draw.text(((width - tw) / 2, y + 6), heading, font=font_bold, fill="#000000")
    y += row_h

    draw.rectangle([pad, y, width - pad, y + row_h], fill="#dbe5f1", outline="#999999")
    draw.text((pad + 8, y + 6), "layout_name", font=font_bold, fill="#1f3864")
    draw.text((pad + col1_w + 8, y + 6), "action_user", font=font_bold, fill="#1f3864")
    count_header = "Count of awb_number"
    ctw = draw.textlength(count_header, font=font_bold)
    draw.text((pad + col1_w + col2_w + (col3_w - ctw) / 2, y + 6), count_header, font=font_bold, fill="#1f3864")
    y += row_h

    for layout in layout_order:
        draw.rectangle([pad, y, width - pad, y + row_h], outline="#cccccc")
        draw.text((pad + 8, y + 6), layout, font=font_bold, fill="#000000")
        y += row_h
        for user, count in grouped[layout].items():
            draw.rectangle([pad, y, width - pad, y + row_h], outline="#eeeeee")
            draw.text((pad + col1_w + 8, y + 6), str(user), font=font, fill="#000000")
            draw.rectangle([pad + col1_w + col2_w, y, width - pad, y + row_h], fill="#f8cbcb")
            ctext = str(count)
            ctw = draw.textlength(ctext, font=font)
            draw.text((pad + col1_w + col2_w + (col3_w - ctw) / 2, y + 6), ctext, font=font, fill="#c00000")
            y += row_h

    draw.rectangle([pad, y, width - pad, y + row_h], fill="#dbe5f1", outline="#999999")
    draw.text((pad + 8, y + 6), "Grand Total", font=font_bold, fill="#000000")
    gtext = str(grand_total)
    gtw = draw.textlength(gtext, font=font_bold)
    draw.text((pad + col1_w + col2_w + (col3_w - gtw) / 2, y + 6), gtext, font=font_bold, fill="#000000")

    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def send_image(topic, title, filename, png_bytes):
    try:
        resp = requests.put(
            f"https://ntfy.sh/{topic}",
            data=png_bytes,
            headers={
                "Title": title,
                "Filename": filename,
                "Content-Type": "image/png",
            },
            timeout=30,
        )
        print(f"  ntfy status {resp.status_code} for {filename}")
    except Exception as e:
        print(f"  failed to send image {filename}: {e}")


def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    window_start, window_end, slot_label, is_full_hour = determine_window()
    kind = "full-hour" if is_full_hour else "half-hour"
    print(f"Slot: {slot_label} ({kind}), querying {window_start.isoformat()} to {window_end.isoformat()}")

    for scan_type, scan_type_label in SCAN_TYPES:
        for block in BLOCKS:
            rows = fetch_events(supabase, scan_type, block, window_start, window_end)
            if not rows:
                print(f"  {block} / {scan_type}: no events -- skipping image.")
                continue

            layout_order, grouped = group_rows(rows)
            grand_total = len(rows)
            png_bytes = render_image(block, scan_type_label, slot_label, layout_order, grouped, grand_total)

            title = f"{block} - {scan_type_label} SCAN RATE ({slot_label})"
            filename = f"{block.replace(' ', '_')}_{scan_type}_{window_start.strftime('%Y%m%d_%H%M')}.png"
            send_image(NTFY_TOPIC, title, filename, png_bytes)
            print(f"  {block} / {scan_type}: {grand_total} events, image sent.")

    print("Done.")


if __name__ == "__main__":
    main()
