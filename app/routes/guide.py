"""EPG / XMLTV guide endpoints — /api/guide.*.

Generates electronic program guide data from the program schedule.
Two formats: XMLTV (standard XML for EPG clients) and JSON (for the
web viewer's EPG strip).

Data comes from program.load_program() for today and tomorrow.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone as tz
from xml.etree.ElementTree import Element, SubElement, tostring

from fastapi import APIRouter
from fastapi.responses import Response

import config
import main
import program

logger = logging.getLogger(__name__)

router = APIRouter(tags=["guide"])


def _get_channel_identity() -> tuple[str, str]:
    """Get the channel ID and name for XMLTV output.

    Uses the TV-prefixed federation ID if available (B2),
    falls back to the slug for backwards compatibility.
    """
    for ctx in main.channels.all():
        fed_id = ctx.channel_id or ctx.id
        return fed_id, ctx.display_name
    return config.DEFAULT_CHANNEL_ID, "TLTV Channel One"


def _block_title(block: dict) -> str:
    """Return the block's title for EPG display."""
    return block.get("title") or block.get("type", "").title() or "Unknown"


def _block_category(block: dict) -> str:
    """Map block type to an EPG category string."""
    return block.get("type", "unknown")


def _get_tz_offset() -> str:
    """Get the XMLTV timezone offset string from the default channel.

    Reads the channel's IANA timezone (e.g. "America/New_York") and
    returns the current UTC offset formatted for XMLTV (e.g. "-0500").
    Falls back to "+0000" if timezone is not set or not parseable.
    """
    try:
        from zoneinfo import ZoneInfo

        for ctx in main.channels.all():
            if ctx.timezone:
                now = datetime.now(ZoneInfo(ctx.timezone))
                offset = now.utcoffset()
                if offset is not None:
                    total_seconds = int(offset.total_seconds())
                    sign = "+" if total_seconds >= 0 else "-"
                    total_seconds = abs(total_seconds)
                    hours = total_seconds // 3600
                    minutes = (total_seconds % 3600) // 60
                    return f"{sign}{hours:02d}{minutes:02d}"
    except Exception:
        pass
    return "+0000"


def _format_xmltv_time(d: date, time_str: str) -> str:
    """Format a date + HH:MM:SS time string into XMLTV timestamp.

    Returns YYYYMMDDHHmmss with timezone offset derived from the
    channel's configured timezone.
    """
    parts = time_str.split(":")
    h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
    dt = datetime(d.year, d.month, d.day, h, m, s)
    return dt.strftime("%Y%m%d%H%M%S") + f" {_get_tz_offset()}"


def _collect_guide_entries(today: date, channel_id: str | None = None) -> list[dict]:
    """Collect guide entries from today and tomorrow's programs."""
    cid = channel_id or config.DEFAULT_CHANNEL_ID
    entries = []
    for offset in (0, 1):
        d = today + timedelta(days=offset)
        prog = program.load_program(d, channel_id=cid)
        if not prog:
            continue
        for block in prog.get("blocks", []):
            start_str = block.get("start", "")
            end_str = block.get("end", "")
            if not start_str or not end_str:
                continue
            entries.append(
                {
                    "date": d,
                    "start": start_str,
                    "stop": end_str,
                    "title": _block_title(block),
                    "category": _block_category(block),
                    "type": block.get("type", "unknown"),
                }
            )
    return entries


def build_xmltv(entries: list[dict], channel_id: str, channel_name: str) -> bytes:
    """Build XMLTV XML bytes from guide entries.

    Reusable by both the management API (/api/guide.xml) and the
    protocol endpoint (/tltv/v1/channels/{id}/guide.xml).

    Args:
        entries: List of guide entry dicts with date, start, stop, title, category.
        channel_id: Channel identifier for <channel id="..."> (TV-prefixed federation ID).
        channel_name: Display name for <display-name>.

    Returns:
        Complete XMLTV document as UTF-8 bytes.
    """
    tv = Element(
        "tv",
        attrib={
            "generator-info-name": "Cathode",
            "generator-info-url": "",
        },
    )

    # Channel element — uses TV-prefixed federation ID (B2)
    ch = SubElement(tv, "channel", id=channel_id)
    dn = SubElement(ch, "display-name")
    dn.text = channel_name

    # Programme elements
    for entry in entries:
        prog_el = SubElement(
            tv,
            "programme",
            attrib={
                "start": _format_xmltv_time(entry["date"], entry["start"]),
                "stop": _format_xmltv_time(entry["date"], entry["stop"]),
                "channel": channel_id,
            },
        )
        title_el = SubElement(prog_el, "title", lang="en")
        title_el.text = entry["title"]
        cat_el = SubElement(prog_el, "category", lang="en")
        cat_el.text = entry["category"]

    return b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(
        tv, encoding="unicode"
    ).encode("utf-8")


@router.get("/api/guide.xml")
async def get_guide_xml() -> Response:
    """Return XMLTV-format EPG for today and tomorrow.

    Standard XMLTV format with <tv> root, <channel>, and <programme>
    elements. Times include Eastern timezone offset.
    """
    today = date.today()
    entries = _collect_guide_entries(today)
    channel_id, channel_name = _get_channel_identity()
    xml_bytes = build_xmltv(entries, channel_id, channel_name)
    return Response(content=xml_bytes, media_type="application/xml")


@router.get("/api/guide.json")
async def get_guide_json() -> list[dict]:
    """Return EPG data as JSON for the web viewer.

    Array of objects with start, stop, title, category, type fields.
    Times are HH:MM:SS strings; date is ISO format.
    """
    today = date.today()
    entries = _collect_guide_entries(today)

    return [
        {
            "date": entry["date"].isoformat(),
            "start": entry["start"],
            "stop": entry["stop"],
            "title": entry["title"],
            "category": entry["category"],
            "type": entry["type"],
        }
        for entry in entries
    ]
