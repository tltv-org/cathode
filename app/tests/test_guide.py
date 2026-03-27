"""Tests for the EPG / XMLTV guide endpoints.

Tests both /api/guide.xml and /api/guide.json with mocked program data.
"""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path
from xml.etree.ElementTree import fromstring

import pytest

APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


# ── Helper: write a program file to the active PROGRAM_DIR ──


def _write_program(d: date, blocks: list[dict]) -> None:
    """Write a program JSON file to the active PROGRAM_DIR.

    Uses program.PROGRAM_DIR directly (patched by conftest) so we write
    to the same temp directory the app reads from.
    """
    import program as prog_mod

    prog_dir = (
        Path(prog_mod.PROGRAM_DIR) / "channel-one" / str(d.year) / f"{d.month:02d}"
    )
    prog_dir.mkdir(parents=True, exist_ok=True)
    prog = {"date": d.isoformat(), "blocks": blocks, "created": "test"}
    (prog_dir / f"{d.isoformat()}.json").write_text(json.dumps(prog))


# ── JSON endpoint tests ──


@pytest.mark.asyncio
async def test_guide_json_empty(client):
    """No program scheduled returns empty array."""
    resp = await client.get("/api/guide.json")
    assert resp.status_code == 200
    data = resp.json()
    assert data == []


@pytest.mark.asyncio
async def test_guide_json_single_block(client, app_with_mocks):
    """Single canvas block appears in JSON output with its title."""
    today = date.today()
    _write_program(
        today,
        [
            {
                "start": "19:00:00",
                "end": "19:30:00",
                "type": "canvas",
                "title": "Channel One Intro",
                "preset": "channel-one-intro",
            }
        ],
    )

    resp = await client.get("/api/guide.json")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    entry = data[0]
    assert entry["date"] == today.isoformat()
    assert entry["start"] == "19:00:00"
    assert entry["stop"] == "19:30:00"
    assert entry["title"] == "Channel One Intro"
    assert entry["category"] == "canvas"
    assert entry["type"] == "canvas"


@pytest.mark.asyncio
async def test_guide_json_multiple_blocks(client, app_with_mocks):
    """Multiple blocks of different types all appear with their titles."""
    today = date.today()
    blocks = [
        {
            "start": "18:00:00",
            "end": "18:30:00",
            "type": "canvas",
            "title": "Mandelbrot Zoom",
            "html": "<div>hi</div>",
        },
        {
            "start": "18:30:00",
            "end": "19:00:00",
            "type": "playlist",
            "title": "Evening Clips",
            "files": ["clip_01.mp4", "clip_02.mp4"],
        },
        {
            "start": "19:00:00",
            "end": "19:30:00",
            "type": "generator",
            "title": "The Seance",
            "name": "seance",
        },
        {
            "start": "19:30:00",
            "end": "20:00:00",
            "type": "playlist",
            "title": "Late Night Rotation",
        },
    ]
    _write_program(today, blocks)

    resp = await client.get("/api/guide.json")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 4

    assert data[0]["title"] == "Mandelbrot Zoom"
    assert data[0]["type"] == "canvas"

    assert data[1]["title"] == "Evening Clips"
    assert data[1]["type"] == "playlist"

    assert data[2]["title"] == "The Seance"
    assert data[2]["type"] == "generator"

    assert data[3]["title"] == "Late Night Rotation"


@pytest.mark.asyncio
async def test_guide_json_includes_tomorrow(client, app_with_mocks):
    """Guide includes both today and tomorrow's blocks."""
    today = date.today()
    tomorrow = today + timedelta(days=1)

    _write_program(
        today,
        [
            {
                "start": "20:00:00",
                "end": "21:00:00",
                "type": "playlist",
                "title": "Today Block",
            },
        ],
    )
    _write_program(
        tomorrow,
        [
            {
                "start": "10:00:00",
                "end": "11:00:00",
                "type": "canvas",
                "title": "Tomorrow Block",
                "preset": "test-preset",
            },
        ],
    )

    resp = await client.get("/api/guide.json")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["date"] == today.isoformat()
    assert data[0]["title"] == "Today Block"
    assert data[1]["date"] == tomorrow.isoformat()
    assert data[1]["title"] == "Tomorrow Block"


# ── XML endpoint tests ──


@pytest.mark.asyncio
async def test_guide_xml_empty(client):
    """Empty program returns valid XMLTV with channel but no programmes."""
    resp = await client.get("/api/guide.xml")
    assert resp.status_code == 200
    assert "application/xml" in resp.headers["content-type"]

    root = fromstring(resp.content)
    assert root.tag == "tv"

    channels = root.findall("channel")
    assert len(channels) == 1
    # B2: uses TV-prefixed federation ID, not slug
    assert channels[0].get("id").startswith("TV")

    display = channels[0].find("display-name")
    assert display is not None
    assert display.text == "TLTV Channel One"

    programmes = root.findall("programme")
    assert len(programmes) == 0


@pytest.mark.asyncio
async def test_guide_xml_with_blocks(client, app_with_mocks):
    """Blocks appear as <programme> elements with correct attributes."""
    today = date.today()
    _write_program(
        today,
        [
            {
                "start": "20:00:00",
                "end": "20:30:00",
                "type": "canvas",
                "title": "Mandelbrot Zoom",
                "preset": "mandelbrot-zoom",
            },
            {
                "start": "20:30:00",
                "end": "21:00:00",
                "type": "playlist",
                "title": "Night Rotation",
            },
        ],
    )

    resp = await client.get("/api/guide.xml")
    assert resp.status_code == 200
    root = fromstring(resp.content)

    programmes = root.findall("programme")
    assert len(programmes) == 2

    # First programme
    p1 = programmes[0]
    # B2: uses TV-prefixed federation ID, not slug
    assert p1.get("channel", "").startswith("TV")
    # Verify time format: YYYYMMDDHHmmss {offset}
    start_str = p1.get("start")
    assert start_str is not None
    # Offset is dynamic based on channel timezone (EST/EDT)
    assert today.strftime("%Y%m%d") + "200000" in start_str

    title1 = p1.find("title")
    assert title1 is not None
    assert title1.text == "Mandelbrot Zoom"
    assert title1.get("lang") == "en"

    cat1 = p1.find("category")
    assert cat1 is not None
    assert cat1.text == "canvas"

    # Second programme
    p2 = programmes[1]
    title2 = p2.find("title")
    cat2 = p2.find("category")
    assert title2 is not None
    assert cat2 is not None
    assert title2.text == "Night Rotation"
    assert cat2.text == "playlist"


@pytest.mark.asyncio
async def test_guide_xml_declaration(client):
    """XML response starts with proper XML declaration."""
    resp = await client.get("/api/guide.xml")
    assert resp.content.startswith(b'<?xml version="1.0" encoding="UTF-8"?>')


@pytest.mark.asyncio
async def test_guide_xml_time_format(client, app_with_mocks):
    """XMLTV times use YYYYMMDDHHmmss {offset} format."""
    today = date.today()
    _write_program(
        today,
        [
            {
                "start": "09:05:30",
                "end": "10:15:00",
                "type": "playlist",
                "title": "Morning Block",
            },
        ],
    )

    resp = await client.get("/api/guide.xml")
    root = fromstring(resp.content)
    prog = root.findall("programme")[0]

    from routes.guide import _get_tz_offset

    tz_offset = _get_tz_offset()
    expected_start = today.strftime("%Y%m%d") + f"090530 {tz_offset}"
    expected_stop = today.strftime("%Y%m%d") + f"101500 {tz_offset}"
    assert prog.get("start") == expected_start
    assert prog.get("stop") == expected_stop


# ── Unit tests for helper functions ──


def test_block_title_uses_title_field():
    """_block_title returns the block's title field."""
    from routes.guide import _block_title

    assert (
        _block_title({"type": "canvas", "title": "Fractal Dreams"}) == "Fractal Dreams"
    )
    assert _block_title({"type": "playlist", "title": "Evening Mix"}) == "Evening Mix"
    assert _block_title({"type": "generator", "title": "The Seance"}) == "The Seance"


def test_block_title_fallback_no_title():
    """Without title field, falls back to type title-cased."""
    from routes.guide import _block_title

    assert _block_title({"type": "canvas"}) == "Canvas"
    assert _block_title({"type": "playlist"}) == "Playlist"
    assert _block_title({"type": "generator"}) == "Generator"


def test_block_title_empty_type():
    """Empty type returns 'Unknown'."""
    from routes.guide import _block_title

    assert _block_title({"type": ""}) == "Unknown"


def test_block_category():
    """Category maps directly from block type."""
    from routes.guide import _block_category

    assert _block_category({"type": "canvas"}) == "canvas"
    assert _block_category({"type": "playlist"}) == "playlist"
    assert _block_category({"type": "generator"}) == "generator"


def test_format_xmltv_time():
    """XMLTV time format is correct with dynamic timezone offset."""
    from routes.guide import _format_xmltv_time, _get_tz_offset

    tz_offset = _get_tz_offset()
    result = _format_xmltv_time(date(2026, 3, 14), "19:30:00")
    assert result == f"20260314193000 {tz_offset}"


def test_format_xmltv_time_midnight():
    """Midnight formats correctly with dynamic timezone offset."""
    from routes.guide import _format_xmltv_time, _get_tz_offset

    tz_offset = _get_tz_offset()
    result = _format_xmltv_time(date(2026, 1, 1), "00:00:00")
    assert result == f"20260101000000 {tz_offset}"
