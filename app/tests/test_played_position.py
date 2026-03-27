"""Tests for played position tracking.

Requires: GStreamer container + nginx-rtmp.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

APP_DIR = Path("/app")
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from playout import PlaylistEntry
from playout.input_layer import SourceType

pytestmark = [
    pytest.mark.rtmp,
]


# ── Initial state ──


@pytest.mark.asyncio
async def test_played_zero_initially(engine):
    """played property returns 0.0 for TEST source."""
    assert engine.input_a.played == 0.0


@pytest.mark.asyncio
async def test_played_zero_for_test_source(engine):
    """TEST source always returns 0.0 for played."""
    engine.input_a.load_test("smpte")
    assert engine.input_a.played == 0.0


# ── Playlist position ──


@pytest.mark.asyncio
async def test_played_increases_for_playlist(engine, playlist_entries):
    """played increases over time for PLAYLIST source."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")

    await asyncio.sleep(0.5)
    pos1 = engine.input_a.played

    await asyncio.sleep(1.5)
    pos2 = engine.input_a.played

    assert pos2 > pos1, f"played should increase: {pos1} -> {pos2}"
    assert pos2 >= 1.0, f"Expected at least 1s elapsed, got {pos2}"


@pytest.mark.asyncio
async def test_played_resets_on_clip_advance(engine, playlist_entries):
    """played resets when clip advances (stays < clip duration)."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")

    # Wait for first clip to finish and second to start
    clip_dur = playlist_entries[0].duration
    await asyncio.sleep(clip_dur + 2.0)

    pos = engine.input_a.played
    # After advancing, played should be less than the clip duration
    assert pos < clip_dur + 1.0, f"played should reset on advance: {pos}"


@pytest.mark.asyncio
async def test_played_stays_under_clip_duration(engine, playlist_entries):
    """played should never exceed current clip's duration significantly."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.5)

    pos = engine.input_a.played
    dur = playlist_entries[0].duration
    assert pos < dur + 1.0, f"played={pos} exceeds duration={dur}"


# ── File loop position ──


@pytest.mark.asyncio
async def test_played_increases_for_file_loop(engine, test_clips):
    """played increases over time for FILE_LOOP source."""
    engine.input_a.load_file_loop(test_clips[0])
    engine.show("input_a")

    await asyncio.sleep(0.5)
    pos1 = engine.input_a.played

    await asyncio.sleep(1.0)
    pos2 = engine.input_a.played

    assert pos2 > pos1, f"played should increase for file loop: {pos1} -> {pos2}"


# ── now_playing includes played field ──


@pytest.mark.asyncio
async def test_now_playing_includes_played(engine, playlist_entries):
    """now_playing dict includes 'played' field matching played property."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.0)

    np = engine.input_a.now_playing
    assert np is not None
    assert "played" in np

    # Should be close to the played property (may differ slightly
    # due to round() in now_playing vs raw played)
    diff = abs(np["played"] - engine.input_a.played)
    assert diff < 0.5, f"now_playing.played and .played differ by {diff}s"


# ── Played after disconnect ──


@pytest.mark.asyncio
async def test_played_zero_after_disconnect(engine, playlist_entries):
    """played returns 0.0 after disconnect."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    await asyncio.sleep(0.5)
    assert engine.input_a.played > 0

    engine.input_a.disconnect()
    assert engine.input_a.played == 0.0
