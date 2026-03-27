"""Tests for different source type loading.

Requires: GStreamer container.
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

# ── Test sources ──


@pytest.mark.asyncio
async def test_load_test_black(engine):
    """load_test('black') sets source_type TEST."""
    engine.input_a.load_test("black")
    assert engine.input_a.source_type == SourceType.TEST


@pytest.mark.asyncio
async def test_load_test_smpte(engine):
    """load_test('smpte') sets source_type TEST."""
    engine.input_a.load_test("smpte")
    assert engine.input_a.source_type == SourceType.TEST


@pytest.mark.asyncio
async def test_load_test_snow(engine):
    """load_test('snow') sets source_type TEST."""
    engine.input_a.load_test("snow")
    assert engine.input_a.source_type == SourceType.TEST


@pytest.mark.asyncio
async def test_load_test_with_tone(engine):
    """load_test with wave=0 (sine) sets source_type TEST."""
    engine.input_a.load_test("smpte", wave=0)
    assert engine.input_a.source_type == SourceType.TEST


# ── File loop ──


@pytest.mark.asyncio
async def test_load_file_loop(engine, test_clips):
    """load_file_loop(path) sets source_type FILE_LOOP."""
    engine.input_a.load_file_loop(test_clips[0])
    assert engine.input_a.source_type == SourceType.FILE_LOOP


@pytest.mark.asyncio
async def test_file_loop_now_playing(engine, test_clips):
    """File loop now_playing has source, index, duration, played."""
    engine.input_a.load_file_loop(test_clips[0])
    engine.show("input_a")
    await asyncio.sleep(0.5)

    np = engine.input_a.now_playing
    assert np is not None
    assert "played" in np


# ── Disconnect ──


@pytest.mark.asyncio
async def test_disconnect(engine, playlist_entries):
    """disconnect() sets source_type NONE."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    assert engine.input_a.source_type == SourceType.PLAYLIST

    engine.input_a.disconnect()
    assert engine.input_a.source_type == SourceType.NONE


@pytest.mark.asyncio
async def test_disconnect_now_playing_is_none(engine, playlist_entries):
    """After disconnect, now_playing is None."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.input_a.disconnect()
    assert engine.input_a.now_playing is None


# ── Hot-swap ──


@pytest.mark.asyncio
async def test_source_hot_swap_playlist_to_test(engine, playlist_entries):
    """load_playlist then load_test replaces cleanly."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    assert engine.input_a.source_type == SourceType.PLAYLIST

    engine.input_a.load_test("smpte")
    assert engine.input_a.source_type == SourceType.TEST


@pytest.mark.asyncio
async def test_source_hot_swap_test_to_file_loop(engine, test_clips):
    """load_test then load_file_loop replaces cleanly."""
    engine.input_a.load_test("black")
    assert engine.input_a.source_type == SourceType.TEST

    engine.input_a.load_file_loop(test_clips[0])
    assert engine.input_a.source_type == SourceType.FILE_LOOP


@pytest.mark.asyncio
async def test_source_hot_swap_file_loop_to_playlist(
    engine, test_clips, playlist_entries
):
    """load_file_loop then load_playlist replaces cleanly."""
    engine.input_a.load_file_loop(test_clips[0])
    assert engine.input_a.source_type == SourceType.FILE_LOOP

    engine.input_a.load_playlist(playlist_entries, loop=True)
    assert engine.input_a.source_type == SourceType.PLAYLIST


# ── Rapid source changes ──


@pytest.mark.asyncio
async def test_rapid_source_changes(engine):
    """Multiple rapid test-pattern source changes don't crash.

    Uses only test patterns — file_loop and playlist involve decoders
    that need time to tear down, so rapid swaps of those are tested
    separately (test_playlist_hot_swap, test_source_hot_swap_*).
    """
    for _ in range(10):
        engine.input_a.load_test("black")
        engine.input_a.load_test("smpte")
        engine.input_a.load_test("snow")
        engine.input_a.disconnect()
        engine.input_a.load_test("black")

    assert engine.input_a.source_type == SourceType.TEST


# ── Multi-channel independent sources ──


@pytest.mark.asyncio
async def test_channels_independent(engine):
    """Different channels can have different source types simultaneously."""
    engine.failover.load_test("black")
    engine.input_a.load_test("smpte")
    engine.input_b.load_test("snow")
    engine.blinder.load_test("black")

    assert engine.failover.source_type == SourceType.TEST
    assert engine.input_a.source_type == SourceType.TEST
    assert engine.input_b.source_type == SourceType.TEST
    assert engine.blinder.source_type == SourceType.TEST


# ── Disconnect safety ──


@pytest.mark.asyncio
async def test_disconnect_from_test_source(engine):
    """disconnect() from TEST source is safe."""
    engine.input_a.disconnect()
    assert engine.input_a.source_type == SourceType.NONE


@pytest.mark.asyncio
async def test_disconnect_from_playlist(engine, playlist_entries):
    """disconnect() cleanly tears down a running playlist."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.0)

    engine.input_a.disconnect()
    assert engine.input_a.source_type == SourceType.NONE
    assert engine.input_a.now_playing is None
