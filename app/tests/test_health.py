"""Tests for engine health dict structure.

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

from playout import PlayoutEngine
from playout.input_layer import SourceType

pytestmark = [
    pytest.mark.rtmp,
]


# ── Health dict structure ──


@pytest.mark.asyncio
async def test_health_has_all_keys(engine):
    """health dict has all required top-level keys."""
    h = engine.health
    assert "running" in h
    assert "state" in h
    assert "uptime" in h
    assert "errors" in h
    assert "last_error" in h
    assert "active_channel" in h
    assert "channels" in h


@pytest.mark.asyncio
async def test_health_running_true(engine):
    """health.running is True when engine is started."""
    assert engine.health["running"] is True


@pytest.mark.asyncio
async def test_health_state_playing(engine):
    """health.state is 'PLAYING' (or PAUSED during async transition) after start."""
    await asyncio.sleep(0.5)
    assert engine.health["state"] in ("PLAYING", "PAUSED")


@pytest.mark.asyncio
async def test_health_errors_zero(engine):
    """health.errors is 0 on a fresh engine."""
    assert engine.health["errors"] == 0


@pytest.mark.asyncio
async def test_health_last_error_none(engine):
    """health.last_error is None on a fresh engine."""
    assert engine.health["last_error"] is None


@pytest.mark.asyncio
async def test_health_active_channel(engine):
    """health.active_channel is 'failover' by default."""
    assert engine.health["active_channel"] == "failover"


# ── Channels dict ──


@pytest.mark.asyncio
async def test_health_has_all_channels(engine):
    """health.channels has all 4 channel entries."""
    channels = engine.health["channels"]
    assert "failover" in channels
    assert "input_a" in channels
    assert "input_b" in channels
    assert "blinder" in channels


@pytest.mark.asyncio
async def test_health_channel_has_source_type(engine):
    """Each channel entry has source_type."""
    for name, ch_info in engine.health["channels"].items():
        assert "source_type" in ch_info, f"channel {name} missing source_type"
        assert ch_info["source_type"] == "test"  # default


@pytest.mark.asyncio
async def test_health_channel_has_now_playing(engine):
    """Each channel entry has now_playing."""
    for name, ch_info in engine.health["channels"].items():
        assert "now_playing" in ch_info, f"channel {name} missing now_playing"


# ── Uptime ──


@pytest.mark.asyncio
async def test_uptime_increases(engine):
    """uptime increases over time."""
    u1 = engine.health["uptime"]
    await asyncio.sleep(1.0)
    u2 = engine.health["uptime"]
    assert u2 > u1, f"uptime should increase: {u1} -> {u2}"


# ── Health with playlist ──


@pytest.mark.asyncio
async def test_health_channel_with_playlist(engine, playlist_entries):
    """After loading playlist, channel now_playing has source/index/duration/played."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.0)

    ch_info = engine.health["channels"]["input_a"]
    assert ch_info["source_type"] == "playlist"

    np = ch_info["now_playing"]
    assert np is not None
    assert "source" in np
    assert "index" in np
    assert "duration" in np
    assert "played" in np


# ── Health when not started ──


def test_health_when_not_started():
    """health returns minimal dict when engine not started."""
    engine = PlayoutEngine()
    h = engine.health
    assert h["running"] is False
    assert h["state"] == "NULL"


# ── Health active channel updates ──


@pytest.mark.asyncio
async def test_health_active_channel_after_show(engine):
    """health.active_channel updates after show()."""
    engine.show("input_a")
    assert engine.health["active_channel"] == "input_a"

    engine.show("input_b")
    assert engine.health["active_channel"] == "input_b"

    engine.hide("input_b")
    assert engine.health["active_channel"] == "failover"


# ── Per-layer visibility in health ──


@pytest.mark.asyncio
async def test_health_channel_has_visibility_fields(engine):
    """Each channel entry has visible, alpha, volume."""
    for name, ch_info in engine.health["channels"].items():
        assert "visible" in ch_info, f"channel {name} missing visible"
        assert "alpha" in ch_info, f"channel {name} missing alpha"
        assert "volume" in ch_info, f"channel {name} missing volume"
        assert isinstance(ch_info["visible"], bool)
        assert isinstance(ch_info["alpha"], (int, float))
        assert isinstance(ch_info["volume"], (int, float))


@pytest.mark.asyncio
async def test_health_failover_visible_by_default(engine):
    """Failover should be visible after engine start."""
    ch_info = engine.health["channels"]["failover"]
    assert ch_info["visible"] is True
    assert ch_info["alpha"] > 0.0


@pytest.mark.asyncio
async def test_health_input_a_hidden_by_default(engine):
    """Input A should be hidden initially (alpha=0)."""
    ch_info = engine.health["channels"]["input_a"]
    assert ch_info["visible"] is False
    assert ch_info["alpha"] == 0.0
    assert ch_info["volume"] == 0.0


@pytest.mark.asyncio
async def test_health_visibility_updates_on_show(engine):
    """Showing a layer updates visible/alpha/volume in health."""
    engine.show("input_a", alpha=0.8, volume=0.6)
    ch_info = engine.health["channels"]["input_a"]
    assert ch_info["visible"] is True
    assert ch_info["alpha"] == 0.8
    assert ch_info["volume"] == 0.6


@pytest.mark.asyncio
async def test_health_visibility_updates_on_hide(engine):
    """Hiding a layer sets visible=False, alpha=0, volume=0."""
    engine.show("input_a")
    engine.hide("input_a")
    ch_info = engine.health["channels"]["input_a"]
    assert ch_info["visible"] is False
    assert ch_info["alpha"] == 0.0
    assert ch_info["volume"] == 0.0


# ── Playlist name in health ──


@pytest.mark.asyncio
async def test_health_channel_has_playlist_name(engine):
    """Each channel entry has playlist_name field."""
    for name, ch_info in engine.health["channels"].items():
        assert "playlist_name" in ch_info, f"channel {name} missing playlist_name"


@pytest.mark.asyncio
async def test_health_playlist_name_null_by_default(engine):
    """playlist_name is None when no named playlist is loaded."""
    ch_info = engine.health["channels"]["input_a"]
    assert ch_info["playlist_name"] is None


@pytest.mark.asyncio
async def test_health_playlist_name_set_when_named(engine, playlist_entries):
    """playlist_name reflects the name passed to load_playlist."""
    engine.input_a.load_playlist(playlist_entries, loop=True, name="my-show")
    engine.show("input_a")
    await asyncio.sleep(0.5)

    ch_info = engine.health["channels"]["input_a"]
    assert ch_info["playlist_name"] == "my-show"


@pytest.mark.asyncio
async def test_health_playlist_name_cleared_on_disconnect(engine, playlist_entries):
    """playlist_name is cleared when source is disconnected."""
    engine.input_a.load_playlist(playlist_entries, loop=True, name="temp")
    await asyncio.sleep(0.5)
    engine.input_a.disconnect()

    ch_info = engine.health["channels"]["input_a"]
    assert ch_info["playlist_name"] is None
