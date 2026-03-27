"""Tests for layer show/hide/active_channel switching.

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

pytestmark = [
    pytest.mark.rtmp,
]


# ── Default state ──


@pytest.mark.asyncio
async def test_active_channel_default_is_failover(engine):
    """After start, active_channel is 'failover'."""
    assert engine.active_channel == "failover"


# ── Show ──


@pytest.mark.asyncio
async def test_show_input_a(engine):
    """show('input_a') sets active_channel to 'input_a'."""
    engine.show("input_a")
    assert engine.active_channel == "input_a"


@pytest.mark.asyncio
async def test_show_input_b(engine):
    """show('input_b') sets active_channel to 'input_b'."""
    engine.show("input_a")
    assert engine.active_channel == "input_a"

    engine.show("input_b")
    assert engine.active_channel == "input_b"


@pytest.mark.asyncio
async def test_show_replaces_previous(engine):
    """Showing input_b hides input_a, sets active to input_b."""
    engine.show("input_a")
    assert engine.active_channel == "input_a"

    engine.show("input_b")
    assert engine.active_channel == "input_b"

    # Showing input_a again should switch back
    engine.show("input_a")
    assert engine.active_channel == "input_a"


# ── Hide ──


@pytest.mark.asyncio
async def test_hide_falls_back_to_failover(engine):
    """hide('input_a') falls back to failover."""
    engine.show("input_a")
    assert engine.active_channel == "input_a"

    engine.hide("input_a")
    assert engine.active_channel == "failover"


@pytest.mark.asyncio
async def test_hide_non_active_channel(engine):
    """Hiding a channel that isn't active doesn't change active."""
    engine.show("input_a")
    assert engine.active_channel == "input_a"

    # Hiding input_b (not active) shouldn't change active
    engine.hide("input_b")
    assert engine.active_channel == "input_a"


# ── Blinder ──


@pytest.mark.asyncio
async def test_show_blinder_doesnt_change_active(engine):
    """show('blinder') shows on top without changing active_channel."""
    engine.show("input_a")
    assert engine.active_channel == "input_a"

    engine.show("blinder")
    # Blinder shows on top but active_channel stays as input_a
    assert engine.active_channel == "input_a"


@pytest.mark.asyncio
async def test_hide_blinder(engine):
    """hide_blinder() removes blinder."""
    engine.show("blinder")
    engine.hide_blinder()
    # Should not crash and active should remain failover
    assert engine.active_channel == "failover"


@pytest.mark.asyncio
async def test_show_then_hide_blinder_preserves_active(engine):
    """Showing and hiding blinder preserves the active channel."""
    engine.show("input_a")
    engine.show("blinder")
    engine.hide_blinder()
    assert engine.active_channel == "input_a"


# ── Invalid channel ──


@pytest.mark.asyncio
async def test_show_invalid_name_raises(engine):
    """show() with invalid name raises ValueError."""
    with pytest.raises(ValueError, match="Unknown channel"):
        engine.show("nonexistent")


@pytest.mark.asyncio
async def test_hide_invalid_name_raises(engine):
    """hide() with invalid name raises ValueError."""
    with pytest.raises(ValueError, match="Unknown channel"):
        engine.hide("nonexistent")


# ── Alpha/volume ──


@pytest.mark.asyncio
async def test_show_with_alpha_volume(engine):
    """show(alpha=0.5, volume=0.5) works without error."""
    engine.show("input_a", alpha=0.5, volume=0.5)
    assert engine.active_channel == "input_a"


@pytest.mark.asyncio
async def test_show_with_zero_alpha(engine):
    """show(alpha=0.0) works (effectively invisible but still active)."""
    engine.show("input_a", alpha=0.0, volume=0.0)
    assert engine.active_channel == "input_a"


# ── Rapid switching ──


@pytest.mark.asyncio
async def test_rapid_switching(engine):
    """Rapid show/hide doesn't crash."""
    for _ in range(10):
        engine.show("input_a")
        engine.show("input_b")
        engine.hide("input_b")
        engine.show("failover")

    # Final state should be consistent
    assert engine.active_channel in ("failover", "input_a", "input_b")


# ── Failover visibility ──


@pytest.mark.asyncio
async def test_failover_always_visible(engine):
    """Failover layer stays visible (show/hide only affects other layers)."""
    engine.show("input_a")
    # Failover should still be running underneath
    assert engine.failover.source_type is not None
