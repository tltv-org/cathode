"""Tests for PlayoutEngine lifecycle — start, stop, error handling.

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

from playout import PlayoutConfig, PlayoutEngine
from playout.input_layer import SourceType

# ── Creation ──


def test_engine_creates_without_error():
    """PlayoutEngine() can be instantiated."""
    engine = PlayoutEngine()
    assert engine is not None
    assert engine.is_running is False


def test_engine_defaults():
    """Verify default state before start."""
    engine = PlayoutEngine()
    assert engine.is_running is False
    assert engine.on_source_lost is None
    assert engine.watchdog_timeout == 3.0


# ── Start ──


@pytest.mark.asyncio
async def test_engine_start(playout_config, hls_output_dir):
    """engine.start() builds pipeline and sets state to PLAYING."""
    from playout import OutputConfig, OutputType

    engine = PlayoutEngine()
    output_cfg = OutputConfig(
        type=OutputType.NULL,
        name="test",
        video_bitrate=playout_config.video_bitrate,
        audio_bitrate=playout_config.audio_bitrate,
    )
    try:
        await engine.start(config=playout_config, default_output=output_cfg)
        assert engine.is_running is True
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_engine_is_running_after_start(engine):
    """is_running is True after start."""
    assert engine.is_running is True


@pytest.mark.asyncio
async def test_engine_health_after_start(engine):
    """health returns valid dict with running=True."""
    # Pipeline may take a moment to transition to PLAYING
    await asyncio.sleep(0.5)
    health = engine.health
    assert health["running"] is True
    assert health["state"] in ("PLAYING", "PAUSED")
    assert isinstance(health["uptime"], float)
    assert health["uptime"] >= 0
    assert "channels" in health


# ── Channels exist ──


@pytest.mark.asyncio
async def test_all_channels_exist(engine):
    """All 4 named channels are accessible."""
    assert engine.failover is not None
    assert engine.input_a is not None
    assert engine.input_b is not None
    assert engine.blinder is not None


@pytest.mark.asyncio
async def test_channel_by_name(engine):
    """engine.channel(name) returns the correct channel."""
    assert engine.channel("failover") is engine.failover
    assert engine.channel("input_a") is engine.input_a
    assert engine.channel("input_b") is engine.input_b
    assert engine.channel("blinder") is engine.blinder


@pytest.mark.asyncio
async def test_channel_invalid_name(engine):
    """engine.channel() with invalid name raises ValueError."""
    with pytest.raises(ValueError, match="Unknown channel"):
        engine.channel("nonexistent")


# ── Default source type ──


@pytest.mark.asyncio
async def test_channels_start_with_test_source(engine):
    """All channels start with TEST source type (black pattern)."""
    assert engine.failover.source_type == SourceType.TEST
    assert engine.input_a.source_type == SourceType.TEST
    assert engine.input_b.source_type == SourceType.TEST
    assert engine.blinder.source_type == SourceType.TEST


# ── Stop ──


@pytest.mark.asyncio
async def test_engine_stop(playout_config, hls_output_dir):
    """engine.stop() cleans up, is_running becomes False."""
    from playout import OutputConfig, OutputType

    engine = PlayoutEngine()
    output_cfg = OutputConfig(type=OutputType.NULL, name="test")
    await engine.start(config=playout_config, default_output=output_cfg)
    assert engine.is_running is True

    await engine.stop()
    assert engine.is_running is False


@pytest.mark.asyncio
async def test_stop_when_not_started():
    """Stop when not started is a no-op (no error)."""
    engine = PlayoutEngine()
    await engine.stop()  # should not raise
    assert engine.is_running is False


@pytest.mark.asyncio
async def test_double_stop(playout_config, hls_output_dir):
    """Stopping twice is safe."""
    from playout import OutputConfig, OutputType

    engine = PlayoutEngine()
    output_cfg = OutputConfig(type=OutputType.NULL, name="test")
    await engine.start(config=playout_config, default_output=output_cfg)
    await engine.stop()
    await engine.stop()  # should not raise
    assert engine.is_running is False


# ── Double start ──


@pytest.mark.asyncio
async def test_double_start_raises(engine, playout_config):
    """Starting an already-started engine raises RuntimeError."""
    with pytest.raises(RuntimeError, match="already started"):
        await engine.start(config=playout_config)


# ── Config propagation ──


@pytest.mark.asyncio
async def test_custom_config_applied(hls_output_dir):
    """Custom PlayoutConfig values are applied to the mixer."""
    from playout import OutputConfig, OutputType

    cfg = PlayoutConfig(
        width=320,
        height=240,
        fps=10,
        video_bitrate=400,
    )
    output_cfg = OutputConfig(type=OutputType.NULL, name="test")
    engine = PlayoutEngine()
    try:
        await engine.start(config=cfg, default_output=output_cfg)
        assert engine._config.width == 320
        assert engine._config.height == 240
        assert engine._config.fps == 10
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_default_config_used(hls_output_dir):
    """When no config is passed, defaults are used."""
    from playout import OutputConfig, OutputType

    output_cfg = OutputConfig(type=OutputType.NULL, name="test")
    engine = PlayoutEngine()
    try:
        await engine.start(default_output=output_cfg)
        assert engine._config.width == 1920
        assert engine._config.height == 1080
    finally:
        await engine.stop()


# ── PIP positioning ──


@pytest.mark.asyncio
async def test_set_position(engine):
    """set_position changes compositor pad geometry."""
    engine.set_position("input_a", x=100, y=100, w=640, h=360)
    # Should not raise — compositor pad properties are set


@pytest.mark.asyncio
async def test_reset_position(engine):
    """reset_position restores full-screen."""
    engine.set_position("input_a", x=100, y=100, w=640, h=360)
    engine.reset_position("input_a")
    # Should not raise


@pytest.mark.asyncio
async def test_set_position_invalid_name(engine):
    """set_position raises ValueError for unknown channel."""
    with pytest.raises(ValueError):
        engine.set_position("bogus", x=0, y=0, w=100, h=100)


@pytest.mark.asyncio
async def test_reset_position_invalid_name(engine):
    """reset_position raises ValueError for unknown channel."""
    with pytest.raises(ValueError):
        engine.reset_position("bogus")


# ── Channel properties ──


@pytest.mark.asyncio
async def test_owns_element(engine):
    """InputLayer.owns_element identifies its own elements."""
    layer = engine.input_a  # channel index 1
    assert layer.owns_element(f"ch{layer.index}_test")
    assert not layer.owns_element("ch99_test")
    assert not layer.owns_element("mixer_something")


@pytest.mark.asyncio
async def test_restart_never_started_works(engine_no_start):
    """restart() on never-started engine starts with fakesink (null output)."""
    await engine_no_start.restart()
    assert engine_no_start.is_running
    await engine_no_start.stop()
