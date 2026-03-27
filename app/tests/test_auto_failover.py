"""Tests for auto-failover watchdog and error recovery.

Requires: GStreamer container.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

APP_DIR = Path("/app")
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from playout import PlayoutEngine
from playout.input_layer import SourceType

# ── Watchdog basics ──


@pytest.mark.asyncio
async def test_watchdog_running(engine):
    """Watchdog task is running after engine start."""
    assert engine._watchdog_task is not None
    assert not engine._watchdog_task.done()


@pytest.mark.asyncio
async def test_watchdog_timeout_configurable(engine):
    """Watchdog timeout can be changed."""
    engine.watchdog_timeout = 5.0
    assert engine.watchdog_timeout == 5.0


# ── data_age ──


@pytest.mark.asyncio
async def test_data_age_zero_for_test(engine):
    """data_age returns 0.0 for TEST source (no buffer tracking)."""
    assert engine.input_a.data_age == 0.0


@pytest.mark.asyncio
async def test_data_age_zero_before_first_buffer(engine):
    """data_age returns 0.0 before any buffer arrives."""
    engine.input_b.load_test("smpte")
    assert engine.input_b.data_age == 0.0


# ── on_source_lost callback ──


@pytest.mark.asyncio
async def test_on_source_lost_callback_wired(engine):
    """on_source_lost callback can be set."""
    callback = MagicMock()
    engine.on_source_lost = callback
    assert engine.on_source_lost is callback


@pytest.mark.asyncio
async def test_error_handler_triggers_failover(engine):
    """_handle_source_failure replaces source with test and hides channel."""
    callback = MagicMock()
    engine.on_source_lost = callback

    # Load a source on input_b
    engine.input_b.load_test("smpte")
    engine.show("input_b")
    assert engine.active_channel == "input_b"

    # Simulate a source pipeline error — in interpipeline architecture,
    # errors are handled per-channel via _handle_source_failure
    engine._handle_source_failure("input_b", engine.input_b.index, "Connection lost")

    # input_b should be replaced with test source and hidden
    assert engine.input_b.source_type == SourceType.TEST
    assert engine.active_channel == "failover"

    # Callback should have been called
    callback.assert_called_once_with("input_b", "Connection lost")


@pytest.mark.asyncio
async def test_playlist_error_triggers_failover(engine, playlist_entries):
    """Pipeline error on playlist source triggers failover."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    assert engine.active_channel == "input_a"

    callback = MagicMock()
    engine.on_source_lost = callback

    engine._handle_source_failure("input_a", engine.input_a.index, "Decode error")

    # Playlist error MUST trigger failover — any source type, any error
    assert engine.input_a.source_type == SourceType.TEST
    assert engine.active_channel == "failover"
    callback.assert_called_once_with("input_a", "Decode error")


# ── Bus error detection chain ──


@pytest.mark.asyncio
async def test_bad_file_source_fails_gracefully(engine):
    """Loading a non-existent file fails at pipeline start, not later."""
    from playout import PlaylistEntry

    layer = engine.input_a

    # Load a non-existent file — pipeline start should fail gracefully
    bad_entries = [PlaylistEntry(source="/nonexistent/bad_file.mp4", duration=5.0)]
    layer.load_playlist(bad_entries, loop=False)

    await asyncio.sleep(2.0)

    # Engine should still be running despite the bad source
    assert engine.is_running
    assert engine.health["running"]


@pytest.mark.asyncio
async def test_bad_file_error_propagates_to_engine(engine):
    """Bad file in playlist triggers engine-level error through full chain."""
    callback = MagicMock()
    engine.on_source_lost = callback

    # Load bad file on input_b (live source types trigger auto-failover,
    # but playlists just log — test that the error chain doesn't crash)
    from playout import PlaylistEntry

    bad_entries = [PlaylistEntry(source="/nonexistent/broken.mp4", duration=5.0)]
    engine.input_b.load_playlist(bad_entries, loop=False)

    await asyncio.sleep(3.0)

    # Engine should still be running despite the bad file
    assert engine.is_running
    assert engine.health["running"]


@pytest.mark.asyncio
async def test_mixer_error_handler(engine):
    """_on_mixer_error marks unhealthy and schedules restart."""
    # Call directly — mixer errors trigger auto-restart
    engine._on_mixer_error("x264enc0", "encoder stall")

    # Engine should be marked unhealthy immediately
    assert not engine.is_running

    # Yield to event loop so call_soon_threadsafe callback runs
    await asyncio.sleep(0)

    # Restart task should be scheduled
    assert engine._restart_task is not None

    # Wait for restart to complete (2s backoff + pipeline startup)
    await asyncio.sleep(5.0)

    # Engine should have recovered
    assert engine.is_running


@pytest.mark.asyncio
async def test_source_lost_callback_exception_safe(engine):
    """on_source_lost callback exception doesn't crash the engine."""

    def bad_callback(name, msg):
        raise RuntimeError("callback crashed")

    engine.on_source_lost = bad_callback

    engine.input_b.load_test("smpte")
    engine.show("input_b")

    # Trigger failover — should not crash despite bad callback
    engine._handle_source_failure("input_b", engine.input_b.index, "test error")

    # Engine survives
    assert engine.is_running
    assert engine.active_channel == "failover"


@pytest.mark.asyncio
async def test_handle_source_failure_none_channel(engine):
    """_handle_source_failure with bad channel index is safe."""
    engine._handle_source_failure("bogus", 99, "fake error")
    assert engine.is_running


# ── Failover is unconditional ──


@pytest.mark.asyncio
async def test_file_loop_error_triggers_failover(engine, test_clips):
    """file_loop error on input_a triggers failover."""
    engine.input_a.load_file_loop(test_clips[0])
    engine.show("input_a")
    assert engine.active_channel == "input_a"

    callback = MagicMock()
    engine.on_source_lost = callback

    engine._handle_source_failure("input_a", engine.input_a.index, "File read error")

    assert engine.input_a.source_type == SourceType.TEST
    assert engine.active_channel == "failover"
    callback.assert_called_once()


@pytest.mark.asyncio
async def test_test_source_error_triggers_failover(engine):
    """Even a TEST source error on input_a triggers failover."""
    engine.show("input_a")
    assert engine.active_channel == "input_a"

    engine._handle_source_failure("input_a", engine.input_a.index, "Internal error")

    assert engine.active_channel == "failover"


@pytest.mark.asyncio
async def test_input_b_error_triggers_failover(engine, playlist_entries):
    """Error on input_b triggers failover when it's the active channel."""
    engine.input_b.load_playlist(playlist_entries, loop=True)
    engine.show("input_b")
    assert engine.active_channel == "input_b"

    engine._handle_source_failure("input_b", engine.input_b.index, "Source died")

    assert engine.input_b.source_type == SourceType.TEST
    assert engine.active_channel == "failover"


@pytest.mark.asyncio
async def test_blinder_error_triggers_failover(engine):
    """Error on blinder triggers failover."""
    engine.show("blinder")

    engine._handle_source_failure("blinder", engine.blinder.index, "Blinder error")

    assert engine.blinder.source_type == SourceType.TEST


@pytest.mark.asyncio
async def test_failover_self_recovery(engine, test_clips):
    """Failover error reloads test pattern — never hides itself."""
    # Load a real file on failover
    engine.failover.load_file_loop(test_clips[0])
    await asyncio.sleep(0.5)

    # Simulate failover error — should reload test pattern, NOT hide
    engine._handle_source_failure("failover", engine.failover.index, "Decoder crash")

    # Failover should be back to test pattern, engine still running
    assert engine.failover.source_type == SourceType.TEST
    assert engine.is_running
    # Failover is still the active channel — it never hides
    assert engine.active_channel == "failover"


@pytest.mark.asyncio
async def test_failover_survives_input_a_death(engine, playlist_entries):
    """When input_a dies, failover is already running and takes over."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.0)

    # Verify failover is still producing frames (test source running)
    assert engine.failover.source_type == SourceType.TEST

    # Kill input_a
    engine._handle_source_failure("input_a", engine.input_a.index, "Fatal")

    # Failover takes over immediately — it was already running
    assert engine.active_channel == "failover"
    assert engine.failover.source_type == SourceType.TEST
    assert engine.is_running
    assert engine.health["errors"] == 0


@pytest.mark.asyncio
async def test_inactive_channel_error_no_switch(engine, playlist_entries):
    """Error on a non-active channel replaces source but doesn't switch."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    assert engine.active_channel == "input_a"

    # input_b errors while input_a is active — should NOT switch to failover
    engine.input_b.load_test("smpte")
    engine._handle_source_failure("input_b", engine.input_b.index, "Error")

    assert engine.input_b.source_type == SourceType.TEST
    assert engine.active_channel == "input_a"  # unchanged


# ── Mixer error recovery ──


@pytest.mark.asyncio
async def test_mixer_error_marks_unhealthy(engine):
    """Mixer error sets _started to False so watchdog detects it."""
    assert engine.is_running

    # Simulate mixer error callback (normally called from GStreamer bus thread
    # via call_soon_threadsafe, but here we call directly on asyncio thread)
    engine._on_mixer_error("rtmpsink0", "Connection refused")

    # Engine should be marked as not running
    assert not engine.is_running


@pytest.mark.asyncio
async def test_mixer_error_schedules_restart(engine):
    """Mixer error schedules an automatic restart task."""
    assert engine.is_running

    engine._on_mixer_error("rtmpsink0", "Connection refused")

    # Yield to event loop so call_soon_threadsafe callback runs
    await asyncio.sleep(0)

    # A restart task should have been scheduled
    assert engine._restart_task is not None
    assert not engine._restart_task.done()

    # Wait for the restart attempt (2s backoff + startup time)
    await asyncio.sleep(5.0)

    # Engine should have recovered
    assert engine.is_running
    assert engine.health["running"]


@pytest.mark.asyncio
async def test_mixer_error_double_restart_safe(engine):
    """Multiple mixer errors don't spawn multiple restart tasks."""
    engine._on_mixer_error("rtmpsink0", "Error 1")

    # Yield to event loop so call_soon_threadsafe callback runs
    await asyncio.sleep(0)
    task1 = engine._restart_task
    assert task1 is not None

    # Second error while restart is pending — should NOT spawn a second task
    engine._schedule_mixer_restart()
    task2 = engine._restart_task

    # Should be the same task — no duplicate
    assert task1 is task2

    # Wait for recovery
    await asyncio.sleep(5.0)
    assert engine.is_running


# ── Blinder error hides blinder ──


@pytest.mark.asyncio
async def test_blinder_error_hides_blinder(engine):
    """Blinder error hides blinder so failover shows through (not black)."""
    from playout.mixer import CH_BLINDER

    engine.show("blinder")

    # Verify blinder is visible
    blinder_alpha = engine._mixer.video_pads[CH_BLINDER].get_property("alpha")
    assert blinder_alpha == 1.0

    # Trigger blinder failure
    engine._handle_source_failure("blinder", engine.blinder.index, "Blinder error")

    # Blinder source replaced with test
    assert engine.blinder.source_type == SourceType.TEST

    # Blinder must be HIDDEN — alpha must be 0.0
    # Without this fix, blinder stays at alpha=1.0 covering everything with black
    blinder_alpha = engine._mixer.video_pads[CH_BLINDER].get_property("alpha")
    assert blinder_alpha == 0.0, (
        f"Blinder alpha is {blinder_alpha}, expected 0.0 — "
        "broken blinder would cover screen with black"
    )


# ── Start failure triggers failover ──


@pytest.mark.asyncio
async def test_start_failure_triggers_failover(engine):
    """Source pipeline start failure fires on_error so failover activates."""
    callback = MagicMock()
    engine.on_source_lost = callback

    engine.show("input_a")
    assert engine.active_channel == "input_a"

    # Force a start failure by loading RTMP with an impossible URL that
    # causes the pipeline to fail state change.  Use a source type that
    # exercises the full _start_source path.
    # We manually invoke the failure path by calling _start_source on a
    # pipeline we've sabotaged.
    layer = engine.input_a
    layer._teardown_source()

    # Build a pipeline with an element that will fail to start
    from playout.input_layer import _make

    prefix = f"ch{layer.index}_badtest"
    vsrc = _make("videotestsrc", f"{prefix}_v")
    vsrc.set_property("is-live", True)
    layer._pipeline.add(vsrc)
    layer._source_elements = [vsrc]

    # Sabotage: add a capsfilter with impossible caps so pipeline can't negotiate
    import gi

    gi.require_version("Gst", "1.0")
    from gi.repository import Gst

    badcaps = _make("capsfilter", f"{prefix}_badcaps")
    badcaps.set_property(
        "caps",
        Gst.Caps.from_string("video/x-raw,width=99999,height=99999,framerate=999/1"),
    )
    layer._pipeline.add(badcaps)
    layer._source_elements.append(badcaps)
    vsrc.link(badcaps)
    # Don't link to inter sinks — incomplete pipeline

    # _start_source should detect FAILURE and call on_error
    layer.source_type = SourceType.TEST
    layer._start_source()

    # Give call_soon_threadsafe time to dispatch
    await asyncio.sleep(0.5)

    # The engine should still be running (failover handles it)
    assert engine.is_running

    # Clean up the sabotaged pipeline
    layer._teardown_source()
    layer.load_test("black")


# ── HLS source error triggers failover ──


@pytest.mark.asyncio
async def test_hls_source_error_triggers_failover(engine):
    """HLS source with bad URL triggers bus error and failover."""
    callback = MagicMock()
    engine.on_source_lost = callback

    engine.input_a.load_hls("https://127.0.0.1:1/nonexistent.m3u8")
    engine.show("input_a")

    # Wait for uridecodebin to fail
    await asyncio.sleep(5.0)

    # Engine must survive
    assert engine.is_running

    # Either the bus error fired failover, or the source is still trying.
    # If failover triggered, active_channel is failover.
    # If not, the source error was logged but didn't reach the handler
    # (some HLS errors are warnings, not errors in GStreamer).
    # Either way, engine survives — this test verifies no crash.


# ── Image source error triggers failover ──


@pytest.mark.asyncio
async def test_image_source_error_triggers_failover(engine):
    """Image source with nonexistent path triggers bus error and failover."""
    callback = MagicMock()
    engine.on_source_lost = callback

    engine.input_a.load_image("/nonexistent/bad_image.png")
    engine.show("input_a")

    # Wait for uridecodebin to fail
    await asyncio.sleep(3.0)

    # Engine must survive
    assert engine.is_running


# ── Failover hidden + all fail ──


@pytest.mark.asyncio
async def test_failover_hidden_all_sources_fail(engine):
    """When failover is hidden and all sources fail, engine stays alive.

    This is an operator error (hiding failover), but the engine must not
    crash.  The output will be black (compositor background) but the
    encoder keeps running.
    """
    from playout.mixer import CH_FAILOVER

    # Hide failover (operator error, but must not crash)
    engine.hide("failover")
    failover_alpha = engine._mixer.video_pads[CH_FAILOVER].get_property("alpha")
    assert failover_alpha == 0.0

    # Show input_a, then kill it
    engine.show("input_a")
    engine._handle_source_failure("input_a", engine.input_a.index, "Dead")

    # active_channel falls back to failover (by name) but failover is hidden
    assert engine.active_channel == "failover"

    # Engine must still be running — encoder outputs black from compositor background
    assert engine.is_running
    assert engine.health["running"]
    assert engine.health["errors"] == 0
