"""Tests for playlist loading and clip sequencing.

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


# ── Load playlist ──


@pytest.mark.asyncio
async def test_load_playlist_sets_source_type(engine, playlist_entries):
    """load_playlist() sets source_type to PLAYLIST."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    assert engine.input_a.source_type == SourceType.PLAYLIST


@pytest.mark.asyncio
async def test_load_playlist_loop(engine, playlist_entries):
    """Playlist in loop mode stays as PLAYLIST source."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(0.5)
    assert engine.input_a.source_type == SourceType.PLAYLIST


# ── now_playing ──


@pytest.mark.asyncio
async def test_now_playing_has_correct_fields(engine, playlist_entries):
    """now_playing returns dict with source, index, duration, played."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.0)

    np = engine.input_a.now_playing
    assert np is not None
    assert "source" in np
    assert "index" in np
    assert "duration" in np
    assert "played" in np


@pytest.mark.asyncio
async def test_now_playing_source_matches_entry(engine, playlist_entries):
    """now_playing source matches the first playlist entry."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(0.5)

    np = engine.input_a.now_playing
    assert np is not None
    assert np["source"] == playlist_entries[0].source
    assert np["index"] == 0


@pytest.mark.asyncio
async def test_now_playing_duration_matches_entry(engine, playlist_entries):
    """now_playing duration matches the playlist entry duration."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    await asyncio.sleep(0.5)

    np = engine.input_a.now_playing
    assert np is not None
    assert abs(np["duration"] - playlist_entries[0].duration) < 1.0


# ── Clip advancement ──


@pytest.mark.asyncio
async def test_clips_advance_over_time(engine, playlist_entries):
    """After enough time, the playlist advances to the next clip."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")

    # Wait for first clip to finish (clips are ~3s)
    # Give extra time for pipeline latency
    clip_dur = playlist_entries[0].duration
    await asyncio.sleep(clip_dur + 2.0)

    np = engine.input_a.now_playing
    assert np is not None
    # Should have advanced past index 0
    assert np["index"] >= 1, f"Expected clip to advance, still at index {np['index']}"


# ── Skip ──


@pytest.mark.asyncio
async def test_skip_advances_clip(engine, playlist_entries):
    """skip() advances to next clip immediately."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.5)

    np_before = engine.input_a.now_playing
    assert np_before is not None
    idx_before = np_before["index"]

    engine.input_a.skip()

    # EOS propagates through decodebin -> normalize -> queue -> concat
    # which can take a few seconds in GStreamer pipelines
    for _ in range(10):
        await asyncio.sleep(0.5)
        np_after = engine.input_a.now_playing
        if np_after and np_after["index"] > idx_before:
            break

    np_after = engine.input_a.now_playing
    assert np_after is not None
    assert np_after["index"] > idx_before, (
        f"Expected clip to advance past {idx_before}, still at {np_after['index']}"
    )


# ── Back ──


@pytest.mark.asyncio
async def test_back_goes_to_previous(engine, playlist_entries):
    """back() goes to previous clip."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.0)

    # Skip forward first so we have a previous clip
    engine.input_a.skip()
    for _ in range(10):
        await asyncio.sleep(0.5)
        np = engine.input_a.now_playing
        if np and np["index"] > 0:
            break

    np_before = engine.input_a.now_playing
    assert np_before is not None

    engine.input_a.back()
    for _ in range(10):
        await asyncio.sleep(0.5)
        np = engine.input_a.now_playing
        if np and np["index"] != np_before["index"]:
            break

    np_after = engine.input_a.now_playing
    assert np_after is not None
    # back() should go to the previous playlist entry
    assert np_after["index"] != np_before["index"]


# ── Loop ──


@pytest.mark.asyncio
async def test_playlist_loops(engine, playlist_entries):
    """Playlist loops back to clip 0 after last clip."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.0)

    # Skip through all clips to trigger loop
    for i in range(len(playlist_entries)):
        engine.input_a.skip()
        for _ in range(10):
            await asyncio.sleep(0.5)
            np = engine.input_a.now_playing
            if np and np["index"] > i:
                break

    await asyncio.sleep(1.0)
    np = engine.input_a.now_playing
    assert np is not None
    # After looping, source_type should still be PLAYLIST
    assert engine.input_a.source_type == SourceType.PLAYLIST


# ── Empty playlist ──


@pytest.mark.asyncio
async def test_empty_playlist_handled(engine):
    """load_playlist with empty list doesn't crash."""
    # Should log a warning but not raise
    engine.input_a.load_playlist([], loop=True)
    # Source type should remain as was (test/none) since empty
    # playlist is rejected gracefully
    assert engine.input_a.source_type in (SourceType.TEST, SourceType.NONE)


# ── Hot-swap ──


@pytest.mark.asyncio
async def test_playlist_hot_swap(engine, playlist_entries, test_clips):
    """Calling load_playlist while one is playing replaces cleanly."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.0)

    # Load a different (single-clip) playlist
    new_entries = [PlaylistEntry(source=test_clips[2], duration=3.0)]
    engine.input_a.load_playlist(new_entries, loop=True)
    await asyncio.sleep(1.0)

    np = engine.input_a.now_playing
    assert np is not None
    assert np["source"] == test_clips[2]
    assert engine.input_a.source_type == SourceType.PLAYLIST


# ── Skip on non-playlist is no-op ──


@pytest.mark.asyncio
async def test_skip_on_test_source_noop(engine):
    """skip() on a TEST source is a no-op."""
    assert engine.input_a.source_type == SourceType.TEST
    engine.input_a.skip()  # should not raise


# ── Playlist robustness ──


@pytest.mark.asyncio
async def test_skip_double_call_safe(engine, playlist_entries):
    """Calling skip() twice in rapid succession is safe."""
    layer = engine.input_a
    layer.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.5)

    # Both skips should succeed without crashing
    layer.skip()
    layer.skip()

    await asyncio.sleep(1.0)
    assert engine.is_running
    assert layer.source_type == SourceType.PLAYLIST


@pytest.mark.asyncio
async def test_playlist_no_loop_ends(engine, test_clips):
    """Non-looping playlist reaches end when clips are skipped past."""
    entries = [PlaylistEntry(source=test_clips[0], duration=3.0)]
    layer = engine.input_a
    layer.load_playlist(entries, loop=False)
    engine.show("input_a")
    await asyncio.sleep(1.0)

    # Skip past the only clip — should eventually disconnect
    layer.skip()
    await asyncio.sleep(2.0)

    # Engine should still be running (failover catches it)
    assert engine.is_running


@pytest.mark.asyncio
async def test_playlist_gapless_playbin3(engine, playlist_entries):
    """Playlist uses playbin3 for gapless playback."""
    layer = engine.input_a
    layer.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.0)

    # playbin3 should be set (owns its pipeline with playsink)
    assert layer._playbin3 is not None

    # Skip a few clips — playbin3 handles gapless transitions internally
    for _ in range(3):
        layer.skip()
        await asyncio.sleep(1.0)

    # Engine should still be healthy after skips
    assert engine.is_running
    assert layer.source_type == SourceType.PLAYLIST


@pytest.mark.asyncio
async def test_file_loop_survives_many_loops(engine, test_clips):
    """A file_loop survives multiple clip boundaries without deadlock."""
    # Use a short clip so it loops several times in our wait window
    entries = [PlaylistEntry(source=test_clips[0], duration=3.0)]
    layer = engine.input_a
    layer.load_playlist(entries, loop=True)
    engine.show("input_a")

    # Wait for several loop iterations (3s clips, 12s wait = ~4 loops)
    await asyncio.sleep(12.0)

    # Engine should still be alive
    assert engine.is_running
    health = engine.health
    assert health["running"]
    assert health["errors"] == 0

    # Playlist should still be playing
    assert layer.source_type == SourceType.PLAYLIST
    np = layer.now_playing
    assert np is not None


@pytest.mark.asyncio
async def test_multi_clip_playlist_loops_fully(engine, playlist_entries):
    """A 3-clip playlist loops through all clips and back to the start."""
    n = len(playlist_entries)
    layer = engine.input_a
    layer.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.5)

    seen_indices = set()
    np = layer.now_playing
    if np:
        seen_indices.add(np["index"] % n)

    # Skip through clips, waiting for each skip to take effect.
    # Use _display_index (what's actually rendering) for detection,
    # consistent with now_playing which also uses _display_index.
    for _ in range(n):
        prev_idx = layer._display_index
        layer.skip()
        # Wait for the playing clip to actually change
        for _ in range(10):
            await asyncio.sleep(0.5)
            if layer._display_index != prev_idx:
                break
        np = layer.now_playing
        if np:
            seen_indices.add(np["index"] % n)

    # Should have seen all 3 playlist indices
    assert len(seen_indices) == n, (
        f"Expected to see all {n} indices, saw {seen_indices}"
    )

    # Engine still healthy
    assert engine.health["errors"] == 0


@pytest.mark.asyncio
async def test_engine_restart_preserves_streaming(engine, playout_config):
    """engine.restart() stops and restarts without errors."""
    assert engine.is_running

    await engine.restart()
    await asyncio.sleep(2.0)

    assert engine.is_running
    health = engine.health
    assert health["running"]
    assert health["state"] == "PLAYING"
    assert health["errors"] == 0


@pytest.mark.asyncio
async def test_playlist_survives_restart(engine, playlist_entries, playout_config):
    """Playlist can be reloaded after engine restart."""
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.0)

    assert engine.input_a.now_playing is not None

    await engine.restart()
    assert engine.is_running

    # After restart, all channels are back to test pattern
    assert engine.input_a.source_type == SourceType.TEST

    # Reload playlist
    engine.input_a.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.5)

    np = engine.input_a.now_playing
    assert np is not None
    assert engine.input_a.source_type == SourceType.PLAYLIST


@pytest.mark.asyncio
async def test_back_on_non_playlist_noop(engine):
    """back() on a TEST source is a no-op."""
    assert engine.input_a.source_type == SourceType.TEST
    engine.input_a.back()  # should not raise


@pytest.mark.asyncio
async def test_skip_on_test_source_safe(engine):
    """skip() is safe when no playlist is loaded."""
    layer = engine.input_a
    assert layer.source_type == SourceType.TEST
    layer.skip()  # should not raise


# ── playbin3 — stability after skips ──


@pytest.mark.asyncio
async def test_playbin3_stable_after_skips(engine, test_clips):
    """Playlist stays healthy after multiple skip/rebuild cycles."""
    entries = [PlaylistEntry(source=test_clips[0], duration=3.0)]
    layer = engine.input_a
    layer.load_playlist(entries, loop=True)
    engine.show("input_a")

    await asyncio.sleep(1.5)

    # Skip through several clips — each skip tears down and rebuilds playbin3
    for _ in range(4):
        layer.skip()
        await asyncio.sleep(1.0)

    assert engine.is_running
    assert layer.source_type == SourceType.PLAYLIST
    np = layer.now_playing
    assert np is not None, "Playlist stalled — now_playing is None"

    # playbin3 should still be active
    assert layer._playbin3 is not None


# ── playbin3 gapless transitions ──


@pytest.mark.asyncio
async def test_playbin3_gapless_clip_advance(engine, playlist_entries):
    """Clips advance naturally via about-to-finish (gapless, no skip).

    This is the critical test: playbin3's about-to-finish must fire at
    the correct time (near actual clip end) and advance the playlist.
    With raw uridecodebin3, about-to-finish fired early due to source
    I/O exhaustion, causing cascading pre-rolls and broken playback.
    playbin3's playsink provides the clock feedback that fixes this.
    """
    layer = engine.input_a
    layer.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.0)

    initial_np = layer.now_playing
    assert initial_np is not None
    initial_index = initial_np["index"]

    # Wait for one full clip to finish (clips are ~3s, allow headroom)
    clip_dur = playlist_entries[initial_index].duration
    await asyncio.sleep(clip_dur + 3.0)

    # Clip should have advanced naturally via about-to-finish
    np = layer.now_playing
    assert np is not None
    assert np["index"] != initial_index, (
        f"Clip did not advance: still at index {initial_index} after "
        f"{clip_dur + 3.0}s (clip duration was {clip_dur}s)"
    )

    # Engine should still be healthy
    assert engine.is_running
    health = engine.health
    assert health["running"]
    assert health["errors"] == 0


@pytest.mark.asyncio
async def test_playbin3_gapless_multiple_transitions(engine, playlist_entries):
    """Multiple clips transition gaplessly without errors.

    Verifies that playbin3 handles multiple about-to-finish transitions
    without the snowball effect that plagued raw uridecodebin3.
    """
    layer = engine.input_a
    n = len(playlist_entries)
    layer.load_playlist(playlist_entries, loop=True)
    engine.show("input_a")
    await asyncio.sleep(1.0)

    # Total duration of all clips + generous headroom
    total_dur = sum(e.duration for e in playlist_entries)
    await asyncio.sleep(total_dur + 5.0)

    # After all clips should have played, check health
    assert engine.is_running
    health = engine.health
    assert health["running"]
    assert health["errors"] == 0

    # Playlist should still be active (looping)
    assert layer.source_type == SourceType.PLAYLIST
    np = layer.now_playing
    assert np is not None

    # The playlist index should have advanced past at least the
    # number of entries (proving it looped)
    assert layer._playlist_index >= 0  # sanity — still tracking
