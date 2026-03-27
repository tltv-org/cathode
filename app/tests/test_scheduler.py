"""Tests for scheduler.py — background task helpers and loop logic.

Tests the pure-logic helpers and async functions by calling them
directly with mocked dependencies.  Background loops are tested
via single-iteration patterns (patch sleep to break early).
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

APP_DIR = Path("/app")
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from channel import ChannelContext
import scheduler


# ── Fixtures ──


@pytest.fixture
def ctx(tmp_path):
    """Minimal ChannelContext for scheduler tests."""
    media = tmp_path / "media"
    media.mkdir()
    generated = media / "cathode"
    generated.mkdir()
    programs = tmp_path / "programs"
    programs.mkdir()
    return ChannelContext(
        id="test-channel",
        display_name="Test Channel",
        media_dir=str(media),
        generated_dir=str(generated),
        program_dir=str(programs),
        channel_id="TVtestid123",
        default_output_type="hls",
        hls_dir="/tmp/hls-test",
    )


@pytest.fixture
def mock_engine():
    """Mock PlayoutEngine with named layer access."""
    engine = MagicMock()
    engine.is_running = True
    engine.health = {
        "running": True,
        "state": "PLAYING",
        "errors": 0,
        "active_channel": "failover",
    }
    engine.failover = MagicMock()
    engine.input_a = MagicMock()
    engine.input_b = MagicMock()
    engine.blinder = MagicMock()
    engine.channel = MagicMock(side_effect=lambda name: getattr(engine, name))
    engine.show = MagicMock()
    engine.hide = MagicMock()
    engine.start = AsyncMock()
    engine.stop = AsyncMock()
    engine.restart = AsyncMock()
    return engine


# ════════════════════════════════════════════════════════════════
# _resolve_local_files
# ════════════════════════════════════════════════════════════════


class TestResolveLocalFiles:
    """Resolve filenames to local paths across media directories."""

    def test_file_in_media_dir(self, ctx):
        """File found in primary media directory."""
        media_file = Path(ctx.media_dir) / "clip.mp4"
        media_file.write_bytes(b"\x00" * 100)

        with patch("scheduler.get_clip_duration", return_value=10.0):
            paths, durs = scheduler._resolve_local_files(["clip.mp4"], ctx)

        assert len(paths) == 1
        assert paths[0] == str(media_file)
        assert durs[0] == 10.0

    def test_file_in_generated_dir(self, ctx):
        """File found in cathode/ dir (not in media root)."""
        gen_file = Path(ctx.generated_dir) / "test.mp4"
        gen_file.write_bytes(b"\x00" * 100)

        with patch("scheduler.get_clip_duration", return_value=5.0):
            paths, durs = scheduler._resolve_local_files(["test.mp4"], ctx)

        assert len(paths) == 1
        assert "cathode" in paths[0]

    def test_file_not_found(self, ctx):
        """Missing file is skipped."""
        paths, durs = scheduler._resolve_local_files(["missing.mp4"], ctx)
        assert len(paths) == 0
        assert len(durs) == 0

    def test_mixed_found_and_missing(self, ctx):
        """Mix of found and missing files."""
        media_file = Path(ctx.media_dir) / "found.mp4"
        media_file.write_bytes(b"\x00" * 100)

        with patch("scheduler.get_clip_duration", return_value=3.0):
            paths, durs = scheduler._resolve_local_files(
                ["found.mp4", "missing.mp4"], ctx
            )

        assert len(paths) == 1
        assert "found.mp4" in paths[0]

    def test_empty_list(self, ctx):
        """Empty filename list returns empty results."""
        paths, durs = scheduler._resolve_local_files([], ctx)
        assert paths == []
        assert durs == []

    def test_media_dir_takes_priority(self, ctx):
        """Same filename in multiple dirs — media_dir wins."""
        media_file = Path(ctx.media_dir) / "dup.mp4"
        media_file.write_bytes(b"\x00" * 100)
        gen_file = Path(ctx.generated_dir) / "dup.mp4"
        gen_file.write_bytes(b"\x00" * 100)

        with patch("scheduler.get_clip_duration", return_value=5.0):
            paths, _ = scheduler._resolve_local_files(["dup.mp4"], ctx)

        assert len(paths) == 1
        assert ctx.media_dir in paths[0]


# ════════════════════════════════════════════════════════════════
# _read_hls_sequence
# ════════════════════════════════════════════════════════════════


class TestReadHlsSequence:
    """Parse EXT-X-MEDIA-SEQUENCE from HLS m3u8 on disk."""

    def test_valid_manifest(self, ctx, tmp_path):
        """Parses sequence number from valid manifest file."""
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()
        m3u8 = hls_dir / "stream.m3u8"
        m3u8.write_text(
            "#EXTM3U\n"
            "#EXT-X-VERSION:3\n"
            "#EXT-X-MEDIA-SEQUENCE:42\n"
            "#EXTINF:4.000,\nseg42.ts\n"
        )
        ctx.hls_dir = str(hls_dir)
        result = scheduler._read_hls_sequence(ctx)
        assert result == 42

    def test_missing_sequence_tag(self, ctx, tmp_path):
        """Manifest without sequence tag returns None."""
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()
        m3u8 = hls_dir / "stream.m3u8"
        m3u8.write_text("#EXTM3U\n#EXT-X-VERSION:3\n")
        ctx.hls_dir = str(hls_dir)
        result = scheduler._read_hls_sequence(ctx)
        assert result is None

    def test_file_not_found(self, ctx, tmp_path):
        """Missing m3u8 file returns None."""
        ctx.hls_dir = str(tmp_path / "nonexistent")
        result = scheduler._read_hls_sequence(ctx)
        assert result is None

    def test_empty_directory(self, ctx, tmp_path):
        """Empty HLS directory (no m3u8) returns None."""
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()
        ctx.hls_dir = str(hls_dir)
        result = scheduler._read_hls_sequence(ctx)
        assert result is None

    def test_large_sequence_number(self, ctx, tmp_path):
        """Large sequence numbers are parsed correctly."""
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()
        m3u8 = hls_dir / "stream.m3u8"
        m3u8.write_text("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:999999\n")
        ctx.hls_dir = str(hls_dir)
        result = scheduler._read_hls_sequence(ctx)
        assert result == 999999


# ════════════════════════════════════════════════════════════════
# _activate_failover
# ════════════════════════════════════════════════════════════════


class TestActivateFailover:
    """Activate failover by hiding input_a."""

    @pytest.mark.asyncio
    async def test_hides_input_a(self, ctx, mock_engine):
        with patch.object(scheduler.main, "playout", mock_engine):
            await scheduler._activate_failover("2026-03-19", ctx)
        mock_engine.hide.assert_called_once_with("input_a")

    @pytest.mark.asyncio
    async def test_engine_none_no_crash(self, ctx):
        with patch.object(scheduler.main, "playout", None):
            await scheduler._activate_failover("2026-03-19", ctx)
        # Should not raise


# ════════════════════════════════════════════════════════════════
# _push_and_reset_playlist
# ════════════════════════════════════════════════════════════════


class TestPushAndResetPlaylist:
    """Load playlist on input_a and persist via PlaylistStore."""

    @pytest.mark.asyncio
    async def test_loads_playlist_and_shows(self, ctx, mock_engine):
        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch("playlist_store.save"),
        ):
            await scheduler._push_and_reset_playlist(
                ["/media/a.mp4", "/media/b.mp4"],
                [10.0, 20.0],
                "2026-03-19",
                ctx,
            )

        mock_engine.input_a.load_playlist.assert_called_once()
        entries = mock_engine.input_a.load_playlist.call_args[0][0]
        assert len(entries) == 2
        assert entries[0].source == "/media/a.mp4"
        assert entries[1].duration == 20.0
        mock_engine.show.assert_called_once_with("input_a")

    @pytest.mark.asyncio
    async def test_persists_to_store(self, ctx, mock_engine):
        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch("playlist_store.save") as mock_save,
        ):
            await scheduler._push_and_reset_playlist(
                ["/media/a.mp4"], [5.0], "2026-03-19", ctx
            )

        mock_save.assert_called_once()
        call_args = mock_save.call_args
        assert call_args[0][0] == date(2026, 3, 19)

    @pytest.mark.asyncio
    async def test_engine_none_no_crash(self, ctx):
        with patch.object(scheduler.main, "playout", None):
            await scheduler._push_and_reset_playlist(
                ["/media/a.mp4"], [5.0], "2026-03-19", ctx
            )

    @pytest.mark.asyncio
    async def test_loads_on_custom_layer(self, ctx, mock_engine):
        """Push playlist onto a non-default layer."""
        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch("playlist_store.save"),
        ):
            await scheduler._push_and_reset_playlist(
                ["/media/a.mp4"],
                [10.0],
                "2026-03-19",
                ctx,
                layer="input_b",
                loop=False,
            )

        mock_engine.input_b.load_playlist.assert_called_once()
        call_kwargs = mock_engine.input_b.load_playlist.call_args
        assert call_kwargs[1]["loop"] is False
        mock_engine.show.assert_called_once_with("input_b")

    @pytest.mark.asyncio
    async def test_custom_layer_no_playlist_store_persist(self, ctx, mock_engine):
        """Non-input_a layers don't persist to PlaylistStore."""
        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch("playlist_store.save") as mock_save,
        ):
            await scheduler._push_and_reset_playlist(
                ["/media/a.mp4"],
                [5.0],
                "2026-03-19",
                ctx,
                layer="blinder",
            )

        mock_save.assert_not_called()


# ════════════════════════════════════════════════════════════════
# _restore_default_playlist
# ════════════════════════════════════════════════════════════════


class TestRestoreDefaultPlaylist:
    """Restore playlist from all available media."""

    @pytest.mark.asyncio
    async def test_scans_and_pushes(self, ctx, mock_engine):
        media_file = Path(ctx.media_dir) / "test.mp4"
        media_file.write_bytes(b"\x00" * 100)

        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch("scheduler.scan_media", return_value=([str(media_file)], [10.0])),
            patch("playlist_store.save"),
        ):
            await scheduler._restore_default_playlist("2026-03-19", ctx)

        mock_engine.input_a.load_playlist.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_media_no_crash(self, ctx, mock_engine):
        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch("scheduler.scan_media", return_value=([], [])),
        ):
            await scheduler._restore_default_playlist("2026-03-19", ctx)

        mock_engine.input_a.load_playlist.assert_not_called()


# ════════════════════════════════════════════════════════════════
# _reload_sources_after_restart
# ════════════════════════════════════════════════════════════════


class TestReloadSourcesAfterRestart:
    """Reload failover and playlist after engine restart."""

    @pytest.mark.asyncio
    async def test_engine_none_returns(self, ctx):
        with patch.object(scheduler.main, "playout", None):
            await scheduler._reload_sources_after_restart(ctx)

    @pytest.mark.asyncio
    async def test_loads_failover_video(self, ctx, mock_engine):
        failover_path = Path(ctx.generated_dir) / "failover.mp4"
        failover_path.write_bytes(b"\x00" * 100)

        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch.object(scheduler.config, "FAILOVER_FILENAME", "failover.mp4"),
            patch("playlist_store.get", return_value=None),
            patch("playlist_store.save"),
        ):
            await scheduler._reload_sources_after_restart(ctx)

        mock_engine.failover.load_file_loop.assert_called_once_with(str(failover_path))
        mock_engine.show.assert_any_call("failover")

    @pytest.mark.asyncio
    async def test_restores_named_playlist(self, ctx, mock_engine):
        """Restores named playlist from persisted playout state."""
        pl_data = {
            "name": "evening",
            "entries": [{"source": "/media/a.mp4", "duration": 10.0}],
        }
        layer_state = {
            "type": "playlist",
            "playlist_name": "evening",
            "loop": True,
        }
        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch.object(scheduler.config, "FAILOVER_FILENAME", "missing.mp4"),
            patch("playout_state.get_layer_state", return_value=layer_state),
            patch("named_playlist_store.get", return_value=pl_data),
        ):
            await scheduler._reload_sources_after_restart(ctx)

        mock_engine.input_a.load_playlist.assert_called_once()

    @pytest.mark.asyncio
    async def test_fallback_to_slate(self, ctx, mock_engine):
        """Falls back to channel slate when no persisted state."""
        slate_path = Path(ctx.generated_dir) / "slate.mp4"
        slate_path.write_bytes(b"\x00" * 100)

        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch.object(scheduler.config, "FAILOVER_FILENAME", "missing.mp4"),
            patch.object(scheduler.config, "SLATE_FILENAME", "slate.mp4"),
            patch("playout_state.get_layer_state", return_value=None),
        ):
            await scheduler._reload_sources_after_restart(ctx)

        mock_engine.input_a.load_file_loop.assert_called_once_with(str(slate_path))
        mock_engine.show.assert_any_call("input_a")


# ════════════════════════════════════════════════════════════════
# _generate_video_gstreamer
# ════════════════════════════════════════════════════════════════


class TestGenerateVideoGstreamer:
    """Generate test/failover video via GStreamer pipeline."""

    def test_creates_parent_directory(self, tmp_path):
        output = str(tmp_path / "subdir" / "test.mp4")
        # Mock GStreamer so the actual pipeline doesn't run
        mock_pipeline = MagicMock()
        mock_bus = MagicMock()
        mock_msg = MagicMock()
        mock_msg.type = 1 << 13  # Gst.MessageType.EOS value
        mock_bus.timed_pop_filtered.return_value = mock_msg
        mock_pipeline.get_bus.return_value = mock_bus

        with patch("scheduler.Gst", create=True) as mock_gst:
            mock_gst.is_initialized.return_value = True
            mock_gst.parse_launch.return_value = mock_pipeline
            mock_gst.State.PLAYING = 4
            mock_gst.State.NULL = 1
            mock_gst.SECOND = 1_000_000_000
            mock_gst.MessageType.EOS = 1 << 13
            mock_gst.MessageType.ERROR = 1 << 2
            scheduler._generate_video_gstreamer(output, "Test")

        assert (tmp_path / "subdir").is_dir()

    def test_returns_false_on_error(self, tmp_path):
        output = str(tmp_path / "test.mp4")
        mock_pipeline = MagicMock()
        mock_bus = MagicMock()
        mock_msg = MagicMock()
        mock_msg.type = 1 << 2  # Gst.MessageType.ERROR value
        mock_msg.parse_error.return_value = (Exception("test"), "debug")
        mock_bus.timed_pop_filtered.return_value = mock_msg
        mock_pipeline.get_bus.return_value = mock_bus

        mock_gst = MagicMock()
        mock_gst.is_initialized.return_value = True
        mock_gst.parse_launch.return_value = mock_pipeline
        mock_gst.State.PLAYING = 4
        mock_gst.State.NULL = 1
        mock_gst.SECOND = 1_000_000_000
        mock_gst.MessageType.EOS = 1 << 13
        mock_gst.MessageType.ERROR = 1 << 2

        with patch.dict("sys.modules", {"gi.repository": MagicMock(Gst=mock_gst)}):
            result = scheduler._generate_video_gstreamer(output, "Test")

        assert result is False

    def test_returns_false_on_timeout(self, tmp_path):
        output = str(tmp_path / "test.mp4")
        mock_pipeline = MagicMock()
        mock_bus = MagicMock()
        mock_bus.timed_pop_filtered.return_value = None  # timeout
        mock_pipeline.get_bus.return_value = mock_bus

        mock_gst = MagicMock()
        mock_gst.is_initialized.return_value = True
        mock_gst.parse_launch.return_value = mock_pipeline
        mock_gst.State.PLAYING = 4
        mock_gst.State.NULL = 1
        mock_gst.SECOND = 1_000_000_000
        mock_gst.MessageType.EOS = 1 << 13
        mock_gst.MessageType.ERROR = 1 << 2

        with patch.dict("sys.modules", {"gi.repository": MagicMock(Gst=mock_gst)}):
            result = scheduler._generate_video_gstreamer(output, "Test")

        assert result is False


# ════════════════════════════════════════════════════════════════
# ensure_failover_video / ensure_slate_video
# ════════════════════════════════════════════════════════════════


class TestEnsureFailoverVideo:
    """Generate failover video if it doesn't exist."""

    def test_already_exists(self, ctx):
        failover_path = Path(ctx.generated_dir) / "failover.mp4"
        failover_path.write_bytes(b"\x00" * 100)

        with (
            patch.object(scheduler.config, "FAILOVER_FILENAME", "failover.mp4"),
            patch("scheduler._generate_video_gstreamer") as mock_gen,
        ):
            scheduler.ensure_failover_video(ctx)
        mock_gen.assert_not_called()

    def test_generates_when_missing(self, ctx):
        with (
            patch.object(scheduler.config, "FAILOVER_FILENAME", "failover.mp4"),
            patch("scheduler._generate_video_gstreamer", return_value=True) as mock_gen,
        ):
            scheduler.ensure_failover_video(ctx)
        mock_gen.assert_called_once()


class TestEnsureSlateVideo:
    """Generate slate video if it doesn't exist."""

    def test_already_exists(self, ctx):
        slate_path = Path(ctx.generated_dir) / "slate.mp4"
        slate_path.write_bytes(b"\x00" * 100)

        with (
            patch.object(scheduler.config, "SLATE_FILENAME", "slate.mp4"),
            patch("scheduler._generate_video_gstreamer") as mock_gen,
        ):
            scheduler.ensure_slate_video(ctx)
        mock_gen.assert_not_called()

    def test_generates_when_missing(self, ctx):
        with (
            patch.object(scheduler.config, "SLATE_FILENAME", "slate.mp4"),
            patch("scheduler._generate_video_gstreamer", return_value=True) as mock_gen,
        ):
            scheduler.ensure_slate_video(ctx)
        mock_gen.assert_called_once()


# ════════════════════════════════════════════════════════════════
# _handle_no_block
# ════════════════════════════════════════════════════════════════


class TestHandleNoBlock:
    """Handle gap between program blocks."""

    @pytest.mark.asyncio
    async def test_restores_playlist(self, ctx, mock_engine):
        ctx.active_block_key = "old_block"

        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch(
                "scheduler._restore_default_playlist", new_callable=AsyncMock
            ) as mock_restore,
        ):
            await scheduler._handle_no_block(datetime.now(), ctx)

        mock_restore.assert_called_once()
        assert ctx.active_block_key is None

    @pytest.mark.asyncio
    async def test_no_state_no_op(self, ctx, mock_engine):
        """No active block — should not restore defaults."""
        ctx.active_block_key = None
        ctx.active_playlist_block_key = None

        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch(
                "scheduler._restore_default_playlist", new_callable=AsyncMock
            ) as mock_restore,
        ):
            await scheduler._handle_no_block(datetime.now(), ctx)

        # When no block was active, no restore needed
        mock_restore.assert_not_called()


# ════════════════════════════════════════════════════════════════
# _handle_playlist_block
# ════════════════════════════════════════════════════════════════


class TestHandlePlaylistBlock:
    """Handle playlist program blocks."""

    @pytest.mark.asyncio
    async def test_same_block_no_op(self, ctx, mock_engine):
        ctx.active_playlist_block_key = "block_1"

        with patch.object(scheduler.main, "playout", mock_engine):
            await scheduler._handle_playlist_block(
                {"files": ["a.mp4"]}, "block_1", datetime.now(), ctx
            )

        mock_engine.input_a.load_playlist.assert_not_called()

    @pytest.mark.asyncio
    async def test_new_block_loads_playlist(self, ctx, mock_engine):
        ctx.active_playlist_block_key = None
        media_file = Path(ctx.media_dir) / "a.mp4"
        media_file.write_bytes(b"\x00" * 100)

        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch(
                "scheduler._resolve_local_files",
                return_value=([str(media_file)], [10.0]),
            ),
            patch(
                "scheduler._push_and_reset_playlist", new_callable=AsyncMock
            ) as mock_push,
        ):
            await scheduler._handle_playlist_block(
                {"files": ["a.mp4"]}, "new_block", datetime.now(), ctx
            )

        mock_push.assert_called_once()
        assert ctx.active_playlist_block_key == "new_block"

    @pytest.mark.asyncio
    async def test_multilayer_block_targets_correct_layer(self, ctx, mock_engine):
        """Playlist block with layer field loads on that layer."""
        ctx.active_playlist_block_key = None
        ctx.active_layer_blocks = {}

        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch("scheduler._resolve_local_files", return_value=(["/a.mp4"], [10.0])),
            patch(
                "scheduler._push_and_reset_playlist", new_callable=AsyncMock
            ) as mock_push,
        ):
            block = {
                "start": "10:00:00",
                "end": "12:00:00",
                "files": ["a.mp4"],
                "layer": "input_b",
                "loop": False,
            }
            await scheduler._handle_playlist_block(
                block, "10:00:00-12:00:00", datetime.now(), ctx
            )

        mock_push.assert_called_once()
        call_kwargs = mock_push.call_args[1]
        assert call_kwargs["layer"] == "input_b"
        assert call_kwargs["loop"] is False
        assert ctx.active_layer_blocks["input_b"] == "input_b:10:00:00-12:00:00"

    @pytest.mark.asyncio
    async def test_named_playlist_block(self, ctx, mock_engine, tmp_path, monkeypatch):
        """Playlist block with playlist_name loads from named store."""
        import config
        import named_playlist_store

        monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path))
        named_playlist_store.save(
            "test-pl",
            [{"source": "/media/clip.mp4", "duration": 30.0}],
        )

        ctx.active_playlist_block_key = None
        ctx.active_layer_blocks = {}

        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch(
                "scheduler._push_and_reset_playlist", new_callable=AsyncMock
            ) as mock_push,
        ):
            block = {
                "start": "10:00:00",
                "end": "12:00:00",
                "playlist_name": "test-pl",
            }
            await scheduler._handle_playlist_block(
                block, "10:00:00-12:00:00", datetime.now(), ctx
            )

        mock_push.assert_called_once()
        paths = mock_push.call_args[0][0]
        assert paths == ["/media/clip.mp4"]


# ════════════════════════════════════════════════════════════════
# schedule_loop (single iteration)
# ════════════════════════════════════════════════════════════════


class TestScheduleLoop:
    """Schedule loop — pre-generate tomorrow's playlist."""

    @pytest.mark.asyncio
    async def test_loop_mode_skips(self, ctx):
        """In playlist_loop mode, schedule_loop does nothing."""
        ctx.playlist_loop = True
        iteration_count = 0

        async def fake_sleep(s):
            nonlocal iteration_count
            iteration_count += 1
            if iteration_count >= 2:
                raise asyncio.CancelledError()

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch("playlist_store.exists") as mock_exists,
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.schedule_loop(ctx)

        mock_exists.assert_not_called()

    @pytest.mark.asyncio
    async def test_generates_tomorrow(self, ctx):
        """Generates tomorrow's playlist when missing."""
        ctx.playlist_loop = False
        iteration_count = 0

        async def fake_sleep(s):
            nonlocal iteration_count
            iteration_count += 1
            if iteration_count >= 2:
                raise asyncio.CancelledError()

        media_file = Path(ctx.media_dir) / "clip.mp4"
        media_file.write_bytes(b"\x00" * 100)

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch("playlist_store.exists", return_value=False),
            patch("playlist_store.save") as mock_save,
            patch("scheduler.scan_media", return_value=([str(media_file)], [10.0])),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.schedule_loop(ctx)

        mock_save.assert_called()


# ════════════════════════════════════════════════════════════════
# Helper: iteration-limited fake_sleep
# ════════════════════════════════════════════════════════════════


def _make_fake_sleep(max_iterations: int = 2):
    """Return a fake asyncio.sleep that raises CancelledError after N calls."""
    state = {"count": 0}

    async def fake_sleep(seconds):
        state["count"] += 1
        if state["count"] >= max_iterations:
            raise asyncio.CancelledError()

    fake_sleep.state = state
    return fake_sleep


# ════════════════════════════════════════════════════════════════
# watchdog_loop
# ════════════════════════════════════════════════════════════════


class TestWatchdogLoop:
    """Background loop: engine health monitoring."""

    @pytest.mark.asyncio
    async def test_engine_none_skips(self, ctx):
        """When engine is None, loop sleeps without action."""
        fake_sleep = _make_fake_sleep(3)

        with (
            patch.object(scheduler.main, "playout", None),
            patch("scheduler.asyncio.sleep", fake_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.watchdog_loop(ctx)

        # Should have slept: initial delay + at least 1 loop iteration
        assert fake_sleep.state["count"] >= 2

    @pytest.mark.asyncio
    async def test_engine_not_running_triggers_restart(self, ctx, mock_engine):
        """Engine health shows not running -> restart + reload."""
        mock_engine.health = {"running": False}
        fake_sleep = _make_fake_sleep(3)

        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch(
                "scheduler._reload_sources_after_restart",
                new_callable=AsyncMock,
            ) as mock_reload,
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.watchdog_loop(ctx)

        mock_engine.restart.assert_called()
        mock_reload.assert_called()

    @pytest.mark.asyncio
    async def test_engine_restart_failure_logged(self, ctx, mock_engine):
        """Engine restart failure is caught and logged, loop continues."""
        mock_engine.health = {"running": False}
        mock_engine.stop = AsyncMock(side_effect=RuntimeError("stop failed"))
        fake_sleep = _make_fake_sleep(3)

        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch("scheduler.asyncio.sleep", fake_sleep),
        ):
            # Should NOT raise RuntimeError — the loop catches it
            with pytest.raises(asyncio.CancelledError):
                await scheduler.watchdog_loop(ctx)

    @pytest.mark.asyncio
    async def test_normal_heartbeat(self, ctx, mock_engine):
        """Running engine -> normal heartbeat path."""
        mock_engine.health = {
            "running": True,
            "active_channel": "failover",
            "uptime": 60.0,
            "channels": {
                "failover": {
                    "source_type": "file_loop",
                    "now_playing": {"source": "failover.mp4", "played": 30.0},
                },
                "input_b": {"source_type": "disconnect"},
            },
        }
        fake_sleep = _make_fake_sleep(3)

        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch("scheduler.asyncio.sleep", fake_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.watchdog_loop(ctx)

    @pytest.mark.asyncio
    async def test_watchdog_restart_calls_engine_restart(self, ctx, mock_engine):
        """Watchdog restart calls engine.restart() (no RTMP URL needed)."""
        mock_engine.health = {"running": False}
        fake_sleep = _make_fake_sleep(3)

        with (
            patch.object(scheduler.main, "playout", mock_engine),
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch(
                "scheduler._reload_sources_after_restart",
                new_callable=AsyncMock,
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.watchdog_loop(ctx)

        mock_engine.restart.assert_called()


# ════════════════════════════════════════════════════════════════
# hls_watchdog_loop
# ════════════════════════════════════════════════════════════════


class TestHlsWatchdogLoop:
    """Background loop: HLS stale detection state machine."""

    @pytest.mark.asyncio
    async def test_sequence_advances_healthy(self, ctx, mock_engine):
        """Sequence increments each poll -> healthy, no restart."""
        call_count = 0

        def advancing_sequence(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return 100 + call_count

        fake_sleep = _make_fake_sleep(4)

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch("scheduler._read_hls_sequence", side_effect=advancing_sequence),
            patch.object(scheduler.main, "playout", mock_engine),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.hls_watchdog_loop(ctx)

        # Engine should NOT have been restarted
        mock_engine.stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_sequence_stale_below_threshold(self, ctx, mock_engine):
        """Sequence stale but under 25s -> no restart."""
        # Return same sequence every time (stale), but loop exits before threshold
        fake_sleep = _make_fake_sleep(4)

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch("scheduler._read_hls_sequence", return_value=42),
            patch.object(scheduler.main, "playout", mock_engine),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.hls_watchdog_loop(ctx)

        # Loop runs too few iterations to exceed 25s threshold
        mock_engine.stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_sequence_stale_triggers_restart(self, ctx, mock_engine):
        """Sequence stale >= 25s threshold triggers engine restart."""
        from datetime import datetime as real_datetime

        call_count = 0
        base_time = real_datetime.now()

        # Simulate time progression > 25s for the stale check
        original_now = real_datetime.now

        def mock_now():
            # Each call advances time by 10s so after 3 stale checks
            # we hit 30s > 25s threshold
            return base_time + timedelta(seconds=call_count * 10)

        def stale_sequence(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return 42  # Never advances

        fake_sleep = _make_fake_sleep(6)

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch("scheduler._read_hls_sequence", side_effect=stale_sequence),
            patch.object(scheduler.main, "playout", mock_engine),
            patch("scheduler.datetime") as mock_dt,
            patch(
                "scheduler._reload_sources_after_restart",
                new_callable=AsyncMock,
            ) as mock_reload,
        ):
            mock_dt.now = mock_now
            mock_dt.side_effect = lambda *a, **kw: real_datetime(*a, **kw)

            with pytest.raises(asyncio.CancelledError):
                await scheduler.hls_watchdog_loop(ctx)

        mock_engine.restart.assert_called()

    @pytest.mark.asyncio
    async def test_sequence_none_manifest_unavailable(self, ctx, mock_engine):
        """Sequence returns None -> manifest unavailable state."""
        fake_sleep = _make_fake_sleep(4)

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch("scheduler._read_hls_sequence", return_value=None),
            patch.object(scheduler.main, "playout", mock_engine),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.hls_watchdog_loop(ctx)

        # Should not crash; engine not restarted (too short)
        mock_engine.stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_recovery_after_stale(self, ctx, mock_engine):
        """Sequence resumes advancing after being stale -> recovery logged."""
        call_count = 0

        def recovery_sequence(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                return 42  # Stale
            return 42 + call_count  # Advancing again

        fake_sleep = _make_fake_sleep(6)

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch("scheduler._read_hls_sequence", side_effect=recovery_sequence),
            patch.object(scheduler.main, "playout", mock_engine),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.hls_watchdog_loop(ctx)

        # No restart should have been triggered (stale duration too short)
        mock_engine.stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_exception_in_loop_body_caught(self, ctx, mock_engine):
        """Exception inside the loop body is caught and logged."""
        call_count = 0

        def failing_sequence(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("unexpected")
            return 100

        fake_sleep = _make_fake_sleep(4)

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch("scheduler._read_hls_sequence", side_effect=failing_sequence),
            patch.object(scheduler.main, "playout", mock_engine),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.hls_watchdog_loop(ctx)


# ════════════════════════════════════════════════════════════════
# program_scheduler_loop
# ════════════════════════════════════════════════════════════════


class TestProgramSchedulerLoop:
    """Background loop: block dispatch + crash detection."""

    @pytest.mark.asyncio
    async def test_no_program_for_today(self, ctx):
        """No program loaded -> cleans up playlist block state."""
        ctx.active_playlist_block_key = "old_block"
        fake_sleep = _make_fake_sleep(3)

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch("scheduler.program.load_program", return_value=None),
            patch(
                "scheduler._restore_default_playlist",
                new_callable=AsyncMock,
            ) as mock_restore,
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.program_scheduler_loop(ctx)

        mock_restore.assert_called()
        assert ctx.active_playlist_block_key is None

    @pytest.mark.asyncio
    async def test_no_program_no_active_block_no_op(self, ctx):
        """No program and no active block -> no restore call."""
        ctx.active_playlist_block_key = None
        ctx.active_block_key = None
        fake_sleep = _make_fake_sleep(3)

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch("scheduler.program.load_program", return_value=None),
            patch(
                "scheduler._restore_default_playlist",
                new_callable=AsyncMock,
            ) as mock_restore,
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.program_scheduler_loop(ctx)

        mock_restore.assert_not_called()

    @pytest.mark.asyncio
    async def test_playlist_block_with_files(self, ctx):
        """Playlist block with files -> _handle_playlist_block called."""
        fake_sleep = _make_fake_sleep(3)
        playlist_block = {
            "type": "playlist",
            "files": ["a.mp4", "b.mp4"],
            "start": "14:00:00",
            "end": "15:00:00",
        }

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch(
                "scheduler.program.load_program",
                return_value={"blocks": [playlist_block]},
            ),
            patch(
                "scheduler.program.find_active_blocks",
                return_value=[playlist_block],
            ),
            patch("scheduler._deactivate_ended_layers", new_callable=AsyncMock),
            patch(
                "scheduler._handle_playlist_block",
                new_callable=AsyncMock,
            ) as mock_handle,
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.program_scheduler_loop(ctx)

        mock_handle.assert_called()

    @pytest.mark.asyncio
    async def test_gap_no_current_block(self, ctx):
        """No current block (gap) -> _handle_no_block called."""
        fake_sleep = _make_fake_sleep(3)

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch(
                "scheduler.program.load_program",
                return_value={"blocks": [{"type": "playlist"}]},
            ),
            patch("scheduler.program.find_active_blocks", return_value=[]),
            patch("scheduler._deactivate_ended_layers", new_callable=AsyncMock),
            patch(
                "scheduler._handle_no_block",
                new_callable=AsyncMock,
            ) as mock_handle,
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.program_scheduler_loop(ctx)

        mock_handle.assert_called()

    @pytest.mark.asyncio
    async def test_exception_in_loop_body_caught(self, ctx):
        """Exception inside program loop is caught, loop continues."""
        fake_sleep = _make_fake_sleep(3)

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch(
                "scheduler.program.load_program",
                side_effect=RuntimeError("disk error"),
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.program_scheduler_loop(ctx)


# ════════════════════════════════════════════════════════════════
# peer_exchange_loop
# ════════════════════════════════════════════════════════════════


class TestPeerExchangeLoop:
    """Background loop: federation peer gossip."""

    @pytest.mark.asyncio
    async def test_peer_store_none_skips(self):
        """When peer_store is None, loop sleeps and continues."""
        fake_sleep = _make_fake_sleep(3)

        with (
            patch.object(scheduler.main, "peer_store", None),
            patch("scheduler.asyncio.sleep", fake_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.peer_exchange_loop()

    @pytest.mark.asyncio
    async def test_no_hints_skips(self):
        """No hints from known peers -> skips fetch."""
        fake_sleep = _make_fake_sleep(3)
        mock_store = MagicMock()
        mock_store.all.return_value = []

        with (
            patch.object(scheduler.main, "peer_store", mock_store),
            patch("scheduler.asyncio.sleep", fake_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.peer_exchange_loop()

        # evict_stale should still be called
        mock_store.evict_stale.assert_called()

    @pytest.mark.asyncio
    async def test_known_peer_refreshed(self):
        """Remote returns already-known verified peer -> refresh last_seen."""
        from protocol.peers import PeerEntry

        fake_sleep = _make_fake_sleep(3)

        existing_peer = MagicMock()
        existing_peer.hints = ["https://peer1.example.com"]
        existing_peer.verified = True
        existing_peer.name = "Peer One"

        mock_store = MagicMock()
        mock_store.all.return_value = [existing_peer]
        mock_store.get.return_value = existing_peer

        mock_channels = MagicMock()
        mock_channels.all.return_value = []

        remote_peers_response = [
            {
                "id": "TVexistingpeer",
                "name": "Peer One",
                "hints": ["https://peer1.example.com"],
            }
        ]

        with (
            patch.object(scheduler.main, "peer_store", mock_store),
            patch.object(scheduler.main, "channels", mock_channels),
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch(
                "protocol.peers.fetch_remote_peers",
                new_callable=AsyncMock,
                return_value=remote_peers_response,
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.peer_exchange_loop()

        # Should have refreshed the peer (add with updated last_seen)
        mock_store.add.assert_called()

    @pytest.mark.asyncio
    async def test_new_peer_validated_and_added(self):
        """Remote returns new unknown peer -> validate and add."""
        fake_sleep = _make_fake_sleep(3)

        existing_peer = MagicMock()
        existing_peer.hints = ["https://peer1.example.com"]

        mock_store = MagicMock()
        mock_store.all.return_value = [existing_peer]
        mock_store.get.return_value = None  # Not known

        mock_channels = MagicMock()
        mock_channels.all.return_value = []

        remote_peers_response = [
            {
                "id": "TVnewpeer456",
                "name": "New Peer",
                "hints": ["https://new.example.com"],
            }
        ]

        with (
            patch.object(scheduler.main, "peer_store", mock_store),
            patch.object(scheduler.main, "channels", mock_channels),
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch(
                "protocol.peers.fetch_remote_peers",
                new_callable=AsyncMock,
                return_value=remote_peers_response,
            ),
            patch(
                "protocol.peers.validate_peer",
                new_callable=AsyncMock,
                return_value={"name": "New Peer", "id": "TVnewpeer456"},
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.peer_exchange_loop()

        mock_store.add.assert_called()
        # Verify it was added as verified
        added_entry = mock_store.add.call_args[0][0]
        assert added_entry.verified is True
        assert added_entry.id == "TVnewpeer456"

    @pytest.mark.asyncio
    async def test_own_channel_skipped(self):
        """Remote returns our own channel ID -> skipped."""
        fake_sleep = _make_fake_sleep(3)

        existing_peer = MagicMock()
        existing_peer.hints = ["https://peer1.example.com"]

        mock_store = MagicMock()
        mock_store.all.return_value = [existing_peer]

        our_ctx = MagicMock()
        our_ctx.channel_id = "TVourchannel"
        mock_channels = MagicMock()
        mock_channels.all.return_value = [our_ctx]

        remote_peers_response = [
            {"id": "TVourchannel", "name": "Us", "hints": ["https://us.example.com"]}
        ]

        with (
            patch.object(scheduler.main, "peer_store", mock_store),
            patch.object(scheduler.main, "channels", mock_channels),
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch(
                "protocol.peers.fetch_remote_peers",
                new_callable=AsyncMock,
                return_value=remote_peers_response,
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.peer_exchange_loop()

        # Should not add our own channel
        mock_store.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_fetch_error_caught(self):
        """Error fetching from a hint -> caught, loop continues."""
        fake_sleep = _make_fake_sleep(3)

        existing_peer = MagicMock()
        existing_peer.hints = ["https://bad.example.com"]

        mock_store = MagicMock()
        mock_store.all.return_value = [existing_peer]
        mock_store.__len__ = MagicMock(return_value=1)

        mock_channels = MagicMock()
        mock_channels.all.return_value = []

        with (
            patch.object(scheduler.main, "peer_store", mock_store),
            patch.object(scheduler.main, "channels", mock_channels),
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch(
                "protocol.peers.fetch_remote_peers",
                new_callable=AsyncMock,
                side_effect=Exception("connection refused"),
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.peer_exchange_loop()

        # Loop should have continued despite the error

    @pytest.mark.asyncio
    async def test_peer_without_id_skipped(self):
        """Remote peer missing 'id' field is skipped."""
        fake_sleep = _make_fake_sleep(3)

        existing_peer = MagicMock()
        existing_peer.hints = ["https://peer1.example.com"]

        mock_store = MagicMock()
        mock_store.all.return_value = [existing_peer]
        mock_store.__len__ = MagicMock(return_value=0)

        mock_channels = MagicMock()
        mock_channels.all.return_value = []

        remote_peers_response = [
            {"name": "No ID Peer", "hints": ["https://noid.example.com"]}
        ]

        with (
            patch.object(scheduler.main, "peer_store", mock_store),
            patch.object(scheduler.main, "channels", mock_channels),
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch(
                "protocol.peers.fetch_remote_peers",
                new_callable=AsyncMock,
                return_value=remote_peers_response,
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.peer_exchange_loop()

        mock_store.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_validation_failure_skips_peer(self):
        """Validation returns None for all hints -> peer not added."""
        fake_sleep = _make_fake_sleep(3)

        existing_peer = MagicMock()
        existing_peer.hints = ["https://peer1.example.com"]

        mock_store = MagicMock()
        mock_store.all.return_value = [existing_peer]
        mock_store.get.return_value = None
        mock_store.__len__ = MagicMock(return_value=0)

        mock_channels = MagicMock()
        mock_channels.all.return_value = []

        remote_peers_response = [
            {
                "id": "TVbadpeer",
                "name": "Bad Peer",
                "hints": ["https://bad.example.com"],
            }
        ]

        with (
            patch.object(scheduler.main, "peer_store", mock_store),
            patch.object(scheduler.main, "channels", mock_channels),
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch(
                "protocol.peers.fetch_remote_peers",
                new_callable=AsyncMock,
                return_value=remote_peers_response,
            ),
            patch(
                "protocol.peers.validate_peer",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.peer_exchange_loop()

        mock_store.add.assert_not_called()


# ════════════════════════════════════════════════════════════════
# relay_metadata_loop
# ════════════════════════════════════════════════════════════════


class TestRelayMetadataLoop:
    """Background loop: relay metadata + guide refresh."""

    @pytest.mark.asyncio
    async def test_relay_manager_none_skips(self):
        """When relay_manager is None, loop sleeps and continues."""
        fake_sleep = _make_fake_sleep(3)

        with (
            patch.object(scheduler.main, "relay_manager", None),
            patch("scheduler.asyncio.sleep", fake_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.relay_metadata_loop()

    @pytest.mark.asyncio
    async def test_refresh_metadata_called(self):
        """Relay manager present -> refresh_all_metadata called."""
        fake_sleep = _make_fake_sleep(3)
        mock_relay = AsyncMock()

        with (
            patch.object(scheduler.main, "relay_manager", mock_relay),
            patch("scheduler.asyncio.sleep", fake_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.relay_metadata_loop()

        mock_relay.refresh_all_metadata.assert_called()

    @pytest.mark.asyncio
    async def test_guide_refresh_periodic(self):
        """Guide refresh happens every N metadata cycles."""
        # Run enough iterations for guide to trigger
        # guide_counter increments each iteration; guide triggers when
        # guide_counter >= RELAY_GUIDE_INTERVAL // metadata_interval
        fake_sleep = _make_fake_sleep(20)
        mock_relay = AsyncMock()

        with (
            patch.object(scheduler.main, "relay_manager", mock_relay),
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch.object(scheduler.config, "RELAY_GUIDE_INTERVAL", 120),
            patch.object(scheduler.config, "ORIGIN_METADATA_REFRESH_INTERVAL", 60),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.relay_metadata_loop()

        # With interval=60 and guide_interval=120, guide triggers every 2 cycles
        mock_relay.refresh_all_guides.assert_called()

    @pytest.mark.asyncio
    async def test_exception_caught(self):
        """Exception in metadata refresh is caught, loop continues."""
        fake_sleep = _make_fake_sleep(3)
        mock_relay = AsyncMock()
        mock_relay.refresh_all_metadata = AsyncMock(
            side_effect=RuntimeError("network error")
        )

        with (
            patch.object(scheduler.main, "relay_manager", mock_relay),
            patch("scheduler.asyncio.sleep", fake_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.relay_metadata_loop()


# ════════════════════════════════════════════════════════════════
# relay_hls_loop
# ════════════════════════════════════════════════════════════════


class TestRelayHlsLoop:
    """Background loop: relay HLS fetch."""

    @pytest.mark.asyncio
    async def test_relay_manager_none_skips(self):
        """When relay_manager is None, loop sleeps."""
        fake_sleep = _make_fake_sleep(3)

        with (
            patch.object(scheduler.main, "relay_manager", None),
            patch("scheduler.asyncio.sleep", fake_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.relay_hls_loop()

    @pytest.mark.asyncio
    async def test_refresh_hls_called(self):
        """Relay manager present -> refresh_all_hls called."""
        fake_sleep = _make_fake_sleep(3)
        mock_relay = AsyncMock()

        with (
            patch.object(scheduler.main, "relay_manager", mock_relay),
            patch("scheduler.asyncio.sleep", fake_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.relay_hls_loop()

        mock_relay.refresh_all_hls.assert_called()

    @pytest.mark.asyncio
    async def test_exception_caught(self):
        """Exception in HLS refresh is caught, loop continues."""
        fake_sleep = _make_fake_sleep(3)
        mock_relay = AsyncMock()
        mock_relay.refresh_all_hls = AsyncMock(
            side_effect=RuntimeError("connection lost")
        )

        with (
            patch.object(scheduler.main, "relay_manager", mock_relay),
            patch("scheduler.asyncio.sleep", fake_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.relay_hls_loop()


# ════════════════════════════════════════════════════════════════
# mirror_loop
# ════════════════════════════════════════════════════════════════


class TestMirrorLoop:
    """Background loop: mirror state machine."""

    @pytest.mark.asyncio
    async def test_no_mirror_managers_returns(self, ctx):
        """No mirror_managers attribute -> loop exits immediately."""
        fake_sleep = _make_fake_sleep(3)

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch.object(scheduler.main, "mirror_managers", None, create=True),
        ):
            # Should return (not raise CancelledError) because mirror is None
            await scheduler.mirror_loop(ctx)

    @pytest.mark.asyncio
    async def test_no_mirror_for_channel_returns(self, ctx):
        """mirror_managers exists but no entry for this channel."""
        fake_sleep = _make_fake_sleep(3)

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch.object(scheduler.main, "mirror_managers", {}, create=True),
        ):
            await scheduler.mirror_loop(ctx)

    @pytest.mark.asyncio
    async def test_replicating_healthy(self, ctx):
        """REPLICATING + should_promote=False -> just polls."""
        from protocol.mirror import MirrorState

        fake_sleep = _make_fake_sleep(3)
        mock_mirror = MagicMock()
        mock_mirror.state = MirrorState.REPLICATING
        mock_mirror.poll_primary_hls = AsyncMock()
        mock_mirror.should_promote.return_value = False

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch.object(
                scheduler.main,
                "mirror_managers",
                {ctx.channel_id: mock_mirror},
                create=True,
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.mirror_loop(ctx)

        mock_mirror.poll_primary_hls.assert_called()
        mock_mirror.should_promote.assert_called()
        mock_mirror.begin_promotion.assert_not_called()

    @pytest.mark.asyncio
    async def test_replicating_promotes(self, ctx, mock_engine):
        """REPLICATING + should_promote=True -> begins promotion."""
        from protocol.mirror import MirrorState

        fake_sleep = _make_fake_sleep(3)
        mock_mirror = MagicMock()
        mock_mirror.state = MirrorState.REPLICATING
        mock_mirror.poll_primary_hls = AsyncMock()
        mock_mirror.should_promote.return_value = True
        mock_mirror.consecutive_failures = 5

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch.object(
                scheduler.main,
                "mirror_managers",
                {ctx.channel_id: mock_mirror},
                create=True,
            ),
            patch.object(scheduler.main, "playout", mock_engine),
            patch("scheduler.ensure_failover_video"),
            patch("scheduler.scan_media", return_value=([], [])),
            patch.object(scheduler.config, "FAILOVER_FILENAME", "missing.mp4"),
            patch.object(scheduler.config, "STARTUP_DELAY", 0),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.mirror_loop(ctx)

        mock_mirror.begin_promotion.assert_called()

    @pytest.mark.asyncio
    async def test_promoted_polls_local_hls(self, ctx):
        """PROMOTED state -> polls local HLS and checks primary health."""
        from protocol.mirror import MirrorState

        fake_sleep = _make_fake_sleep(8)  # Enough for health check counter
        mock_mirror = MagicMock()
        mock_mirror.state = MirrorState.PROMOTED
        mock_mirror.poll_local_hls = AsyncMock()
        mock_mirror.check_primary_health = AsyncMock(return_value=False)

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch.object(
                scheduler.main,
                "mirror_managers",
                {ctx.channel_id: mock_mirror},
                create=True,
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.mirror_loop(ctx)

        mock_mirror.poll_local_hls.assert_called()
        mock_mirror.check_primary_health.assert_called()
        mock_mirror.mark_primary_unreachable.assert_called()

    @pytest.mark.asyncio
    async def test_promoted_demotes_on_primary_recovery(self, ctx, mock_engine):
        """PROMOTED + primary healthy + demotion delay elapsed -> demotes."""
        from protocol.mirror import MirrorState

        fake_sleep = _make_fake_sleep(8)
        mock_mirror = MagicMock()
        mock_mirror.state = MirrorState.PROMOTED
        mock_mirror.poll_local_hls = AsyncMock()
        mock_mirror.check_primary_health = AsyncMock(return_value=True)
        mock_mirror.should_demote.return_value = True

        # Create real futures that raise CancelledError when awaited after cancel
        loop = asyncio.get_event_loop()
        fut1 = loop.create_future()
        fut1.cancel()
        fut2 = loop.create_future()
        fut2.cancel()
        ctx.watchdog_task = fut1
        ctx.hls_watchdog_task = fut2

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch.object(
                scheduler.main,
                "mirror_managers",
                {ctx.channel_id: mock_mirror},
                create=True,
            ),
            patch.object(scheduler.main, "playout", mock_engine),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.mirror_loop(ctx)

        mock_mirror.begin_demotion.assert_called()
        mock_mirror.complete_demotion.assert_called()

    @pytest.mark.asyncio
    async def test_demoting_state_completes(self, ctx):
        """DEMOTING state -> complete_demotion called."""
        from protocol.mirror import MirrorState

        fake_sleep = _make_fake_sleep(3)
        mock_mirror = MagicMock()
        mock_mirror.state = MirrorState.DEMOTING

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch.object(
                scheduler.main,
                "mirror_managers",
                {ctx.channel_id: mock_mirror},
                create=True,
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.mirror_loop(ctx)

        mock_mirror.complete_demotion.assert_called()

    @pytest.mark.asyncio
    async def test_exception_in_loop_caught(self, ctx):
        """Exception in mirror loop body is caught."""
        from protocol.mirror import MirrorState

        fake_sleep = _make_fake_sleep(3)
        mock_mirror = MagicMock()
        mock_mirror.state = MirrorState.REPLICATING
        mock_mirror.poll_primary_hls = AsyncMock(
            side_effect=RuntimeError("network error")
        )

        with (
            patch("scheduler.asyncio.sleep", fake_sleep),
            patch.object(
                scheduler.main,
                "mirror_managers",
                {ctx.channel_id: mock_mirror},
                create=True,
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await scheduler.mirror_loop(ctx)
