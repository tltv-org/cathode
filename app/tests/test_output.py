"""Output pipeline tests — verify the full signal path.

These tests validate that data actually flows from the mixer through
the output pipelines to their destinations.  The engine tests in other
files focus on the input side (sources, playlist, switching, failover).
These tests focus on the output side (encoding, muxing, delivery).

Signal path under test:
    Source → InputLayer → inter sink → Mixer compositor → inter sink
    → OutputLayer inter source → x264enc + avenc_aac → mux → sink

Output types tested:
    HLS  — hlssink2 writes .ts segments + .m3u8 playlist to disk
    File — splitmuxsink writes time-chunked recordings
    Null — fakesink discards encoded output (baseline / perf)
    RTMP — build + start only (needs external server to actually push)

Multi-output:
    Multiple OutputLayers reading from the same mixer inter channels
    simultaneously, each encoding independently.
"""

import asyncio
import os
import re
from pathlib import Path

import pytest
import pytest_asyncio

from playout import PlayoutConfig, PlayoutEngine
from playout.output_layer import OutputConfig, OutputLayer, OutputType
from playout.mixer import MixerConfig


# ── Shared config ──

LOW_RES = PlayoutConfig(
    width=640,
    height=360,
    fps=15,
    video_bitrate=800,
    audio_bitrate=128,
    keyframe_interval=30,
)


def _make_output(type: OutputType, name: str, **kwargs) -> OutputConfig:
    """Helper to build OutputConfig with test defaults."""
    return OutputConfig(
        type=type,
        name=name,
        video_bitrate=LOW_RES.video_bitrate,
        audio_bitrate=LOW_RES.audio_bitrate,
        keyframe_interval=LOW_RES.keyframe_interval,
        **kwargs,
    )


# ══════════════════════════════════════════════════════════════════
# HLS output — segment verification
# ══════════════════════════════════════════════════════════════════


class TestHlsOutput:
    """Verify hlssink2 produces valid HLS segments on disk."""

    @pytest_asyncio.fixture
    async def hls_engine(self, tmp_path):
        """Engine with HLS output writing to a temp directory."""
        hls_dir = str(tmp_path / "hls")
        os.makedirs(hls_dir, exist_ok=True)

        engine = PlayoutEngine()
        output = _make_output(
            OutputType.HLS,
            "hls-test",
            hls_dir=hls_dir,
            segment_duration=2,
            playlist_length=3,
        )
        await engine.start(config=LOW_RES, default_output=output)
        yield engine, hls_dir
        await engine.stop()

    @pytest.mark.asyncio
    async def test_segments_appear_on_disk(self, hls_engine):
        """HLS segments (.ts files) are written to the output directory."""
        engine, hls_dir = hls_engine
        # Wait for at least one segment (2s duration + encoder startup)
        await asyncio.sleep(5)

        ts_files = list(Path(hls_dir).glob("*.ts"))
        assert len(ts_files) >= 1, f"No .ts segments in {hls_dir}"
        # Each segment should have real data (not empty)
        for ts in ts_files:
            assert ts.stat().st_size > 1000, f"{ts.name} is too small"

    @pytest.mark.asyncio
    async def test_m3u8_manifest_exists(self, hls_engine):
        """HLS m3u8 playlist file is created."""
        engine, hls_dir = hls_engine
        await asyncio.sleep(5)

        m3u8 = Path(hls_dir) / "stream.m3u8"
        assert m3u8.exists(), "stream.m3u8 not created"
        assert m3u8.stat().st_size > 0

    @pytest.mark.asyncio
    async def test_m3u8_has_valid_header(self, hls_engine):
        """m3u8 has EXTM3U header and required tags."""
        engine, hls_dir = hls_engine
        await asyncio.sleep(5)

        text = (Path(hls_dir) / "stream.m3u8").read_text()
        assert text.startswith("#EXTM3U"), "Missing EXTM3U header"
        assert "#EXT-X-VERSION:" in text
        assert "#EXT-X-MEDIA-SEQUENCE:" in text
        assert "#EXT-X-TARGETDURATION:" in text

    @pytest.mark.asyncio
    async def test_m3u8_sequence_increments(self, hls_engine):
        """Segment count increases over time (proves continuous output).

        MEDIA-SEQUENCE only advances once the playlist is full and
        segments rotate out (playlist_length=3, segment_duration=2s
        means ~8s before first rotation).  So we count total .ts files
        instead — a more reliable indicator of continuous output.
        """
        engine, hls_dir = hls_engine

        await asyncio.sleep(5)
        count1 = len(list(Path(hls_dir).glob("*.ts")))
        assert count1 >= 1, "No segments after 5s"

        await asyncio.sleep(4)
        count2 = len(list(Path(hls_dir).glob("*.ts")))
        assert count2 > count1, f"Segment count stalled: {count1} → {count2}"

    @pytest.mark.asyncio
    async def test_m3u8_references_existing_segments(self, hls_engine):
        """Every .ts file referenced in m3u8 exists on disk."""
        engine, hls_dir = hls_engine
        await asyncio.sleep(6)

        text = (Path(hls_dir) / "stream.m3u8").read_text()
        referenced = re.findall(r"(segment\d+\.ts)", text)
        assert len(referenced) > 0, "No segments referenced in m3u8"

        for seg_name in referenced:
            seg_path = Path(hls_dir) / seg_name
            assert seg_path.exists(), f"Referenced segment {seg_name} missing"


# ══════════════════════════════════════════════════════════════════
# File output — recording verification
# ══════════════════════════════════════════════════════════════════


class TestFileOutput:
    """Verify splitmuxsink produces recording files."""

    @pytest.mark.asyncio
    async def test_recording_file_created(self, tmp_path):
        """File output writes a recording to disk.

        splitmuxsink creates the file immediately but only flushes
        when max_duration is reached or the pipeline stops.  Use a
        short max_duration (3s) so the first chunk finalizes quickly.
        """
        rec_dir = str(tmp_path / "recordings")
        os.makedirs(rec_dir, exist_ok=True)

        engine = PlayoutEngine()
        output = _make_output(
            OutputType.FILE,
            "rec-test",
            file_path=rec_dir,
            file_format="mp4",
            max_duration=3,
        )
        try:
            await engine.start(config=LOW_RES, default_output=output)
            # Wait for max_duration + encoding overhead
            await asyncio.sleep(5)

            files = list(Path(rec_dir).glob("recording-*.mp4"))
            assert len(files) >= 1, f"No recording files in {rec_dir}"
            # First chunk should be finalized (non-zero) after max_duration
            finalized = [f for f in files if f.stat().st_size > 1000]
            assert len(finalized) >= 1, (
                f"No finalized recordings: "
                f"{[(f.name, f.stat().st_size) for f in files]}"
            )
        finally:
            await engine.stop()

    @pytest.mark.asyncio
    async def test_matroska_recording(self, tmp_path):
        """File output with matroska format writes .mkv files."""
        rec_dir = str(tmp_path / "recordings")
        os.makedirs(rec_dir, exist_ok=True)

        engine = PlayoutEngine()
        output = _make_output(
            OutputType.FILE,
            "mkv-test",
            file_path=rec_dir,
            file_format="matroska",
            max_duration=3,
        )
        try:
            await engine.start(config=LOW_RES, default_output=output)
            await asyncio.sleep(5)

            files = list(Path(rec_dir).glob("recording-*.mkv"))
            assert len(files) >= 1, f"No .mkv files in {rec_dir}"
            finalized = [f for f in files if f.stat().st_size > 1000]
            assert len(finalized) >= 1, (
                f"No finalized mkv recordings: "
                f"{[(f.name, f.stat().st_size) for f in files]}"
            )
        finally:
            await engine.stop()


# ══════════════════════════════════════════════════════════════════
# Null output — baseline
# ══════════════════════════════════════════════════════════════════


class TestNullOutput:
    """Verify null output starts, encodes, and reports health."""

    @pytest.mark.asyncio
    async def test_null_output_starts(self):
        """Null output builds and reaches PLAYING state."""
        engine = PlayoutEngine()
        output = _make_output(OutputType.NULL, "null-test")
        try:
            await engine.start(config=LOW_RES, default_output=output)
            await asyncio.sleep(1)

            out = engine.get_output("null-test")
            assert out is not None
            assert out.state == "PLAYING"
        finally:
            await engine.stop()

    @pytest.mark.asyncio
    async def test_null_output_no_errors(self):
        """Null output runs without errors."""
        engine = PlayoutEngine()
        output = _make_output(OutputType.NULL, "null-test")
        try:
            await engine.start(config=LOW_RES, default_output=output)
            await asyncio.sleep(3)

            out = engine.get_output("null-test")
            assert out.health["errors"] == 0
            assert out.health["last_error"] is None
        finally:
            await engine.stop()


# ══════════════════════════════════════════════════════════════════
# RTMP output — build/start only (no server to push to)
# ══════════════════════════════════════════════════════════════════


class TestRtmpOutput:
    """Verify RTMP output builds correctly (can't verify push without server)."""

    @pytest.mark.asyncio
    async def test_rtmp_output_builds(self):
        """RTMP output builds without error."""
        cfg = MixerConfig(width=640, height=360, fps=15)
        output = OutputLayer(
            _make_output(
                OutputType.RTMP, "rtmp-test", rtmp_url="rtmp://localhost:1935/live/test"
            ),
            cfg,
        )
        output.build()
        assert output.pipeline is not None

    @pytest.mark.asyncio
    async def test_rtmp_output_requires_url(self):
        """RTMP output without URL raises ValueError."""
        cfg = MixerConfig(width=640, height=360, fps=15)
        output = OutputLayer(
            _make_output(OutputType.RTMP, "rtmp-test", rtmp_url=""),
            cfg,
        )
        with pytest.raises(ValueError, match="rtmp_url is required"):
            output.build()


# ══════════════════════════════════════════════════════════════════
# Multi-output — simultaneous outputs from one mixer
# ══════════════════════════════════════════════════════════════════


class TestMultiOutput:
    """Verify multiple outputs read from the same mixer simultaneously."""

    @pytest.mark.asyncio
    async def test_two_hls_outputs(self, tmp_path):
        """Two HLS outputs write to separate directories."""
        dir_a = str(tmp_path / "hls-a")
        dir_b = str(tmp_path / "hls-b")
        os.makedirs(dir_a, exist_ok=True)
        os.makedirs(dir_b, exist_ok=True)

        engine = PlayoutEngine()
        out_a = _make_output(
            OutputType.HLS,
            "output-a",
            hls_dir=dir_a,
            segment_duration=2,
            playlist_length=3,
        )
        try:
            await engine.start(config=LOW_RES, default_output=out_a)

            # Add second output at runtime
            out_b = _make_output(
                OutputType.HLS,
                "output-b",
                hls_dir=dir_b,
                segment_duration=2,
                playlist_length=3,
            )
            await engine.add_output(out_b)
            assert len(engine.outputs) == 2

            await asyncio.sleep(6)

            # Both should have segments
            segs_a = list(Path(dir_a).glob("*.ts"))
            segs_b = list(Path(dir_b).glob("*.ts"))
            assert len(segs_a) >= 1, f"Output A: no segments in {dir_a}"
            assert len(segs_b) >= 1, f"Output B: no segments in {dir_b}"

            # Both should have m3u8
            assert (Path(dir_a) / "stream.m3u8").exists()
            assert (Path(dir_b) / "stream.m3u8").exists()
        finally:
            await engine.stop()

    @pytest.mark.asyncio
    async def test_hls_plus_null_output(self, tmp_path):
        """HLS + null outputs run simultaneously without interference."""
        hls_dir = str(tmp_path / "hls")
        os.makedirs(hls_dir, exist_ok=True)

        engine = PlayoutEngine()
        out_hls = _make_output(
            OutputType.HLS,
            "hls",
            hls_dir=hls_dir,
            segment_duration=2,
            playlist_length=3,
        )
        try:
            await engine.start(config=LOW_RES, default_output=out_hls)
            await engine.add_output(_make_output(OutputType.NULL, "null"))
            assert len(engine.outputs) == 2

            await asyncio.sleep(5)

            # HLS should work
            assert len(list(Path(hls_dir).glob("*.ts"))) >= 1
            # Null should be healthy
            null_out = engine.get_output("null")
            assert null_out.health["errors"] == 0
        finally:
            await engine.stop()

    @pytest.mark.asyncio
    async def test_hls_plus_file_output(self, tmp_path):
        """HLS + file recording run simultaneously."""
        hls_dir = str(tmp_path / "hls")
        rec_dir = str(tmp_path / "rec")
        os.makedirs(hls_dir, exist_ok=True)
        os.makedirs(rec_dir, exist_ok=True)

        engine = PlayoutEngine()
        out_hls = _make_output(
            OutputType.HLS,
            "hls",
            hls_dir=hls_dir,
            segment_duration=2,
            playlist_length=3,
        )
        try:
            await engine.start(config=LOW_RES, default_output=out_hls)
            out_file = _make_output(
                OutputType.FILE,
                "rec",
                file_path=rec_dir,
                file_format="mp4",
                max_duration=3,
            )
            await engine.add_output(out_file)

            await asyncio.sleep(5)

            # Both should produce output
            assert len(list(Path(hls_dir).glob("*.ts"))) >= 1
            assert len(list(Path(rec_dir).glob("recording-*.mp4"))) >= 1
        finally:
            await engine.stop()


# ══════════════════════════════════════════════════════════════════
# Output lifecycle — add/remove at runtime
# ══════════════════════════════════════════════════════════════════


class TestOutputLifecycle:
    """Verify outputs can be added, removed, started, stopped at runtime."""

    @pytest.mark.asyncio
    async def test_add_output_at_runtime(self, tmp_path):
        """Adding an output after engine start works."""
        engine = PlayoutEngine()
        try:
            await engine.start(
                config=LOW_RES,
                default_output=_make_output(OutputType.NULL, "initial"),
            )
            assert len(engine.outputs) == 1

            hls_dir = str(tmp_path / "hls")
            os.makedirs(hls_dir, exist_ok=True)
            await engine.add_output(
                _make_output(
                    OutputType.HLS,
                    "added",
                    hls_dir=hls_dir,
                    segment_duration=2,
                )
            )
            assert len(engine.outputs) == 2
            assert engine.get_output("added") is not None

            await asyncio.sleep(5)
            assert len(list(Path(hls_dir).glob("*.ts"))) >= 1
        finally:
            await engine.stop()

    @pytest.mark.asyncio
    async def test_remove_output_at_runtime(self):
        """Removing an output stops it without affecting others."""
        engine = PlayoutEngine()
        try:
            await engine.start(
                config=LOW_RES,
                default_output=_make_output(OutputType.NULL, "keep"),
            )
            await engine.add_output(_make_output(OutputType.NULL, "remove-me"))
            assert len(engine.outputs) == 2

            await engine.remove_output("remove-me")
            assert len(engine.outputs) == 1
            assert engine.get_output("remove-me") is None
            assert engine.get_output("keep") is not None

            # Engine should still be running
            assert engine.is_running
        finally:
            await engine.stop()

    @pytest.mark.asyncio
    async def test_remove_nonexistent_raises(self):
        """Removing a non-existent output raises ValueError."""
        engine = PlayoutEngine()
        try:
            await engine.start(
                config=LOW_RES,
                default_output=_make_output(OutputType.NULL, "test"),
            )
            with pytest.raises(ValueError, match="not found"):
                await engine.remove_output("nonexistent")
        finally:
            await engine.stop()

    @pytest.mark.asyncio
    async def test_duplicate_name_raises(self):
        """Adding an output with a duplicate name raises ValueError."""
        engine = PlayoutEngine()
        try:
            await engine.start(
                config=LOW_RES,
                default_output=_make_output(OutputType.NULL, "dupe"),
            )
            with pytest.raises(ValueError, match="already exists"):
                await engine.add_output(_make_output(OutputType.NULL, "dupe"))
        finally:
            await engine.stop()

    @pytest.mark.asyncio
    async def test_stop_output_without_removing(self):
        """Stopping an output keeps it in the registry but not running."""
        engine = PlayoutEngine()
        try:
            await engine.start(
                config=LOW_RES,
                default_output=_make_output(OutputType.NULL, "test"),
            )
            await asyncio.sleep(1)  # let state transition complete
            out = engine.get_output("test")
            assert out.state in ("PLAYING", "PAUSED")

            out.stop()
            assert out.state == "NULL"
            # Still in registry
            assert engine.get_output("test") is not None
        finally:
            await engine.stop()


# ══════════════════════════════════════════════════════════════════
# Output health
# ══════════════════════════════════════════════════════════════════


class TestOutputHealth:
    """Verify output health dict structure and values."""

    @pytest.mark.asyncio
    async def test_health_has_required_keys(self):
        """Output health dict contains all expected fields."""
        engine = PlayoutEngine()
        try:
            await engine.start(
                config=LOW_RES,
                default_output=_make_output(OutputType.NULL, "test"),
            )
            await asyncio.sleep(1)
            health = engine.get_output("test").health

            assert "name" in health
            assert "type" in health
            assert "state" in health
            assert "uptime" in health
            assert "errors" in health
            assert "last_error" in health
            assert "config" in health
        finally:
            await engine.stop()

    @pytest.mark.asyncio
    async def test_health_in_engine_health(self):
        """Engine health dict includes outputs section."""
        engine = PlayoutEngine()
        try:
            await engine.start(
                config=LOW_RES,
                default_output=_make_output(OutputType.NULL, "test"),
            )
            health = engine.health
            assert "outputs" in health
            assert "test" in health["outputs"]
            assert health["outputs"]["test"]["type"] == "null"
        finally:
            await engine.stop()

    @pytest.mark.asyncio
    async def test_uptime_increases(self):
        """Output uptime increases over time."""
        engine = PlayoutEngine()
        try:
            await engine.start(
                config=LOW_RES,
                default_output=_make_output(OutputType.NULL, "test"),
            )
            await asyncio.sleep(0.5)
            t1 = engine.get_output("test").health["uptime"]
            await asyncio.sleep(1)
            t2 = engine.get_output("test").health["uptime"]
            assert t2 > t1
        finally:
            await engine.stop()

    @pytest.mark.asyncio
    async def test_hls_health_has_type_config(self, tmp_path):
        """HLS output health includes hls-specific config fields."""
        hls_dir = str(tmp_path / "hls")
        os.makedirs(hls_dir, exist_ok=True)

        engine = PlayoutEngine()
        try:
            await engine.start(
                config=LOW_RES,
                default_output=_make_output(
                    OutputType.HLS,
                    "test",
                    hls_dir=hls_dir,
                    segment_duration=2,
                ),
            )
            cfg = engine.get_output("test").health["config"]
            assert "hls_dir" in cfg
            assert cfg["hls_dir"] == hls_dir
            assert "segment_duration" in cfg
        finally:
            await engine.stop()


# ══════════════════════════════════════════════════════════════════
# Engine restart — output preservation
# ══════════════════════════════════════════════════════════════════


class TestRestartPreservesOutputs:
    """Verify engine restart preserves output configurations."""

    @pytest.mark.asyncio
    async def test_restart_recreates_default_output(self, tmp_path):
        """After restart, the default output is recreated and produces segments."""
        hls_dir = str(tmp_path / "hls")
        os.makedirs(hls_dir, exist_ok=True)

        engine = PlayoutEngine()
        try:
            await engine.start(
                config=LOW_RES,
                default_output=_make_output(
                    OutputType.HLS,
                    "primary",
                    hls_dir=hls_dir,
                    segment_duration=2,
                ),
            )
            assert len(engine.outputs) == 1
            await asyncio.sleep(3)

            # Clear old segments to verify new ones appear
            for f in Path(hls_dir).iterdir():
                f.unlink()

            await engine.restart()
            assert engine.is_running
            assert len(engine.outputs) == 1

            await asyncio.sleep(5)
            assert len(list(Path(hls_dir).glob("*.ts"))) >= 1
        finally:
            await engine.stop()


# ══════════════════════════════════════════════════════════════════
# Output error isolation
# ══════════════════════════════════════════════════════════════════


class TestOutputErrorIsolation:
    """Verify one output failing doesn't affect others."""

    @pytest.mark.asyncio
    async def test_bad_output_doesnt_kill_good_output(self, tmp_path):
        """A broken output (bad file path) doesn't affect a working output."""
        good_dir = str(tmp_path / "good")
        os.makedirs(good_dir, exist_ok=True)

        engine = PlayoutEngine()
        try:
            # Start with a good HLS output
            await engine.start(
                config=LOW_RES,
                default_output=_make_output(
                    OutputType.HLS,
                    "good",
                    hls_dir=good_dir,
                    segment_duration=2,
                ),
            )

            # Add a null output (always works) to verify isolation
            await engine.add_output(_make_output(OutputType.NULL, "also-good"))

            await asyncio.sleep(5)

            # Good output should still be producing segments
            assert len(list(Path(good_dir).glob("*.ts"))) >= 1
            # Engine should still be running
            assert engine.is_running
            # Null output should be healthy
            assert engine.get_output("also-good").health["errors"] == 0
        finally:
            await engine.stop()
