"""InputLayer — compositor layer with isolated source pipeline.

Each InputLayer owns an independent GStreamer pipeline that feeds
decoded A/V to the mixer via intervideosink/interaudiosink.  The inter
elements use named shared-memory channels to bridge between pipelines.

Architecture:
    Source pipeline: [source] → decode → normalize → intervideosink/interaudiosink
    Mixer pipeline:  intervideosrc/interaudiosrc → compositor/audiomixer (in mixer.py)

Source types:
    playlist  — playbin3 gapless sequencer (local files)
    file_loop — single file gapless loop (via playbin3)
    srt       — SRT listener (accepts push from OBS etc.)
    tcp       — TCP/Matroska receiver (renderers)
    hls       — pull from remote HLS stream
    test      — videotestsrc + audiotestsrc (fallback)

Only one source is active at a time.  Switching sources tears down
the source pipeline (set to NULL — safe and atomic since it's
independent from the mixer) and rebuilds with the new source.  This
eliminates the hot-swap deadlock present in the single-pipeline
architecture where setting interconnected elements to NULL while
the pipeline is PLAYING blocks streaming threads.

Playlist/file loop architecture:
    playbin3 wraps uridecodebin3 + playsink.  playsink provides the
    clock feedback loop that makes about-to-finish fire at the correct
    wall-clock time, solving the timing problem where raw uridecodebin3
    fires about-to-finish based on source I/O exhaustion rather than
    playback position.  Custom video-sink and audio-sink bins route
    decoded output through normalize chains to inter sinks.

    When a clip is about to finish, playbin3 emits about-to-finish,
    and we set the next URI.  The internal uridecodebin3 pre-rolls
    the next file, reuses decoders, and seamlessly switches streams.
    Zero gap, zero element accumulation, zero memory growth.

    playbin3 IS a GstPipeline subclass — it owns its own pipeline.
    In playlist/file_loop mode, self._pipeline is unused and
    self._playbin3 holds the active pipeline.  For all other source
    types, self._pipeline is used with its permanent inter sinks.
"""

from __future__ import annotations

import logging
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import gi

gi.require_version("Gst", "1.0")
from gi.repository import Gst  # noqa: E402

logger = logging.getLogger(__name__)


class SourceType(str, Enum):
    NONE = "none"
    TEST = "test"
    FILE_LOOP = "file_loop"
    PLAYLIST = "playlist"
    SRT = "srt"
    TCP = "tcp"
    HLS = "hls"
    IMAGE = "image"
    PLUGIN = "plugin"  # plugin-provided source via SourceFactory


@dataclass
class PlaylistEntry:
    """A single clip in a playlist."""

    source: str  # absolute local file path
    duration: float  # seconds


def _make(factory: str, name: str) -> Gst.Element:
    elem = Gst.ElementFactory.make(factory, name)
    if elem is None:
        raise RuntimeError(f"GStreamer element '{factory}' not found")
    return elem


def _make_video_norm(name: str, config) -> Gst.Bin:
    """Normalize video to target format."""
    vbin = Gst.Bin.new(name)
    convert = _make("videoconvert", f"{name}_conv")
    scale = _make("videoscale", f"{name}_scale")
    rate = _make("videorate", f"{name}_rate")
    caps = _make("capsfilter", f"{name}_caps")
    caps.set_property(
        "caps",
        Gst.Caps.from_string(
            f"video/x-raw,width={config.width},height={config.height},"
            f"framerate={config.fps}/1,format=I420"
        ),
    )
    for e in (convert, scale, rate, caps):
        vbin.add(e)
    convert.link(scale)
    scale.link(rate)
    rate.link(caps)
    vbin.add_pad(Gst.GhostPad.new("sink", convert.get_static_pad("sink")))
    vbin.add_pad(Gst.GhostPad.new("src", caps.get_static_pad("src")))
    return vbin


def _make_audio_norm(name: str, config) -> Gst.Bin:
    """Normalize audio to target format."""
    abin = Gst.Bin.new(name)
    convert = _make("audioconvert", f"{name}_conv")
    resample = _make("audioresample", f"{name}_res")
    caps = _make("capsfilter", f"{name}_caps")
    caps.set_property(
        "caps",
        Gst.Caps.from_string(
            f"audio/x-raw,rate={config.audio_samplerate},"
            f"channels={config.audio_channels},format=F32LE,layout=interleaved"
        ),
    )
    for e in (convert, resample, caps):
        abin.add(e)
    convert.link(resample)
    resample.link(caps)
    abin.add_pad(Gst.GhostPad.new("sink", convert.get_static_pad("sink")))
    abin.add_pad(Gst.GhostPad.new("src", caps.get_static_pad("src")))
    return abin


class InputLayer:
    """One compositor layer with its own isolated source pipeline.

    The source pipeline feeds decoded A/V to the mixer via
    intervideosink/interaudiosink.  The inter elements are permanent
    residents of the source pipeline.  Source chains are built and
    torn down as needed — teardown is safe because the source
    pipeline is independent from the mixer.
    """

    _LAYER_NAMES = {0: "failover", 1: "input_a", 2: "input_b", 3: "blinder"}

    def __init__(self, channel_index: int, config) -> None:
        self.index = channel_index
        self.name = self._LAYER_NAMES.get(channel_index, f"layer{channel_index}")
        self._config = config

        # ── Own pipeline with permanent inter sinks ──
        self._pipeline = Gst.Pipeline.new(f"src-ch{channel_index}")

        self._v_sink = _make("intervideosink", f"ch{channel_index}_ivsink")
        self._v_sink.set_property("channel", f"ch{channel_index}-video")
        self._a_sink = _make("interaudiosink", f"ch{channel_index}_iasink")
        self._a_sink.set_property("channel", f"ch{channel_index}-audio")

        self._pipeline.add(self._v_sink)
        self._pipeline.add(self._a_sink)

        # Current source state
        self.source_type: SourceType = SourceType.NONE
        self._source_elements: list[Gst.Element] = []

        # Playlist / file loop state (unified via playbin3 gapless)
        self._playlist_entries: list[PlaylistEntry] = []
        self._playlist_loop: bool = False
        self._playlist_name: str | None = None  # named playlist reference
        self._playlist_index: int = 0  # transport index — what about-to-finish sets
        self._display_index: int = (
            0  # what's actually rendering (updated on STREAM_START)
        )
        self._udb3: Gst.Element | None = None  # uridecodebin3 reference (legacy)
        self._playbin3: Gst.Element | None = None  # playbin3 pipeline for gapless

        # Data-flow watchdog (for live sources)
        self._last_buffer_time: float = 0.0

        # Position tracking
        self._source_start_time: float = 0.0
        self._clip_start_time: float = 0.0

        # Bus thread for this source pipeline
        self._bus_thread: threading.Thread | None = None
        self._bus_running = False

        # Asyncio loop reference — set by _start_source().
        # Used for call_soon_threadsafe from GStreamer streaming threads
        # (clip EOS, playlist re-queue, error callbacks).
        self._loop = None

        # Error callback: (element_name, error_msg) -> None
        # Set by PlayoutEngine to wire per-channel errors to
        # the engine's failover handler.
        self.on_error: Callable | None = None

    # ── Source management ──

    def _teardown_source(self) -> None:
        """Tear down the current source — safe and atomic.

        Handles two paths:
        1. playbin3 mode: playbin3 IS the pipeline, tear it down entirely.
        2. Normal mode: self._pipeline has source elements + permanent
           inter sinks.  Source elements are removed, inter sinks stay.

        Both paths are safe and atomic.  READY first to cancel any
        in-progress preroll from about-to-finish, then NULL.
        """
        # Stop bus thread
        self._bus_running = False
        if self._bus_thread:
            self._bus_thread.join(timeout=5)
            self._bus_thread = None

        if self._playbin3 is not None:
            # ── playbin3 path ──
            # playbin3 owns its pipeline.  Transition through READY to
            # cancel any mid-preroll from about-to-finish, then NULL.
            self._playbin3.set_state(Gst.State.READY)
            self._playbin3.get_state(3 * Gst.SECOND)
            self._playbin3.set_state(Gst.State.NULL)
            self._playbin3.get_state(3 * Gst.SECOND)
            self._playbin3 = None
            self._source_elements.clear()  # safety — should already be empty
            logger.debug("%s: playbin3 torn down", self.name)
        else:
            # ── Normal path (non-playbin3 sources) ──
            self._pipeline.set_state(Gst.State.READY)
            self._pipeline.get_state(3 * Gst.SECOND)
            self._pipeline.set_state(Gst.State.NULL)
            self._pipeline.get_state(3 * Gst.SECOND)

            # Remove source elements (inter sinks stay).
            for elem in reversed(list(self._source_elements)):
                self._pipeline.remove(elem)
            self._source_elements.clear()

        self._udb3 = None
        self._playlist_name = None
        self._last_buffer_time = 0.0
        self._source_start_time = 0.0
        self._clip_start_time = 0.0
        self._display_index = 0
        self.source_type = SourceType.NONE
        logger.debug("%s: source torn down", self.name)

    def _link_to_output(self, v_src_pad: Gst.Pad, a_src_pad: Gst.Pad) -> None:
        """Link a source chain's output pads to the inter sinks."""
        v_sink_pad = self._v_sink.get_static_pad("sink")
        a_sink_pad = self._a_sink.get_static_pad("sink")
        ret_v = v_src_pad.link(v_sink_pad)
        ret_a = a_src_pad.link(a_sink_pad)
        if ret_v != Gst.PadLinkReturn.OK:
            logger.error("%s: video→inter link failed: %s", self.name, ret_v)
        if ret_a != Gst.PadLinkReturn.OK:
            logger.error("%s: audio→inter link failed: %s", self.name, ret_a)

    def _start_source(self) -> None:
        """Start the source pipeline and bus thread."""
        if self._loop is None:
            try:
                import asyncio

                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                pass

        # Set _bus_running before state change — callbacks on GStreamer
        # streaming threads check this flag.
        self._bus_running = True

        ret = self._pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            self._bus_running = False
            logger.error("%s: failed to start source pipeline", self.name)
            # Notify the engine so failover can activate.  Without this,
            # a channel stays visible with no data flowing and no error
            # callback — the source just silently produces nothing.
            if self.on_error:
                if self._loop and self._loop.is_running():
                    self._loop.call_soon_threadsafe(
                        self.on_error, self.name, "Source pipeline failed to start"
                    )
                else:
                    # No asyncio loop — call directly (startup path)
                    try:
                        self.on_error(self.name, "Source pipeline failed to start")
                    except Exception:
                        logger.exception("%s: on_error callback failed", self.name)
            return

        # Start bus thread for this source pipeline
        self._bus_thread = threading.Thread(
            target=self._bus_poll_loop,
            name=f"gst-bus-ch{self.index}",
            daemon=True,
        )
        self._bus_thread.start()

    def _start_playbin3(self) -> None:
        """Start the playbin3 pipeline and bus thread.

        playbin3 is a GstPipeline subclass — it has its own bus.
        We monitor it the same way we monitor self._pipeline.

        IMPORTANT: _bus_running must be set BEFORE set_state(PLAYING).
        about-to-finish can fire during the state transition (on a
        GStreamer streaming thread), and the handler guards on
        _bus_running.  If we set it after, about-to-finish is silently
        dropped and gapless transitions never happen.
        """
        if self._loop is None:
            try:
                import asyncio

                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                pass

        pb3 = self._playbin3
        if pb3 is None:
            return

        # Set _bus_running BEFORE state change — about-to-finish can
        # fire during set_state(PLAYING) on a streaming thread.
        self._bus_running = True

        ret = pb3.set_state(Gst.State.PLAYING)
        logger.info(
            "%s: playbin3 set_state(PLAYING) returned %s",
            self.name,
            ret,
        )
        if ret == Gst.StateChangeReturn.FAILURE:
            self._bus_running = False
            logger.error("%s: playbin3 failed to start", self.name)
            if self.on_error:
                if self._loop and self._loop.is_running():
                    self._loop.call_soon_threadsafe(
                        self.on_error, self.name, "playbin3 failed to start"
                    )
                else:
                    try:
                        self.on_error(self.name, "playbin3 failed to start")
                    except Exception:
                        logger.exception("%s: on_error callback failed", self.name)
            return

        # Start bus thread for playbin3's bus
        self._bus_thread = threading.Thread(
            target=self._bus_poll_loop,
            name=f"gst-bus-ch{self.index}",
            daemon=True,
        )
        self._bus_thread.start()

    def owns_element(self, element_name: str) -> bool:
        """Check if this channel owns an element by name."""
        prefix = f"ch{self.index}_"
        return element_name.startswith(prefix)

    # ── Source types ──

    def load_test(self, pattern: str = "black", wave: int = 4) -> None:
        """Load a test pattern source (always produces frames).

        Args:
            pattern: videotestsrc pattern name ("black", "smpte", "snow", etc.)
            wave: audiotestsrc wave (4=silence, 0=sine, etc.)
        """
        self._teardown_source()

        prefix = f"ch{self.index}_test"
        vsrc = _make("videotestsrc", f"{prefix}_v")
        vsrc.set_property("is-live", True)
        vsrc.set_property("pattern", pattern)

        vcaps = _make("capsfilter", f"{prefix}_vcaps")
        vcaps.set_property(
            "caps",
            Gst.Caps.from_string(
                f"video/x-raw,width={self._config.width},"
                f"height={self._config.height},"
                f"framerate={self._config.fps}/1,format=I420"
            ),
        )

        asrc = _make("audiotestsrc", f"{prefix}_a")
        asrc.set_property("is-live", True)
        asrc.set_property("wave", wave)

        acaps = _make("capsfilter", f"{prefix}_acaps")
        acaps.set_property(
            "caps",
            Gst.Caps.from_string(
                f"audio/x-raw,rate={self._config.audio_samplerate},"
                f"channels={self._config.audio_channels},"
                f"format=F32LE,layout=interleaved"
            ),
        )

        vq = _make("queue", f"{prefix}_vq")
        aq = _make("queue", f"{prefix}_aq")

        elements = [vsrc, vcaps, vq, asrc, acaps, aq]
        for elem in elements:
            self._pipeline.add(elem)
        self._source_elements = elements

        vsrc.link(vcaps)
        vcaps.link(vq)
        asrc.link(acaps)
        acaps.link(aq)

        self._link_to_output(vq.get_static_pad("src"), aq.get_static_pad("src"))
        self._start_source()
        self._source_start_time = time.monotonic()
        self.source_type = SourceType.TEST
        logger.info("%s: test source loaded (pattern=%s)", self.name, pattern)

    def load_failover(
        self,
        channel_name: str = "",
        channel_id: str = "",
        description: str = "",
        origins: list[str] | None = None,
    ) -> None:
        """Load the failover source — informative standby screen.

        Entirely self-contained within this source pipeline.  No
        dependency on the mixer text overlay or any external file.
        This is the most reliable source possible: pure in-memory
        generation with zero I/O and zero decoding.

        Displays:
        - Channel name (if set)
        - Channel ID (federation ID)
        - "No content scheduled" status message
        - Channel description (if set)
        - Origin URL (if set)
        - Running timecode (top-right)
        - Silent audio (no annoying sine tone)

        Pipeline:
            videotestsrc(smpte) → caps → textoverlay → timeoverlay → queue → inter
            audiotestsrc(silence) → caps → queue → inter
        """
        self._teardown_source()

        prefix = f"ch{self.index}_fo"

        # Video: SMPTE bars
        vsrc = _make("videotestsrc", f"{prefix}_v")
        vsrc.set_property("is-live", True)
        vsrc.set_property("pattern", "smpte")

        vcaps = _make("capsfilter", f"{prefix}_vcaps")
        vcaps.set_property(
            "caps",
            Gst.Caps.from_string(
                f"video/x-raw,width={self._config.width},"
                f"height={self._config.height},"
                f"framerate={self._config.fps}/1,format=I420"
            ),
        )

        # Build informative text block
        lines = []
        if channel_name:
            lines.append(channel_name)
        if channel_id:
            lines.append(channel_id)
        lines.append("")
        lines.append("No content scheduled")
        if description:
            lines.append(description)
        if origins:
            lines.append(origins[0])
        if not channel_name and not channel_id:
            # No identity configured — show setup hint
            lines = [
                "Cathode",
                "",
                "No content scheduled",
                "Configure via /api/channel",
            ]

        textov = _make("textoverlay", f"{prefix}_text")
        textov.set_property("text", "\n".join(lines))
        textov.set_property("valignment", "center")
        textov.set_property("halignment", "center")
        textov.set_property("font-desc", "Sans Bold 36")
        textov.set_property("shaded-background", True)
        textov.set_property("line-alignment", "center")

        # Timecode: running clock in top-right
        timeov = _make("timeoverlay", f"{prefix}_time")
        timeov.set_property("halignment", "right")
        timeov.set_property("valignment", "top")
        timeov.set_property("font-desc", "Monospace 20")
        timeov.set_property("shaded-background", True)

        vconv = _make("videoconvert", f"{prefix}_vconv")

        # Audio: silence (no sine tone — annoying for always-on failover)
        asrc = _make("audiotestsrc", f"{prefix}_a")
        asrc.set_property("is-live", True)
        asrc.set_property("wave", 4)  # silence
        asrc.set_property("volume", 0.0)

        acaps = _make("capsfilter", f"{prefix}_acaps")
        acaps.set_property(
            "caps",
            Gst.Caps.from_string(
                f"audio/x-raw,rate={self._config.audio_samplerate},"
                f"channels={self._config.audio_channels},"
                f"format=F32LE,layout=interleaved"
            ),
        )

        vq = _make("queue", f"{prefix}_vq")
        aq = _make("queue", f"{prefix}_aq")

        elements = [vsrc, vcaps, textov, timeov, vconv, vq, asrc, acaps, aq]
        for elem in elements:
            self._pipeline.add(elem)
        self._source_elements = elements

        vsrc.link(vcaps)
        vcaps.link(textov)
        textov.link(timeov)
        timeov.link(vconv)
        vconv.link(vq)
        asrc.link(acaps)
        acaps.link(aq)

        self._link_to_output(vq.get_static_pad("src"), aq.get_static_pad("src"))
        self._start_source()
        self._source_start_time = time.monotonic()
        self.source_type = SourceType.TEST
        logger.info("%s: failover source loaded (bars+tone+text)", self.name)

    def load_file_loop(self, path: str) -> None:
        """Load a single file that loops forever.

        Uses playbin3 with about-to-finish: when the file is about to
        end, the same URI is queued again.  Gapless transition handled
        internally — zero gap, zero element creation.
        """
        dur = self._probe_duration(path)
        self.load_playlist([PlaylistEntry(source=path, duration=dur)], loop=True)
        self.source_type = SourceType.FILE_LOOP  # override for display
        logger.info("%s: file loop: %s (%.1fs)", self.name, path, dur)

    @staticmethod
    def _probe_duration(path: str, fallback: float = 10.0) -> float:
        """Get file duration via ffprobe."""
        try:
            r = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "quiet",
                    "-show_entries",
                    "format=duration",
                    "-of",
                    "default=noprint_wrappers=1:nokey=1",
                    path,
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            return float(r.stdout.strip())
        except Exception:
            return fallback

    def load_playlist(
        self, entries: list[PlaylistEntry], loop: bool = True, name: str | None = None
    ) -> None:
        """Load a playlist of clips using playbin3 gapless playback.

        playbin3 wraps uridecodebin3 + playsink.  playsink provides
        the clock feedback loop that makes about-to-finish fire at the
        correct time (near actual playback end, not source exhaustion).

        Custom video-sink and audio-sink bins route decoded output
        through normalize chains to intervideosink/interaudiosink for
        the mixer.  No clocksync needed — playsink handles sync.

        Why playbin3 and not raw uridecodebin3:
            Raw uridecodebin3's about-to-finish fires based on internal
            buffer depletion (source I/O level), not output clock.  In
            interpipeline architecture where clocksync throttles output
            but uridecodebin3's internal multiqueues decouple source
            from output, about-to-finish fires early — causing a
            snowball of cascading pre-rolls.  playbin3's playsink
            constrains the entire pipeline so about-to-finish fires at
            the right wall-clock time.
        """
        if not entries:
            logger.warning("%s: empty playlist", self.name)
            return

        self._teardown_source()

        self._playlist_entries = list(entries)
        self._playlist_loop = loop
        self._playlist_name = name
        self._playlist_index = 0
        self._display_index = 0

        prefix = f"ch{self.index}_pl"
        uri = Path(entries[0].source).as_uri()

        # ── playbin3: uridecodebin3 + playsink with custom sinks ──
        pb3 = _make("playbin3", f"{prefix}_pb3")
        pb3.set_property("uri", uri)
        # flags: video(0x01) + audio(0x02) only — no text/subtitles, no vis
        pb3.set_property("flags", 0x03)

        # ── Custom video sink bin → tee → fakesink + intervideosink ──
        #
        # Topology:
        #   vnorm → tee → queue → fakesink(sync=true)  [clock sync]
        #               → queue → intervideosink        [mixer data]
        #
        # fakesink(sync=true) provides real-time clock sync as a proper
        # GstBaseSink.  This is what playsink needs for correct preroll
        # and about-to-finish timing.  intervideosink receives the same
        # frames for the mixer.  tee propagates fakesink's backpressure
        # upstream to playsink, constraining the entire pipeline.
        #
        # Why not clocksync?  clocksync blocks during preroll, preventing
        # the pipeline from ever reaching PLAYING state (deadlock).
        # fakesink handles preroll correctly via GstBaseSink machinery.
        vsink_bin = Gst.Bin.new(f"{prefix}_vsinkbin")
        vnorm = _make_video_norm(f"{prefix}_vnorm", self._config)
        vtee = _make("tee", f"{prefix}_vtee")
        # Sync branch: fakesink provides clock reference
        vsync_q = _make("queue", f"{prefix}_vsync_q")
        vsync_q.set_property("max-size-buffers", 2)
        vsync_q.set_property("max-size-time", 0)
        vsync_q.set_property("max-size-bytes", 0)
        vfake = _make("fakesink", f"{prefix}_vfake")
        vfake.set_property("sync", True)
        # Inter branch: feeds mixer via shared memory
        vinter_q = _make("queue", f"{prefix}_vinter_q")
        vinter_q.set_property("max-size-time", 3 * Gst.SECOND)
        vinter_q.set_property("max-size-buffers", 0)
        vinter_q.set_property("max-size-bytes", 0)
        v_inter = _make("intervideosink", f"{prefix}_ivsink")
        v_inter.set_property("channel", f"ch{self.index}-video")
        for e in (vnorm, vtee, vsync_q, vfake, vinter_q, v_inter):
            vsink_bin.add(e)
        vnorm.link(vtee)
        vtee.link(vsync_q)
        vsync_q.link(vfake)
        vtee.link(vinter_q)
        vinter_q.link(v_inter)
        ghost_v = Gst.GhostPad.new("sink", vnorm.get_static_pad("sink"))
        vsink_bin.add_pad(ghost_v)

        # ── Custom audio sink bin → tee → fakesink + interaudiosink ──
        asink_bin = Gst.Bin.new(f"{prefix}_asinkbin")
        anorm = _make_audio_norm(f"{prefix}_anorm", self._config)
        atee = _make("tee", f"{prefix}_atee")
        async_q = _make("queue", f"{prefix}_async_q")
        async_q.set_property("max-size-buffers", 10)
        async_q.set_property("max-size-time", 0)
        async_q.set_property("max-size-bytes", 0)
        afake = _make("fakesink", f"{prefix}_afake")
        afake.set_property("sync", True)
        ainter_q = _make("queue", f"{prefix}_ainter_q")
        ainter_q.set_property("max-size-time", 3 * Gst.SECOND)
        ainter_q.set_property("max-size-buffers", 0)
        ainter_q.set_property("max-size-bytes", 0)
        a_inter = _make("interaudiosink", f"{prefix}_iasink")
        a_inter.set_property("channel", f"ch{self.index}-audio")
        for e in (anorm, atee, async_q, afake, ainter_q, a_inter):
            asink_bin.add(e)
        anorm.link(atee)
        atee.link(async_q)
        async_q.link(afake)
        atee.link(ainter_q)
        ainter_q.link(a_inter)
        ghost_a = Gst.GhostPad.new("sink", anorm.get_static_pad("sink"))
        asink_bin.add_pad(ghost_a)

        pb3.set_property("video-sink", vsink_bin)
        pb3.set_property("audio-sink", asink_bin)

        # Wire gapless about-to-finish
        pb3.connect("about-to-finish", self._on_about_to_finish)

        # Data-flow watchdog probe on inter video queue output
        vinter_q.get_static_pad("src").add_probe(
            Gst.PadProbeType.BUFFER,
            self._on_buffer_probe,
            None,
        )

        # playbin3 IS a GstPipeline — it owns its pipeline.
        # Store as _playbin3; _pipeline is unused in this mode.
        self._playbin3 = pb3

        self._start_playbin3()

        self._source_start_time = time.monotonic()
        # Don't set _clip_start_time here — let the first STREAM_START
        # set it.  This lets the bus handler distinguish initial play
        # (clip_start_time == 0) from gapless transitions.
        self._clip_start_time = 0.0
        self.source_type = SourceType.PLAYLIST
        logger.info(
            "%s: playlist loaded (%d clips, loop=%s) [playbin3 gapless]",
            self.name,
            len(entries),
            loop,
        )

    def load_hls(self, url: str) -> None:
        """Pull from a remote HLS stream.

        Pipeline::

            uridecodebin(uri)
                video pad → vnorm → queue → intervideosink
                audio pad → anorm → queue → interaudiosink

        Args:
            url: HLS URL (e.g. ``"https://example.com/live/stream.m3u8"``)
        """
        self._teardown_source()

        prefix = f"ch{self.index}_hls"
        decodebin = _make("uridecodebin", f"{prefix}_dec")
        decodebin.set_property("uri", url)

        vnorm = _make_video_norm(f"{prefix}_vnorm", self._config)
        anorm = _make_audio_norm(f"{prefix}_anorm", self._config)

        vq = _make("queue", f"{prefix}_vq")
        aq = _make("queue", f"{prefix}_aq")
        vq.set_property("max-size-time", 3 * Gst.SECOND)
        aq.set_property("max-size-time", 3 * Gst.SECOND)

        elements = [decodebin, vnorm, anorm, vq, aq]
        for elem in elements:
            self._pipeline.add(elem)
        self._source_elements = elements

        vnorm.link(vq)
        anorm.link(aq)
        self._link_to_output(vq.get_static_pad("src"), aq.get_static_pad("src"))

        decodebin.connect("pad-added", self._on_decode_pad, vnorm, anorm)

        vq.get_static_pad("src").add_probe(
            Gst.PadProbeType.EVENT_DOWNSTREAM,
            self._on_live_eos,
            "video",
        )
        aq.get_static_pad("src").add_probe(
            Gst.PadProbeType.EVENT_DOWNSTREAM,
            self._on_live_eos,
            "audio",
        )

        vq.get_static_pad("src").add_probe(
            Gst.PadProbeType.BUFFER,
            self._on_buffer_probe,
            None,
        )

        self._start_source()
        self._source_start_time = time.monotonic()
        self.source_type = SourceType.HLS
        logger.info("%s: HLS source: %s", self.name, url)

    def load_image(self, path: str) -> None:
        """Load a static image that produces infinite video frames.

        Pipeline::

            uridecodebin(uri) → imagefreeze → vnorm → queue → intervideosink
            audiotestsrc(silence, is-live) → acaps → queue → interaudiosink

        ``imagefreeze`` takes a single decoded frame and re-timestamps it
        as an infinite stream at the downstream framerate.  Audio is
        silent since images have no audio track.

        Args:
            path: Absolute path to an image file (PNG, JPEG, etc.)
        """
        self._teardown_source()

        prefix = f"ch{self.index}_img"
        uri = Path(path).as_uri()

        decodebin = _make("uridecodebin", f"{prefix}_dec")
        decodebin.set_property("uri", uri)

        imgfreeze = _make("imagefreeze", f"{prefix}_freeze")
        # imagefreeze outputs at the rate demanded by downstream caps
        # so the videorate in vnorm will pull at config.fps

        vnorm = _make_video_norm(f"{prefix}_vnorm", self._config)
        vq = _make("queue", f"{prefix}_vq")
        vq.set_property("max-size-time", 2 * Gst.SECOND)

        # Silent audio — images have no audio track
        asrc = _make("audiotestsrc", f"{prefix}_asrc")
        asrc.set_property("is-live", True)
        asrc.set_property("wave", 4)  # silence
        acaps = _make("capsfilter", f"{prefix}_acaps")
        acaps.set_property(
            "caps",
            Gst.Caps.from_string(
                f"audio/x-raw,rate={self._config.audio_samplerate},"
                f"channels={self._config.audio_channels},"
                f"format=F32LE,layout=interleaved"
            ),
        )
        aq = _make("queue", f"{prefix}_aq")

        elements = [decodebin, imgfreeze, vnorm, vq, asrc, acaps, aq]
        for elem in elements:
            self._pipeline.add(elem)
        self._source_elements = elements

        imgfreeze.link(vnorm)
        vnorm.link(vq)
        asrc.link(acaps)
        acaps.link(aq)

        self._link_to_output(vq.get_static_pad("src"), aq.get_static_pad("src"))

        # decodebin pad-added → imagefreeze
        def on_pad(element, pad, freeze=imgfreeze):
            caps = pad.get_current_caps() or pad.query_caps(None)
            if caps and caps.get_size() > 0:
                struct = caps.get_structure(0).get_name()
                if struct.startswith("video/"):
                    sink = freeze.get_static_pad("sink")
                    if sink and not sink.is_linked():
                        pad.link(sink)

        decodebin.connect("pad-added", on_pad)

        self._start_source()
        self._source_start_time = time.monotonic()
        self.source_type = SourceType.IMAGE
        logger.info("%s: image source: %s", self.name, path)

    def load_plugin_source(self, type_name: str, config: dict) -> None:
        """Load a plugin-provided source via its registered SourceFactory.

        The plugin's factory builds GStreamer elements, adds them to this
        layer's pipeline, and links to the inter sinks.  The InputLayer
        handles lifecycle (teardown, start, error handling, failover).

        Args:
            type_name: Plugin source type name (e.g. "html", "script",
                       "generator").  Must be registered in the plugin
                       source_types registry.
            config: Source-specific configuration dict passed to the
                    factory's build() method.
        """
        import plugins

        source_info = plugins.source_types().get(type_name)
        if source_info is None:
            raise ValueError(
                f"Unknown plugin source type: '{type_name}'. "
                f"Available: {list(plugins.source_types().keys())}"
            )

        factory = source_info.get("factory")
        if factory is None:
            raise ValueError(f"Plugin source type '{type_name}' has no factory")

        self._teardown_source()

        try:
            # The factory builds GStreamer elements on this layer.
            # It receives the layer instance and source config, and must:
            #   1. Create elements and add them to self._pipeline
            #   2. Link the final video/audio src pads to self._v_sink / self._a_sink
            #      (or call self._link_to_output(v_pad, a_pad))
            #   3. Store created elements in self._source_elements for teardown
            factory.build(self, config)

            self._start_source()
            self._source_start_time = time.monotonic()
            self.source_type = SourceType.PLUGIN
            logger.info(
                "%s: plugin source loaded (type=%s, plugin=%s)",
                self.name,
                type_name,
                source_info.get("plugin", "unknown"),
            )

        except Exception as exc:
            logger.error(
                "%s: plugin source '%s' failed to build: %s",
                self.name,
                type_name,
                exc,
            )
            # Clean up partial build and fall back to black
            self._teardown_source()
            self.load_test("black")
            if self.on_error:
                try:
                    self.on_error(
                        self.name,
                        f"Plugin source '{type_name}' build failed: {exc}",
                    )
                except Exception:
                    logger.exception("%s: on_error callback failed", self.name)

    def disconnect(self) -> None:
        """Disconnect current source.  Inter sinks stop sending data;
        the mixer's inter sources will produce black/silence after timeout."""
        self._teardown_source()
        logger.info("%s: disconnected", self.name)

    # ── Playlist operations ──

    def skip(self) -> None:
        """Skip to next clip in playlist.

        Reloads the playlist starting from the next clip.
        Automatic gapless transitions use about-to-finish (zero gap).
        Manual skip uses a full reload (brief gap — acceptable for
        user-initiated action).
        """
        if self.source_type not in (SourceType.PLAYLIST, SourceType.FILE_LOOP):
            return
        if not self._playlist_entries:
            return
        n = len(self._playlist_entries)
        next_idx = (self._playlist_index + 1) % n
        saved_entries = list(self._playlist_entries)
        saved_loop = self._playlist_loop
        saved_type = self.source_type
        self._teardown_source()
        self._playlist_entries = saved_entries
        self._playlist_loop = saved_loop
        self._playlist_index = next_idx
        self.source_type = saved_type
        self._source_start_time = time.monotonic()
        self._rebuild_from_current()
        logger.info("%s: skip → clip %d", self.name, self._playlist_index)

    def back(self) -> None:
        """Go back one clip in playlist.

        Reloads the playlist starting from the previous clip.
        """
        if self.source_type not in (SourceType.PLAYLIST, SourceType.FILE_LOOP):
            return
        if not self._playlist_entries:
            return
        n = len(self._playlist_entries)
        prev_idx = (self._playlist_index - 1) % n
        saved_entries = list(self._playlist_entries)
        saved_loop = self._playlist_loop
        saved_type = self.source_type
        self._teardown_source()
        self._playlist_entries = saved_entries
        self._playlist_loop = saved_loop
        self._playlist_index = prev_idx
        self.source_type = saved_type
        self._source_start_time = time.monotonic()
        self._rebuild_from_current()
        logger.info("%s: back → clip %d", self.name, self._playlist_index)

    @property
    def played(self) -> float:
        """Seconds into current clip (playlist/file_loop) or since source loaded."""
        if self.source_type in (SourceType.PLAYLIST, SourceType.FILE_LOOP):
            if self._clip_start_time > 0:
                return time.monotonic() - self._clip_start_time
            return 0.0
        if self.source_type in (
            SourceType.HLS,
            SourceType.SRT,
            SourceType.IMAGE,
        ):
            if self._source_start_time > 0:
                return time.monotonic() - self._source_start_time
            return 0.0
        return 0.0

    @property
    def now_playing(self) -> dict | None:
        """Current playback info including position.

        Uses _display_index (updated on STREAM_START) rather than
        _playlist_index (transport index for about-to-finish URI
        management).  This ensures now_playing reflects what's
        actually being rendered, not what's been pre-rolled.
        """
        if self.source_type in (SourceType.PLAYLIST, SourceType.FILE_LOOP):
            if self._playlist_entries:
                idx = self._display_index % len(self._playlist_entries)
                entry = self._playlist_entries[idx]
                return {
                    "source": entry.source,
                    "index": self._display_index,
                    "duration": entry.duration,
                    "played": round(self.played, 2),
                }
        elif self.source_type == SourceType.IMAGE and self._source_elements:
            uri = None
            for elem in self._source_elements:
                if hasattr(elem, "get_property") and elem.get_name().endswith("_dec"):
                    try:
                        uri = elem.get_property("uri")
                    except Exception:
                        pass
            return {
                "source": uri or "unknown",
                "duration": 0,
                "played": round(self.played, 2),
            }
        return None

    # ── Playlist pipeline builder ──

    def _rebuild_from_current(self) -> None:
        """Build and start a playbin3 pipeline for the current clip.

        Used by skip() and back() to reload from a specific playlist
        position.  The about-to-finish signal handles subsequent
        gapless transitions automatically.
        """
        if not self._playlist_entries:
            return

        idx = self._playlist_index % len(self._playlist_entries)
        entry = self._playlist_entries[idx]
        uri = Path(entry.source).as_uri()

        prefix = f"ch{self.index}_pl"

        # ── playbin3 with custom sinks (same as load_playlist) ──
        pb3 = _make("playbin3", f"{prefix}_pb3")
        pb3.set_property("uri", uri)
        pb3.set_property("flags", 0x03)  # video + audio only

        # Custom video sink bin: tee → fakesink(sync) + intervideosink
        vsink_bin = Gst.Bin.new(f"{prefix}_vsinkbin")
        vnorm = _make_video_norm(f"{prefix}_vnorm", self._config)
        vtee = _make("tee", f"{prefix}_vtee")
        vsync_q = _make("queue", f"{prefix}_vsync_q")
        vsync_q.set_property("max-size-buffers", 2)
        vsync_q.set_property("max-size-time", 0)
        vsync_q.set_property("max-size-bytes", 0)
        vfake = _make("fakesink", f"{prefix}_vfake")
        vfake.set_property("sync", True)
        vinter_q = _make("queue", f"{prefix}_vinter_q")
        vinter_q.set_property("max-size-time", 3 * Gst.SECOND)
        vinter_q.set_property("max-size-buffers", 0)
        vinter_q.set_property("max-size-bytes", 0)
        v_inter = _make("intervideosink", f"{prefix}_ivsink")
        v_inter.set_property("channel", f"ch{self.index}-video")
        for e in (vnorm, vtee, vsync_q, vfake, vinter_q, v_inter):
            vsink_bin.add(e)
        vnorm.link(vtee)
        vtee.link(vsync_q)
        vsync_q.link(vfake)
        vtee.link(vinter_q)
        vinter_q.link(v_inter)
        vsink_bin.add_pad(Gst.GhostPad.new("sink", vnorm.get_static_pad("sink")))

        # Custom audio sink bin: tee → fakesink(sync) + interaudiosink
        asink_bin = Gst.Bin.new(f"{prefix}_asinkbin")
        anorm = _make_audio_norm(f"{prefix}_anorm", self._config)
        atee = _make("tee", f"{prefix}_atee")
        async_q = _make("queue", f"{prefix}_async_q")
        async_q.set_property("max-size-buffers", 10)
        async_q.set_property("max-size-time", 0)
        async_q.set_property("max-size-bytes", 0)
        afake = _make("fakesink", f"{prefix}_afake")
        afake.set_property("sync", True)
        ainter_q = _make("queue", f"{prefix}_ainter_q")
        ainter_q.set_property("max-size-time", 3 * Gst.SECOND)
        ainter_q.set_property("max-size-buffers", 0)
        ainter_q.set_property("max-size-bytes", 0)
        a_inter = _make("interaudiosink", f"{prefix}_iasink")
        a_inter.set_property("channel", f"ch{self.index}-audio")
        for e in (anorm, atee, async_q, afake, ainter_q, a_inter):
            asink_bin.add(e)
        anorm.link(atee)
        atee.link(async_q)
        async_q.link(afake)
        atee.link(ainter_q)
        ainter_q.link(a_inter)
        asink_bin.add_pad(Gst.GhostPad.new("sink", anorm.get_static_pad("sink")))

        pb3.set_property("video-sink", vsink_bin)
        pb3.set_property("audio-sink", asink_bin)
        pb3.connect("about-to-finish", self._on_about_to_finish)

        # Data-flow watchdog probe
        vinter_q.get_static_pad("src").add_probe(
            Gst.PadProbeType.BUFFER,
            self._on_buffer_probe,
            None,
        )

        self._playbin3 = pb3
        self._start_playbin3()
        self._clip_start_time = time.monotonic()

    # ── Playlist callbacks ──

    def _on_about_to_finish(self, element: Gst.Element) -> None:
        """Called by uridecodebin3/playbin3 when the current clip is about to end.

        Sets the next URI so the element can pre-roll and seamlessly
        switch.  Runs on a GStreamer streaming thread — must be fast.
        """
        if not self._playlist_entries or not self._bus_running:
            return

        n = len(self._playlist_entries)
        idx = self._playlist_index
        current_entry = self._playlist_entries[idx % n]
        elapsed = (
            time.monotonic() - self._clip_start_time
            if self._clip_start_time > 0
            else 0.0
        )
        expected = current_entry.duration

        # Diagnostic: log when about-to-finish fires relative to clip duration
        pct = (elapsed / expected * 100) if expected > 0 else 0
        logger.info(
            "%s: about-to-finish at %.1fs of %.1fs (%.0f%%) clip %d/%d '%s'",
            self.name,
            elapsed,
            expected,
            pct,
            idx + 1,
            n,
            Path(current_entry.source).name,
        )

        next_idx = idx + 1

        if next_idx >= n and not self._playlist_loop:
            # Playlist exhausted — don't set a URI, let EOS propagate
            logger.info("%s: playlist finished (no loop)", self.name)
            return

        self._playlist_index = next_idx % n
        next_entry = self._playlist_entries[self._playlist_index]
        next_uri = Path(next_entry.source).as_uri()

        element.set_property("uri", next_uri)

        # Update clip tracking on asyncio thread
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._on_clip_changed)

        logger.debug(
            "%s: about-to-finish → queued clip %d/%d '%s'",
            self.name,
            self._playlist_index + 1,
            n,
            Path(next_entry.source).name,
        )

    def _on_clip_changed(self) -> None:
        """Update clip tracking when uridecodebin3 queues the next clip.

        Called from about-to-finish.  Does NOT reset _clip_start_time —
        that is done when STREAM_START is received on the bus, which
        indicates the new clip has actually started producing data.
        """
        if self._playlist_entries:
            n = len(self._playlist_entries)
            idx = self._playlist_index % n
            entry = self._playlist_entries[idx]
            logger.info(
                "%s: queued next clip %d/%d '%s'",
                self.name,
                idx + 1,
                n,
                Path(entry.source).name,
            )

    # ── Pad callbacks (streaming thread) ──

    def _on_decode_pad(
        self, element: Gst.Element, pad: Gst.Pad, vnorm: Gst.Bin, anorm: Gst.Bin
    ) -> None:
        """Route uridecodebin pads to video/audio normalize bins."""
        caps = pad.get_current_caps() or pad.query_caps(None)
        if not caps or caps.get_size() == 0:
            return
        struct = caps.get_structure(0).get_name()

        if struct.startswith("video/"):
            sink = vnorm.get_static_pad("sink")
            if sink and not sink.is_linked():
                pad.link(sink)
        elif struct.startswith("audio/"):
            sink = anorm.get_static_pad("sink")
            if sink and not sink.is_linked():
                pad.link(sink)

    def _on_buffer_probe(
        self, pad: Gst.Pad, info: Gst.PadProbeInfo, _data
    ) -> Gst.PadProbeReturn:
        """Record that a buffer arrived (for watchdog)."""
        self._last_buffer_time = time.monotonic()
        return Gst.PadProbeReturn.OK

    @property
    def data_age(self) -> float:
        """Seconds since the last buffer arrived on this source."""
        if self._last_buffer_time == 0.0:
            return 0.0
        return time.monotonic() - self._last_buffer_time

    def _on_live_eos(
        self, pad: Gst.Pad, info: Gst.PadProbeInfo, stream_name: str
    ) -> Gst.PadProbeReturn:
        """Drop EOS from live sources to prevent starvation."""
        if info.get_event().type == Gst.EventType.EOS:
            logger.warning(
                "%s: live source %s EOS (dropped)",
                self.name,
                stream_name,
            )
            return Gst.PadProbeReturn.DROP
        return Gst.PadProbeReturn.OK

    # ── Bus thread ──

    def _bus_poll_loop(self) -> None:
        """Poll source pipeline bus for errors and stream transitions.

        Works with both self._pipeline and self._playbin3 — whichever
        is active when the thread starts.
        """
        pipeline = self._playbin3 if self._playbin3 is not None else self._pipeline
        bus = pipeline.get_bus()
        while self._bus_running:
            msg = bus.timed_pop_filtered(
                100 * Gst.MSECOND,
                Gst.MessageType.ERROR
                | Gst.MessageType.WARNING
                | Gst.MessageType.STREAM_START,
            )
            if msg is None:
                continue

            if msg.type == Gst.MessageType.STREAM_START:
                # New clip started producing data — reset clip timer
                # and advance display index.
                #
                # _display_index tracks what's actually rendering.
                # On initial play, it stays at 0 (already correct).
                # On gapless transition, advance by 1 from the last
                # displayed clip.  We use _display_index + 1 rather
                # than _playlist_index because about-to-finish may have
                # fired multiple times (snowball) between STREAM_STARTs,
                # advancing _playlist_index by more than 1.
                if self.source_type in (SourceType.PLAYLIST, SourceType.FILE_LOOP):
                    now = time.monotonic()
                    if self._clip_start_time > 0:
                        # Not the first clip — advance display index
                        n = len(self._playlist_entries) if self._playlist_entries else 1
                        self._display_index = (self._display_index + 1) % n
                    self._clip_start_time = now
                continue

            if msg.type == Gst.MessageType.ERROR:
                err, debug = msg.parse_error()
                src = msg.src.get_name() if msg.src else "unknown"
                error_msg = f"{src}: {err.message}"
                logger.error(
                    "%s error: %s (debug: %s)",
                    self.name,
                    error_msg,
                    debug,
                )
                if self.on_error and self._loop and self._loop.is_running():
                    self._loop.call_soon_threadsafe(self.on_error, src, err.message)

            elif msg.type == Gst.MessageType.WARNING:
                err, _ = msg.parse_warning()
                src = msg.src.get_name() if msg.src else "unknown"
                logger.warning("%s warning: %s: %s", self.name, src, err.message)
