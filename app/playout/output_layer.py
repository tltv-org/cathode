"""OutputLayer — independent output pipeline with its own encoder.

Each OutputLayer owns an independent GStreamer pipeline that reads
composited raw A/V from the mixer via intervideosrc/interaudiosrc
and encodes + delivers to a destination (HLS, RTMP, file, null).

Architecture:
    Mixer pipeline:  compositor/audiomixer → intervideosink/interaudiosink
                     (writes to "mix-video" / "mix-audio" inter channels)
    Output pipeline: intervideosrc/interaudiosrc → encode → mux → sink
                     (reads from "mix-video" / "mix-audio")

Multiple OutputLayers can read from the same mixer inter channels
simultaneously.  Each has its own encoder instance — different outputs
can have different bitrates, codecs, and destinations.

Output types:
    hls  — hlssink2 (direct HLS segment writing to disk)
    rtmp — flvmux + rtmpsink (Twitch, YouTube, relay)
    file — splitmuxsink (time-chunked recording)
    null — fakesink (health monitoring, testing)

Fully isolated from the mixer and other outputs.  One output
crashing does not affect the compositor, inputs, or other outputs.
Follows the same interpipeline pattern as InputLayer but in reverse.

Queue placement follows voctomix convention: 3-second buffer queues
before muxers to absorb jitter.  Leaky queue before encoder to
decouple inter source timing from encoder init.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

import gi

gi.require_version("Gst", "1.0")
from gi.repository import Gst  # noqa: E402

logger = logging.getLogger(__name__)

if not Gst.is_initialized():
    Gst.init(None)


class OutputType(str, Enum):
    HLS = "hls"
    RTMP = "rtmp"
    FILE = "file"
    NULL = "null"


@dataclass
class OutputConfig:
    """Configuration for a single output pipeline."""

    type: OutputType
    name: str

    # Encoding
    video_bitrate: int = 3000  # kbps
    audio_bitrate: int = 128  # kbps (converted to bps for avenc_aac)
    keyframe_interval: int = 60  # frames
    preset: str = "ultrafast"  # x264enc speed-preset

    # HLS-specific
    hls_dir: str = ""
    segment_duration: int = 4  # seconds
    playlist_length: int = 5  # entries in m3u8

    # RTMP-specific
    rtmp_url: str = ""

    # File-specific
    file_path: str = ""  # directory for recordings
    file_format: str = "mp4"  # mp4 or matroska
    max_duration: int = 3600  # seconds per chunk


def _make(factory: str, name: str) -> Gst.Element:
    """Create a GStreamer element or raise."""
    elem = Gst.ElementFactory.make(factory, name)
    if elem is None:
        raise RuntimeError(f"GStreamer element '{factory}' not found")
    return elem


class OutputLayer:
    """Independent output pipeline with its own encoder.

    Reads composited raw A/V from the mixer's inter sinks via
    intervideosrc/interaudiosrc.  Encodes and delivers to the
    configured destination.

    The pipeline is fully independent — it has its own GstPipeline,
    its own clock, its own bus thread.  start() and stop() control
    this output without affecting the mixer or other outputs.

    Threading: GStreamer runs in its own threads.  Bus messages
    are polled in a daemon thread and bridged to asyncio.
    """

    def __init__(self, config: OutputConfig, mixer_config) -> None:
        self.config = config
        self._mixer_config = mixer_config

        self.pipeline: Gst.Pipeline | None = None

        # Bus thread
        self._bus_thread: threading.Thread | None = None
        self._running = False
        self._loop: asyncio.AbstractEventLoop | None = None

        # Stats
        self._start_time: float = 0
        self._error_count: int = 0
        self._last_error: str | None = None

        # Callbacks
        self.on_error: Callable | None = None

    def build(self) -> None:
        """Construct the output pipeline.  Call once before start().

        Pipeline topology:
          Video: intervideosrc → videoconvert → capsfilter → queue(leaky)
                 → x264enc → h264_caps → [type-specific mux+sink]
          Audio: interaudiosrc → audioconvert → audioresample → capsfilter
                 → queue(3s) → avenc_aac → [type-specific mux+sink]
        """
        name = self.config.name
        mc = self._mixer_config

        self.pipeline = Gst.Pipeline.new(f"output-{name}")

        v_caps_str = (
            f"video/x-raw,width={mc.width},height={mc.height},"
            f"framerate={mc.fps}/1,format=I420"
        )
        a_caps_str = (
            f"audio/x-raw,rate={mc.audio_samplerate},"
            f"channels={mc.audio_channels},"
            f"format=F32LE,layout=interleaved"
        )

        # ── Inter sources (read from mixer output) ──
        ivsrc = _make("intervideosrc", f"out-{name}-ivsrc")
        ivsrc.set_property("channel", "mix-video")
        ivsrc.set_property("timeout", 500_000_000)  # 500ms, then black

        iasrc = _make("interaudiosrc", f"out-{name}-iasrc")
        iasrc.set_property("channel", "mix-audio")
        # CRITICAL: buffer-time in nanoseconds.  Must be >= period-time
        # (default 25ms) or interaudiosink returns GST_FLOW_ERROR.
        iasrc.set_property("buffer-time", 2_000_000_000)

        # ── Video normalize + encoder ──
        vconv = _make("videoconvert", f"out-{name}-vconv")
        vcaps = _make("capsfilter", f"out-{name}-vcaps")
        vcaps.set_property("caps", Gst.Caps.from_string(v_caps_str))

        # Leaky queue before encoder — decouples inter source timing
        # from encoder init.  Inter source frames arrive later than
        # direct sources; without this, encoder may fail to initialize.
        vqueue = _make("queue", f"out-{name}-vq")
        vqueue.set_property("max-size-buffers", 2)
        vqueue.set_property("leaky", 2)  # downstream

        x264enc = _make("x264enc", f"out-{name}-venc")
        x264enc.set_property("bitrate", self.config.video_bitrate)
        x264enc.set_property("key-int-max", self.config.keyframe_interval)
        x264enc.set_property("byte-stream", True)
        x264enc.set_property("bframes", 0)
        x264enc.set_property("tune", "zerolatency")
        x264enc.set_property("speed-preset", self.config.preset)

        h264_caps = _make("capsfilter", f"out-{name}-h264caps")
        h264_caps.set_property(
            "caps", Gst.Caps.from_string("video/x-h264,profile=baseline")
        )

        # ── Audio normalize + encoder ──
        aconv = _make("audioconvert", f"out-{name}-aconv")
        ares = _make("audioresample", f"out-{name}-ares")
        acaps = _make("capsfilter", f"out-{name}-acaps")
        acaps.set_property("caps", Gst.Caps.from_string(a_caps_str))

        # 3s buffer queue before audio encoder (voctomix convention)
        aqueue = _make("queue", f"out-{name}-aq")
        aqueue.set_property("max-size-time", 3 * Gst.SECOND)

        aacenc = _make("avenc_aac", f"out-{name}-aenc")
        aacenc.set_property("bitrate", self.config.audio_bitrate * 1000)

        # ── Add common elements to pipeline ──
        for elem in (
            ivsrc,
            vconv,
            vcaps,
            vqueue,
            x264enc,
            h264_caps,
            iasrc,
            aconv,
            ares,
            acaps,
            aqueue,
            aacenc,
        ):
            self.pipeline.add(elem)

        # ── Link common video chain ──
        assert ivsrc.link(vconv)
        assert vconv.link(vcaps)
        assert vcaps.link(vqueue)
        assert vqueue.link(x264enc)
        assert x264enc.link(h264_caps)

        # ── Link common audio chain ──
        assert iasrc.link(aconv)
        assert aconv.link(ares)
        assert ares.link(acaps)
        assert acaps.link(aqueue)
        assert aqueue.link(aacenc)

        # ── Type-specific mux + sink ──
        output_type = self.config.type
        if isinstance(output_type, OutputType):
            output_type = output_type.value

        if output_type == OutputType.NULL.value:
            self._build_null(h264_caps, aacenc)
        elif output_type == OutputType.HLS.value:
            self._build_hls(h264_caps, aacenc)
        elif output_type == OutputType.RTMP.value:
            self._build_rtmp(h264_caps, aacenc)
        elif output_type == OutputType.FILE.value:
            self._build_file(h264_caps, aacenc)
        else:
            # Check plugin output type registry
            self._build_plugin_output(output_type, h264_caps, aacenc)

        logger.info(
            "Output '%s' built: type=%s, %dkbps video, %dkbps audio",
            name,
            self.config.type.value,
            self.config.video_bitrate,
            self.config.audio_bitrate,
        )

    # ── Plugin output types ──

    def _build_plugin_output(
        self, type_name: str, h264_caps: Gst.Element, aacenc: Gst.Element
    ) -> None:
        """Build a plugin-registered output type.

        The plugin's OutputFactory.build_pipeline() is called with this
        OutputLayer's pipeline, the encoded video/audio elements, and the
        output config.
        """
        import plugins

        type_info = plugins.output_types().get(type_name)
        if type_info is None:
            available = list(OutputType.__members__.values()) + list(
                plugins.output_types().keys()
            )
            raise ValueError(
                f"Unknown output type '{type_name}'. "
                f"Available: {[t.value for t in OutputType] + list(plugins.output_types().keys())}"
            )

        factory = type_info.get("factory")
        if factory is None:
            raise ValueError(f"Plugin output type '{type_name}' has no factory")

        factory.build_pipeline(self, h264_caps, aacenc)
        logger.info(
            "Output '%s': plugin type '%s' (plugin: %s)",
            self.config.name,
            type_name,
            type_info.get("plugin", "unknown"),
        )

    # ── Type-specific builders ──

    def _build_null(self, h264_caps: Gst.Element, aacenc: Gst.Element) -> None:
        """Null output: encode but discard.  For testing and monitoring."""
        name = self.config.name

        v_fakesink = _make("fakesink", f"out-{name}-vsink")
        v_fakesink.set_property("sync", True)
        a_fakesink = _make("fakesink", f"out-{name}-asink")
        a_fakesink.set_property("sync", True)

        self.pipeline.add(v_fakesink)
        self.pipeline.add(a_fakesink)

        assert h264_caps.link(v_fakesink)
        assert aacenc.link(a_fakesink)

    def _build_hls(self, h264_caps: Gst.Element, aacenc: Gst.Element) -> None:
        """HLS output: hlssink2 writes segments + m3u8 to disk."""
        name = self.config.name
        cfg = self.config

        if not cfg.hls_dir:
            raise ValueError(f"Output '{name}': hls_dir is required for HLS output")

        # Ensure output directory exists
        os.makedirs(cfg.hls_dir, exist_ok=True)

        # h264parse converts byte-stream to proper NAL framing for mpegtsmux
        h264parse = _make("h264parse", f"out-{name}-h264parse")

        # Buffer queue before hlssink2 (3s, absorbs jitter)
        mux_vq = _make("queue", f"out-{name}-muxvq")
        mux_vq.set_property("max-size-time", 3 * Gst.SECOND)
        mux_aq = _make("queue", f"out-{name}-muxaq")
        mux_aq.set_property("max-size-time", 3 * Gst.SECOND)

        hlssink = _make("hlssink2", f"out-{name}-hlssink")
        hlssink.set_property("target-duration", cfg.segment_duration)
        hlssink.set_property("max-files", cfg.playlist_length + 2)
        hlssink.set_property("playlist-length", cfg.playlist_length)
        hlssink.set_property("location", os.path.join(cfg.hls_dir, "segment%05d.ts"))
        hlssink.set_property(
            "playlist-location", os.path.join(cfg.hls_dir, "stream.m3u8")
        )
        hlssink.set_property("send-keyframe-requests", True)

        for elem in (h264parse, mux_vq, mux_aq, hlssink):
            self.pipeline.add(elem)

        # Video: h264_caps → h264parse → mux_vq → hlssink2 video pad
        assert h264_caps.link(h264parse)
        assert h264parse.link(mux_vq)
        vq_src = mux_vq.get_static_pad("src")
        hls_video = hlssink.request_pad_simple("video")
        assert vq_src.link(hls_video) == Gst.PadLinkReturn.OK

        # Audio: aacenc → mux_aq → hlssink2 audio pad
        assert aacenc.link(mux_aq)
        aq_src = mux_aq.get_static_pad("src")
        hls_audio = hlssink.request_pad_simple("audio")
        assert aq_src.link(hls_audio) == Gst.PadLinkReturn.OK

    def _build_rtmp(self, h264_caps: Gst.Element, aacenc: Gst.Element) -> None:
        """RTMP output: flvmux + rtmpsink for streaming to Twitch/YouTube/relay."""
        name = self.config.name
        cfg = self.config

        if not cfg.rtmp_url:
            raise ValueError(f"Output '{name}': rtmp_url is required for RTMP output")

        flvmux = _make("flvmux", f"out-{name}-mux")
        flvmux.set_property("streamable", True)

        rtmpsink = _make("rtmpsink", f"out-{name}-sink")
        rtmpsink.set_property("location", cfg.rtmp_url)

        # Buffer queues before mux (3s, absorbs jitter)
        mux_vq = _make("queue", f"out-{name}-muxvq")
        mux_vq.set_property("max-size-time", 3 * Gst.SECOND)
        mux_aq = _make("queue", f"out-{name}-muxaq")
        mux_aq.set_property("max-size-time", 3 * Gst.SECOND)

        for elem in (mux_vq, mux_aq, flvmux, rtmpsink):
            self.pipeline.add(elem)

        # Video: h264_caps → mux_vq → flvmux video pad
        assert h264_caps.link(mux_vq)
        vq_src = mux_vq.get_static_pad("src")
        mux_video = flvmux.request_pad_simple("video")
        assert vq_src.link(mux_video) == Gst.PadLinkReturn.OK

        # Audio: aacenc → mux_aq → flvmux audio pad
        assert aacenc.link(mux_aq)
        aq_src = mux_aq.get_static_pad("src")
        mux_audio = flvmux.request_pad_simple("audio")
        assert aq_src.link(mux_audio) == Gst.PadLinkReturn.OK

        assert flvmux.link(rtmpsink)

    def _build_file(self, h264_caps: Gst.Element, aacenc: Gst.Element) -> None:
        """File output: splitmuxsink for time-chunked recording."""
        name = self.config.name
        cfg = self.config

        if not cfg.file_path:
            raise ValueError(f"Output '{name}': file_path is required for file output")

        os.makedirs(cfg.file_path, exist_ok=True)

        # h264parse for proper NAL framing (mp4mux/matroskamux need it)
        h264parse = _make("h264parse", f"out-{name}-h264parse")

        # Buffer queues before splitmux
        mux_vq = _make("queue", f"out-{name}-muxvq")
        mux_vq.set_property("max-size-time", 3 * Gst.SECOND)
        mux_aq = _make("queue", f"out-{name}-muxaq")
        mux_aq.set_property("max-size-time", 3 * Gst.SECOND)

        ext = "mkv" if cfg.file_format == "matroska" else cfg.file_format
        splitmux = _make("splitmuxsink", f"out-{name}-sink")
        splitmux.set_property(
            "location", os.path.join(cfg.file_path, f"recording-%05d.{ext}")
        )
        splitmux.set_property("max-size-time", cfg.max_duration * Gst.SECOND)

        # Set muxer based on format
        if cfg.file_format == "matroska":
            muxer = _make("matroskamux", f"out-{name}-filemux")
        else:
            muxer = _make("mp4mux", f"out-{name}-filemux")
            muxer.set_property("faststart", True)
        splitmux.set_property("muxer", muxer)

        for elem in (h264parse, mux_vq, mux_aq, splitmux):
            self.pipeline.add(elem)

        # Video: h264_caps → h264parse → mux_vq → splitmuxsink video pad
        assert h264_caps.link(h264parse)
        assert h264parse.link(mux_vq)
        vq_src = mux_vq.get_static_pad("src")
        split_video = splitmux.request_pad_simple("video")
        assert vq_src.link(split_video) == Gst.PadLinkReturn.OK

        # Audio: aacenc → mux_aq → splitmuxsink audio_0 pad
        assert aacenc.link(mux_aq)
        aq_src = mux_aq.get_static_pad("src")
        split_audio = splitmux.request_pad_simple("audio_0")
        assert aq_src.link(split_audio) == Gst.PadLinkReturn.OK

    # ── Lifecycle ──

    def start(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Set pipeline to PLAYING and start bus thread.

        Must be called AFTER the mixer is started — the mixer's
        intervideosink/interaudiosink must be running so the output
        pipeline's inter sources have a surface to read from.
        """
        if not self.pipeline:
            raise RuntimeError(f"Output '{self.config.name}' not built")

        self._loop = loop or asyncio.get_running_loop()
        self._running = True
        self._start_time = time.monotonic()

        self._bus_thread = threading.Thread(
            target=self._bus_poll_loop,
            name=f"gst-bus-out-{self.config.name}",
            daemon=True,
        )
        self._bus_thread.start()

        ret = self.pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            self._running = False
            raise RuntimeError(f"Failed to start output pipeline '{self.config.name}'")

        logger.info(
            "Output '%s' started (%s)", self.config.name, self.config.type.value
        )

    def stop(self) -> None:
        """Stop the output pipeline."""
        self._running = False
        if self.pipeline:
            self.pipeline.set_state(Gst.State.NULL)
        if self._bus_thread:
            self._bus_thread.join(timeout=5)
            self._bus_thread = None
        logger.info("Output '%s' stopped", self.config.name)

    # ── Properties ──

    @property
    def state(self) -> str:
        if not self.pipeline:
            return "NULL"
        _, st, _ = self.pipeline.get_state(0)
        return Gst.Element.state_get_name(st)

    @property
    def uptime(self) -> float:
        return time.monotonic() - self._start_time if self._start_time else 0.0

    @property
    def health(self) -> dict:
        """Output health dict for API responses."""
        return {
            "name": self.config.name,
            "type": self.config.type.value,
            "state": self.state,
            "uptime": round(self.uptime, 1),
            "errors": self._error_count,
            "last_error": self._last_error,
            "config": {
                "video_bitrate": self.config.video_bitrate,
                "audio_bitrate": self.config.audio_bitrate,
                "keyframe_interval": self.config.keyframe_interval,
                "preset": self.config.preset,
                **self._type_config(),
            },
        }

    def _type_config(self) -> dict:
        """Type-specific config fields for health response."""
        cfg = self.config
        if cfg.type == OutputType.HLS:
            return {
                "hls_dir": cfg.hls_dir,
                "segment_duration": cfg.segment_duration,
                "playlist_length": cfg.playlist_length,
            }
        elif cfg.type == OutputType.RTMP:
            return {"rtmp_url": cfg.rtmp_url}
        elif cfg.type == OutputType.FILE:
            return {
                "file_path": cfg.file_path,
                "file_format": cfg.file_format,
                "max_duration": cfg.max_duration,
            }
        return {}

    # ── Bus thread ──

    def _bus_poll_loop(self) -> None:
        """Poll pipeline bus for errors and state changes."""
        bus = self.pipeline.get_bus()
        while self._running:
            msg = bus.timed_pop_filtered(
                100 * Gst.MSECOND,
                Gst.MessageType.ERROR
                | Gst.MessageType.WARNING
                | Gst.MessageType.STATE_CHANGED,
            )
            if msg is None:
                continue

            if msg.type == Gst.MessageType.ERROR:
                err, debug = msg.parse_error()
                src = msg.src.get_name() if msg.src else "unknown"
                error_msg = f"{src}: {err.message}"
                logger.error(
                    "Output '%s' error: %s (debug: %s)",
                    self.config.name,
                    error_msg,
                    debug,
                )
                self._error_count += 1
                self._last_error = error_msg
                if self.on_error and self._loop and self._loop.is_running():
                    self._loop.call_soon_threadsafe(
                        self.on_error, self.config.name, err.message
                    )

            elif msg.type == Gst.MessageType.WARNING:
                err, _ = msg.parse_warning()
                src = msg.src.get_name() if msg.src else "unknown"
                logger.warning(
                    "Output '%s' warning: %s: %s",
                    self.config.name,
                    src,
                    err.message,
                )

            elif msg.type == Gst.MessageType.STATE_CHANGED:
                if msg.src == self.pipeline:
                    old, new, _ = msg.parse_state_changed()
                    logger.debug(
                        "Output '%s': %s -> %s",
                        self.config.name,
                        Gst.Element.state_get_name(old),
                        Gst.Element.state_get_name(new),
                    )
