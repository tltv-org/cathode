"""Mixer — compositor + audiomixer (raw A/V output).

The static core of the playout pipeline.  Built once, never restructured.
Each input channel slot has an intervideosrc + interaudiosrc pair that
receives decoded A/V from an independent source pipeline via shared-memory
named channels.  The mixer composites all layers and writes raw A/V to
output inter sinks ("mix-video" / "mix-audio").

Independent OutputLayer pipelines read from those inter sinks and handle
encoding + delivery (HLS, RTMP, file recording, etc.).  The mixer has
no encoder — it only composites.

Interpipeline architecture:
    Source pipelines write to intervideosink/interaudiosink → shared channel
    Mixer reads from intervideosrc/interaudiosrc ← shared channel → compositor
    Mixer writes composited output to intervideosink/interaudiosink → shared channel
    Output pipelines read from intervideosrc/interaudiosrc ← shared channel → encode → deliver

When no source pipeline is connected (or a source pipeline stops),
the inter src elements produce black/silence automatically after a
configurable timeout.  This provides inherent failover.

Layer stack (bottom to top):
    Layer 0: Failover  — always running, shows through when others are hidden
    Layer 1: Input A   — primary content (playlist, live, HLS, etc.)
    Layer 2: Input B   — secondary / override
    Layer 3: Blinder   — emergency, covers everything

All switching is done by changing pad properties (alpha, volume,
position, zorder) — never by relinking.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

import gi

gi.require_version("Gst", "1.0")
from gi.repository import Gst  # noqa: E402

logger = logging.getLogger(__name__)

# Initialize GStreamer once
if not Gst.is_initialized():
    Gst.init(None)


@dataclass
class LayerConfig:
    """Configuration for a single compositor layer.

    Roles:
      safety   — always running, auto-loads failover, self-healing on error
      content  — normal show/hide with active_channel tracking
      override — overlay semantics (show doesn't change active_channel,
                 doesn't hide other layers, sits on top of everything)
      overlay  — like override but intended for plugin graphics layers
    """

    name: str
    role: str = "content"  # safety | content | override | overlay
    zorder: int | None = None  # auto-assigned from list index if None


# ── Built-in layer presets ──

PRESET_MINIMAL: list[LayerConfig] = [
    LayerConfig(name="failover", role="safety"),
    LayerConfig(name="content", role="content"),
]

PRESET_STANDARD: list[LayerConfig] = [
    LayerConfig(name="failover", role="safety"),
    LayerConfig(name="input_a", role="content"),
    LayerConfig(name="input_b", role="content"),
    LayerConfig(name="blinder", role="override"),
]

LAYER_PRESETS: dict[str, list[LayerConfig]] = {
    "minimal": PRESET_MINIMAL,
    "standard": PRESET_STANDARD,
}

# Default preset
DEFAULT_LAYERS = PRESET_STANDARD

# Valid layer roles
VALID_ROLES = {"safety", "content", "override", "overlay"}


@dataclass
class MixerConfig:
    """Composition parameters (no encoding — that's per-output)."""

    width: int = 1920
    height: int = 1080
    fps: int = 30
    audio_samplerate: int = 48000
    audio_channels: int = 2
    layers: list[LayerConfig] | None = None  # None = use DEFAULT_LAYERS


# Legacy channel index constants — used by PlayoutEngine for backward-compat named accessors.
CH_FAILOVER = 0
CH_A = 1
CH_B = 2
CH_BLINDER = 3

# Inter element video timeout: nanoseconds to wait for data before
# producing black frames.  500ms balances responsiveness with avoiding
# flicker during source transitions.
INTER_VIDEO_TIMEOUT_NS = 500_000_000

# Inter element audio buffer-time: nanoseconds.  Controls the maximum
# audio data the shared adapter accumulates.  Must be >= period-time
# (default 25ms) or interaudiosink returns GST_FLOW_ERROR on every
# render() call.  2 seconds gives plenty of headroom for output
# encoder back-pressure without excessive latency.
INTER_AUDIO_BUFFER_TIME_NS = 2_000_000_000

# Output inter channel names — OutputLayer pipelines read from these.
MIX_VIDEO_CHANNEL = "mix-video"
MIX_AUDIO_CHANNEL = "mix-audio"


def _make(factory: str, name: str) -> Gst.Element:
    """Create a GStreamer element or raise."""
    elem = Gst.ElementFactory.make(factory, name)
    if elem is None:
        raise RuntimeError(f"GStreamer element '{factory}' not found")
    return elem


class Mixer:
    """Static compositor pipeline with interpipeline I/O.

    Reads decoded A/V from 4 input channels via inter sources,
    composites them, and writes the raw composited output to inter
    sinks ("mix-video" / "mix-audio").  No encoder, no output
    destination — OutputLayer pipelines handle that independently.

    The mixer pipeline is fully self-contained — no external elements
    link into it.  Source pipelines connect by writing to the matching
    inter channel name (e.g. "ch0-video", "ch0-audio").  Output
    pipelines connect by reading from "mix-video" / "mix-audio".

    Threading: GStreamer runs in its own threads.  Bus messages
    are polled in a daemon thread and bridged to asyncio.
    """

    def __init__(self, config: MixerConfig) -> None:
        self.config = config

        # Resolve layer config — default to PRESET_STANDARD
        self.layer_configs: list[LayerConfig] = list(config.layers or DEFAULT_LAYERS)
        # Auto-assign zorders from list index where not specified
        for i, lc in enumerate(self.layer_configs):
            if lc.zorder is None:
                lc.zorder = i
        self.num_channels: int = len(self.layer_configs)

        # Name → index mapping for layer access
        self.layer_index: dict[str, int] = {
            lc.name: i for i, lc in enumerate(self.layer_configs)
        }

        self.pipeline: Gst.Pipeline | None = None
        self.compositor: Gst.Element | None = None
        self.audiomixer: Gst.Element | None = None

        # Per-channel compositor/audiomixer pads (for alpha/volume control)
        self.video_pads: dict[int, Gst.Pad] = {}
        self.audio_pads: dict[int, Gst.Pad] = {}

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
        """Construct the full pipeline.  Call once before start().

        Pipeline topology:
          4x [intervideosrc → vnorm → queue → compositor]
          4x [interaudiosrc → anorm → queue → audiomixer]
          compositor → comp_caps → textoverlay → intervideosink(mix-video)
          audiomixer → audio_caps → interaudiosink(mix-audio)
        """
        self.pipeline = Gst.Pipeline.new("playout")

        # ── Compositor + audiomixer ──
        self.compositor = _make("compositor", "vmix")
        self.compositor.set_property("background", 1)  # black

        self.audiomixer = _make("audiomixer", "amix")

        # ── Output inter sinks (raw composited A/V for OutputLayers) ──
        mix_vsink = _make("intervideosink", "mix_vsink")
        mix_vsink.set_property("channel", MIX_VIDEO_CHANNEL)

        mix_asink = _make("interaudiosink", "mix_asink")
        mix_asink.set_property("channel", MIX_AUDIO_CHANNEL)

        # ── Static elements ──
        static_elems = [
            self.compositor,
            self.audiomixer,
            mix_vsink,
            mix_asink,
        ]
        for elem in static_elems:
            self.pipeline.add(elem)

        # ── Create intervideosrc/interaudiosrc per input channel ──
        v_caps_str = (
            f"video/x-raw,width={self.config.width},"
            f"height={self.config.height},"
            f"framerate={self.config.fps}/1,format=I420"
        )
        a_caps_str = (
            f"audio/x-raw,rate={self.config.audio_samplerate},"
            f"channels={self.config.audio_channels},"
            f"format=F32LE,layout=interleaved"
        )

        for ch, lc in enumerate(self.layer_configs):
            # Video: intervideosrc → convert+scale+rate → capsfilter → queue → compositor
            ivsrc = _make("intervideosrc", f"ch{ch}_ivsrc")
            ivsrc.set_property("channel", f"ch{ch}-video")
            ivsrc.set_property("timeout", INTER_VIDEO_TIMEOUT_NS)

            vconv = _make("videoconvert", f"ch{ch}_ivconv")
            vscale = _make("videoscale", f"ch{ch}_ivscale")
            vrate = _make("videorate", f"ch{ch}_ivrate")
            vcaps = _make("capsfilter", f"ch{ch}_ivcaps")
            vcaps.set_property("caps", Gst.Caps.from_string(v_caps_str))

            vq = _make("queue", f"ch{ch}_ivq")
            vq.set_property("max-size-time", 2 * Gst.SECOND)

            v_elems = [ivsrc, vconv, vscale, vrate, vcaps, vq]
            for e in v_elems:
                self.pipeline.add(e)
            ivsrc.link(vconv)
            vconv.link(vscale)
            vscale.link(vrate)
            vrate.link(vcaps)
            vcaps.link(vq)

            v_pad = self.compositor.request_pad_simple("sink_%u")
            v_pad.set_property("alpha", 0.0)  # all hidden initially
            v_pad.set_property("zorder", lc.zorder)
            assert vq.get_static_pad("src").link(v_pad) == Gst.PadLinkReturn.OK
            self.video_pads[ch] = v_pad

            # Audio: interaudiosrc → convert+resample → capsfilter → queue → audiomixer
            iasrc = _make("interaudiosrc", f"ch{ch}_iasrc")
            iasrc.set_property("channel", f"ch{ch}-audio")
            # CRITICAL: buffer-time is in NANOSECONDS.  Must be >=
            # period-time (default 25ms = 25_000_000 ns) or
            # interaudiosink returns GST_FLOW_ERROR on every render().
            iasrc.set_property("buffer-time", INTER_AUDIO_BUFFER_TIME_NS)

            aconv = _make("audioconvert", f"ch{ch}_iaconv")
            aresample = _make("audioresample", f"ch{ch}_iares")
            acaps = _make("capsfilter", f"ch{ch}_iacaps")
            acaps.set_property("caps", Gst.Caps.from_string(a_caps_str))

            aq = _make("queue", f"ch{ch}_iaq")
            aq.set_property("max-size-time", 2 * Gst.SECOND)

            a_elems = [iasrc, aconv, aresample, acaps, aq]
            for e in a_elems:
                self.pipeline.add(e)
            iasrc.link(aconv)
            aconv.link(aresample)
            aresample.link(acaps)
            acaps.link(aq)

            a_pad = self.audiomixer.request_pad_simple("sink_%u")
            a_pad.set_property("volume", 0.0)  # all silent initially
            assert aq.get_static_pad("src").link(a_pad) == Gst.PadLinkReturn.OK
            self.audio_pads[ch] = a_pad

        # ── Link compositor output chain ──
        # Capsfilter pins the raw output format so downstream inter
        # sinks negotiate correctly.
        comp_caps = _make("capsfilter", "comp_caps")
        comp_caps.set_property("caps", Gst.Caps.from_string(v_caps_str))
        self.pipeline.add(comp_caps)

        # compositor → comp_caps → [plugin overlays] → intervideosink(mix-video)
        # If no overlay plugin is loaded, comp_caps links directly to
        # mix_vsink — zero overhead, pure compositor output.
        assert self.compositor.link(comp_caps)

        # ── Plugin overlay extension point ──
        last_video = comp_caps
        self._overlay_elements: list[Gst.Element] = []
        try:
            import plugins

            for (
                factory_name,
                elem_name,
                default_props,
                plugin_name,
            ) in plugins.overlay_elements():
                overlay_elem = Gst.ElementFactory.make(factory_name, elem_name)
                if overlay_elem is None:
                    logger.warning(
                        "Overlay element '%s' not found (plugin '%s') — skipping",
                        factory_name,
                        plugin_name,
                    )
                    continue
                # Apply default properties (e.g. alpha=0.0 to start hidden)
                for prop, val in default_props.items():
                    try:
                        overlay_elem.set_property(prop, val)
                    except Exception:
                        pass
                self.pipeline.add(overlay_elem)
                if last_video.link(overlay_elem):
                    last_video = overlay_elem
                    self._overlay_elements.append(overlay_elem)
                    logger.info(
                        "Mixer: added overlay '%s' from plugin '%s'",
                        elem_name,
                        plugin_name,
                    )
                else:
                    # Link failed — remove and continue without it
                    self.pipeline.remove(overlay_elem)
                    logger.warning(
                        "Mixer: overlay '%s' failed to link — skipping",
                        elem_name,
                    )
        except ImportError:
            pass  # plugins module not available (shouldn't happen)

        assert last_video.link(mix_vsink)

        # ── Link audiomixer output chain ──
        audio_caps = _make("capsfilter", "audio_out_caps")
        audio_caps.set_property("caps", Gst.Caps.from_string(a_caps_str))
        self.pipeline.add(audio_caps)

        # audiomixer → audio_caps → interaudiosink(mix-audio)
        assert self.audiomixer.link(audio_caps)
        assert audio_caps.link(mix_asink)

        layer_names = [lc.name for lc in self.layer_configs]
        logger.info(
            "Mixer built: %dx%d@%dfps, %d layers (%s) → inter sinks (%s, %s)",
            self.config.width,
            self.config.height,
            self.config.fps,
            self.num_channels,
            ", ".join(layer_names),
            MIX_VIDEO_CHANNEL,
            MIX_AUDIO_CHANNEL,
        )

    # ── Lifecycle ──

    def start(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Set pipeline to PLAYING and start bus thread.

        IMPORTANT: The mixer must start BEFORE source pipelines AND
        output pipelines.  interaudiosrc.start() writes buffer-time/
        period-time to the shared GstInterSurface.  If interaudiosink
        render() runs before these are set, it reads default values
        which are fine — but the mixer must be PLAYING so its
        interaudiosrc elements have called start() and written the
        correct (non-default) buffer-time.

        Start ordering:
          1. Mixer (this) — inter sources set buffer-time
          2. InputLayers — source pipelines start writing
          3. OutputLayers — read from mix-video/mix-audio
        """
        if not self.pipeline:
            raise RuntimeError("Mixer not built")

        self._loop = loop or asyncio.get_running_loop()
        self._running = True
        self._start_time = time.monotonic()

        self._bus_thread = threading.Thread(
            target=self._bus_poll_loop, name="gst-bus-mixer", daemon=True
        )
        self._bus_thread.start()

        ret = self.pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            self._running = False
            raise RuntimeError("Failed to start mixer pipeline")

        logger.info("Mixer started")

    def stop(self) -> None:
        """Stop the pipeline."""
        self._running = False
        if self.pipeline:
            self.pipeline.set_state(Gst.State.NULL)
        if self._bus_thread:
            self._bus_thread.join(timeout=5)
            self._bus_thread = None
        logger.info("Mixer stopped")

    @property
    def state(self) -> str:
        if not self.pipeline:
            return "NULL"
        _, st, _ = self.pipeline.get_state(0)
        return Gst.Element.state_get_name(st)

    @property
    def uptime(self) -> float:
        return time.monotonic() - self._start_time if self._start_time else 0.0

    # ── Channel visibility ──

    def show_channel(self, ch: int, alpha: float = 1.0, volume: float = 1.0) -> None:
        """Make a channel visible."""
        if ch in self.video_pads:
            self.video_pads[ch].set_property("alpha", alpha)
        if ch in self.audio_pads:
            self.audio_pads[ch].set_property("volume", volume)

    def hide_channel(self, ch: int) -> None:
        """Make a channel invisible."""
        if ch in self.video_pads:
            self.video_pads[ch].set_property("alpha", 0.0)
        if ch in self.audio_pads:
            self.audio_pads[ch].set_property("volume", 0.0)

    def set_channel_position(
        self, ch: int, x: int = 0, y: int = 0, w: int = 0, h: int = 0
    ) -> None:
        """Set channel position/size on compositor (for PIP)."""
        pad = self.video_pads.get(ch)
        if not pad:
            return
        pad.set_property("xpos", x)
        pad.set_property("ypos", y)
        if w > 0:
            pad.set_property("width", w)
        if h > 0:
            pad.set_property("height", h)

    # ── Plugin overlay access ──

    def get_overlay_element(self, name: str) -> Gst.Element | None:
        """Get a plugin overlay element by name for runtime control.

        Plugin graphics controllers use this to set properties on their
        registered overlay elements (location, alpha, offset, etc.).
        """
        for elem in self._overlay_elements:
            if elem.get_name() == name:
                return elem
        return None

    @property
    def overlay_element_names(self) -> list[str]:
        """Names of all plugin overlay elements in the pipeline."""
        return [elem.get_name() for elem in self._overlay_elements]

    # ── Bus thread ──

    def _bus_poll_loop(self) -> None:
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
                logger.error("Mixer error: %s (debug: %s)", error_msg, debug)
                self._error_count += 1
                self._last_error = error_msg
                if self.on_error and self._loop and self._loop.is_running():
                    self._loop.call_soon_threadsafe(self.on_error, src, err.message)

            elif msg.type == Gst.MessageType.WARNING:
                err, _ = msg.parse_warning()
                src = msg.src.get_name() if msg.src else "unknown"
                logger.warning("Mixer warning: %s: %s", src, err.message)

            elif msg.type == Gst.MessageType.STATE_CHANGED:
                if msg.src == self.pipeline:
                    old, new, _ = msg.parse_state_changed()
                    logger.debug(
                        "Mixer: %s -> %s",
                        Gst.Element.state_get_name(old),
                        Gst.Element.state_get_name(new),
                    )
