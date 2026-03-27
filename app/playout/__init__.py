"""Cathode playout engine — GStreamer-based broadcast playout.

Interpipeline architecture: each input channel owns its own GStreamer
pipeline that feeds decoded A/V to the mixer via shared-memory inter
elements (intervideosink/interaudiosink → intervideosrc/interaudiosrc).

The mixer composites all layers and writes raw A/V to output inter
sinks.  Independent OutputLayer pipelines read from those sinks and
handle encoding + delivery (HLS, RTMP, file recording, etc.).

4-layer compositor architecture:

    Layer 0 (FAILOVER): Looping branded video or black. Always running.
    Layer 1 (INPUT_A):  Primary content — playlist, live, HLS, etc.
    Layer 2 (INPUT_B):  Secondary / override — same capabilities as A.
    Layer 3 (BLINDER):  Emergency screen — covers everything when active.

Each layer is an InputLayer with its own pipeline that can be
independently sourced from: playlist, file_loop, srt, tcp,
hls, or test pattern.

All switching is compositor alpha/volume property changes.  Source
pipelines are fully isolated — teardown is safe and atomic
(set_state(NULL) on the source pipeline).

Start ordering:
    1. Mixer — composites raw A/V, sets inter buffer-time
    2. InputLayers — source pipelines feed the mixer
    3. OutputLayers — encode + deliver the composited output

Usage:
    engine = PlayoutEngine()
    output_cfg = OutputConfig(type=OutputType.HLS, name="primary",
                              hls_dir="/data/hls")
    await engine.start(config=PlayoutConfig(), default_output=output_cfg)

    engine.failover.load_file_loop("/media/failover.mp4")
    engine.input_a.load_playlist(entries, loop=True)
    engine.show("input_a")

    # Add a second output at runtime
    await engine.add_output(OutputConfig(
        type=OutputType.RTMP, name="twitch",
        video_bitrate=2500,
    ))
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass

from playout.input_layer import InputLayer, PlaylistEntry, SourceType
from playout.mixer import (
    CH_A,
    CH_B,
    CH_BLINDER,
    CH_FAILOVER,
    DEFAULT_LAYERS,
    LAYER_PRESETS,
    LayerConfig,
    Mixer,
    MixerConfig,
)
from playout.output_layer import OutputConfig, OutputLayer, OutputType

logger = logging.getLogger(__name__)

__all__ = [
    "PlayoutEngine",
    "PlayoutConfig",
    "PlaylistEntry",
    "SourceType",
    "OutputConfig",
    "OutputType",
    "LayerConfig",
    "LAYER_PRESETS",
]


@dataclass
class PlayoutConfig:
    """User-facing configuration.

    Composition params are used for the mixer.  Encoding params are
    defaults for the primary output (can be overridden per-output via
    OutputConfig).
    """

    # Composition (→ MixerConfig)
    width: int = 1920
    height: int = 1080
    fps: int = 30
    audio_samplerate: int = 48000
    audio_channels: int = 2

    # Default encoding (→ primary OutputConfig)
    video_bitrate: int = 3000  # kbps
    audio_bitrate: int = 128  # kbps
    keyframe_interval: int = 60

    # Layer configuration (None = standard 4-layer preset)
    layers: list[LayerConfig] | None = None


class PlayoutEngine:
    """GStreamer playout engine with configurable compositor layers + multiple outputs.

    Interpipeline architecture: the mixer pipeline composites raw A/V
    from N input channels and writes to inter sinks.  OutputLayer
    pipelines read from those sinks and encode + deliver independently.

    Layer count and roles are data-driven via LayerConfig.  Presets:
    "minimal" (2 layers: failover + content), "standard" (4 layers:
    failover + A + B + blinder).  Custom configs supported.

    Provides named access to input channels and mixer controls.
    All source and switching operations are available at runtime
    via the channel objects and show/hide methods.

    Outputs are managed via add_output() / remove_output() and
    the /api/outputs REST API.
    """

    def __init__(self) -> None:
        self._mixer: Mixer | None = None
        self._channels: dict[int, InputLayer] = {}
        self._outputs: dict[str, OutputLayer] = {}
        self._config: PlayoutConfig = PlayoutConfig()
        self._started = False

        # Layer config — resolved at start()
        self._layer_configs: list[LayerConfig] = []
        self._layer_map: dict[str, int] = {}  # name -> index
        self._safety_idx: int | None = None  # index of the safety layer
        self._active_channel: int = 0

        # Optional callback: (channel_name: str, error_msg: str) -> None
        self.on_source_lost: Callable | None = None

        # Data-flow watchdog timeout (seconds)
        self.watchdog_timeout: float = 3.0
        self._watchdog_task: asyncio.Task | None = None
        self._restart_task: asyncio.Task | None = None

    # ── Lifecycle ──

    async def start(
        self,
        config: PlayoutConfig | None = None,
        default_output: OutputConfig | None = None,
    ) -> None:
        """Build and start the playout pipeline.

        Startup ordering matters for interpipeline:
        1. Build and start mixer pipeline (interaudiosrc sets buffer-time
           on the shared GstInterSurface during start())
        2. Create InputLayers (each builds its own pipeline with
           inter sinks) and load default test sources
        3. Create and start output pipelines (read from mixer's
           inter sinks)
        """
        if self._started:
            raise RuntimeError("Engine already started")

        self._config = config or PlayoutConfig()

        # Resolve layer config
        self._layer_configs = list(self._config.layers or DEFAULT_LAYERS)
        self._layer_map = {lc.name: i for i, lc in enumerate(self._layer_configs)}

        # Find the safety layer (failover)
        self._safety_idx = None
        for i, lc in enumerate(self._layer_configs):
            if lc.role == "safety":
                self._safety_idx = i
                break
        self._active_channel = self._safety_idx if self._safety_idx is not None else 0

        mixer_config = MixerConfig(
            width=self._config.width,
            height=self._config.height,
            fps=self._config.fps,
            audio_samplerate=self._config.audio_samplerate,
            audio_channels=self._config.audio_channels,
            layers=self._layer_configs,
        )

        # 1. Build and start mixer FIRST — inter sources must set
        #    buffer-time on the shared surface before sinks render.
        self._mixer = Mixer(mixer_config)
        self._mixer.build()

        loop = asyncio.get_running_loop()
        self._mixer.on_error = self._on_mixer_error
        self._mixer.start(loop)

        # 2. Create input channels — each has its own pipeline with
        #    inter sinks.  Safety layer gets failover pattern, others
        #    start with black.
        # Channel metadata for failover display (if available)
        failover_kwargs = {}
        if hasattr(self._config, "channel_name"):
            failover_kwargs["channel_name"] = self._config.channel_name or ""
        if hasattr(self._config, "channel_id"):
            failover_kwargs["channel_id"] = self._config.channel_id or ""
        if hasattr(self._config, "description"):
            failover_kwargs["description"] = self._config.description or ""
        if hasattr(self._config, "origins"):
            failover_kwargs["origins"] = self._config.origins

        for ch, lc in enumerate(self._layer_configs):
            channel = InputLayer(ch, mixer_config)
            channel._loop = loop
            channel.on_error = self._make_channel_error_handler(ch)
            if lc.role == "safety":
                channel.load_failover(**failover_kwargs)
            else:
                channel.load_test("black")
            self._channels[ch] = channel

        # Show safety layer by default
        if self._safety_idx is not None:
            self._mixer.show_channel(self._safety_idx)
        self._started = True

        # 3. Create and start default output (if provided)
        if default_output:
            await self.add_output(default_output)

        # Start data-flow watchdog for live sources
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())

        layer_names = [lc.name for lc in self._layer_configs]
        output_names = list(self._outputs.keys()) or ["(none)"]
        logger.info(
            "Playout engine started, layers: %s, outputs: %s",
            ", ".join(layer_names),
            ", ".join(output_names),
        )

    async def stop(self) -> None:
        """Stop the engine.

        Stop ordering: outputs first, then source pipelines, then mixer.
        No deadlock risk because all pipelines are fully isolated.
        """
        if not self._started:
            return
        self._started = False

        # Cancel watchdog
        if self._watchdog_task:
            self._watchdog_task.cancel()
            self._watchdog_task = None

        # Stop output pipelines first
        for output in self._outputs.values():
            output.stop()
        self._outputs.clear()

        # Stop source pipelines
        for ch in self._channels.values():
            ch._teardown_source()

        # Stop mixer pipeline
        if self._mixer:
            self._mixer.stop()

        self._channels.clear()
        logger.info("Playout engine stopped")

    async def restart(
        self,
        config: PlayoutConfig | None = None,
        default_output: OutputConfig | None = None,
    ) -> None:
        """Stop and restart with optional new config.

        Preserves the config and default output from the previous
        start unless overridden.  Sources must be reloaded by the
        caller after restart (engine comes up with black test patterns).

        If default_output is not provided and the engine previously
        had outputs, the first output's config is reused.
        """
        cfg = config or self._config

        # Preserve first output config for restart if not overridden
        if default_output is None and self._outputs:
            first_output = next(iter(self._outputs.values()))
            default_output = first_output.config

        await self.stop()
        await self.start(cfg, default_output)

    @property
    def is_running(self) -> bool:
        return self._started

    @property
    def config(self) -> PlayoutConfig:
        """Current engine configuration (read-only)."""
        return self._config

    # ── Output management ──

    async def add_output(self, config: OutputConfig) -> OutputLayer:
        """Create, build, and start a new output pipeline.

        The output immediately begins encoding the composited A/V
        from the mixer.  Multiple outputs can run simultaneously
        with different encoding params and destinations.
        """
        if config.name in self._outputs:
            raise ValueError(f"Output '{config.name}' already exists")

        if not self._mixer:
            raise RuntimeError("Engine not started — cannot add output")

        output = OutputLayer(config, self._mixer.config)
        output.build()
        output.on_error = self._on_output_error
        output.start(self._mixer._loop)

        self._outputs[config.name] = output
        logger.info("Output added: %s (%s)", config.name, config.type.value)
        return output

    async def remove_output(self, name: str) -> None:
        """Stop and remove an output pipeline."""
        output = self._outputs.pop(name, None)
        if output is None:
            raise ValueError(f"Output '{name}' not found")
        output.stop()
        logger.info("Output removed: %s", name)

    def get_output(self, name: str) -> OutputLayer | None:
        """Get an output by name."""
        return self._outputs.get(name)

    @property
    def outputs(self) -> dict[str, OutputLayer]:
        """All active outputs."""
        return dict(self._outputs)

    # ── Named channel access ──

    @property
    def failover(self) -> InputLayer:
        """Legacy accessor — returns the safety layer."""
        if self._safety_idx is not None:
            return self._channels[self._safety_idx]
        # Backward compat fallback
        return self._channels.get(CH_FAILOVER, next(iter(self._channels.values())))

    @property
    def input_a(self) -> InputLayer:
        """Legacy accessor — returns input_a or the first content layer."""
        idx = self._layer_map.get("input_a")
        if idx is not None:
            return self._channels[idx]
        # Minimal preset: return the content layer
        for i, lc in enumerate(self._layer_configs):
            if lc.role == "content":
                return self._channels[i]
        raise RuntimeError("No content layer configured")

    @property
    def input_b(self) -> InputLayer:
        """Legacy accessor — returns input_b if it exists."""
        idx = self._layer_map.get("input_b")
        if idx is None:
            raise AttributeError("No input_b layer in current config")
        return self._channels[idx]

    @property
    def blinder(self) -> InputLayer:
        """Legacy accessor — returns the blinder/override layer."""
        idx = self._layer_map.get("blinder")
        if idx is None:
            raise AttributeError("No blinder layer in current config")
        return self._channels[idx]

    def channel(self, name: str) -> InputLayer:
        """Get a channel by name (config-driven)."""
        idx = self._layer_map.get(name)
        if idx is None:
            raise ValueError(
                f"Unknown channel: {name} (valid: {list(self._layer_map)})"
            )
        return self._channels[idx]

    def layer_config(self, name: str) -> LayerConfig:
        """Get the LayerConfig for a named layer."""
        idx = self._layer_map.get(name)
        if idx is None:
            raise ValueError(f"Unknown layer: {name}")
        return self._layer_configs[idx]

    @property
    def layer_names(self) -> list[str]:
        """All configured layer names in order."""
        return [lc.name for lc in self._layer_configs]

    @property
    def layer_configs_list(self) -> list[LayerConfig]:
        """Current layer configs."""
        return list(self._layer_configs)

    # ── Switching ──

    def show(self, name: str, alpha: float = 1.0, volume: float = 1.0) -> None:
        """Show a channel (make visible + audible).

        Role-based behavior:
        - override/overlay: shown without changing _active_channel
          (overlay semantics — doesn't hide other layers)
        - content: hides the previous active content channel, becomes
          the new active channel
        - safety: always visible, show() is a no-op (already showing)
        """
        idx = self._layer_map.get(name)
        if idx is None:
            raise ValueError(
                f"Unknown channel: {name} (valid: {list(self._layer_map)})"
            )

        role = self._layer_configs[idx].role

        if role in ("override", "overlay"):
            # Overlay semantics — don't change active channel
            self._mixer.show_channel(idx, alpha, volume)
            logger.debug("Overlay shown: %s", name)
            return

        safety = self._safety_idx
        if self._active_channel not in (safety, idx):
            self._mixer.hide_channel(self._active_channel)

        self._mixer.show_channel(idx, alpha, volume)
        self._active_channel = idx
        logger.debug("Showing: %s", name)

    def hide(self, name: str) -> None:
        """Hide a channel."""
        idx = self._layer_map.get(name)
        if idx is None:
            raise ValueError(
                f"Unknown channel: {name} (valid: {list(self._layer_map)})"
            )
        self._mixer.hide_channel(idx)
        if idx == self._active_channel:
            self._active_channel = (
                self._safety_idx if self._safety_idx is not None else 0
            )
        logger.debug("Hidden: %s", name)

    def hide_blinder(self) -> None:
        """Deactivate the blinder (legacy compat).

        Hides the first override-role layer, or the layer named 'blinder'.
        """
        idx = self._layer_map.get("blinder")
        if idx is None:
            # Find first override layer
            for i, lc in enumerate(self._layer_configs):
                if lc.role == "override":
                    idx = i
                    break
        if idx is not None:
            self._mixer.hide_channel(idx)
            logger.debug("Override layer deactivated")

    @property
    def active_channel(self) -> str:
        """Name of the currently visible channel."""
        if self._active_channel < len(self._layer_configs):
            return self._layer_configs[self._active_channel].name
        return self._layer_configs[0].name if self._layer_configs else "unknown"

    # ── Plugin overlay access ──

    def get_overlay_element(self, name: str):
        """Get a plugin overlay GStreamer element by name."""
        if self._mixer:
            return self._mixer.get_overlay_element(name)
        return None

    @property
    def overlay_elements(self) -> list[str]:
        """Names of all plugin overlay elements."""
        if self._mixer:
            return self._mixer.overlay_element_names
        return []

    # ── PIP positioning ──

    def set_position(
        self, name: str, x: int = 0, y: int = 0, w: int = 0, h: int = 0
    ) -> None:
        """Set a channel's position and size on the compositor."""
        idx = self._layer_map.get(name)
        if idx is None:
            raise ValueError(f"Unknown channel: {name}")
        if self._mixer:
            self._mixer.set_channel_position(idx, x, y, w, h)

    def reset_position(self, name: str) -> None:
        """Reset a channel to full-screen."""
        idx = self._layer_map.get(name)
        if idx is None:
            raise ValueError(f"Unknown channel: {name}")
        if self._mixer:
            self._mixer.set_channel_position(idx, 0, 0, 0, 0)

    # ── Data-flow watchdog ──

    async def _watchdog_loop(self) -> None:
        """Periodically check that live sources are still producing data."""
        try:
            while self._started:
                await asyncio.sleep(2)
                for idx, lc in enumerate(self._layer_configs):
                    ch = self._channels.get(idx)
                    if ch is None:
                        continue
                    if ch.source_type not in (
                        SourceType.HLS,
                        SourceType.SRT,
                    ):
                        continue
                    age = ch.data_age
                    if age > self.watchdog_timeout:
                        logger.warning(
                            "Watchdog: %s has no data for %.1fs — failing over",
                            lc.name,
                            age,
                        )
                        self._handle_source_failure(
                            lc.name, idx, f"No data for {age:.1f}s on {lc.name}"
                        )
        except asyncio.CancelledError:
            pass

    # ── Error handling ──

    def _make_channel_error_handler(self, ch_idx: int):
        """Create an error callback for a specific channel."""

        def handler(element_name: str, error_msg: str) -> None:
            if ch_idx < len(self._layer_configs):
                name = self._layer_configs[ch_idx].name
                self._handle_source_failure(name, ch_idx, error_msg)

        return handler

    def _on_mixer_error(self, element_name: str, error_msg: str) -> None:
        """Handle errors from the mixer pipeline.

        Mixer errors kill the composited output.  Mark the engine as
        unhealthy so the watchdog loop detects the failure and restarts
        within one poll cycle (~15 seconds).  Also schedule an immediate
        restart attempt with exponential backoff so recovery is
        typically < 5 seconds.
        """
        logger.error("Mixer pipeline error: %s: %s", element_name, error_msg)

        # Mark unhealthy so watchdog_loop detects the failure
        self._started = False

        # Schedule restart from asyncio thread (mixer bus runs on GStreamer thread)
        loop = self._mixer._loop if self._mixer else None
        if loop and loop.is_running():
            loop.call_soon_threadsafe(self._schedule_mixer_restart)

    def _on_output_error(self, output_name: str, error_msg: str) -> None:
        """Handle errors from an output pipeline.

        Output errors are isolated — one output crashing does not
        affect the mixer, inputs, or other outputs.  Log the error
        for now.  Future: auto-restart the failed output, notify
        via webhook.
        """
        logger.error(
            "Output '%s' error: %s — output may need manual restart",
            output_name,
            error_msg,
        )

    def _schedule_mixer_restart(self) -> None:
        """Schedule a mixer restart with backoff.  Runs on asyncio thread."""
        if self._restart_task and not self._restart_task.done():
            return  # restart already in progress
        self._restart_task = asyncio.create_task(self._do_mixer_restart())

    async def _do_mixer_restart(self) -> None:
        """Attempt to restart the mixer pipeline with backoff."""
        backoff = 2.0
        max_backoff = 30.0
        for attempt in range(5):
            logger.warning(
                "Mixer restart attempt %d/%d (backoff %.1fs)",
                attempt + 1,
                5,
                backoff,
            )
            await asyncio.sleep(backoff)
            try:
                cfg = self._config
                # Preserve output configs for restart
                output_configs = [o.config for o in self._outputs.values()]
                default_output = output_configs[0] if output_configs else None

                await self.stop()
                await self.start(cfg, default_output)

                # Re-add additional outputs beyond the default
                for oc in output_configs[1:]:
                    await self.add_output(oc)

                logger.info("Mixer restart succeeded on attempt %d", attempt + 1)
                return
            except Exception as exc:
                logger.error("Mixer restart attempt %d failed: %s", attempt + 1, exc)
                backoff = min(backoff * 2, max_backoff)
        logger.critical(
            "Mixer restart failed after 5 attempts — stream is dead, "
            "waiting for external watchdog"
        )

    def _handle_source_failure(self, name: str, idx: int, error_msg: str) -> None:
        """Handle a source channel failure — failover for ANY source type.

        Role-based behavior:
        - safety: never hides. Reloads the failover pattern as last resort.
        - content: replaced with black, hidden, safety shows through.
        - override/overlay: replaced with black, hidden explicitly
          (never tracked as _active_channel, so must be hidden directly).
        """
        ch = self._channels.get(idx)
        if ch is None:
            return

        lc = self._layer_configs[idx] if idx < len(self._layer_configs) else None
        role = lc.role if lc else "content"

        # Safety layer is the last line of defense — never hide it.
        if role == "safety":
            logger.warning(
                "Safety source error: %s — reloading failover pattern",
                error_msg,
            )
            ch.disconnect()
            ch.load_failover()
            return

        logger.warning(
            "Source error on %s (%s): %s — failing over",
            name,
            ch.source_type.value,
            error_msg,
        )

        # Replace broken source with black silence
        ch.disconnect()
        ch.load_test("black")

        # Role-based hide logic
        if role in ("override", "overlay"):
            # Override/overlay layers are never the _active_channel,
            # so we must hide them explicitly.
            self._mixer.hide_channel(idx)
            logger.info("Auto-failover: %s error — hiding %s layer", name, role)
        elif self._active_channel == idx:
            self._mixer.hide_channel(idx)
            self._active_channel = (
                self._safety_idx if self._safety_idx is not None else 0
            )
            logger.info("Auto-failover: %s → safety", name)

        # Notify application
        if self.on_source_lost:
            try:
                self.on_source_lost(name, error_msg)
            except Exception:
                logger.exception("on_source_lost callback error")

    # ── Layer visibility ──

    def layer_visibility(self, idx: int) -> dict:
        """Read current alpha/volume from the mixer's compositor pads."""
        if not self._mixer:
            return {"visible": False, "alpha": 0.0, "volume": 0.0}

        alpha = 0.0
        volume = 0.0
        v_pad = self._mixer.video_pads.get(idx)
        a_pad = self._mixer.audio_pads.get(idx)
        if v_pad:
            alpha = v_pad.get_property("alpha")
        if a_pad:
            volume = a_pad.get_property("volume")
        return {
            "visible": alpha > 0.0,
            "alpha": round(alpha, 3),
            "volume": round(volume, 3),
        }

    # ── Health ──

    @property
    def health(self) -> dict:
        if not self._mixer:
            return {"running": False, "state": "NULL"}
        return {
            "running": self._started,
            "state": self._mixer.state,
            "uptime": self._mixer.uptime,
            "errors": self._mixer._error_count,
            "last_error": self._mixer._last_error,
            "active_channel": self.active_channel,
            "layer_preset": None,
            "channels": {
                lc.name: {
                    "source_type": ch.source_type.value,
                    "now_playing": ch.now_playing,
                    "playlist_name": ch._playlist_name,
                    "role": lc.role,
                    **self.layer_visibility(idx),
                }
                for idx, lc in enumerate(self._layer_configs)
                for ch in [self._channels[idx]]
            },
            "outputs": {name: output.health for name, output in self._outputs.items()},
        }
