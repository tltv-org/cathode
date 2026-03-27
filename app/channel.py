"""Channel context and registry — per-channel state and service instances.

Each channel gets its own ChannelContext holding service instances,
scheduler state, background tasks, paths, and network config.
The ChannelRegistry holds all contexts and provides lookup by channel ID.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from protocol.identity import ensure_channel_keypair  # noqa: F401

logger = logging.getLogger(__name__)


# ── Channel Context ──


@dataclass
class ChannelContext:
    """All per-channel state and service instances.

    Holds everything needed to operate a single channel:
    service clients, scheduler state, background tasks,
    filesystem paths, and network configuration.
    """

    # Identity
    id: str  # "channel-one", "channel-two"
    display_name: str  # "TLTV Channel One"

    # Plugin services are accessed via plugins.get_service() — not held
    # on the context.  This decouples channel creation from optional
    # plugin availability.

    # Scheduler state (moved from scheduler.py module globals)
    active_block_key: str | None = None
    active_playlist_block_key: str | None = None
    crashed_block_key: str | None = None

    # Per-layer block tracking for multi-layer scheduling.
    # Maps layer name → block_key ("HH:MM:SS-HH:MM:SS") for the
    # currently active block on that layer.
    active_layer_blocks: dict = field(default_factory=dict)

    # Hot-reload state: set of layer names that were active before
    # a hot-reload clear.  The scheduler uses this on the next tick
    # to hide layers that are no longer in the new program.
    hot_reload_layers: set = field(default_factory=set)

    # Background tasks (one set per channel)
    watchdog_task: asyncio.Task | None = field(default=None, repr=False)
    scheduler_task: asyncio.Task | None = field(default=None, repr=False)
    program_task: asyncio.Task | None = field(default=None, repr=False)
    hls_watchdog_task: asyncio.Task | None = field(default=None, repr=False)

    # Per-channel paths
    media_dir: str = ""
    generated_dir: str = ""  # Defaults to {media_dir}/generated/ — set at creation
    program_dir: str = ""  # Base dir for programs (channel_id added by program.py)

    # Per-channel output config
    default_output_type: str = "hls"  # "hls", "rtmp", "null"
    hls_dir: str = ""  # Per-channel HLS output directory

    # Identity (federation) — optional, populated if keypair exists
    channel_id: str | None = None  # TV-prefixed federation ID (e.g. "TVMkVH...")
    private_key_path: str | None = None

    # Federation metadata (from channel config identity section)
    description: str | None = None
    language: str | None = None
    tags: list[str] = field(default_factory=list)
    access: str = "public"
    origins: list[str] = field(default_factory=list)
    icon: str | None = None  # Path to channel icon (section 5.9)
    on_demand: bool = False  # On-demand mode (section 5.11)

    # Timezone — IANA name (e.g. "America/New_York") from channel config.
    # Used for UTC conversion of guide times (spec section 5.12) and
    # included in signed metadata as a client display hint.
    timezone: str | None = None

    # Playlist loop — when true, the engine loops the current playlist
    # forever instead of switching to the next day's file at midnight.
    # Auto-enabled on first start when no media files exist.
    playlist_loop: bool = False

    # Day start — "HH:MM:SS" defining when schedule mode's day begins.
    # Used by the program scheduler to align block times.
    day_start: str = "00:00:00"

    # Channel status — "active" (default) or "retired".
    # Retired channels continue serving signed metadata with
    # "status": "retired" for 30 days.  Relays stop polling,
    # peer exchange excludes them.
    status: str = "active"

    # Mirror mode (PROTOCOL.md section 10.8) — when true, this channel
    # replicates HLS from a primary origin rather than generating its own.
    # The mirror holds the same private key as the primary.
    mirror_mode: bool = False
    mirror_primary: str | None = None  # Primary origin hint (host:port)

    # Failover config — safety layer video parameters.
    # Customizable via PATCH /api/playout/failover.  Persisted to YAML.
    failover_title: str | None = None  # defaults to display_name
    failover_subtitle: str | None = None
    failover_duration: int = 60
    failover_pattern: str = "smpte"

    # Slate config — default input_a content parameters.
    # Customizable via PATCH /api/playout/slate.  Persisted to YAML.
    slate_title: str | None = None  # defaults to display_name
    slate_subtitle: str = "No content scheduled"
    slate_duration: int = 300
    slate_pattern: str = "black"

    # Key migration (PROTOCOL.md section 5.14) — when set, this channel
    # has migrated to a new identity.  The signed migration document is
    # served at the metadata endpoint instead of regular metadata.
    # Migration is permanent and irreversible.
    migration: dict | None = field(default=None, repr=False)

    # Per-channel playout engine (GStreamer) — set during lifespan.
    # Each channel has its own engine with its own mixer, encoder,
    # and layer stack.  None until started.
    engine: Any = field(default=None, repr=False)


# ── Channel Registry ──


class ChannelRegistry:
    """Registry of all channel contexts. Thread-safe for asyncio."""

    def __init__(self) -> None:
        self._channels: dict[str, ChannelContext] = {}

    def register(self, ctx: ChannelContext) -> None:
        """Register a channel context."""
        if ctx.id in self._channels:
            raise ValueError(f"Channel '{ctx.id}' already registered")
        self._channels[ctx.id] = ctx
        logger.info("Registered channel '%s' (%s)", ctx.id, ctx.display_name)

    def unregister(self, channel_id: str) -> ChannelContext | None:
        """Remove a channel from the registry. Returns the context or None."""
        ctx = self._channels.pop(channel_id, None)
        if ctx:
            logger.info("Unregistered channel '%s'", channel_id)
        return ctx

    def get(self, channel_id: str) -> ChannelContext:
        """Get a channel context by ID. Raises KeyError if not found."""
        return self._channels[channel_id]

    def get_or_none(self, channel_id: str) -> ChannelContext | None:
        """Get a channel context by ID, or None if not found."""
        return self._channels.get(channel_id)

    def default(self) -> ChannelContext:
        """Return the first registered channel (default for backward compat).

        Raises RuntimeError if no channels are registered.
        """
        if not self._channels:
            raise RuntimeError("No channels registered")
        return next(iter(self._channels.values()))

    def all(self) -> list[ChannelContext]:
        """Return all registered channel contexts."""
        return list(self._channels.values())

    def __contains__(self, channel_id: str) -> bool:
        return channel_id in self._channels

    def __len__(self) -> int:
        return len(self._channels)

    def __bool__(self) -> bool:
        return bool(self._channels)
