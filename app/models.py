"""Pydantic request models for the Cathode management API."""

from __future__ import annotations

from pydantic import BaseModel


class PlaylistRequest(BaseModel):
    """Set a new playlist by providing file paths within the media directory.

    Optionally provide a 'name' to also save this as a named playlist
    (equivalent to POST /api/playlists/{name}).
    """

    files: list[str]  # e.g. ["clip_01.mp4", "clip_02.mp4"]
    name: str | None = None  # Optional: also save as a named playlist


class GenerateScheduleRequest(BaseModel):
    """Generate a playlist for a specific date.

    Provide one of: ``files`` or ``playlist_name``.
    If none are given, all clips in the media library are used.
    """

    date: str  # YYYY-MM-DD
    files: list[str] | None = None  # Override: specific files to use
    playlist_name: str | None = None  # Use a named playlist


class ProgramBlock(BaseModel):
    """One block in a broadcast program.

    Core block types: playlist, file, image, redirect, flex.
    Plugin block types (generator, html, script) are registered by
    source plugins and dispatched by the scheduler.

    ``layer`` controls which compositor layer the block targets.  Default
    None means "input_a" for backward compatibility.

    ``loop`` controls whether the playlist loops.  Default True.
    When False, the playlist plays once and the layer hides when done.

    Plugin block types pass additional fields (preset, video_pattern,
    location, etc.) through to the plugin's SourceBlockHandler.
    """

    start: str  # "HH:MM:SS"
    end: str  # "HH:MM:SS"
    type: str  # "playlist", "file", "image", "redirect", "flex", or plugin types
    title: str  # EPG display title
    # Playlist blocks
    files: list[str] | None = None  # File list for playlist blocks
    playlist_name: str | None = None  # Named playlist reference
    loop: bool = True  # Loop playlist (default True)
    # File / image blocks
    file: str | None = None  # Single file path
    # Redirect blocks
    url: str | None = None  # HLS URL for redirect blocks
    # Flex blocks
    filler_pool: str | None = None  # Filler pool name
    # Plugin block types (generator, html, script)
    preset: str | None = None  # Plugin preset name
    html: str | None = None  # Inline HTML for html blocks
    location: str | None = None  # URL for html blocks
    name: str | None = None  # Legacy: generator block name
    params: dict | None = None  # Plugin-specific params
    # Common
    layer: str | None = None  # Target layer (default None = "input_a")


class ProgramRequest(BaseModel):
    """Set a day's broadcast program with mixed blocks.

    Each block defines a time window and what to put on air.
    Core types: playlist, file, image, redirect, flex.
    Plugin types: generator, html, script (if plugins loaded).

    Gaps between blocks default to the playout playlist.
    """

    blocks: list[ProgramBlock]


class MigrationRequest(BaseModel):
    """Create a key migration document (PROTOCOL.md section 5.14).

    Permanently migrates the channel to a new identity.  The server
    signs the migration document with the old key and stores it.
    Once created, the migration cannot be reversed.
    """

    to: str  # New channel ID (TV-prefixed, must differ from current)
    reason: str | None = None  # Human-readable reason (max 256 chars)


# ── Settings API models ──


class PlayoutModeRequest(BaseModel):
    """Switch playout scheduling mode between loop and day-schedule."""

    mode: str  # "loop" or "schedule"
    day_start: str | None = None  # "HH:MM:SS" — only meaningful for schedule mode


class EncodingRequest(BaseModel):
    """Update video/audio encoding parameters.

    All fields are optional — only supplied fields are changed.
    Changes require an engine restart to take effect.
    """

    width: int | None = None
    height: int | None = None
    fps: float | None = None
    bitrate: str | None = None  # e.g. "3000k"
    preset: str | None = None  # ultrafast, veryfast, fast, medium, etc.
    audio_bitrate: str | None = None  # e.g. "128k"
    volume: float | None = None  # Master audio gain (1.0 = unity, 0.5 = -6dB)


class StorageRequest(BaseModel):
    """Update storage settings.

    All fields are optional — only supplied fields are changed.
    Changes take effect immediately for filler; extensions affect future media scans.
    """

    filler: str | None = None  # Absolute path to failover/filler clip
    shuffle: bool | None = None  # Randomize playlist order
    extensions: list[str] | None = None  # Recognized media file types


class ChannelMetadataRequest(BaseModel):
    """Update channel federation metadata fields.

    All fields are optional — only supplied fields are changed.
    Changes take effect immediately in-memory; no engine restart needed.
    The signed metadata document at /tltv/v1/channels/{id} reflects
    the update immediately.
    """

    display_name: str | None = None
    description: str | None = None
    language: str | None = None
    tags: list[str] | None = None
    access: str | None = None  # "public" or "private"
    origins: list[str] | None = None
    timezone: str | None = None
    on_demand: bool | None = None
    status: str | None = None  # "active" or "retired"
