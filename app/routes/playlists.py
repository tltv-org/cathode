"""Named playlist CRUD endpoints — /api/playlists.

Reusable named playlists that can be referenced from program blocks
and loaded onto layers.

    GET    /api/playlists              — List all saved playlists
    GET    /api/playlists/{name}       — Get a playlist by name
    POST   /api/playlists/{name}       — Create/update a named playlist
    DELETE /api/playlists/{name}       — Delete a named playlist
    POST   /api/playlists/{name}/load  — Load a named playlist onto a layer
"""

from __future__ import annotations

import logging
import os

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import config
import main
import named_playlist_store
from utils import get_clip_duration

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/playlists", tags=["playlists"])

DEFAULT_LAYER_NAMES = ("failover", "input_a", "input_b", "blinder")


# ── Request models ──


class NamedPlaylistRequest(BaseModel):
    """Create or update a named playlist."""

    files: list[str]  # File paths (relative to MEDIA_DIR or absolute)


class LoadPlaylistRequest(BaseModel):
    """Load a named playlist onto a layer."""

    layer: str = "input_a"
    loop: bool = True


# ── Endpoints ──


@router.get("")
async def list_playlists() -> dict:
    """List all saved named playlists (name, file count, total duration)."""
    playlists = named_playlist_store.list_all()
    return {"playlists": playlists, "count": len(playlists)}


@router.get("/{name}")
async def get_playlist(name: str) -> dict:
    """Get a named playlist with full entry details."""
    playlist = named_playlist_store.get(name)
    if playlist is None:
        raise HTTPException(404, f"Playlist '{name}' not found")
    return playlist


@router.post("/{name}")
async def save_playlist(name: str, req: NamedPlaylistRequest) -> dict:
    """Create or update a named playlist.

    Validates that all files exist and probes their durations.
    Files can be relative (resolved against MEDIA_DIR) or absolute.
    """
    # Validate name
    err = named_playlist_store.validate_name(name)
    if err:
        raise HTTPException(400, err)

    if not req.files:
        raise HTTPException(400, "files list cannot be empty")

    # Resolve and validate files, probe durations
    entries = []
    for f in req.files:
        # Resolve relative paths against MEDIA_DIR (absolute paths rejected)
        if os.path.isabs(f):
            raise HTTPException(
                400,
                "Absolute paths are not allowed — use a path relative to the media directory",
            )
        if ".." in f:
            raise HTTPException(400, "Path traversal ('..') is not allowed")
        local_path = os.path.join(config.MEDIA_DIR, f)

        if not os.path.isfile(local_path):
            raise HTTPException(404, f"File not found: {f}")

        duration = get_clip_duration(local_path)
        entries.append({"source": local_path, "duration": duration})

    playlist = named_playlist_store.save(name, entries)
    total_duration = sum(e["duration"] for e in entries)
    is_new = playlist.get("created") == playlist.get("updated")

    return {
        "ok": True,
        "name": name,
        "entry_count": len(entries),
        "total_duration": round(total_duration, 2),
        "created": is_new,
    }


@router.delete("/{name}")
async def delete_playlist(name: str) -> dict:
    """Delete a named playlist."""
    if not named_playlist_store.delete(name):
        raise HTTPException(404, f"Playlist '{name}' not found")
    return {"ok": True, "deleted": name}


@router.post("/{name}/load")
async def load_playlist(name: str, req: LoadPlaylistRequest | None = None) -> dict:
    """Load a named playlist onto a layer.

    Defaults: layer="input_a", loop=true.
    """
    playlist = named_playlist_store.get(name)
    if playlist is None:
        raise HTTPException(404, f"Playlist '{name}' not found")

    if main.playout is None:
        raise HTTPException(501, "Playout engine not started")

    layer = req.layer if req else "input_a"
    loop = req.loop if req else True

    if layer not in DEFAULT_LAYER_NAMES:
        raise HTTPException(
            400,
            f"Unknown layer '{layer}'. Valid: {', '.join(DEFAULT_LAYER_NAMES)}",
        )

    entries = playlist.get("entries", [])
    if not entries:
        raise HTTPException(400, f"Playlist '{name}' is empty")

    try:
        from playout.input_layer import PlaylistEntry

        pl_entries = [
            PlaylistEntry(source=e["source"], duration=e.get("duration", 0))
            for e in entries
        ]
        ch = main.playout.channel(layer)
        ch.load_playlist(pl_entries, loop=loop, name=name)
        main.playout.show(layer)

        # Persist active playlist state for restart recovery
        import playout_state

        ctx = main.channels.all()[0] if main.channels else None
        if ctx:
            playout_state.save_layer_state(ctx.id, layer, playlist_name=name, loop=loop)
    except Exception as exc:
        logger.error(
            "Failed to load playlist '%s' on %s: %s", name, layer, exc, exc_info=True
        )
        raise HTTPException(500, "Failed to load playlist")

    return {
        "ok": True,
        "name": name,
        "layer": layer,
        "loop": loop,
        "entry_count": len(entries),
    }


# ── Plugin playlist tools ──


@router.get("/{name}/tools")
async def list_playlist_tools(name: str, channel: str | None = None):
    """List available playlist tools for a named playlist.

    Returns tools registered by plugins (sort, pad, dedup, etc.).
    Empty list if no playlist tool plugins are loaded.
    """
    import plugins

    ctx = None
    if channel:
        from routes.channel_resolve import resolve_channel_ctx

        ctx = resolve_channel_ctx(channel)

    ch_id = ctx.id if ctx else config.DEFAULT_CHANNEL_ID
    playlist = named_playlist_store.get(name, channel_id=ch_id)
    if playlist is None:
        raise HTTPException(404, f"Playlist '{name}' not found")

    tools = {}
    for tool_name, info in plugins.playlist_tools().items():
        tools[tool_name] = {
            "description": info.get("description", ""),
            "params": info.get("params", {}),
            "plugin": info.get("plugin", ""),
        }

    return {"playlist": name, "tools": tools}


class PlaylistToolRequest(BaseModel):
    """Apply a plugin tool to a playlist."""

    params: dict = {}
    save: bool = False  # If True, persist the transformed result


@router.post("/{name}/tools/{tool_name}")
async def apply_playlist_tool(
    name: str, tool_name: str, req: PlaylistToolRequest, channel: str | None = None
):
    """Apply a plugin-registered tool to a named playlist.

    By default returns the transformed playlist without saving.
    Set save=true to persist the result.

    Plugin tools can sort, pad, deduplicate, or otherwise transform
    playlist entries.
    """
    import plugins

    ctx = None
    if channel:
        from routes.channel_resolve import resolve_channel_ctx

        ctx = resolve_channel_ctx(channel)

    ch_id = ctx.id if ctx else config.DEFAULT_CHANNEL_ID
    playlist = named_playlist_store.get(name, channel_id=ch_id)
    if playlist is None:
        raise HTTPException(404, f"Playlist '{name}' not found")

    tool_info = plugins.playlist_tools().get(tool_name)
    if tool_info is None:
        available = list(plugins.playlist_tools().keys())
        raise HTTPException(
            404,
            f"Playlist tool '{tool_name}' not found. Available: {available}",
        )

    handler = tool_info.get("handler")
    if handler is None:
        raise HTTPException(500, f"Tool '{tool_name}' has no handler")

    try:
        entries = playlist.get("entries", [])
        transformed = handler.apply(entries, req.params)

        if req.save:
            named_playlist_store.save(
                name, transformed, channel_id=ch_id or config.DEFAULT_CHANNEL_ID
            )
            logger.info(
                "Playlist '%s' transformed by tool '%s' and saved (%d entries)",
                name,
                tool_name,
                len(transformed),
            )

        return {
            "ok": True,
            "playlist": name,
            "tool": tool_name,
            "saved": req.save,
            "entry_count": len(transformed),
            "entries": transformed,
        }

    except Exception as exc:
        logger.error("Playlist tool '%s' failed: %s", tool_name, exc, exc_info=True)
        raise HTTPException(500, f"Tool '{tool_name}' failed")
