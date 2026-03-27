"""Playout engine control and settings endpoints.

Settings API — runtime-configurable playout parameters:
    GET/POST  /api/playout/mode            — playlist mode (loop / schedule)
    POST      /api/playout/schedule/resync — clear overrides, resync to program
    GET/PATCH /api/playout/encoding        — video/audio encoding config
    GET/PATCH /api/playout/storage         — failover filler, media extensions

Text overlay, text presets, image bugs, and SVG overlays are provided
by the overlay plugin at /api/overlay/*.

Engine control — direct GStreamer compositor layer management:
    GET  /api/playout/health                  — engine health
    GET  /api/playout/layers/{name}           — layer status + playlist
    POST /api/playout/layers/{name}/source    — load a source on a layer
    POST /api/playout/layers/{name}/show      — show a layer
    POST /api/playout/layers/{name}/hide      — hide a layer
    POST /api/playout/layers/{name}/position  — PIP position/size
    DELETE /api/playout/layers/{name}/position — reset to full-screen
"""

from __future__ import annotations

import logging
import os
import re

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import config
import main
from models import (
    EncodingRequest,
    PlayoutModeRequest,
    StorageRequest,
)
from routes.channel_resolve import resolve_channel, resolve_channel_ctx

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/playout", tags=["playout"])

# Default layer names — source of truth is the engine's runtime config,
# this is the fallback when no engine is available.
DEFAULT_LAYER_NAMES = ("failover", "input_a", "input_b", "blinder")


def _require_engine(channel: str | None = None):
    """Return the playout engine for a channel, or raise 501.

    When channel is None, uses the default channel.
    Also supports legacy path via main.playout for backward compat.
    """
    if channel is not None:
        _, engine = resolve_channel(channel)
        return engine

    # Legacy: try main.playout first, then resolve default channel
    if main.playout is not None:
        return main.playout

    _, engine = resolve_channel(None)
    return engine


def _validate_layer(name: str, engine=None) -> str:
    """Validate layer name against the engine's current config.

    Falls back to DEFAULT_LAYER_NAMES if no engine is available
    or if the engine doesn't have config-driven layer names.
    """
    valid = list(DEFAULT_LAYER_NAMES)
    try:
        if engine is not None:
            names = engine.layer_names
            if isinstance(names, list) and names:
                valid = names
    except (AttributeError, TypeError):
        pass
    if name not in valid:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown layer '{name}'. Valid: {', '.join(valid)}",
        )
    return name


def _safe_media_path(raw: str) -> str:
    """Resolve a media path safely within MEDIA_DIR.

    Relative paths are joined to MEDIA_DIR.  Absolute paths are rejected
    to prevent reading arbitrary files from the filesystem.  Path
    traversal (``..``) is also rejected.
    """
    if os.path.isabs(raw):
        raise HTTPException(
            400,
            "Absolute paths are not allowed — use a path relative to the media directory",
        )
    if ".." in raw:
        raise HTTPException(400, "Path traversal ('..') is not allowed")
    return os.path.join(config.MEDIA_DIR, raw)


def _resolve_ctx(channel: str | None = None):
    """Get channel context, with backward compat."""
    return resolve_channel_ctx(channel)


# ── Settings API ──


@router.get("/mode")
async def get_playout_mode(channel: str | None = None):
    """Current playlist mode and scheduling config."""
    ctx = _resolve_ctx(channel)
    return {
        "mode": "loop" if ctx.playlist_loop else "schedule",
        "day_start": ctx.day_start,
        "length": "24:00:00",
    }


@router.post("/mode")
async def set_playout_mode(req: PlayoutModeRequest, channel: str | None = None):
    """Switch between loop and day-schedule mode.

    Persists the change to the channel YAML so it survives restarts.
    """
    if req.mode not in ("loop", "schedule"):
        raise HTTPException(400, "mode must be 'loop' or 'schedule'")

    # Validate day_start format if provided
    if req.day_start is not None:
        if not re.match(r"^\d{2}:\d{2}:\d{2}$", req.day_start):
            raise HTTPException(400, "day_start must be 'HH:MM:SS'")

    ctx = _resolve_ctx(channel)
    was_loop = ctx.playlist_loop
    ctx.playlist_loop = req.mode == "loop"

    if req.day_start is not None:
        ctx.day_start = req.day_start

    # Switching to schedule mode: clear block tracking so the scheduler
    # re-evaluates and loads whatever the program says for right now.
    if was_loop and not ctx.playlist_loop:
        from routes.program import _clear_scheduler_tracking

        _clear_scheduler_tracking(ctx)

    # Persist to channel YAML (non-fatal on failure)
    try:
        _persist_mode_yaml(ctx)
    except Exception as exc:
        logger.warning("Could not persist mode to YAML: %s", exc)

    logger.info("Playout mode changed to '%s' (day_start=%s)", req.mode, ctx.day_start)
    result = {"ok": True, "mode": req.mode, "day_start": ctx.day_start}
    return result


@router.post("/schedule/resync")
async def schedule_resync(channel: str | None = None):
    """Clear manual overrides and resync to the program schedule.

    Resets all scheduler block tracking so the program scheduler
    re-evaluates current blocks on its next tick (~5s).  Any
    manually loaded sources are replaced by whatever the program
    schedule dictates for the current time.  If no block is active,
    the default loop playlist is restored.
    """
    ctx = _resolve_ctx(channel)

    from routes.program import _clear_scheduler_tracking

    _clear_scheduler_tracking(ctx)

    logger.info("Schedule resync: cleared block tracking for channel '%s'", ctx.id)
    return {"ok": True, "message": "Scheduler state cleared, will resync on next tick"}


# ── Encoding ──


@router.get("/encoding")
async def get_encoding(channel: str | None = None):
    """Current video/audio encoding parameters."""
    engine = _require_engine(channel)
    cfg = engine.config
    return {
        "width": cfg.width,
        "height": cfg.height,
        "fps": float(cfg.fps),
        "bitrate": f"{cfg.video_bitrate}k",
        "preset": "ultrafast",
        "audio_bitrate": f"{cfg.audio_bitrate}k",
        "volume": 1.0,
    }


@router.patch("/encoding")
async def patch_encoding(req: EncodingRequest, channel: str | None = None):
    """Update encoding parameters and restart the engine.

    The GStreamer pipeline must be rebuilt for encoding changes to
    take effect.  The engine is stopped and restarted transparently.
    Sources must be reloaded after restart.
    """
    engine = _require_engine(channel)
    cfg = engine.config
    changed = {}

    if req.width is not None:
        cfg.width = req.width
        changed["width"] = req.width
    if req.height is not None:
        cfg.height = req.height
        changed["height"] = req.height
    if req.fps is not None:
        cfg.fps = int(req.fps)
        changed["fps"] = req.fps
    if req.bitrate is not None:
        cfg.video_bitrate = int(req.bitrate.rstrip("k"))
        changed["bitrate"] = req.bitrate
    if req.preset is not None:
        changed["preset"] = req.preset
    if req.audio_bitrate is not None:
        cfg.audio_bitrate = int(req.audio_bitrate.rstrip("k"))
        changed["audio_bitrate"] = req.audio_bitrate
    if req.volume is not None:
        changed["volume"] = req.volume

    if not changed:
        raise HTTPException(400, "No fields provided to update")

    # Restart engine with updated config
    try:
        await engine.restart(config=cfg)
        ctx = _resolve_ctx(channel)
        from scheduler import _reload_sources_after_restart

        await _reload_sources_after_restart(ctx)
        logger.info("Encoding updated: %s", changed)
    except Exception as exc:
        logger.error(
            "Engine restart after encoding change failed: %s", exc, exc_info=True
        )
        raise HTTPException(500, "Engine restart failed")

    # Count active outputs that were rebuilt with the engine
    try:
        outputs_updated = len(engine.outputs)
    except Exception:
        outputs_updated = 0

    return {
        "ok": True,
        "restarted": True,
        "outputs_updated": outputs_updated,
        "width": cfg.width,
        "height": cfg.height,
        "fps": float(cfg.fps),
        "bitrate": f"{cfg.video_bitrate}k",
        "audio_bitrate": f"{cfg.audio_bitrate}k",
    }


# ── Storage ──


@router.get("/storage")
async def get_storage(channel: str | None = None):
    """Current storage/filler settings."""
    ctx = _resolve_ctx(channel)
    failover_path = os.path.join(ctx.generated_dir, config.FAILOVER_FILENAME)
    return {
        "filler": failover_path,
        "shuffle": False,
        "extensions": ["mp4", "mkv", "webm", "avi", "ogv"],
    }


@router.patch("/storage")
async def patch_storage(req: StorageRequest, channel: str | None = None):
    """Update storage settings (partial update)."""
    changed = {}

    if req.filler is not None:
        changed["filler"] = req.filler
    if req.shuffle is not None:
        changed["shuffle"] = req.shuffle
    if req.extensions is not None:
        changed["extensions"] = req.extensions

    if not changed:
        raise HTTPException(400, "No fields provided to update")

    # Merge with current values
    ctx = _resolve_ctx(channel)
    failover_path = os.path.join(ctx.generated_dir, config.FAILOVER_FILENAME)
    result = {
        "ok": True,
        "filler": changed.get("filler", failover_path),
        "shuffle": changed.get("shuffle", False),
        "extensions": changed.get("extensions", ["mp4", "mkv", "webm", "avi", "ogv"]),
    }

    # If filler changed, reload the failover video on the engine
    if "filler" in changed:
        engine = _require_engine(channel)
        new_path = _safe_media_path(changed["filler"])
        if os.path.isfile(new_path):
            engine.failover.load_file_loop(new_path)
            logger.info("Failover video updated: %s", new_path)

    return result


# ── Engine control ──


class SourceRequest(BaseModel):
    """Request to load a source on a channel.

    Core source types: test, failover_file, failover_debug, file_loop,
    playlist, hls, image, disconnect.  Plugin source types are accepted
    via the plugin registry — extra fields are passed through to the
    plugin's factory.
    """

    model_config = {"extra": "allow"}  # accept plugin-specific fields

    type: str  # core type or plugin-registered type name

    # test source options
    pattern: str = "black"
    wave: int = 4  # 4=silence

    # file_loop / playlist source options
    path: str | None = None  # single file for file_loop
    entries: list[dict] | None = None  # [{"source": path, "duration": secs}]
    loop: bool = True

    # hls source options
    url: str | None = None

    # Plugin sources can include any additional fields (e.g. location,
    # preset, fps for html-source).  These are passed to the plugin's
    # SourceFactory.build() as the config dict.


class ShowRequest(BaseModel):
    """Optional alpha/volume overrides for show."""

    alpha: float = 1.0
    volume: float = 1.0


# ── Endpoints ──


@router.get("/health")
async def playout_health(channel: str | None = None):
    """Engine health and per-layer status."""
    engine = _require_engine(channel)
    return engine.health


class EngineConfigRequest(BaseModel):
    """Optional encoding overrides for engine start/restart."""

    width: int | None = None
    height: int | None = None
    fps: int | None = None
    video_bitrate: int | None = None
    audio_bitrate: int | None = None


# Aliases for backward-compat in OpenAPI docs
StartRequest = EngineConfigRequest
RestartRequest = EngineConfigRequest


class FailoverConfigRequest(BaseModel):
    """Failover video configuration update."""

    title: str | None = None
    subtitle: str | None = None
    duration: int | None = None
    pattern: str | None = None


class SlateConfigRequest(BaseModel):
    """Slate video configuration update."""

    title: str | None = None
    subtitle: str | None = None
    duration: int | None = None
    pattern: str | None = None


def _apply_config_overrides(cfg, req) -> None:
    """Apply optional encoding overrides from a request to a config."""
    if req is None:
        return
    if req.width is not None:
        cfg.width = req.width
    if req.height is not None:
        cfg.height = req.height
    if req.fps is not None:
        cfg.fps = req.fps
    if req.video_bitrate is not None:
        cfg.video_bitrate = req.video_bitrate
    if req.audio_bitrate is not None:
        cfg.audio_bitrate = req.audio_bitrate


@router.post("/stop")
async def stop_engine(channel: str | None = None):
    """Stop the playout engine gracefully.

    Stops all source pipelines and the mixer pipeline.  The engine
    can be restarted via POST /start or POST /restart.
    """
    engine = _require_engine(channel)
    if not engine.is_running:
        return {"ok": True, "was_running": False}
    await engine.stop()
    logger.info("Engine stopped via API")
    return {"ok": True, "was_running": True}


@router.post("/start")
async def start_engine(req: StartRequest | None = None, channel: str | None = None):
    """Start the playout engine (if stopped).

    Uses the previous config unless overridden.  Outputs are
    preserved from the previous session.  After start, failover
    and today's playlist are reloaded.
    """
    engine = _require_engine(channel)
    if engine.is_running:
        raise HTTPException(409, "Engine is already running")

    cfg = engine.config
    _apply_config_overrides(cfg, req)

    try:
        await engine.start(config=cfg)
        ctx = _resolve_ctx(channel)
        from scheduler import _reload_sources_after_restart

        await _reload_sources_after_restart(ctx)
    except Exception as exc:
        logger.error("Engine start failed: %s", exc, exc_info=True)
        raise HTTPException(500, "Engine start failed")

    return {
        "ok": True,
        "width": cfg.width,
        "height": cfg.height,
        "fps": cfg.fps,
        "video_bitrate": cfg.video_bitrate,
        "audio_bitrate": cfg.audio_bitrate,
        "outputs": list(engine.outputs.keys()),
    }


@router.post("/restart")
async def restart_engine(req: RestartRequest | None = None, channel: str | None = None):
    """Restart the playout engine.

    Optionally provide new encoding parameters.  Omitted fields keep
    their current values.  Outputs are preserved and restarted.
    After restart, failover and today's playlist are reloaded.
    """
    engine = _require_engine(channel)
    cfg = engine.config
    _apply_config_overrides(cfg, req)

    try:
        await engine.restart(config=cfg)
        ctx = _resolve_ctx(channel)
        from scheduler import _reload_sources_after_restart

        await _reload_sources_after_restart(ctx)
    except Exception as exc:
        logger.error("Engine restart failed: %s", exc, exc_info=True)
        raise HTTPException(500, "Engine restart failed")

    return {
        "ok": True,
        "width": cfg.width,
        "height": cfg.height,
        "fps": cfg.fps,
        "video_bitrate": cfg.video_bitrate,
        "audio_bitrate": cfg.audio_bitrate,
        "outputs": list(engine.outputs.keys()),
    }


# ── Layer config ──
# Must be defined BEFORE /layers/{name} to avoid route conflict.


@router.get("/layers/config")
async def get_layer_config(channel: str | None = None):
    """Get the current layer configuration.

    Returns the list of layers with their names, roles, and zorders.
    """
    engine = _require_engine(channel)
    from playout.mixer import LAYER_PRESETS

    layers = []
    for lc in engine.layer_configs_list:
        layers.append(
            {
                "name": lc.name,
                "role": lc.role,
                "zorder": lc.zorder,
            }
        )

    return {
        "layers": layers,
        "presets": list(LAYER_PRESETS.keys()),
    }


class LayerConfigRequest(BaseModel):
    """Request body for setting layer config."""

    layers: list[dict] | None = None
    preset: str | None = None


@router.put("/layers/config")
async def set_layer_config(req: LayerConfigRequest, channel: str | None = None):
    """Set a custom layer configuration.

    Provide either a preset name or a custom layers list.
    Requires an engine restart to take effect.
    """
    from playout.mixer import LAYER_PRESETS, LayerConfig, VALID_ROLES

    if req.preset:
        if req.preset not in LAYER_PRESETS:
            raise HTTPException(
                400,
                f"Unknown preset '{req.preset}'. Valid: {', '.join(LAYER_PRESETS)}",
            )
        return {
            "ok": True,
            "preset": req.preset,
            "layers": [
                {"name": lc.name, "role": lc.role} for lc in LAYER_PRESETS[req.preset]
            ],
            "message": "Restart the engine to apply this layer config.",
        }

    if not req.layers:
        raise HTTPException(400, "Provide either 'preset' or 'layers'")

    # Validate custom config
    names_seen = set()
    has_safety = False
    parsed = []
    for i, layer in enumerate(req.layers):
        name = layer.get("name")
        role = layer.get("role", "content")
        zorder = layer.get("zorder")

        if not name:
            raise HTTPException(400, f"Layer {i} missing 'name'")
        if name in names_seen:
            raise HTTPException(400, f"Duplicate layer name: '{name}'")
        if role not in VALID_ROLES:
            raise HTTPException(
                400,
                f"Invalid role '{role}' for layer '{name}'. Valid: {', '.join(sorted(VALID_ROLES))}",
            )
        if role == "safety":
            if has_safety:
                raise HTTPException(400, "Only one safety layer allowed")
            has_safety = True

        names_seen.add(name)
        parsed.append(LayerConfig(name=name, role=role, zorder=zorder))

    if not has_safety:
        raise HTTPException(400, "At least one layer must have role 'safety'")

    return {
        "ok": True,
        "layers": [{"name": lc.name, "role": lc.role} for lc in parsed],
        "message": "Restart the engine to apply this layer config.",
    }


# ── Layer source / show / hide / position ──


@router.post("/layers/{name}/source")
async def load_source(name: str, req: SourceRequest, channel: str | None = None):
    """Load a source on a channel.

    Source types:
        test           — test pattern (pattern, wave)
        failover_file  — load the failover video from storage settings
        failover_debug — live SMPTE bars with channel info (no file)
        file_loop      — single file looping (path)
        playlist       — clip sequence (entries, loop)
        hls            — HLS pull (url)
        image          — static image (path)
        disconnect     — remove current source
    """
    engine = _require_engine(channel)
    name = _validate_layer(name)
    ch = engine.channel(name)

    if req.type == "test":
        ch.load_test(pattern=req.pattern, wave=req.wave)
    elif req.type == "failover_file":
        # Load the filler clip from storage settings (failover.mp4)
        ctx = _resolve_ctx(channel)
        failover_path = os.path.join(ctx.generated_dir, config.FAILOVER_FILENAME)
        if not os.path.isfile(failover_path):
            raise HTTPException(404, f"Failover video not found: {failover_path}")
        ch.load_file_loop(failover_path)
    elif req.type == "failover_debug":
        # Load the live SMPTE debug pattern with channel info
        ctx = _resolve_ctx(channel)
        ch.load_failover(
            channel_name=ctx.display_name,
            channel_id=ctx.channel_id or "",
            description=ctx.description or "",
            origins=ctx.origins,
        )
    elif req.type == "file_loop":
        if not req.path:
            raise HTTPException(400, "path required for file_loop source")
        ch.load_file_loop(_safe_media_path(req.path))
    elif req.type == "playlist":
        if not req.entries:
            raise HTTPException(400, "entries required for playlist source")
        from playout.input_layer import PlaylistEntry

        entries = [
            PlaylistEntry(
                source=_safe_media_path(e["source"]),
                duration=e.get("duration", 0),
            )
            for e in req.entries
        ]
        ch.load_playlist(entries, loop=req.loop)
    elif req.type == "hls":
        if not req.url:
            raise HTTPException(400, "url required for hls source")
        # SSRF protection — reject private/loopback URLs
        from protocol.uri import _is_private_or_loopback
        from urllib.parse import urlparse

        parsed = urlparse(req.url)
        if parsed.hostname and _is_private_or_loopback(parsed.hostname):
            raise HTTPException(
                400, "HLS URL must not target private or loopback addresses"
            )
        ch.load_hls(req.url)
    elif req.type == "image":
        if not req.path:
            raise HTTPException(400, "path required for image source")
        ch.load_image(_safe_media_path(req.path))
    elif req.type == "disconnect":
        ch.disconnect()
    else:
        # Check plugin source type registry
        import plugins

        plugin_sources = plugins.source_types()
        if req.type in plugin_sources:
            # Pass the full request as config (plugin extracts what it needs)
            plugin_config = req.model_dump(exclude_none=True)
            ch.load_plugin_source(req.type, plugin_config)
        else:
            valid_core = [
                "test",
                "failover_file",
                "failover_debug",
                "file_loop",
                "playlist",
                "hls",
                "image",
                "disconnect",
            ]
            valid_plugin = list(plugin_sources.keys())
            all_valid = valid_core + valid_plugin
            raise HTTPException(
                400, f"Unknown source type '{req.type}'. Valid: {', '.join(all_valid)}"
            )

    return {
        "layer": name,
        "source_type": ch.source_type.value,
        "now_playing": ch.now_playing,
    }


@router.post("/layers/{name}/show")
async def show_channel(
    name: str, req: ShowRequest | None = None, channel: str | None = None
):
    """Show a channel (make it visible in the compositor)."""
    engine = _require_engine(channel)
    name = _validate_layer(name)
    alpha = req.alpha if req else 1.0
    volume = req.volume if req else 1.0
    engine.show(name, alpha=alpha, volume=volume)
    return {"layer": name, "visible": True, "active_layer": engine.active_channel}


@router.post("/layers/{name}/hide")
async def hide_channel(name: str, channel: str | None = None):
    """Hide a channel (set alpha=0, volume=0)."""
    engine = _require_engine(channel)
    name = _validate_layer(name)
    engine.hide(name)
    return {"layer": name, "visible": False, "active_layer": engine.active_channel}


@router.get("/layers/{name}")
async def channel_status(name: str, channel: str | None = None):
    """Per-layer status: source type, visibility, now_playing, playlist info."""
    engine = _require_engine(channel)
    name = _validate_layer(name, engine)
    ch = engine.channel(name)

    # Read visibility from live GStreamer mixer pads (config-driven index)
    idx = engine._layer_map.get(name, 0)
    vis = engine.layer_visibility(idx)

    result = {
        "layer": name,
        "source_type": ch.source_type.value,
        "visible": vis["visible"],
        "alpha": vis["alpha"],
        "volume": vis["volume"],
        "now_playing": ch.now_playing,
        "played": round(ch.played, 2),
    }

    # Playlist details when applicable
    if ch.source_type.value in ("playlist", "file_loop"):
        entries = [
            {"source": e.source, "duration": e.duration} for e in ch._playlist_entries
        ]
        result["playlist"] = {
            "entries": entries,
            "total": len(entries),
            "current_index": ch._display_index,
            "loop": ch._playlist_loop,
        }
        result["playlist_name"] = ch._playlist_name

    # Data age for live sources
    if ch.source_type.value in ("hls", "srt"):
        result["data_age"] = round(ch.data_age, 2)

    return result


class PositionRequest(BaseModel):
    """PIP position and size."""

    x: int = 0
    y: int = 0
    width: int = 0
    height: int = 0


@router.post("/layers/{name}/position")
async def set_position(name: str, req: PositionRequest, channel: str | None = None):
    """Set channel position/size on the compositor for PIP layouts.

    Set width=0 and height=0 to reset to full-screen.
    """
    engine = _require_engine(channel)
    name = _validate_layer(name)
    engine.set_position(name, x=req.x, y=req.y, w=req.width, h=req.height)
    return {
        "layer": name,
        "x": req.x,
        "y": req.y,
        "width": req.width,
        "height": req.height,
    }


@router.delete("/layers/{name}/position")
async def reset_position(name: str, channel: str | None = None):
    """Reset channel to full-screen (undo PIP)."""
    engine = _require_engine(channel)
    name = _validate_layer(name)
    engine.reset_position(name)
    return {"layer": name, "position": "full-screen"}


# ── Failover / Slate customization ──


@router.get("/failover")
async def get_failover_config(channel: str | None = None):
    """Get failover video configuration."""
    ctx = _resolve_ctx(channel)
    return {
        "title": ctx.failover_title or ctx.display_name,
        "subtitle": ctx.failover_subtitle or "",
        "duration": ctx.failover_duration,
        "pattern": ctx.failover_pattern,
    }


@router.patch("/failover")
async def set_failover_config(req: FailoverConfigRequest, channel: str | None = None):
    """Update failover video configuration.

    Accepts any subset of: title, subtitle, duration, pattern.
    Changes are persisted to channel YAML.  To regenerate the
    failover video with new settings, delete the existing file
    and restart, or call POST /api/playout/failover/regenerate.
    """
    ctx = _resolve_ctx(channel)
    fields = req.model_dump(exclude_none=True)

    if "title" in fields:
        ctx.failover_title = fields["title"] or None
    if "subtitle" in fields:
        ctx.failover_subtitle = fields["subtitle"] or None
    if "duration" in fields:
        ctx.failover_duration = max(10, min(3600, int(fields["duration"])))
    if "pattern" in fields:
        ctx.failover_pattern = fields["pattern"]

    try:
        _persist_failover_slate_yaml(ctx)
    except Exception as exc:
        logger.warning("Failed to persist failover config: %s", exc)

    return await get_failover_config(channel)


@router.post("/failover/regenerate")
async def regenerate_failover(channel: str | None = None):
    """Delete and regenerate the failover video with current settings."""
    import scheduler

    ctx = _resolve_ctx(channel)
    failover_path = os.path.join(ctx.generated_dir, config.FAILOVER_FILENAME)

    # Delete existing file so ensure_failover_video() regenerates it
    if os.path.isfile(failover_path):
        os.unlink(failover_path)

    scheduler.ensure_failover_video(ctx)

    # Reload on safety layer if engine is running
    engine = main.playout
    if engine and engine.is_running and os.path.isfile(failover_path):
        engine.failover.load_file_loop(failover_path)
        logger.info("Failover video regenerated and reloaded")

    return {"ok": True, "path": failover_path}


@router.get("/slate")
async def get_slate_config(channel: str | None = None):
    """Get slate video configuration."""
    ctx = _resolve_ctx(channel)
    return {
        "title": ctx.slate_title or ctx.display_name,
        "subtitle": ctx.slate_subtitle,
        "duration": ctx.slate_duration,
        "pattern": ctx.slate_pattern,
    }


@router.patch("/slate")
async def set_slate_config(req: SlateConfigRequest, channel: str | None = None):
    """Update slate video configuration.

    Accepts any subset of: title, subtitle, duration, pattern.
    Changes are persisted to channel YAML.  To regenerate the
    slate video with new settings, call POST /api/playout/slate/regenerate.
    """
    ctx = _resolve_ctx(channel)
    fields = req.model_dump(exclude_none=True)

    if "title" in fields:
        ctx.slate_title = fields["title"] or None
    if "subtitle" in fields:
        ctx.slate_subtitle = fields["subtitle"] or ""
    if "duration" in fields:
        ctx.slate_duration = max(10, min(3600, int(fields["duration"])))
    if "pattern" in fields:
        ctx.slate_pattern = fields["pattern"]

    try:
        _persist_failover_slate_yaml(ctx)
    except Exception as exc:
        logger.warning("Failed to persist slate config: %s", exc)

    return await get_slate_config(channel)


@router.post("/slate/regenerate")
async def regenerate_slate(channel: str | None = None):
    """Delete and regenerate the slate video with current settings."""
    import scheduler

    ctx = _resolve_ctx(channel)
    slate_path = os.path.join(ctx.generated_dir, config.SLATE_FILENAME)

    if os.path.isfile(slate_path):
        os.unlink(slate_path)

    scheduler.ensure_slate_video(ctx)

    # Reload on input_a if engine is running and currently showing slate
    engine = main.playout
    if engine and engine.is_running and os.path.isfile(slate_path):
        # Only reload if input_a is currently playing the slate
        health = engine.health
        active = health.get("active_channel", "")
        if active == "input_a":
            ch_info = health.get("channels", {}).get("input_a", {})
            np = ch_info.get("now_playing", {})
            if np and "slate" in str(np.get("source", "")):
                engine.input_a.load_file_loop(slate_path)
                logger.info("Slate video regenerated and reloaded")

    return {"ok": True, "path": slate_path}


# ── Helpers ──


def _update_channel_yaml(ctx, updater_fn) -> None:
    """Read-modify-write a channel YAML config file."""
    from pathlib import Path

    import yaml  # type: ignore[import]

    config_dir = Path(config.CHANNEL_CONFIG_DIR)
    yaml_path = config_dir / f"{ctx.id}.yaml"
    if not yaml_path.exists():
        return

    with open(yaml_path) as f:
        doc = yaml.safe_load(f)

    if not isinstance(doc, dict):
        return

    updater_fn(doc)

    with open(yaml_path, "w") as f:
        yaml.dump(doc, f, default_flow_style=False, allow_unicode=True)


def _persist_failover_slate_yaml(ctx) -> None:
    """Write failover and slate config to the channel YAML config file."""

    def _update(doc):
        failover = doc.setdefault("failover", {})
        failover["title"] = ctx.failover_title
        failover["subtitle"] = ctx.failover_subtitle
        failover["duration"] = ctx.failover_duration
        failover["pattern"] = ctx.failover_pattern

        slate = doc.setdefault("slate", {})
        slate["title"] = ctx.slate_title
        slate["subtitle"] = ctx.slate_subtitle
        slate["duration"] = ctx.slate_duration
        slate["pattern"] = ctx.slate_pattern

    _update_channel_yaml(ctx, _update)
    logger.debug("Persisted failover/slate config for channel '%s'", ctx.id)


def _persist_mode_yaml(ctx) -> None:
    """Write playlist_loop and day_start to the channel YAML config file."""

    def _update(doc):
        doc["playlist_loop"] = ctx.playlist_loop
        doc["day_start"] = ctx.day_start

    _update_channel_yaml(ctx, _update)
    logger.debug("Persisted mode for channel '%s'", ctx.id)
