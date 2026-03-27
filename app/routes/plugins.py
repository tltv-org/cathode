"""Plugin management endpoints — /api/plugins/*.

Provides visibility into loaded and disabled plugins, their services,
settings, categories, and registered extensions.  Plugins are loaded
at startup; enable/disable requires an engine restart.

Preset CRUD is available for plugins that register a preset provider.
Media generation is available for plugins that register a generate handler.
"""

from __future__ import annotations

import logging
import os
import re

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import config
import plugins

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/plugins", tags=["plugins"])

# Preset names: alphanumeric, hyphens, underscores, 1-64 chars
_PRESET_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$")


class PluginSettingsUpdate(BaseModel):
    """Partial settings update for a plugin."""

    settings: dict


class PresetSaveRequest(BaseModel):
    """Create or update a plugin preset.

    The exact fields depend on the plugin type:
    - HTML presets: ``content`` (HTML string) + optional ``description``
    - JSON presets: any config dict fields + optional ``description``
    - Python presets: ``content`` (script source) + optional ``description``
    """

    model_config = {"extra": "allow"}


class GenerateRequest(BaseModel):
    """Generate a media file from a source plugin.

    Renders a source plugin to a media file that appears in the
    library.  Duration is in seconds.  If no filename is provided,
    one is generated from the preset name and timestamp.
    """

    preset: str | None = None
    duration: int = 30  # seconds
    filename: str | None = None  # output filename (auto-generated if omitted)
    width: int | None = None  # override default resolution
    height: int | None = None
    fps: int | None = None

    model_config = {"extra": "allow"}  # plugin-specific params


@router.get("")
async def list_plugins():
    """List all plugins (loaded + disabled) with summary info.

    Returns plugin name, category, enabled/loaded status, registered
    services, settings, and extensions for each plugin.
    """
    return {
        "plugins": plugins.all_plugin_details(),
        "loaded": len(plugins.loaded_plugins()),
        "disabled": len(plugins.disabled_plugins()),
    }


@router.get("/{name}")
async def get_plugin(name: str):
    """Get detailed info for a specific plugin."""
    if plugins.is_disabled(name):
        return {
            "name": name,
            "enabled": False,
            "loaded": False,
            "message": "Plugin is disabled. Enable it and restart the engine.",
        }

    if not plugins.has_plugin(name):
        raise HTTPException(404, f"Plugin '{name}' is not loaded")

    info = plugins.plugin_info(name)
    settings = info.get("settings") or {}

    return {
        "name": name,
        "enabled": True,
        "loaded": True,
        "category": info.get("category"),
        "settings": {k: v for k, v in settings.items()},
        "has_shutdown": info.get("shutdown") is not None,
        "has_tasks": bool(info.get("tasks")),
        "system_deps": info.get("system_deps") or [],
    }


@router.get("/{name}/settings")
async def get_plugin_settings(name: str):
    """Get current settings for a plugin."""
    if not plugins.has_plugin(name):
        raise HTTPException(404, f"Plugin '{name}' is not loaded")

    settings = plugins.plugin_settings(name)
    if not settings:
        return {
            "name": name,
            "settings": {},
            "message": "Plugin has no configurable settings",
        }

    return {"name": name, "settings": settings}


@router.patch("/{name}/settings")
async def patch_plugin_settings(name: str, req: PluginSettingsUpdate):
    """Update plugin settings at runtime.

    Only updates the keys provided.  The plugin is notified of changes
    via its on_settings_changed callback (if registered).
    """
    if not plugins.has_plugin(name):
        raise HTTPException(404, f"Plugin '{name}' is not loaded")

    if not req.settings:
        raise HTTPException(400, "No settings provided")

    try:
        updated = await plugins.update_plugin_settings(name, req.settings)
    except KeyError as exc:
        raise HTTPException(404, str(exc))
    except ValueError as exc:
        raise HTTPException(400, str(exc))

    return {"ok": True, "name": name, "settings": updated}


@router.post("/{name}/enable")
async def enable_plugin(name: str):
    """Enable a disabled plugin.

    Removes the .disabled marker file.  The plugin will be loaded on
    the next engine restart.
    """
    if plugins.has_plugin(name):
        return {"ok": True, "name": name, "message": "Plugin is already loaded"}

    if plugins.enable_plugin(name):
        return {
            "ok": True,
            "name": name,
            "message": "Plugin enabled. Restart the engine to load it.",
        }

    raise HTTPException(404, f"Plugin '{name}' not found in plugins directory")


@router.post("/{name}/disable")
async def disable_plugin(name: str):
    """Disable a loaded or enabled plugin.

    Creates a .disabled marker file.  The plugin will be skipped on
    the next engine restart.
    """
    if plugins.disable_plugin(name):
        return {
            "ok": True,
            "name": name,
            "message": "Plugin disabled. Restart the engine to unload it.",
        }

    raise HTTPException(404, f"Plugin '{name}' not found in plugins directory")


# ── Extension registry endpoints ──


@router.get("/registry/source-types")
async def list_source_types():
    """List all plugin-registered source types.

    These are in addition to the core source types (test, failover,
    file_loop, playlist, image, hls, disconnect).
    """
    types = {}
    for type_name, info in plugins.source_types().items():
        types[type_name] = {
            "description": info.get("description", ""),
            "params": info.get("params", {}),
            "plugin": info.get("plugin", ""),
        }
    return {"source_types": types}


@router.get("/registry/output-types")
async def list_output_types():
    """List all plugin-registered output types.

    These are in addition to the core output types (hls, rtmp, file, null).
    """
    types = {}
    for type_name, info in plugins.output_types().items():
        types[type_name] = {
            "description": info.get("description", ""),
            "params": info.get("params", {}),
            "plugin": info.get("plugin", ""),
        }
    return {"output_types": types}


@router.get("/registry/block-types")
async def list_block_types():
    """List all plugin-registered schedule block types.

    These are in addition to the core block types (playlist, file,
    image, redirect, flex).
    """
    types = {}
    for type_name, info in plugins.block_types().items():
        types[type_name] = {
            "description": info.get("description", ""),
            "params": info.get("params", {}),
            "plugin": info.get("plugin", ""),
        }
    return {"block_types": types}


@router.get("/registry/playlist-tools")
async def list_playlist_tools():
    """List all plugin-registered playlist tools."""
    tools = {}
    for tool_name, info in plugins.playlist_tools().items():
        tools[tool_name] = {
            "description": info.get("description", ""),
            "params": info.get("params", {}),
            "plugin": info.get("plugin", ""),
        }
    return {"playlist_tools": tools}


# ── Preset CRUD endpoints ──


def _get_preset_provider(name: str):
    """Get a plugin's preset provider or raise appropriate HTTP error."""
    if not plugins.has_plugin(name):
        raise HTTPException(404, f"Plugin '{name}' is not loaded")
    provider = plugins.plugin_presets(name)
    if provider is None:
        raise HTTPException(404, f"Plugin '{name}' does not provide presets")
    return provider


@router.get("/{name}/presets")
async def list_presets(name: str):
    """List all presets for a plugin.

    Returns metadata only (name, description, category).  Use
    ``GET /api/plugins/{name}/presets/{preset}`` for full content.
    """
    provider = _get_preset_provider(name)
    return {"plugin": name, "presets": provider.list()}


@router.get("/{name}/presets/{preset_name}")
async def get_preset(name: str, preset_name: str):
    """Get a single preset with full content."""
    provider = _get_preset_provider(name)
    preset = provider.get(preset_name)
    if preset is None:
        raise HTTPException(404, f"Preset '{preset_name}' not found")
    return {"plugin": name, "preset": preset}


@router.put("/{name}/presets/{preset_name}")
async def save_preset(name: str, preset_name: str, req: PresetSaveRequest):
    """Create or update a preset.

    The request body fields are plugin-specific.  Common fields:
    ``content`` (HTML/Python source), ``description``, ``config`` (JSON).
    """
    provider = _get_preset_provider(name)

    if not _PRESET_NAME_RE.match(preset_name):
        raise HTTPException(
            400,
            "Preset name must be 1-64 chars: alphanumeric, hyphens, underscores",
        )

    data = req.model_dump(exclude_none=True)
    try:
        preset = provider.save(preset_name, data)
    except Exception as exc:
        logger.error(
            "Failed to save preset '%s' for plugin '%s': %s", preset_name, name, exc
        )
        raise HTTPException(500, "Failed to save preset")

    return {"ok": True, "plugin": name, "preset": preset}


@router.delete("/{name}/presets/{preset_name}")
async def delete_preset(name: str, preset_name: str):
    """Delete a preset."""
    provider = _get_preset_provider(name)

    if not provider.delete(preset_name):
        raise HTTPException(404, f"Preset '{preset_name}' not found")

    return {"ok": True, "plugin": name, "deleted": preset_name}


# ── Media generation endpoint ──


@router.post("/{name}/generate")
async def generate_media(name: str, req: GenerateRequest):
    """Generate a media file from a source plugin.

    Renders a plugin source (by preset or params) to a media file.
    The file is placed in the media directory and appears in the
    library.  This is a synchronous operation — the response is
    returned when generation is complete.

    Requires the plugin to register a ``generate`` handler.
    """
    if not plugins.has_plugin(name):
        raise HTTPException(404, f"Plugin '{name}' is not loaded")

    generate_fn = plugins.plugin_generator(name)
    if generate_fn is None:
        raise HTTPException(
            400,
            f"Plugin '{name}' does not support media generation",
        )

    # Validate duration
    if req.duration < 1 or req.duration > 3600:
        raise HTTPException(400, "Duration must be 1-3600 seconds")

    # Validate filename if provided
    if req.filename:
        if not _PRESET_NAME_RE.match(req.filename.rsplit(".", 1)[0]):
            raise HTTPException(400, "Invalid filename")
        if not req.filename.endswith(".mp4"):
            req.filename += ".mp4"

    # Build config for the plugin's generate handler.
    # Each plugin writes to its own subfolder: {MEDIA_DIR}/{plugin-name}/
    plugin_media_dir = os.path.join(config.MEDIA_DIR, name)
    os.makedirs(plugin_media_dir, exist_ok=True)

    generate_config = req.model_dump(exclude_none=True)
    generate_config["media_dir"] = plugin_media_dir
    generate_config.setdefault("width", config.DEFAULT_WIDTH)
    generate_config.setdefault("height", config.DEFAULT_HEIGHT)
    generate_config.setdefault("fps", config.DEFAULT_FPS)

    try:
        result = await generate_fn(generate_config)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    except TimeoutError as exc:
        logger.error(
            "Generation timed out for plugin '%s': %s", name, exc, exc_info=True
        )
        raise HTTPException(504, "Generation timed out")
    except Exception as exc:
        logger.error("Generation failed for plugin '%s': %s", name, exc, exc_info=True)
        raise HTTPException(500, "Generation failed")

    return {"ok": True, "plugin": name, **result}
