"""Plugin loader — discovers and loads optional cathode plugins.

Plugins extend cathode with additional routes, services, and background
tasks.  Cathode works without any plugins installed, serving only the
TLTV protocol and basic playlist streaming.

Discovery order:
  1. Drop-in modules in /app/plugins/  (scanned alphabetically)
  2. pip packages with entry point group 'cathode.plugins'

Drop-in takes precedence: if a plugin named "renderer" exists in both,
the drop-in version wins.

Each plugin must expose a register(app, config) function that returns
an optional dict::

    {
        # --- Identity & metadata ---
        "category": "source",          # source | content | schedule | graphics
                                       # | output | integration

        # --- Core contract (unchanged) ---
        "services": {"name": instance},
        "shutdown": async_cleanup_fn,
        "tasks": [coroutine_fn],
        "settings": {"key": {"type": "str", "value": ..., "description": ...}},
        "on_settings_changed": async_callback(settings_dict),

        # --- Engine extensions (require engine restart) ---
        "source_types": {"html": {"factory": ..., "description": ..., "params": ...}},
        "output_types": {"srt": {"factory": ..., "description": ..., "params": ...}},
        "block_types":  {"auto-bumper": {"handler": ..., "description": ..., "params": ...}},
        "overlay_elements": [("gdkpixbufoverlay", "bug-overlay", {"alpha": 0.0})],
        "layers": [{"name": "graphics", "role": "overlay"}],
        "playlist_tools": {"sort": {"handler": ..., "description": ..., "params": ...}},

        # --- Presets (CRUD via /api/plugins/{name}/presets) ---
        "presets": FilePresetStore("/app/plugins/my-plugin/presets", ".json"),
        # Any object with list()/get(name)/save(name,data)/delete(name) methods.
        # FilePresetStore is a convenience helper for filesystem-backed presets.

        # --- Media generation (POST /api/plugins/{name}/generate) ---
        "generate": async_generate_fn,
        # async callable(config: dict) -> dict.  Renders a source to a media file.
        # config keys: preset, duration, filename, width, height, fps.
        # Returns: {filename, path, duration}.

        # --- Documentation ---
        "system_deps": ["libwpe-1.0-1"],
    }

Plugin names use the directory/entry-point name as-is (e.g. "html-source",
"ffmpeg-gen", "auto-schedule").  Hyphens are valid in plugin names.

Categories:
  source      — new input types for playout layers
  content     — produce/manage media files
  schedule    — auto-populate program blocks
  graphics    — inline overlay on the mix (post-compositor)
  output      — new output destinations
  integration — external service bridges (webhook, metrics)

A plugin may declare multiple categories as a comma-separated string
(e.g. "source,content") if it spans boundaries.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.metadata
import importlib.util
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

# Valid plugin categories
VALID_CATEGORIES = {
    "source",
    "content",
    "schedule",
    "graphics",
    "output",
    "integration",
}

# ── Plugin app wrapper ──


class PluginApp:
    """Wrapper around FastAPI app that enforces plugin route namespacing.

    Plugins receive this instead of the raw FastAPI app.  When a plugin
    calls app.include_router(router), the wrapper forces the prefix to
    /api/plugins/{plugin_name}/ regardless of what the plugin set.

    All other attribute access is proxied to the real app.
    """

    def __init__(self, app: Any, plugin_name: str):
        self._app = app
        self._plugin_name = plugin_name
        self._prefix = f"/api/plugins/{plugin_name}"

    def include_router(self, router: Any, **kwargs) -> None:
        """Include a router with enforced plugin prefix.

        Rewrites all route paths from the plugin's chosen prefix to
        the enforced /api/plugins/{name}/ namespace.  FastAPI bakes
        the prefix into route paths at definition time, so we must
        rewrite each route's path directly.
        """
        old_prefix = getattr(router, "prefix", "") or ""

        # Rewrite each route's path: /api/overlay/text → /api/plugins/overlay/text
        for route in router.routes:
            if (
                hasattr(route, "path")
                and old_prefix
                and route.path.startswith(old_prefix)
            ):
                route.path = self._prefix + route.path[len(old_prefix) :]
            elif hasattr(route, "path") and not route.path.startswith("/api/plugins/"):
                route.path = self._prefix + route.path

        # Clear the prefix so include_router doesn't double-prefix
        router.prefix = ""

        # Force OpenAPI tag
        kwargs.setdefault("tags", ["plugins"])

        self._app.include_router(router, **kwargs)
        logger.info(
            "Plugin '%s' routes at %s (was %s)",
            self._plugin_name,
            self._prefix,
            old_prefix,
        )

    def __getattr__(self, name: str) -> Any:
        """Proxy all other attribute access to the real app."""
        return getattr(self._app, name)


# ── Preset provider protocol & helper ──


@runtime_checkable
class PresetProvider(Protocol):
    """Protocol for plugin preset CRUD.

    Plugins return an object implementing this protocol as ``"presets"``
    in their ``register()`` return dict.  Cathode core routes
    ``/api/plugins/{name}/presets`` to these methods.

    Each preset is a dict with at minimum ``"name"`` (str).  Additional
    keys (``description``, ``category``, ``content``, ``config``,
    ``params``) are plugin-specific.
    """

    def list(self) -> list[dict[str, Any]]:
        """Return all available presets (metadata only, no content)."""
        ...

    def get(self, name: str) -> dict[str, Any] | None:
        """Return a preset with full content, or None if not found."""
        ...

    def save(self, name: str, data: dict[str, Any]) -> dict[str, Any]:
        """Create or update a preset.  Returns the saved preset."""
        ...

    def delete(self, name: str) -> bool:
        """Delete a preset.  Returns True if it existed."""
        ...


class FilePresetStore:
    """Filesystem-backed preset store for plugins.

    Common helper for plugins that store presets as files in a directory.
    Handles JSON, HTML, and Python preset files with consistent CRUD
    semantics.  Plugins instantiate this and return it as ``"presets"``
    from ``register()``.

    JSON presets: stored as ``{name}.json``.  The file contents are the
    preset config dict.  ``description`` is read from the dict if present.

    HTML presets: stored as ``{name}.html``.  Metadata (description) is
    stored in a sidecar ``{name}.meta.json`` file if present.

    Python presets: stored as ``{name}.py``.  Description extracted from
    the module docstring.  Save creates the file; content is the script
    source.

    Usage in a plugin::

        from plugins import FilePresetStore

        def register(app, config):
            presets = FilePresetStore(
                directory="/app/plugins/my-plugin/presets",
                extension=".json",
            )
            return {"presets": presets, ...}
    """

    def __init__(
        self,
        directory: str | Path,
        extension: str = ".json",
        category: str | None = None,
    ):
        self._dir = Path(directory)
        self._ext = extension
        self._category = category  # default category for all presets

    def list(self) -> list[dict[str, Any]]:
        """List all presets (metadata only)."""
        presets: list[dict[str, Any]] = []
        if not self._dir.exists():
            return presets
        for path in sorted(self._dir.glob(f"*{self._ext}")):
            if path.stem.endswith(".meta"):
                continue
            preset = self._read_metadata(path)
            if preset:
                presets.append(preset)
        return presets

    def get(self, name: str) -> dict[str, Any] | None:
        """Get a preset with full content."""
        path = self._dir / f"{name}{self._ext}"
        if not path.exists():
            return None
        return self._read_full(path)

    def save(self, name: str, data: dict[str, Any]) -> dict[str, Any]:
        """Create or update a preset."""
        self._dir.mkdir(parents=True, exist_ok=True)
        path = self._dir / f"{name}{self._ext}"

        if self._ext == ".json":
            # Store the full data dict as JSON
            save_data = {k: v for k, v in data.items() if k != "name"}
            with open(path, "w") as f:
                json.dump(save_data, f, indent=2)
        elif self._ext == ".html":
            content = data.get("content", data.get("html", ""))
            path.write_text(content)
            # Write sidecar metadata if description provided
            desc = data.get("description")
            if desc:
                meta_path = self._dir / f"{name}.meta.json"
                with open(meta_path, "w") as f:
                    json.dump({"description": desc}, f, indent=2)
        else:
            # .py or other text formats
            content = data.get("content", data.get("code", ""))
            path.write_text(content)
            desc = data.get("description")
            if desc:
                meta_path = self._dir / f"{name}.meta.json"
                with open(meta_path, "w") as f:
                    json.dump({"description": desc}, f, indent=2)

        return self.get(name) or {"name": name}

    def delete(self, name: str) -> bool:
        """Delete a preset and its sidecar metadata."""
        path = self._dir / f"{name}{self._ext}"
        if not path.exists():
            return False
        path.unlink()
        # Clean up sidecar metadata
        meta_path = self._dir / f"{name}.meta.json"
        if meta_path.exists():
            meta_path.unlink()
        return True

    def _read_metadata(self, path: Path) -> dict[str, Any]:
        """Read preset metadata (no full content)."""
        name = path.stem
        preset: dict[str, Any] = {"name": name}
        if self._category:
            preset["category"] = self._category

        try:
            if self._ext == ".json":
                with open(path) as f:
                    data = json.load(f)
                preset["description"] = data.get("description", "")
                # Include param keys for UI hints
                params = data.get("params") or data.get("parameters")
                if params:
                    preset["params"] = params
            elif self._ext == ".html":
                preset["description"] = self._read_sidecar_desc(path)
            elif self._ext == ".py":
                preset["description"] = self._read_py_docstring(path)
            else:
                preset["description"] = self._read_sidecar_desc(path)
        except Exception as exc:
            logger.debug("Preset read error for %s: %s", path, exc)
            preset["description"] = ""

        return preset

    def _read_full(self, path: Path) -> dict[str, Any]:
        """Read preset with full content."""
        name = path.stem
        preset: dict[str, Any] = {"name": name}
        if self._category:
            preset["category"] = self._category

        try:
            if self._ext == ".json":
                with open(path) as f:
                    data = json.load(f)
                preset.update(data)
            elif self._ext == ".html":
                preset["content"] = path.read_text()
                preset["description"] = self._read_sidecar_desc(path)
            elif self._ext == ".py":
                preset["content"] = path.read_text()
                preset["description"] = self._read_py_docstring(path)
            else:
                preset["content"] = path.read_text()
                preset["description"] = self._read_sidecar_desc(path)
        except Exception as exc:
            logger.debug("Preset read error for %s: %s", path, exc)

        return preset

    def _read_sidecar_desc(self, path: Path) -> str:
        """Read description from a sidecar .meta.json file."""
        meta_path = path.parent / f"{path.stem}.meta.json"
        if meta_path.exists():
            try:
                with open(meta_path) as f:
                    return json.load(f).get("description", "")
            except Exception:
                pass
        return ""

    @staticmethod
    def _read_py_docstring(path: Path) -> str:
        """Extract the module docstring from a Python file."""
        try:
            source = path.read_text()
            # Simple extraction: look for triple-quoted string at top
            for quote in ('"""', "'''"):
                if quote in source[:500]:
                    start = source.index(quote) + 3
                    end = source.index(quote, start)
                    return source[start:end].strip()
        except Exception:
            pass
        return ""


# ── Module state ──

_plugins: dict[str, dict[str, Any]] = {}  # name -> register() return value
_disabled: set[str] = set()  # names of discovered-but-disabled plugins
_services: dict[str, Any] = {}  # service_name -> instance
_tasks: list[asyncio.Task] = []  # background tasks from plugins
_shutdown_hooks: list[Callable] = []  # async cleanup functions

# ── Extension registries (populated during load, read by engine) ──

_source_types: dict[
    str, dict[str, Any]
] = {}  # type_name -> {factory, description, params, plugin}
_output_types: dict[
    str, dict[str, Any]
] = {}  # type_name -> {factory, description, params, plugin}
_block_types: dict[
    str, dict[str, Any]
] = {}  # type_name -> {handler, description, params, plugin}
_overlay_elements: list[
    tuple[str, str, dict, str]
] = []  # [(factory, name, props, plugin)]
_plugin_layers: list[dict[str, Any]] = []  # [{name, role, plugin}]
_playlist_tools: dict[
    str, dict[str, Any]
] = {}  # tool_name -> {handler, description, params, plugin}
_presets: dict[str, Any] = {}  # plugin_name -> PresetProvider instance
_generators: dict[str, Any] = {}  # plugin_name -> async generate(config) callable


# ── Public API ──


def has_plugin(name: str) -> bool:
    """Check whether a plugin is loaded."""
    return name in _plugins


def is_disabled(name: str) -> bool:
    """Check whether a discovered plugin is disabled."""
    return name in _disabled


def get_service(name: str) -> Any | None:
    """Get a service registered by a plugin.  Returns None if not found."""
    return _services.get(name)


def get_service_or_raise(name: str) -> Any:
    """Get a service or raise RuntimeError if the required plugin isn't loaded."""
    svc = _services.get(name)
    if svc is None:
        raise RuntimeError(f"Service '{name}' not available (is the plugin installed?)")
    return svc


def loaded_plugins() -> list[str]:
    """Return names of all loaded plugins."""
    return list(_plugins.keys())


def disabled_plugins() -> list[str]:
    """Return names of all discovered-but-disabled plugins."""
    return list(_disabled)


def plugin_info(name: str) -> dict[str, Any] | None:
    """Return the full register() return dict for a plugin, or None."""
    return _plugins.get(name)


def plugin_category(name: str) -> str | None:
    """Return the category string for a loaded plugin."""
    info = _plugins.get(name)
    if info is None:
        return None
    return info.get("category")


def plugin_services(name: str) -> dict[str, Any]:
    """Return the services dict registered by a plugin."""
    info = _plugins.get(name, {})
    return dict(info.get("services") or {})


def plugin_settings(name: str) -> dict[str, Any]:
    """Return the current settings dict for a plugin.

    Returns empty dict if the plugin has no settings or is not loaded.
    """
    info = _plugins.get(name, {})
    return dict(info.get("settings") or {})


# ── Extension registry accessors ──


def source_types() -> dict[str, dict[str, Any]]:
    """Return all registered plugin source types."""
    return dict(_source_types)


def output_types() -> dict[str, dict[str, Any]]:
    """Return all registered plugin output types."""
    return dict(_output_types)


def block_types() -> dict[str, dict[str, Any]]:
    """Return all registered plugin block types."""
    return dict(_block_types)


def overlay_elements() -> list[tuple[str, str, dict, str]]:
    """Return all registered overlay elements [(factory, name, props, plugin)]."""
    return list(_overlay_elements)


def plugin_layers() -> list[dict[str, Any]]:
    """Return all plugin-contributed layer configs."""
    return list(_plugin_layers)


def playlist_tools() -> dict[str, dict[str, Any]]:
    """Return all registered playlist tools."""
    return dict(_playlist_tools)


def plugin_presets(name: str) -> Any | None:
    """Return the PresetProvider for a plugin, or None if it has no presets."""
    return _presets.get(name)


def plugin_generator(name: str) -> Any | None:
    """Return the generate callable for a plugin, or None."""
    return _generators.get(name)


async def update_plugin_settings(name: str, updates: dict[str, Any]) -> dict[str, Any]:
    """Update plugin settings and notify the plugin.

    Returns the full updated settings dict.
    Raises KeyError if plugin not loaded, ValueError if key unknown.
    """
    if name not in _plugins:
        raise KeyError(f"Plugin '{name}' is not loaded")

    info = _plugins[name]
    settings = info.get("settings")
    if not settings:
        raise ValueError(f"Plugin '{name}' has no configurable settings")

    for key, value in updates.items():
        if key not in settings:
            raise ValueError(f"Unknown setting '{key}' for plugin '{name}'")
        settings[key]["value"] = value

    # Notify plugin of changes
    callback = info.get("on_settings_changed")
    if callback:
        # Build a flat {key: value} dict for the callback
        flat = {k: v["value"] for k, v in settings.items()}
        await callback(flat)

    return dict(settings)


def all_plugin_details() -> list[dict[str, Any]]:
    """Return summary info for all loaded and disabled plugins.

    Used by the /api/plugins management endpoint.
    """
    details = []
    for name, info in _plugins.items():
        settings = info.get("settings") or {}
        has_shutdown = info.get("shutdown") is not None
        has_tasks = bool(info.get("tasks"))
        detail: dict[str, Any] = {
            "name": name,
            "enabled": True,
            "loaded": True,
            "category": info.get("category"),
            "settings": {k: v for k, v in settings.items()},
            "has_shutdown": has_shutdown,
            "has_tasks": has_tasks,
        }
        # Report what extensions this plugin provides
        source_t = list((info.get("source_types") or {}).keys())
        output_t = list((info.get("output_types") or {}).keys())
        block_t = list((info.get("block_types") or {}).keys())
        tools = list((info.get("playlist_tools") or {}).keys())
        overlays = [el[1] for el in (info.get("overlay_elements") or [])]
        layers = [lc.get("name") for lc in (info.get("layers") or [])]
        sys_deps = info.get("system_deps") or []
        has_presets = name in _presets
        has_generate = name in _generators
        extensions: dict[str, Any] = {}
        if source_t:
            extensions["source_types"] = source_t
        if output_t:
            extensions["output_types"] = output_t
        if block_t:
            extensions["block_types"] = block_t
        if tools:
            extensions["playlist_tools"] = tools
        if overlays:
            extensions["overlay_elements"] = overlays
        if layers:
            extensions["layers"] = layers
        if has_presets:
            extensions["has_presets"] = True
        if has_generate:
            extensions["has_generate"] = True
        if extensions:
            detail["extensions"] = extensions
        if sys_deps:
            detail["system_deps"] = sys_deps
        details.append(detail)

    # Include disabled plugins so the UI shows them
    for name in sorted(_disabled):
        details.append(
            {
                "name": name,
                "enabled": False,
                "loaded": False,
                "category": None,
            }
        )

    return details


def register_service(name: str, instance: Any, plugin_name: str | None = None) -> None:
    """Register a service directly.

    Used by tests to inject mock services without loading real plugins.
    If plugin_name is provided and not already tracked, marks it as loaded.
    """
    _services[name] = instance
    if plugin_name and plugin_name not in _plugins:
        _plugins[plugin_name] = {}


def mark_loaded(name: str) -> None:
    """Mark a plugin as loaded without calling register().

    Used by tests to satisfy has_plugin() checks.
    """
    if name not in _plugins:
        _plugins[name] = {}


def load_plugins(app: Any, config: Any) -> None:
    """Discover and load all plugins.

    Called once during app lifespan startup.  Scans drop-in directory
    first, then entry points.  Each plugin's register() is called
    immediately; returned services and shutdown hooks are stored.
    Background tasks are NOT started here — call start_tasks() after.

    Plugins with a .disabled file in their directory are discovered but
    not loaded.  Their names are recorded in the disabled set so the
    management API can list them.

    Plugin Python dependencies (requirements.txt) are installed before
    each plugin is imported, using a constraints file to protect core
    cathode packages.
    """
    _load_dropin_plugins(app, config)
    _load_entrypoint_plugins(app, config)

    loaded = list(_plugins.keys())
    disabled = list(_disabled)

    if loaded:
        logger.info(
            "Loaded %d plugin(s): %s",
            len(loaded),
            ", ".join(loaded),
        )
    if disabled:
        logger.info(
            "Disabled %d plugin(s): %s",
            len(disabled),
            ", ".join(disabled),
        )
    if not loaded and not disabled:
        logger.info("No plugins found (cathode running in core-only mode)")


def start_tasks() -> None:
    """Start background tasks returned by plugins during register().

    Call this AFTER load_plugins() and after all other startup is done.
    """
    for name, info in _plugins.items():
        for coro_fn in info.get("tasks") or []:
            try:
                task = asyncio.create_task(coro_fn(), name=f"plugin-{name}")
                _tasks.append(task)
                logger.debug("Started background task for plugin '%s'", name)
            except Exception as exc:
                logger.error("Failed to start task for plugin '%s': %s", name, exc)


async def shutdown_plugins() -> None:
    """Cancel plugin tasks and run shutdown hooks.

    Called during app lifespan shutdown.
    """
    # Cancel background tasks
    for task in _tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # Run shutdown hooks
    for hook in _shutdown_hooks:
        try:
            await hook()
        except Exception as exc:
            logger.error("Plugin shutdown hook error: %s", exc)

    _tasks.clear()
    _shutdown_hooks.clear()
    logger.debug("Plugin shutdown complete")


# ── Internal discovery ──


def _register_plugin(name: str, info: dict[str, Any] | None) -> None:
    """Process a plugin's register() return value."""
    if info is None:
        info = {}

    _plugins[name] = info

    # Validate category if provided
    cat = info.get("category")
    if cat:
        cats = {c.strip() for c in cat.split(",")}
        unknown = cats - VALID_CATEGORIES
        if unknown:
            logger.warning(
                "Plugin '%s' declares unknown category: %s (valid: %s)",
                name,
                ", ".join(unknown),
                ", ".join(sorted(VALID_CATEGORIES)),
            )

    # Register services
    for svc_name, svc_instance in (info.get("services") or {}).items():
        if svc_name in _services:
            logger.warning(
                "Service '%s' already registered — overwriting (plugin '%s')",
                svc_name,
                name,
            )
        _services[svc_name] = svc_instance
        logger.debug("Registered service '%s' from plugin '%s'", svc_name, name)

    # Store shutdown hook
    if info.get("shutdown"):
        _shutdown_hooks.append(info["shutdown"])

    # ── Extension registries ──

    for type_name, type_info in (info.get("source_types") or {}).items():
        _source_types[type_name] = {**type_info, "plugin": name}
        logger.debug("Registered source type '%s' from plugin '%s'", type_name, name)

    for type_name, type_info in (info.get("output_types") or {}).items():
        _output_types[type_name] = {**type_info, "plugin": name}
        logger.debug("Registered output type '%s' from plugin '%s'", type_name, name)

    for type_name, type_info in (info.get("block_types") or {}).items():
        _block_types[type_name] = {**type_info, "plugin": name}
        logger.debug("Registered block type '%s' from plugin '%s'", type_name, name)

    for el in info.get("overlay_elements") or []:
        factory, el_name = el[0], el[1]
        props = el[2] if len(el) > 2 else {}
        _overlay_elements.append((factory, el_name, props, name))
        logger.debug("Registered overlay element '%s' from plugin '%s'", el_name, name)

    for lc in info.get("layers") or []:
        _plugin_layers.append({**lc, "plugin": name})
        logger.debug("Registered layer '%s' from plugin '%s'", lc.get("name"), name)

    for tool_name, tool_info in (info.get("playlist_tools") or {}).items():
        _playlist_tools[tool_name] = {**tool_info, "plugin": name}
        logger.debug("Registered playlist tool '%s' from plugin '%s'", tool_name, name)

    # Register preset provider
    presets_provider = info.get("presets")
    if presets_provider is not None:
        _presets[name] = presets_provider
        logger.debug("Registered preset provider from plugin '%s'", name)

    # Register generate handler
    generate_fn = info.get("generate")
    if generate_fn is not None:
        _generators[name] = generate_fn
        logger.debug("Registered generate handler from plugin '%s'", name)

    # Check system deps
    for dep in info.get("system_deps") or []:
        _check_system_dep(dep, name)


def _check_system_dep(package: str, plugin_name: str) -> None:
    """Log a warning if a system (apt) package is not installed."""
    try:
        result = subprocess.run(
            ["dpkg-query", "-W", "-f", "${Status}", package],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if "install ok installed" not in result.stdout:
            logger.warning(
                "Plugin '%s' needs system package '%s' which is not installed. "
                "Some features may not work. Install with: apt-get install %s",
                plugin_name,
                package,
                package,
            )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass  # dpkg not available or too slow — skip check


def _load_module_from_path(name: str, path: Path) -> Any:
    """Load a Python module from an explicit file path.

    Uses spec_from_file_location to avoid polluting sys.path and
    prevent naming conflicts with existing modules (e.g. a plugin
    directory named 'archive' won't shadow app/archive.py).

    Directory-based plugins (__init__.py) are loaded as packages with
    submodule_search_locations set, enabling relative imports
    (e.g. ``from .client import HTMLRenderer``).
    """
    module_name = f"cathode_plugin_{name.replace('-', '_')}"

    if path.name == "__init__.py":
        # Package — enable relative imports from sibling modules
        spec = importlib.util.spec_from_file_location(
            module_name,
            str(path),
            submodule_search_locations=[str(path.parent)],
        )
    else:
        # Single-file module
        spec = importlib.util.spec_from_file_location(module_name, str(path))

    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot create module spec for {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_dropin_plugins(app: Any, config: Any) -> None:
    """Scan /app/plugins/ for drop-in plugin modules.

    Each subdirectory with an __init__.py containing register() is a plugin.
    Also loads single-file plugins (name.py with register()).

    Plugins with a .disabled file are discovered but not loaded — their
    names are recorded so the management API can list them.

    Before importing each plugin, its requirements.txt is checked and
    missing Python dependencies are installed via pip.

    Uses spec_from_file_location for isolation — plugin directory names
    like 'archive' or 'generators' won't conflict with existing cathode
    modules of the same name.
    """
    plugins_dir = Path(__file__).parent / "plugins"
    if not plugins_dir.is_dir():
        logger.debug("No drop-in plugins directory: %s", plugins_dir)
        return

    for entry in sorted(plugins_dir.iterdir()):
        name: str | None = None
        load_path: Path | None = None
        plugin_dir: Path | None = None

        if entry.is_dir() and (entry / "__init__.py").exists():
            name = entry.name
            load_path = entry / "__init__.py"
            plugin_dir = entry
        elif entry.is_file() and entry.suffix == ".py" and entry.stem != "__init__":
            name = entry.stem
            load_path = entry

        if name is None or load_path is None or name.startswith("_"):
            continue

        # Check for .disabled marker
        if plugin_dir and (plugin_dir / ".disabled").exists():
            _disabled.add(name)
            logger.debug("Plugin '%s' is disabled (.disabled file present)", name)
            continue

        if name in _plugins:
            logger.debug("Plugin '%s' already loaded, skipping drop-in", name)
            continue

        # Install Python dependencies before importing
        if plugin_dir:
            _ensure_plugin_deps(name, plugin_dir)

        try:
            mod = _load_module_from_path(name, load_path)
            register_fn = getattr(mod, "register", None)
            if register_fn is None:
                logger.warning(
                    "Drop-in plugin '%s' has no register() function, skipping",
                    name,
                )
                continue

            plugin_app = PluginApp(app, name)
            info = register_fn(plugin_app, config)
            _register_plugin(name, info)
            logger.info("Loaded drop-in plugin: %s", name)

        except Exception as exc:
            logger.warning("Failed to load drop-in plugin '%s': %s", name, exc)


def _load_entrypoint_plugins(app: Any, config: Any) -> None:
    """Load plugins registered as pip entry points (cathode.plugins group)."""
    try:
        if sys.version_info >= (3, 10):
            from importlib.metadata import entry_points

            eps = entry_points(group="cathode.plugins")
        else:
            from importlib.metadata import entry_points

            all_eps = entry_points()
            eps = all_eps.get("cathode.plugins", [])
    except Exception:
        logger.debug("Entry point discovery not available")
        return

    for ep in eps:
        name = ep.name
        if name in _plugins:
            logger.debug(
                "Plugin '%s' already loaded (drop-in), skipping entry point",
                name,
            )
            continue

        try:
            mod = ep.load()
            register_fn = getattr(mod, "register", None)
            if register_fn is None:
                logger.warning(
                    "Entry point plugin '%s' has no register(), skipping", name
                )
                continue

            plugin_app = PluginApp(app, name)
            info = register_fn(plugin_app, config)
            _register_plugin(name, info)
            logger.info("Loaded entry point plugin: %s", name)

        except Exception as exc:
            logger.warning("Failed to load entry point plugin '%s': %s", name, exc)


# ── Dependency installation ──


_CONSTRAINTS_PATH = Path(__file__).parent / "plugin_constraints.txt"


def _ensure_plugin_deps(name: str, plugin_dir: Path) -> None:
    """Install Python deps from a plugin's requirements.txt if needed.

    Uses pip with a constraints file to protect cathode's core packages.
    Skips installation if all requirements are already satisfied.
    """
    req_file = plugin_dir / "requirements.txt"
    if not req_file.exists():
        return

    # Quick check: are all packages already installed?
    try:
        with open(req_file) as f:
            reqs = [
                line.strip() for line in f if line.strip() and not line.startswith("#")
            ]
        if not reqs:
            return

        # Fast path: check if all packages are importable
        all_satisfied = True
        for req in reqs:
            # Strip version specifiers for the check
            pkg_name = (
                req.split(">=")[0].split("==")[0].split("<")[0].split("!")[0].strip()
            )
            try:
                importlib.metadata.distribution(pkg_name)
            except importlib.metadata.PackageNotFoundError:
                all_satisfied = False
                break

        if all_satisfied:
            logger.debug("Plugin '%s' deps already satisfied", name)
            return

    except Exception as exc:
        logger.debug("Dep check for '%s' failed, running pip: %s", name, exc)

    # Install missing deps
    logger.info("Installing dependencies for plugin '%s'...", name)
    cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--quiet",
        "--disable-pip-version-check",
        "-r",
        str(req_file),
    ]
    if _CONSTRAINTS_PATH.exists():
        cmd.extend(["-c", str(_CONSTRAINTS_PATH)])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            logger.error(
                "pip install failed for plugin '%s': %s",
                name,
                result.stderr.strip(),
            )
        else:
            logger.info("Dependencies installed for plugin '%s'", name)
    except subprocess.TimeoutExpired:
        logger.error("pip install timed out for plugin '%s'", name)
    except Exception as exc:
        logger.error("pip install error for plugin '%s': %s", name, exc)


# ── Enable / disable ──


def enable_plugin(name: str) -> bool:
    """Remove the .disabled marker for a plugin.

    Returns True if the marker was removed, False if it didn't exist.
    The plugin will be loaded on next engine restart.
    """
    plugins_dir = Path(__file__).parent / "plugins"
    marker = plugins_dir / name / ".disabled"
    if marker.exists():
        marker.unlink()
        _disabled.discard(name)
        return True
    return False


def disable_plugin(name: str) -> bool:
    """Create a .disabled marker for a plugin.

    Returns True if the marker was created, False if plugin dir not found.
    The plugin will be skipped on next engine restart.
    """
    plugins_dir = Path(__file__).parent / "plugins"
    plugin_dir = plugins_dir / name
    if not plugin_dir.is_dir():
        return False
    marker = plugin_dir / ".disabled"
    marker.touch()
    _disabled.add(name)
    return True


def reset() -> None:
    """Reset all plugin state.  For testing only."""
    _plugins.clear()
    _disabled.clear()
    _services.clear()
    _tasks.clear()
    _shutdown_hooks.clear()
    _source_types.clear()
    _output_types.clear()
    _block_types.clear()
    _overlay_elements.clear()
    _plugin_layers.clear()
    _playlist_tools.clear()
    _presets.clear()
    _generators.clear()
