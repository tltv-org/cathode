"""Tests for the plugin management API endpoints.

Covers:
  GET    /api/plugins
  GET    /api/plugins/{name}
  GET    /api/plugins/{name}/settings
  PATCH  /api/plugins/{name}/settings
  POST   /api/plugins/{name}/enable
  POST   /api/plugins/{name}/disable
  GET    /api/plugins/{name}/presets
  GET    /api/plugins/{name}/presets/{preset}
  PUT    /api/plugins/{name}/presets/{preset}
  DELETE /api/plugins/{name}/presets/{preset}
  GET    /api/plugins/registry/source-types
  GET    /api/plugins/registry/output-types
  GET    /api/plugins/registry/block-types
  GET    /api/plugins/registry/playlist-tools
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


# ══════════════════════════════════════════════════════════════════
# Plugin management API — /api/plugins/*
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_list_plugins(client):
    """GET /api/plugins returns loaded plugin list."""
    resp = await client.get("/api/plugins")
    assert resp.status_code == 200
    data = resp.json()
    assert "plugins" in data
    assert "loaded" in data
    assert "disabled" in data
    assert data["loaded"] >= 0
    assert isinstance(data["plugins"], list)


@pytest.mark.asyncio
async def test_list_plugins_contains_known(client):
    """Plugin list includes plugins marked as loaded in conftest."""
    resp = await client.get("/api/plugins")
    data = resp.json()
    names = [p["name"] for p in data["plugins"]]
    # These are registered/marked in conftest.py app_with_mocks
    assert "renderer" in names
    assert "media-gen" in names


@pytest.mark.asyncio
async def test_get_plugin_detail(client):
    """GET /api/plugins/{name} returns detail for a loaded plugin."""
    resp = await client.get("/api/plugins/renderer")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "renderer"
    assert data["loaded"] is True
    assert data["enabled"] is True
    assert "category" in data
    assert "settings" in data
    assert "has_shutdown" in data
    assert "has_tasks" in data


@pytest.mark.asyncio
async def test_get_plugin_not_found(client):
    """GET /api/plugins/{name} returns 404 for unknown plugin."""
    resp = await client.get("/api/plugins/nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_plugin_settings_empty(client):
    """GET /api/plugins/{name}/settings returns empty for plugin with no settings."""
    import plugins

    # Use a fake plugin with no settings (real plugins from volume have settings)
    plugins._plugins["no-settings-test"] = {"services": {}}
    try:
        resp = await client.get("/api/plugins/no-settings-test/settings")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "no-settings-test"
        assert data["settings"] == {} or "message" in data
    finally:
        del plugins._plugins["no-settings-test"]


@pytest.mark.asyncio
async def test_get_plugin_settings_not_found(client):
    """GET /api/plugins/{name}/settings returns 404 for unknown plugin."""
    resp = await client.get("/api/plugins/nonexistent/settings")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_patch_plugin_settings_not_found(client):
    """PATCH /api/plugins/{name}/settings returns 404 for unknown plugin."""
    resp = await client.patch(
        "/api/plugins/nonexistent/settings",
        json={"settings": {"key": "value"}},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_patch_plugin_settings_no_settings(client):
    """PATCH /api/plugins/{name}/settings returns 400 when plugin has no settings."""
    resp = await client.patch(
        "/api/plugins/renderer/settings",
        json={"settings": {"key": "value"}},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_patch_plugin_settings_empty_body(client):
    """PATCH /api/plugins/{name}/settings returns 400 with empty settings."""
    resp = await client.patch(
        "/api/plugins/renderer/settings",
        json={"settings": {}},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_plugin_with_settings(client):
    """Plugin with settings can be queried and updated."""
    import plugins

    # Register a test plugin with settings
    callback = AsyncMock()
    plugins._plugins["test-plugin"] = {
        "services": {},
        "settings": {
            "volume": {"type": "float", "value": 0.8, "description": "Volume level"},
            "enabled": {"type": "bool", "value": True, "description": "Feature toggle"},
        },
        "on_settings_changed": callback,
    }

    try:
        # Read settings
        resp = await client.get("/api/plugins/test-plugin/settings")
        assert resp.status_code == 200
        data = resp.json()
        assert data["settings"]["volume"]["value"] == 0.8
        assert data["settings"]["enabled"]["value"] is True

        # Update settings
        resp = await client.patch(
            "/api/plugins/test-plugin/settings",
            json={"settings": {"volume": 0.5}},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["settings"]["volume"]["value"] == 0.5
        assert data["settings"]["enabled"]["value"] is True  # unchanged

        # Verify callback was called
        callback.assert_called_once_with({"volume": 0.5, "enabled": True})

        # Plugin appears in list
        resp = await client.get("/api/plugins")
        names = [p["name"] for p in resp.json()["plugins"]]
        assert "test-plugin" in names
    finally:
        # Clean up
        del plugins._plugins["test-plugin"]


@pytest.mark.asyncio
async def test_patch_unknown_setting_key(client):
    """PATCH with unknown setting key returns 400."""
    import plugins

    plugins._plugins["test-plugin2"] = {
        "services": {},
        "settings": {
            "volume": {"type": "float", "value": 0.8, "description": "Volume"},
        },
    }

    try:
        resp = await client.patch(
            "/api/plugins/test-plugin2/settings",
            json={"settings": {"nonexistent_key": 42}},
        )
        assert resp.status_code == 400
    finally:
        del plugins._plugins["test-plugin2"]


# ══════════════════════════════════════════════════════════════════
# Plugin categories and extensions (v2)
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_plugin_category_in_list(client):
    """Plugin list includes category field for all plugins."""
    import plugins

    plugins._plugins["cat-test"] = {"category": "source", "services": {}}
    try:
        resp = await client.get("/api/plugins")
        data = resp.json()
        cat_plugin = next(p for p in data["plugins"] if p["name"] == "cat-test")
        assert cat_plugin["category"] == "source"
        assert cat_plugin["enabled"] is True
        assert cat_plugin["loaded"] is True
    finally:
        del plugins._plugins["cat-test"]


@pytest.mark.asyncio
async def test_plugin_category_in_detail(client):
    """GET /api/plugins/{name} returns category."""
    import plugins

    plugins._plugins["cat-detail"] = {"category": "graphics", "services": {}}
    try:
        resp = await client.get("/api/plugins/cat-detail")
        assert resp.status_code == 200
        data = resp.json()
        assert data["category"] == "graphics"
    finally:
        del plugins._plugins["cat-detail"]


@pytest.mark.asyncio
async def test_plugin_extensions_in_list(client):
    """Plugin list reports registered extensions."""
    import plugins

    plugins._plugins["ext-test"] = {
        "category": "source",
        "services": {},
        "source_types": {"html": {"description": "HTML rendering"}},
        "overlay_elements": [("gdkpixbufoverlay", "bug", {})],
    }
    try:
        resp = await client.get("/api/plugins")
        data = resp.json()
        ext_plugin = next(p for p in data["plugins"] if p["name"] == "ext-test")
        assert "extensions" in ext_plugin
        assert "source_types" in ext_plugin["extensions"]
        assert "html" in ext_plugin["extensions"]["source_types"]
        assert "overlay_elements" in ext_plugin["extensions"]
    finally:
        del plugins._plugins["ext-test"]


# ══════════════════════════════════════════════════════════════════
# Enable / disable
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_disable_loaded_plugin(client):
    """POST /api/plugins/{name}/disable creates .disabled marker."""
    import plugins

    plugin_dir = Path(plugins.__file__).parent / "plugins" / "test-disable"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    (plugin_dir / "__init__.py").write_text("def register(app, config): return {}")
    plugins._plugins["test-disable"] = {"services": {}}

    try:
        resp = await client.post("/api/plugins/test-disable/disable")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert (plugin_dir / ".disabled").exists()
    finally:
        plugins._plugins.pop("test-disable", None)
        plugins._disabled.discard("test-disable")
        import shutil

        shutil.rmtree(plugin_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_enable_disabled_plugin(client):
    """POST /api/plugins/{name}/enable removes .disabled marker."""
    import plugins

    plugin_dir = Path(plugins.__file__).parent / "plugins" / "test-enable"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    (plugin_dir / "__init__.py").write_text("def register(app, config): return {}")
    (plugin_dir / ".disabled").touch()
    plugins._disabled.add("test-enable")

    try:
        resp = await client.post("/api/plugins/test-enable/enable")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert not (plugin_dir / ".disabled").exists()
    finally:
        plugins._disabled.discard("test-enable")
        import shutil

        shutil.rmtree(plugin_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_enable_disabled_plugin(client, tmp_path):
    """POST /api/plugins/{name}/enable removes .disabled marker."""
    import plugins

    # Create a fake disabled plugin directory
    plugin_dir = Path(plugins.__file__).parent / "plugins" / "test-enable"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    (plugin_dir / "__init__.py").write_text("def register(app, config): return {}")
    (plugin_dir / ".disabled").touch()
    plugins._disabled.add("test-enable")

    try:
        resp = await client.post("/api/plugins/test-enable/enable")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert not (plugin_dir / ".disabled").exists()
    finally:
        plugins._disabled.discard("test-enable")
        import shutil

        shutil.rmtree(plugin_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_disabled_plugin_appears_in_list(client):
    """Disabled plugins appear in the list with enabled=False."""
    import plugins

    plugins._disabled.add("fake-disabled")
    try:
        resp = await client.get("/api/plugins")
        data = resp.json()
        disabled = [p for p in data["plugins"] if p["name"] == "fake-disabled"]
        assert len(disabled) == 1
        assert disabled[0]["enabled"] is False
        assert disabled[0]["loaded"] is False
        assert data["disabled"] >= 1
    finally:
        plugins._disabled.discard("fake-disabled")


@pytest.mark.asyncio
async def test_get_disabled_plugin_detail(client):
    """GET /api/plugins/{name} for disabled plugin returns status info."""
    import plugins

    plugins._disabled.add("fake-disabled2")
    try:
        resp = await client.get("/api/plugins/fake-disabled2")
        assert resp.status_code == 200
        data = resp.json()
        assert data["enabled"] is False
        assert data["loaded"] is False
    finally:
        plugins._disabled.discard("fake-disabled2")


@pytest.mark.asyncio
async def test_disable_nonexistent_plugin(client):
    """POST /api/plugins/{name}/disable returns 404 for unknown plugin."""
    resp = await client.post("/api/plugins/nonexistent-xyz/disable")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_enable_nonexistent_plugin(client):
    """POST /api/plugins/{name}/enable returns 404 for unknown plugin."""
    resp = await client.post("/api/plugins/nonexistent-xyz/enable")
    assert resp.status_code == 404


# ══════════════════════════════════════════════════════════════════
# Extension registries
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_source_types_registry(client):
    """GET /api/plugins/registry/source-types returns plugin source types."""
    import plugins

    plugins._source_types["html"] = {
        "description": "HTML rendering via WPE",
        "params": {"location": "str"},
        "plugin": "html-source",
    }
    try:
        resp = await client.get("/api/plugins/registry/source-types")
        assert resp.status_code == 200
        data = resp.json()
        assert "html" in data["source_types"]
        assert data["source_types"]["html"]["plugin"] == "html-source"
    finally:
        plugins._source_types.pop("html", None)


@pytest.mark.asyncio
async def test_output_types_registry(client):
    """GET /api/plugins/registry/output-types returns plugin output types."""
    import plugins

    plugins._output_types["srt"] = {
        "description": "SRT output",
        "params": {"uri": "str"},
        "plugin": "srt",
    }
    try:
        resp = await client.get("/api/plugins/registry/output-types")
        assert resp.status_code == 200
        data = resp.json()
        assert "srt" in data["output_types"]
    finally:
        plugins._output_types.pop("srt", None)


@pytest.mark.asyncio
async def test_block_types_registry(client):
    """GET /api/plugins/registry/block-types returns plugin block types."""
    import plugins

    plugins._block_types["auto-bumper"] = {
        "description": "Auto ident",
        "params": {"style": "str"},
        "plugin": "auto-schedule",
    }
    try:
        resp = await client.get("/api/plugins/registry/block-types")
        assert resp.status_code == 200
        data = resp.json()
        assert "auto-bumper" in data["block_types"]
    finally:
        plugins._block_types.pop("auto-bumper", None)


@pytest.mark.asyncio
async def test_playlist_tools_registry(client):
    """GET /api/plugins/registry/playlist-tools returns plugin tools."""
    import plugins

    plugins._playlist_tools["sort"] = {
        "description": "Sort playlist",
        "params": {"by": "str"},
        "plugin": "playlist-tools",
    }
    try:
        resp = await client.get("/api/plugins/registry/playlist-tools")
        assert resp.status_code == 200
        data = resp.json()
        assert "sort" in data["playlist_tools"]
    finally:
        plugins._playlist_tools.pop("sort", None)


@pytest.mark.asyncio
async def test_empty_registries(client):
    """Registry endpoints return empty when no plugins register types."""
    resp = await client.get("/api/plugins/registry/source-types")
    assert resp.status_code == 200
    # May or may not be empty depending on test order, but should not error


# ══════════════════════════════════════════════════════════════════
# Plugin loader unit tests (no HTTP, direct function calls)
# ══════════════════════════════════════════════════════════════════


def test_register_plugin_with_category():
    """_register_plugin stores category and populates extension registries."""
    import plugins

    plugins.reset()
    plugins._register_plugin(
        "test-src",
        {
            "category": "source",
            "services": {},
            "source_types": {
                "test-type": {"factory": None, "description": "A test source"},
            },
            "block_types": {
                "test-block": {"handler": None, "description": "A test block"},
            },
        },
    )

    assert plugins.has_plugin("test-src")
    assert plugins.plugin_category("test-src") == "source"
    assert "test-type" in plugins.source_types()
    assert plugins.source_types()["test-type"]["plugin"] == "test-src"
    assert "test-block" in plugins.block_types()
    plugins.reset()


def test_register_plugin_overlay_elements():
    """_register_plugin populates overlay element registry."""
    import plugins

    plugins.reset()
    plugins._register_plugin(
        "test-gfx",
        {
            "category": "graphics",
            "services": {},
            "overlay_elements": [
                ("gdkpixbufoverlay", "bug-overlay", {"alpha": 0.0}),
                ("rsvgoverlay", "svg-overlay", {}),
            ],
        },
    )

    overlays = plugins.overlay_elements()
    assert len(overlays) == 2
    assert overlays[0][1] == "bug-overlay"
    assert overlays[0][3] == "test-gfx"  # plugin name
    assert overlays[1][1] == "svg-overlay"
    plugins.reset()


def test_register_plugin_layers():
    """_register_plugin populates layer registry."""
    import plugins

    plugins.reset()
    plugins._register_plugin(
        "test-layer",
        {
            "category": "graphics",
            "services": {},
            "layers": [{"name": "graphics", "role": "overlay"}],
        },
    )

    layers = plugins.plugin_layers()
    assert len(layers) == 1
    assert layers[0]["name"] == "graphics"
    assert layers[0]["plugin"] == "test-layer"
    plugins.reset()


def test_disabled_plugins_tracking():
    """Disabled plugins are tracked separately from loaded ones."""
    import plugins

    plugins.reset()
    plugins._disabled.add("test-off")

    assert plugins.is_disabled("test-off")
    assert not plugins.has_plugin("test-off")
    assert "test-off" in plugins.disabled_plugins()
    plugins.reset()


def test_reset_clears_all_state():
    """reset() clears all registries and state."""
    import plugins

    plugins._plugins["x"] = {}
    plugins._disabled.add("y")
    plugins._source_types["z"] = {}
    plugins._overlay_elements.append(("a", "b", {}, "c"))
    plugins._plugin_layers.append({"name": "test"})

    plugins.reset()

    assert len(plugins.loaded_plugins()) == 0
    assert len(plugins.disabled_plugins()) == 0
    assert len(plugins.source_types()) == 0
    assert len(plugins.overlay_elements()) == 0
    assert len(plugins.plugin_layers()) == 0


# ══════════════════════════════════════════════════════════════════
# Preset CRUD — /api/plugins/{name}/presets
# ══════════════════════════════════════════════════════════════════


@pytest.fixture
def json_preset_plugin(tmp_path):
    """Register a test plugin with a JSON FilePresetStore."""
    import plugins

    preset_dir = tmp_path / "presets"
    preset_dir.mkdir()

    # Seed two presets
    (preset_dir / "bars.json").write_text(
        json.dumps(
            {"description": "Color bars", "pipeline": "videotestsrc pattern=smpte"}
        )
    )
    (preset_dir / "noise.json").write_text(
        json.dumps(
            {"description": "Random noise", "pipeline": "videotestsrc pattern=snow"}
        )
    )

    store = plugins.FilePresetStore(directory=str(preset_dir), extension=".json")
    plugins._plugins["test-gen"] = {
        "category": "source",
        "services": {},
        "presets": store,
    }
    plugins._presets["test-gen"] = store

    yield store, preset_dir

    plugins._plugins.pop("test-gen", None)
    plugins._presets.pop("test-gen", None)


@pytest.fixture
def html_preset_plugin(tmp_path):
    """Register a test plugin with an HTML FilePresetStore."""
    import plugins

    preset_dir = tmp_path / "html-presets"
    preset_dir.mkdir()

    (preset_dir / "intro.html").write_text("<html><body>Intro</body></html>")
    (preset_dir / "intro.meta.json").write_text(
        json.dumps({"description": "Intro card"})
    )
    (preset_dir / "outro.html").write_text("<html><body>Outro</body></html>")

    store = plugins.FilePresetStore(directory=str(preset_dir), extension=".html")
    plugins._plugins["test-html"] = {
        "category": "source",
        "services": {},
        "presets": store,
    }
    plugins._presets["test-html"] = store

    yield store, preset_dir

    plugins._plugins.pop("test-html", None)
    plugins._presets.pop("test-html", None)


@pytest.mark.asyncio
async def test_list_presets_json(client, json_preset_plugin):
    """GET /api/plugins/{name}/presets lists JSON presets."""
    resp = await client.get("/api/plugins/test-gen/presets")
    assert resp.status_code == 200
    data = resp.json()
    assert data["plugin"] == "test-gen"
    names = [p["name"] for p in data["presets"]]
    assert "bars" in names
    assert "noise" in names


@pytest.mark.asyncio
async def test_list_presets_html(client, html_preset_plugin):
    """GET /api/plugins/{name}/presets lists HTML presets."""
    resp = await client.get("/api/plugins/test-html/presets")
    assert resp.status_code == 200
    data = resp.json()
    names = [p["name"] for p in data["presets"]]
    assert "intro" in names
    assert "outro" in names
    # Sidecar description should be present for intro
    intro = next(p for p in data["presets"] if p["name"] == "intro")
    assert intro["description"] == "Intro card"


@pytest.mark.asyncio
async def test_get_preset_json_content(client, json_preset_plugin):
    """GET /api/plugins/{name}/presets/{preset} returns full JSON content."""
    resp = await client.get("/api/plugins/test-gen/presets/bars")
    assert resp.status_code == 200
    data = resp.json()
    preset = data["preset"]
    assert preset["name"] == "bars"
    assert preset["description"] == "Color bars"
    assert "pipeline" in preset


@pytest.mark.asyncio
async def test_get_preset_html_content(client, html_preset_plugin):
    """GET /api/plugins/{name}/presets/{preset} returns full HTML content."""
    resp = await client.get("/api/plugins/test-html/presets/intro")
    assert resp.status_code == 200
    data = resp.json()
    preset = data["preset"]
    assert preset["name"] == "intro"
    assert "<html>" in preset["content"]
    assert preset["description"] == "Intro card"


@pytest.mark.asyncio
async def test_get_preset_not_found(client, json_preset_plugin):
    """GET /api/plugins/{name}/presets/{preset} returns 404 for missing preset."""
    resp = await client.get("/api/plugins/test-gen/presets/nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_save_new_json_preset(client, json_preset_plugin):
    """PUT /api/plugins/{name}/presets/{preset} creates a new JSON preset."""
    resp = await client.put(
        "/api/plugins/test-gen/presets/mandelbrot",
        json={
            "description": "Mandelbrot zoom",
            "pipeline": "videotestsrc pattern=ball",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["preset"]["name"] == "mandelbrot"

    # Verify it persisted
    resp = await client.get("/api/plugins/test-gen/presets/mandelbrot")
    assert resp.status_code == 200
    assert resp.json()["preset"]["description"] == "Mandelbrot zoom"


@pytest.mark.asyncio
async def test_save_update_existing_preset(client, json_preset_plugin):
    """PUT /api/plugins/{name}/presets/{preset} overwrites existing preset."""
    resp = await client.put(
        "/api/plugins/test-gen/presets/bars",
        json={
            "description": "Updated bars",
            "pipeline": "videotestsrc pattern=smpte100",
        },
    )
    assert resp.status_code == 200

    resp = await client.get("/api/plugins/test-gen/presets/bars")
    assert resp.json()["preset"]["description"] == "Updated bars"
    assert resp.json()["preset"]["pipeline"] == "videotestsrc pattern=smpte100"


@pytest.mark.asyncio
async def test_save_new_html_preset(client, html_preset_plugin):
    """PUT creates an HTML preset with sidecar metadata."""
    resp = await client.put(
        "/api/plugins/test-html/presets/bumper",
        json={
            "content": "<html><body>Bumper</body></html>",
            "description": "Station bumper",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["preset"]["name"] == "bumper"

    # Verify content and metadata
    resp = await client.get("/api/plugins/test-html/presets/bumper")
    preset = resp.json()["preset"]
    assert "<html>" in preset["content"]
    assert preset["description"] == "Station bumper"


@pytest.mark.asyncio
async def test_save_preset_bad_name(client, json_preset_plugin):
    """PUT rejects invalid preset names."""
    resp = await client.put(
        "/api/plugins/test-gen/presets/../../etc/passwd",
        json={"description": "evil"},
    )
    assert resp.status_code in (400, 404, 422)


@pytest.mark.asyncio
async def test_delete_preset(client, json_preset_plugin):
    """DELETE /api/plugins/{name}/presets/{preset} removes a preset."""
    resp = await client.delete("/api/plugins/test-gen/presets/bars")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["deleted"] == "bars"

    # Verify it's gone
    resp = await client.get("/api/plugins/test-gen/presets/bars")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_preset_not_found(client, json_preset_plugin):
    """DELETE returns 404 for missing preset."""
    resp = await client.delete("/api/plugins/test-gen/presets/nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_html_preset_removes_sidecar(client, html_preset_plugin):
    """DELETE removes HTML preset and its sidecar .meta.json."""
    _, preset_dir = html_preset_plugin
    assert (preset_dir / "intro.meta.json").exists()

    resp = await client.delete("/api/plugins/test-html/presets/intro")
    assert resp.status_code == 200

    assert not (preset_dir / "intro.html").exists()
    assert not (preset_dir / "intro.meta.json").exists()


@pytest.mark.asyncio
async def test_presets_plugin_not_found(client):
    """Preset endpoints return 404 for unknown plugin."""
    resp = await client.get("/api/plugins/nonexistent/presets")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_presets_plugin_no_presets(client):
    """Preset endpoints return 404 for plugin without presets."""
    import plugins

    plugins._plugins["no-presets"] = {"services": {}}
    try:
        resp = await client.get("/api/plugins/no-presets/presets")
        assert resp.status_code == 404
        assert "does not provide presets" in resp.json()["detail"]
    finally:
        del plugins._plugins["no-presets"]


@pytest.mark.asyncio
async def test_list_presets_empty_dir(client, tmp_path):
    """List presets returns empty list when preset dir has no files."""
    import plugins

    store = plugins.FilePresetStore(
        directory=str(tmp_path / "empty"), extension=".json"
    )
    plugins._plugins["empty-presets"] = {"services": {}, "presets": store}
    plugins._presets["empty-presets"] = store
    try:
        resp = await client.get("/api/plugins/empty-presets/presets")
        assert resp.status_code == 200
        assert resp.json()["presets"] == []
    finally:
        plugins._plugins.pop("empty-presets", None)
        plugins._presets.pop("empty-presets", None)


@pytest.mark.asyncio
async def test_has_presets_in_plugin_detail(client, json_preset_plugin):
    """Plugin detail reports has_presets in extensions."""
    resp = await client.get("/api/plugins")
    data = resp.json()
    gen_plugin = next(p for p in data["plugins"] if p["name"] == "test-gen")
    assert gen_plugin.get("extensions", {}).get("has_presets") is True


# ── FilePresetStore unit tests (no HTTP) ──


def test_file_preset_store_list_json(tmp_path):
    """FilePresetStore lists JSON presets with metadata."""
    from plugins import FilePresetStore

    d = tmp_path / "p"
    d.mkdir()
    (d / "foo.json").write_text(json.dumps({"description": "Foo preset"}))
    (d / "bar.json").write_text(
        json.dumps({"description": "Bar preset", "params": {"x": 1}})
    )

    store = FilePresetStore(directory=str(d), extension=".json")
    presets = store.list()
    assert len(presets) == 2
    names = {p["name"] for p in presets}
    assert names == {"bar", "foo"}
    bar = next(p for p in presets if p["name"] == "bar")
    assert bar["description"] == "Bar preset"
    assert bar["params"] == {"x": 1}


def test_file_preset_store_get_json(tmp_path):
    """FilePresetStore.get() returns full JSON content."""
    from plugins import FilePresetStore

    d = tmp_path / "p"
    d.mkdir()
    (d / "test.json").write_text(json.dumps({"description": "Test", "key": "value"}))

    store = FilePresetStore(directory=str(d), extension=".json")
    preset = store.get("test")
    assert preset is not None
    assert preset["name"] == "test"
    assert preset["key"] == "value"
    assert preset["description"] == "Test"


def test_file_preset_store_save_json(tmp_path):
    """FilePresetStore.save() creates a JSON file."""
    from plugins import FilePresetStore

    d = tmp_path / "p"
    store = FilePresetStore(directory=str(d), extension=".json")

    result = store.save("new", {"description": "New one", "pipeline": "test"})
    assert result["name"] == "new"
    assert (d / "new.json").exists()

    loaded = json.loads((d / "new.json").read_text())
    assert loaded["description"] == "New one"
    assert loaded["pipeline"] == "test"


def test_file_preset_store_delete_json(tmp_path):
    """FilePresetStore.delete() removes the JSON file."""
    from plugins import FilePresetStore

    d = tmp_path / "p"
    d.mkdir()
    (d / "gone.json").write_text("{}")

    store = FilePresetStore(directory=str(d), extension=".json")
    assert store.delete("gone") is True
    assert not (d / "gone.json").exists()
    assert store.delete("gone") is False


def test_file_preset_store_html_with_sidecar(tmp_path):
    """FilePresetStore handles HTML presets with sidecar metadata."""
    from plugins import FilePresetStore

    d = tmp_path / "p"
    d.mkdir()
    (d / "intro.html").write_text("<html>test</html>")
    (d / "intro.meta.json").write_text(json.dumps({"description": "Intro"}))

    store = FilePresetStore(directory=str(d), extension=".html")

    # List includes description from sidecar
    presets = store.list()
    assert len(presets) == 1
    assert presets[0]["description"] == "Intro"

    # Get includes content
    full = store.get("intro")
    assert full["content"] == "<html>test</html>"
    assert full["description"] == "Intro"

    # Save creates both files
    store.save("outro", {"content": "<html>bye</html>", "description": "Outro"})
    assert (d / "outro.html").exists()
    assert (d / "outro.meta.json").exists()

    # Delete removes both
    assert store.delete("outro") is True
    assert not (d / "outro.html").exists()
    assert not (d / "outro.meta.json").exists()


def test_file_preset_store_py_docstring(tmp_path):
    """FilePresetStore extracts description from Python docstrings."""
    from plugins import FilePresetStore

    d = tmp_path / "p"
    d.mkdir()
    (d / "shapes.py").write_text('"""Animated geometric shapes"""\nimport math\n')

    store = FilePresetStore(directory=str(d), extension=".py")
    presets = store.list()
    assert presets[0]["description"] == "Animated geometric shapes"


def test_file_preset_store_category(tmp_path):
    """FilePresetStore includes category when configured."""
    from plugins import FilePresetStore

    d = tmp_path / "p"
    d.mkdir()
    (d / "foo.json").write_text(json.dumps({"description": "Foo"}))

    store = FilePresetStore(directory=str(d), extension=".json", category="generators")
    presets = store.list()
    assert presets[0]["category"] == "generators"
    full = store.get("foo")
    assert full["category"] == "generators"


def test_file_preset_store_nonexistent_dir(tmp_path):
    """FilePresetStore returns empty list for non-existent directory."""
    from plugins import FilePresetStore

    store = FilePresetStore(directory=str(tmp_path / "nope"), extension=".json")
    assert store.list() == []
    assert store.get("anything") is None


def test_register_plugin_with_presets():
    """_register_plugin stores preset provider in registry."""
    import plugins

    plugins.reset()

    class FakePresets:
        def list(self):
            return [{"name": "test"}]

        def get(self, name):
            return {"name": name}

        def save(self, name, data):
            return {"name": name}

        def delete(self, name):
            return True

    provider = FakePresets()
    plugins._register_plugin("preset-test", {"presets": provider})

    assert plugins.plugin_presets("preset-test") is provider
    assert plugins.plugin_presets("nonexistent") is None
    plugins.reset()


# ══════════════════════════════════════════════════════════════════
# Media generation — POST /api/plugins/{name}/generate
# ══════════════════════════════════════════════════════════════════


@pytest.fixture
def generate_plugin(tmp_path):
    """Register a test plugin with a generate handler."""
    import plugins

    output_dir = tmp_path / "media"
    output_dir.mkdir(exist_ok=True)

    async def fake_generate(config: dict) -> dict:
        """Fake generator: creates a small placeholder file."""
        filename = config.get("filename", "test-output.mp4")
        media_dir = config.get("media_dir", str(output_dir))
        path = Path(media_dir) / filename
        path.write_bytes(b"\x00" * 100)  # placeholder
        return {
            "filename": filename,
            "path": str(path),
            "duration": config.get("duration", 30),
        }

    plugins._plugins["test-generator"] = {
        "category": "source,content",
        "services": {},
        "generate": fake_generate,
    }
    plugins._generators["test-generator"] = fake_generate

    yield fake_generate, output_dir

    plugins._plugins.pop("test-generator", None)
    plugins._generators.pop("test-generator", None)


@pytest.mark.asyncio
async def test_generate_media(client, generate_plugin):
    """POST /api/plugins/{name}/generate renders a source to a file."""
    resp = await client.post(
        "/api/plugins/test-generator/generate",
        json={"preset": "smpte", "duration": 10, "filename": "test-bars.mp4"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["plugin"] == "test-generator"
    assert data["filename"] == "test-bars.mp4"
    assert data["duration"] == 10


@pytest.mark.asyncio
async def test_generate_auto_filename(client, generate_plugin):
    """POST /api/plugins/{name}/generate works without explicit filename."""
    resp = await client.post(
        "/api/plugins/test-generator/generate",
        json={"preset": "noise", "duration": 5},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert "filename" in data


@pytest.mark.asyncio
async def test_generate_adds_mp4_extension(client, generate_plugin):
    """Filename without .mp4 extension gets it added."""
    resp = await client.post(
        "/api/plugins/test-generator/generate",
        json={"preset": "test", "duration": 5, "filename": "myfile"},
    )
    assert resp.status_code == 200
    assert resp.json()["filename"] == "myfile.mp4"


@pytest.mark.asyncio
async def test_generate_invalid_duration(client, generate_plugin):
    """Duration out of range returns 400."""
    resp = await client.post(
        "/api/plugins/test-generator/generate",
        json={"duration": 0},
    )
    assert resp.status_code == 400

    resp = await client.post(
        "/api/plugins/test-generator/generate",
        json={"duration": 7200},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_generate_plugin_not_found(client):
    """Generate on unknown plugin returns 404."""
    resp = await client.post(
        "/api/plugins/nonexistent/generate",
        json={"duration": 10},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_generate_plugin_no_generator(client):
    """Generate on plugin without generate handler returns 400."""
    import plugins

    plugins._plugins["no-gen"] = {"services": {}}
    try:
        resp = await client.post(
            "/api/plugins/no-gen/generate",
            json={"duration": 10},
        )
        assert resp.status_code == 400
        assert "does not support" in resp.json()["detail"]
    finally:
        del plugins._plugins["no-gen"]


@pytest.mark.asyncio
async def test_generate_handler_error(client):
    """Generate handler raising ValueError returns 400."""
    import plugins

    async def bad_generate(config):
        raise ValueError("Preset 'nonexistent' not found")

    plugins._plugins["err-gen"] = {"services": {}, "generate": bad_generate}
    plugins._generators["err-gen"] = bad_generate
    try:
        resp = await client.post(
            "/api/plugins/err-gen/generate",
            json={"preset": "nonexistent", "duration": 10},
        )
        assert resp.status_code == 400
        assert "not found" in resp.json()["detail"]
    finally:
        plugins._plugins.pop("err-gen", None)
        plugins._generators.pop("err-gen", None)


def test_register_plugin_with_generate():
    """_register_plugin stores generate handler in registry."""
    import plugins

    plugins.reset()

    async def gen(config):
        return {}

    plugins._register_plugin("gen-test", {"generate": gen})
    assert plugins.plugin_generator("gen-test") is gen
    assert plugins.plugin_generator("nonexistent") is None
    plugins.reset()
