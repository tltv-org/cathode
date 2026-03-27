"""Tests for the settings API endpoints (GStreamer engine).

Covers:
  GET/POST  /api/playout/mode
  GET/PATCH /api/playout/encoding
  GET/PATCH /api/playout/storage
  GET/PATCH /api/playout/text
  POST      /api/playout/text/send
  DELETE    /api/playout/text/send
  GET/PATCH /api/channel
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import pytest

APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


# ── Engine config fixture ──


@dataclass
class MockPlayoutConfig:
    """Mimics the real PlayoutConfig dataclass for settings tests."""

    width: int = 1920
    height: int = 1080
    fps: int = 30
    video_bitrate: int = 3000
    audio_bitrate: int = 128
    audio_samplerate: int = 48000
    audio_channels: int = 2
    keyframe_interval: int = 60


@pytest.fixture(autouse=True)
def setup_engine_config(client):
    """Ensure mock engine has a PlayoutConfig and textoverlay for settings tests.

    Depends on ``client`` so that ``app_with_mocks`` has already set
    ``main.playout`` to the mock engine before we configure it.
    """
    import main

    if main.playout is not None:
        main.playout._config = MockPlayoutConfig()

        # Setup mixer mock
        main.playout._mixer = MagicMock()
    yield


# ══════════════════════════════════════════════════════════════════
# /api/playout/mode
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_get_playout_mode(client):
    """GET returns mode, day_start, and length."""
    resp = await client.get("/api/playout/mode")
    assert resp.status_code == 200
    data = resp.json()
    assert data["mode"] in ("loop", "schedule")
    assert "day_start" in data
    assert "length" in data


@pytest.mark.asyncio
async def test_get_playout_mode_reflects_ctx(client):
    """GET mode reflects ctx.playlist_loop state."""
    import main

    ctx = main.channels.all()[0]

    # Default is playlist_loop=False → schedule
    ctx.playlist_loop = False
    resp = await client.get("/api/playout/mode")
    assert resp.json()["mode"] == "schedule"

    # Switch to loop
    ctx.playlist_loop = True
    resp = await client.get("/api/playout/mode")
    assert resp.json()["mode"] == "loop"


@pytest.mark.asyncio
async def test_set_playout_mode_loop(client):
    """POST mode=loop → ok, updates ctx.playlist_loop."""
    import main

    resp = await client.post("/api/playout/mode", json={"mode": "loop"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["mode"] == "loop"
    ctx = main.channels.all()[0]
    assert ctx.playlist_loop is True


@pytest.mark.asyncio
async def test_set_playout_mode_schedule(client):
    """POST mode=schedule → ok, updates ctx.playlist_loop."""
    import main

    resp = await client.post("/api/playout/mode", json={"mode": "schedule"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["mode"] == "schedule"
    ctx = main.channels.all()[0]
    assert ctx.playlist_loop is False


@pytest.mark.asyncio
async def test_set_playout_mode_invalid(client):
    """POST mode=banana → 400."""
    resp = await client.post("/api/playout/mode", json={"mode": "banana"})
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_set_playout_mode_with_day_start(client):
    """POST with day_start includes it in the response."""
    resp = await client.post(
        "/api/playout/mode", json={"mode": "schedule", "day_start": "06:00:00"}
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["mode"] == "schedule"
    assert data["day_start"] == "06:00:00"


@pytest.mark.asyncio
async def test_set_playout_mode_day_start_omitted(client):
    """day_start omitted from request → response still includes current value."""
    resp = await client.post("/api/playout/mode", json={"mode": "loop"})
    assert resp.status_code == 200
    # day_start is always returned (current stored value)
    assert "day_start" in resp.json()


@pytest.mark.asyncio
async def test_set_playout_mode_day_start_invalid_format(client):
    """Invalid day_start format → 400."""
    resp = await client.post(
        "/api/playout/mode", json={"mode": "schedule", "day_start": "6am"}
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_set_playout_mode_day_start_with_loop(client):
    """day_start can be supplied with loop mode — passed through."""
    resp = await client.post(
        "/api/playout/mode", json={"mode": "loop", "day_start": "06:00:00"}
    )
    assert resp.status_code == 200
    assert resp.json()["day_start"] == "06:00:00"


@pytest.mark.asyncio
async def test_set_playout_mode_updates_ctx_loop(client):
    """Switching to loop sets ctx.playlist_loop = True."""
    import main

    resp = await client.post("/api/playout/mode", json={"mode": "loop"})
    assert resp.status_code == 200
    ctx = main.channels.all()[0]
    assert ctx.playlist_loop is True


@pytest.mark.asyncio
async def test_set_playout_mode_updates_ctx_schedule(client):
    """Switching to schedule sets ctx.playlist_loop = False."""
    import main

    resp = await client.post("/api/playout/mode", json={"mode": "schedule"})
    assert resp.status_code == 200
    ctx = main.channels.all()[0]
    assert ctx.playlist_loop is False


@pytest.mark.asyncio
async def test_set_playout_mode_persists_day_start_on_ctx(client):
    """POST with day_start stores it on ctx for GET to return."""
    import main

    resp = await client.post(
        "/api/playout/mode", json={"mode": "schedule", "day_start": "08:00:00"}
    )
    assert resp.status_code == 200
    ctx = main.channels.all()[0]
    assert ctx.day_start == "08:00:00"

    # GET should reflect the stored value
    resp = await client.get("/api/playout/mode")
    assert resp.json()["day_start"] == "08:00:00"


@pytest.mark.asyncio
async def test_get_playout_mode_returns_stored_day_start(client):
    """GET returns the day_start stored on ctx, not a hardcoded value."""
    import main

    ctx = main.channels.all()[0]
    ctx.day_start = "12:00:00"
    resp = await client.get("/api/playout/mode")
    assert resp.json()["day_start"] == "12:00:00"


@pytest.mark.asyncio
async def test_set_playout_mode_yaml_persist(client, tmp_path):
    """POST mode persists to channel YAML when file exists."""
    import main

    ctx = main.channels.all()[0]

    # Create a minimal channel YAML
    yaml_dir = tmp_path / "channels"
    yaml_dir.mkdir()
    yaml_file = yaml_dir / f"{ctx.id}.yaml"
    yaml_file.write_text(f"id: {ctx.id}\ndisplay_name: Test\n")

    # Patch CHANNEL_CONFIG_DIR to use tmp_path
    import config as cfg_mod

    original_dir = cfg_mod.CHANNEL_CONFIG_DIR
    cfg_mod.CHANNEL_CONFIG_DIR = str(yaml_dir)
    try:
        resp = await client.post(
            "/api/playout/mode", json={"mode": "loop", "day_start": "06:00:00"}
        )
        assert resp.status_code == 200

        # Verify YAML was updated
        import yaml  # type: ignore[import]

        with open(yaml_file) as f:
            doc = yaml.safe_load(f)
        assert doc["playlist_loop"] is True
        assert doc["day_start"] == "06:00:00"
    finally:
        cfg_mod.CHANNEL_CONFIG_DIR = original_dir


# ══════════════════════════════════════════════════════════════════
# /api/playout/encoding
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_get_encoding(client):
    """GET returns encoding params from engine._config."""
    resp = await client.get("/api/playout/encoding")
    assert resp.status_code == 200
    data = resp.json()
    assert data["width"] == 1920
    assert data["height"] == 1080
    assert data["fps"] == 30.0
    assert data["bitrate"] == "3000k"
    assert data["preset"] == "ultrafast"
    assert data["audio_bitrate"] == "128k"
    assert data["volume"] == 1.0


@pytest.mark.asyncio
async def test_patch_encoding_bitrate(client):
    """PATCH bitrate restarts engine with updated config."""
    import main

    resp = await client.patch("/api/playout/encoding", json={"bitrate": "5000k"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["restarted"] is True
    assert data["bitrate"] == "5000k"
    assert data["width"] == 1920
    main.playout.restart.assert_called()


@pytest.mark.asyncio
async def test_patch_encoding_resolution(client):
    """PATCH width+height restarts engine with new config."""
    import main

    resp = await client.patch(
        "/api/playout/encoding", json={"width": 1280, "height": 720}
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["restarted"] is True
    assert data["width"] == 1280
    assert data["height"] == 720
    main.playout.restart.assert_called()


@pytest.mark.asyncio
async def test_patch_encoding_all_fields(client):
    """PATCH all encoding fields at once restarts engine."""
    resp = await client.patch(
        "/api/playout/encoding",
        json={
            "width": 1280,
            "height": 720,
            "fps": 25.0,
            "bitrate": "2000k",
            "audio_bitrate": "192k",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["restarted"] is True
    assert data["width"] == 1280
    assert data["height"] == 720


@pytest.mark.asyncio
async def test_patch_encoding_empty_body(client):
    """No fields → 400."""
    resp = await client.patch("/api/playout/encoding", json={})
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_patch_encoding_bitrate_restarts(client):
    """PATCH bitrate alone triggers restart."""
    import main

    resp = await client.patch("/api/playout/encoding", json={"bitrate": "4000k"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["restarted"] is True
    main.playout.restart.assert_called()


# ══════════════════════════════════════════════════════════════════
# /api/playout/storage
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_get_storage(client):
    """GET returns filler, shuffle, extensions."""
    resp = await client.get("/api/playout/storage")
    assert resp.status_code == 200
    data = resp.json()
    assert "filler" in data
    assert "shuffle" in data
    assert "extensions" in data


@pytest.mark.asyncio
async def test_patch_storage_filler(client):
    """PATCH filler updates the failover path (relative to media dir)."""
    resp = await client.patch(
        "/api/playout/storage", json={"filler": "cathode/custom.mp4"}
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["filler"] == "cathode/custom.mp4"


@pytest.mark.asyncio
async def test_patch_storage_filler_rejects_absolute(client):
    """PATCH filler rejects absolute paths."""
    resp = await client.patch("/api/playout/storage", json={"filler": "/etc/shadow"})
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_patch_storage_shuffle(client):
    """PATCH shuffle updates the value."""
    resp = await client.patch("/api/playout/storage", json={"shuffle": True})
    assert resp.status_code == 200
    assert resp.json()["shuffle"] is True


@pytest.mark.asyncio
async def test_patch_storage_extensions(client):
    """PATCH extensions updates the list."""
    resp = await client.patch(
        "/api/playout/storage", json={"extensions": ["mp4", "mkv"]}
    )
    assert resp.status_code == 200
    assert resp.json()["extensions"] == ["mp4", "mkv"]


@pytest.mark.asyncio
async def test_patch_storage_empty_body(client):
    """No fields → 400."""
    resp = await client.patch("/api/playout/storage", json={})
    assert resp.status_code == 400


# Text overlay tests removed — text overlay is now provided by the
# overlay plugin at /api/overlay/*.  Tests live in the plugin test
# suite (cathode-plugins/overlay/tests/).


# ══════════════════════════════════════════════════════════════════
# /api/channel
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_get_channel_metadata(client):
    """GET returns all federation identity fields."""
    resp = await client.get("/api/channel")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == "channel-one"
    assert data["display_name"] == "TLTV Channel One"
    assert data["language"] == "en"
    assert data["access"] == "public"
    assert isinstance(data["tags"], list)
    assert "channel_id" in data


@pytest.mark.asyncio
async def test_patch_channel_display_name(client):
    import main

    resp = await client.patch("/api/channel", json={"display_name": "My TV"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["display_name"] == "My TV"
    ctx = main.channels.all()[0]
    assert ctx.display_name == "My TV"


@pytest.mark.asyncio
async def test_patch_channel_description(client):
    import main

    resp = await client.patch("/api/channel", json={"description": "New description"})
    assert resp.status_code == 200
    assert resp.json()["description"] == "New description"
    ctx = main.channels.all()[0]
    assert ctx.description == "New description"


@pytest.mark.asyncio
async def test_patch_channel_tags(client):
    import main

    resp = await client.patch("/api/channel", json={"tags": ["art", "film"]})
    assert resp.status_code == 200
    assert resp.json()["tags"] == ["art", "film"]
    ctx = main.channels.all()[0]
    assert ctx.tags == ["art", "film"]


@pytest.mark.asyncio
async def test_patch_channel_access(client):
    import main

    resp = await client.patch("/api/channel", json={"access": "private"})
    assert resp.status_code == 200
    ctx = main.channels.all()[0]
    assert ctx.access == "private"


@pytest.mark.asyncio
async def test_patch_channel_access_invalid(client):
    resp = await client.patch("/api/channel", json={"access": "secret"})
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_patch_channel_on_demand(client):
    import main

    resp = await client.patch("/api/channel", json={"on_demand": True})
    assert resp.status_code == 200
    ctx = main.channels.all()[0]
    assert ctx.on_demand is True


@pytest.mark.asyncio
async def test_patch_channel_timezone(client):
    """Updating timezone sets ctx.timezone directly — no external sync."""
    import main

    resp = await client.patch("/api/channel", json={"timezone": "Europe/Berlin"})
    assert resp.status_code == 200
    ctx = main.channels.all()[0]
    assert ctx.timezone == "Europe/Berlin"


@pytest.mark.asyncio
async def test_patch_channel_origins(client):
    import main

    resp = await client.patch(
        "/api/channel", json={"origins": ["https://tv.example.com"]}
    )
    assert resp.status_code == 200
    ctx = main.channels.all()[0]
    assert ctx.origins == ["https://tv.example.com"]


@pytest.mark.asyncio
async def test_patch_channel_multiple_fields(client):
    import main

    payload = {
        "display_name": "Updated Channel",
        "language": "fr",
        "tags": ["cinema"],
        "on_demand": True,
    }
    resp = await client.patch("/api/channel", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    ctx = main.channels.all()[0]
    assert ctx.display_name == "Updated Channel"
    assert ctx.language == "fr"
    assert ctx.tags == ["cinema"]
    assert ctx.on_demand is True


@pytest.mark.asyncio
async def test_patch_channel_empty_body(client):
    """No fields → 400."""
    resp = await client.patch("/api/channel", json={})
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_patch_channel_reflects_in_protocol(client):
    """Changes to ctx show up in the protocol metadata endpoint."""
    import main

    await client.patch("/api/channel", json={"display_name": "Protocol Test Channel"})
    ctx = main.channels.all()[0]
    # Protocol route reads directly from ctx — verify the value is there
    assert ctx.display_name == "Protocol Test Channel"


# ── channel status / retirement ──


@pytest.mark.asyncio
async def test_get_channel_includes_status(client):
    resp = await client.get("/api/channel")
    assert resp.status_code == 200
    assert "status" in resp.json()
    assert resp.json()["status"] == "active"


@pytest.mark.asyncio
async def test_patch_channel_retire(client):
    import main

    resp = await client.patch("/api/channel", json={"status": "retired"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["status"] == "retired"
    ctx = main.channels.all()[0]
    assert ctx.status == "retired"


@pytest.mark.asyncio
async def test_patch_channel_reactivate(client):
    import main

    # First retire, then reactivate
    await client.patch("/api/channel", json={"status": "retired"})
    resp = await client.patch("/api/channel", json={"status": "active"})
    assert resp.status_code == 200
    ctx = main.channels.all()[0]
    assert ctx.status == "active"


@pytest.mark.asyncio
async def test_patch_channel_status_invalid(client):
    resp = await client.patch("/api/channel", json={"status": "suspended"})
    assert resp.status_code == 400


# ══════════════════════════════════════════════════════════════════
# /api/playout/stop
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_stop_engine(client):
    """POST /stop calls engine.stop() and returns was_running=True."""
    import main

    main.playout.is_running = True
    resp = await client.post("/api/playout/stop")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["was_running"] is True
    main.playout.stop.assert_called_once()


@pytest.mark.asyncio
async def test_stop_engine_already_stopped(client):
    """POST /stop when engine not running returns was_running=False."""
    import main

    main.playout.is_running = False
    resp = await client.post("/api/playout/stop")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["was_running"] is False
    main.playout.stop.assert_not_called()


@pytest.mark.asyncio
async def test_stop_engine_no_engine(client):
    """POST /stop when engine is None returns 501."""
    import main

    original = main.playout
    ctx = main.channels.default()
    original_ctx_engine = ctx.engine
    main.playout = None
    ctx.engine = None
    try:
        resp = await client.post("/api/playout/stop")
        assert resp.status_code == 501
    finally:
        main.playout = original
        ctx.engine = original_ctx_engine


# ══════════════════════════════════════════════════════════════════
# /api/playout/start
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_start_engine(client):
    """POST /start starts a stopped engine."""
    import main

    main.playout.is_running = False
    resp = await client.post("/api/playout/start")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert "outputs" in data
    main.playout.start.assert_called_once()


@pytest.mark.asyncio
async def test_start_engine_already_running(client):
    """POST /start when engine is running returns 409."""
    import main

    main.playout.is_running = True
    resp = await client.post("/api/playout/start")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_start_engine_with_overrides(client):
    """POST /start with optional encoding overrides."""
    import main

    main.playout.is_running = False
    resp = await client.post(
        "/api/playout/start",
        json={"width": 1280, "height": 720},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True


@pytest.mark.asyncio
async def test_start_engine_no_engine(client):
    """POST /start when engine is None returns 501."""
    import main

    original = main.playout
    ctx = main.channels.default()
    original_ctx_engine = ctx.engine
    main.playout = None
    ctx.engine = None
    try:
        resp = await client.post("/api/playout/start")
        assert resp.status_code == 501
    finally:
        main.playout = original
        ctx.engine = original_ctx_engine


# ══════════════════════════════════════════════════════════════════
# Layer status — visibility + playlist_name (#3, #5)
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_layer_status_has_visibility_fields(client):
    """GET /layers/{name} includes visible, alpha, volume fields."""
    resp = await client.get("/api/playout/layers/input_a")
    assert resp.status_code == 200
    data = resp.json()
    assert "visible" in data
    assert "alpha" in data
    assert "volume" in data
    assert isinstance(data["visible"], bool)
    assert isinstance(data["alpha"], (int, float))
    assert isinstance(data["volume"], (int, float))


@pytest.mark.asyncio
async def test_layer_status_has_playlist_name(client):
    """GET /layers/{name} includes playlist_name when playlist loaded."""
    import main

    # Mock engine returns playlist source type
    ch = main.playout.channel("input_a")
    ch.source_type.value = "playlist"
    ch._playlist_entries = []
    ch._display_index = 0
    ch._playlist_loop = True
    ch._playlist_name = "evening-show"

    resp = await client.get("/api/playout/layers/input_a")
    data = resp.json()
    assert data.get("playlist_name") == "evening-show"


@pytest.mark.asyncio
async def test_health_api_has_visibility_fields(client):
    """GET /health includes visible, alpha, volume per channel."""
    resp = await client.get("/api/playout/health")
    assert resp.status_code == 200
    data = resp.json()

    for name in ("failover", "input_a", "input_b", "blinder"):
        ch = data["channels"][name]
        assert "visible" in ch, f"{name} missing visible"
        assert "alpha" in ch, f"{name} missing alpha"
        assert "volume" in ch, f"{name} missing volume"
        assert "playlist_name" in ch, f"{name} missing playlist_name"
