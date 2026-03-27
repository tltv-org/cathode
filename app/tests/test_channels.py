"""Tests for multi-channel support.

Covers:
  GET    /api/channels           — List channels
  POST   /api/channels           — Create channel
  GET    /api/channels/{id}      — Get channel detail
  PATCH  /api/channels/{id}      — Update channel
  DELETE /api/channels/{id}      — Delete channel
  ?channel= query param on per-channel endpoints
  ChannelRegistry methods
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


# ══════════════════════════════════════════════════════════════════
# ChannelRegistry unit tests
# ══════════════════════════════════════════════════════════════════


class TestChannelRegistry:
    """Test ChannelRegistry methods."""

    def test_register_and_get(self):
        from channel import ChannelContext, ChannelRegistry

        reg = ChannelRegistry()
        ctx = ChannelContext(id="ch-one", display_name="Channel One")
        reg.register(ctx)
        assert reg.get("ch-one") is ctx

    def test_duplicate_register_raises(self):
        from channel import ChannelContext, ChannelRegistry

        reg = ChannelRegistry()
        ctx = ChannelContext(id="ch-one", display_name="Channel One")
        reg.register(ctx)
        with pytest.raises(ValueError, match="already registered"):
            reg.register(ctx)

    def test_get_or_none(self):
        from channel import ChannelContext, ChannelRegistry

        reg = ChannelRegistry()
        assert reg.get_or_none("missing") is None
        ctx = ChannelContext(id="ch-one", display_name="Channel One")
        reg.register(ctx)
        assert reg.get_or_none("ch-one") is ctx

    def test_default(self):
        from channel import ChannelContext, ChannelRegistry

        reg = ChannelRegistry()
        ctx1 = ChannelContext(id="first", display_name="First")
        ctx2 = ChannelContext(id="second", display_name="Second")
        reg.register(ctx1)
        reg.register(ctx2)
        assert reg.default() is ctx1

    def test_default_empty_raises(self):
        from channel import ChannelRegistry

        reg = ChannelRegistry()
        with pytest.raises(RuntimeError, match="No channels"):
            reg.default()

    def test_unregister(self):
        from channel import ChannelContext, ChannelRegistry

        reg = ChannelRegistry()
        ctx = ChannelContext(id="ch-one", display_name="Channel One")
        reg.register(ctx)
        removed = reg.unregister("ch-one")
        assert removed is ctx
        assert "ch-one" not in reg
        assert len(reg) == 0

    def test_unregister_missing(self):
        from channel import ChannelRegistry

        reg = ChannelRegistry()
        assert reg.unregister("missing") is None

    def test_len_and_bool(self):
        from channel import ChannelContext, ChannelRegistry

        reg = ChannelRegistry()
        assert not reg
        assert len(reg) == 0
        reg.register(ChannelContext(id="ch", display_name="Ch"))
        assert reg
        assert len(reg) == 1


# ══════════════════════════════════════════════════════════════════
# Channels API endpoints
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_list_channels(client):
    """GET /api/channels returns the registered channels."""
    resp = await client.get("/api/channels")
    assert resp.status_code == 200
    data = resp.json()
    assert "channels" in data
    assert "count" in data
    assert data["count"] >= 1  # at least the test channel


@pytest.mark.asyncio
async def test_list_channels_includes_default(client):
    """The default test channel appears in the list."""
    resp = await client.get("/api/channels")
    ids = [ch["id"] for ch in resp.json()["channels"]]
    assert "channel-one" in ids


@pytest.mark.asyncio
async def test_get_channel_detail(client):
    """GET /api/channels/{id} returns full channel metadata."""
    resp = await client.get("/api/channels/channel-one")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == "channel-one"
    assert data["display_name"] == "TLTV Channel One"
    assert "channel_id" in data
    assert "status" in data


@pytest.mark.asyncio
async def test_get_channel_not_found(client):
    """GET /api/channels/{id} returns 404 for unknown channel."""
    resp = await client.get("/api/channels/nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_create_channel(client, tmp_path, monkeypatch):
    """POST /api/channels creates a new channel."""
    import config

    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(config, "MEDIA_DIR", str(tmp_path / "media"))

    resp = await client.post(
        "/api/channels",
        json={"id": "channel-two", "display_name": "Channel Two"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["id"] == "channel-two"
    assert "channel_id" in data  # federation ID

    # Clean up
    import main

    main.channels.unregister("channel-two")


@pytest.mark.asyncio
async def test_create_channel_duplicate(client):
    """POST /api/channels returns 409 for existing channel."""
    resp = await client.post(
        "/api/channels",
        json={"id": "channel-one", "display_name": "Duplicate"},
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_create_channel_invalid_id(client):
    """POST /api/channels returns 400 for invalid channel ID."""
    resp = await client.post(
        "/api/channels",
        json={"id": "bad id!", "display_name": "Bad"},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_update_channel(client):
    """PATCH /api/channels/{id} updates metadata."""
    resp = await client.patch(
        "/api/channels/channel-one",
        json={"description": "Updated description"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["updated"]["description"] == "Updated description"


@pytest.mark.asyncio
async def test_delete_channel_only_one(client):
    """DELETE /api/channels/{id} returns 409 if it's the only channel."""
    resp = await client.delete("/api/channels/channel-one")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_delete_channel_not_found(client):
    """DELETE /api/channels/{id} returns 404 for unknown channel."""
    resp = await client.delete("/api/channels/nonexistent")
    assert resp.status_code == 404


# ══════════════════════════════════════════════════════════════════
# ?channel= query param on per-channel endpoints
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_health_with_channel_param(client):
    """GET /api/playout/health?channel=channel-one works."""
    resp = await client.get("/api/playout/health?channel=channel-one")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_health_with_bad_channel(client):
    """GET /api/playout/health?channel=nonexistent returns 404."""
    resp = await client.get("/api/playout/health?channel=nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_health_without_channel_param(client):
    """GET /api/playout/health without ?channel= uses default."""
    resp = await client.get("/api/playout/health")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_mode_with_channel_param(client):
    """GET /api/playout/mode?channel=channel-one works."""
    resp = await client.get("/api/playout/mode?channel=channel-one")
    assert resp.status_code == 200
    data = resp.json()
    assert "mode" in data


@pytest.mark.asyncio
async def test_program_with_channel_param(client):
    """GET /api/program?channel=channel-one works."""
    resp = await client.get("/api/program?channel=channel-one")
    assert resp.status_code == 200
    data = resp.json()
    assert "programs" in data


@pytest.mark.asyncio
async def test_layers_with_channel_param(client):
    """GET /api/playout/layers/input_a?channel=channel-one works."""
    resp = await client.get("/api/playout/layers/input_a?channel=channel-one")
    assert resp.status_code == 200
