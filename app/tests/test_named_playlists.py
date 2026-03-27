"""Tests for named playlist CRUD — /api/playlists endpoints.

Covers:
    GET    /api/playlists              — List all saved playlists
    GET    /api/playlists/{name}       — Get a playlist by name
    POST   /api/playlists/{name}       — Create/update a named playlist
    DELETE /api/playlists/{name}       — Delete a named playlist
    POST   /api/playlists/{name}/load  — Load a named playlist onto a layer

Also covers:
    - Named playlist store CRUD operations
    - Name validation
    - Legacy POST /api/playlist with 'name' field
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


# ══════════════════════════════════════════════════════════════════
# Named playlist store unit tests
# ══════════════════════════════════════════════════════════════════


class TestNameValidation:
    """Test playlist name validation rules."""

    def test_valid_names(self):
        import named_playlist_store

        assert named_playlist_store.validate_name("morning-show") is None
        assert named_playlist_store.validate_name("playlist_1") is None
        assert named_playlist_store.validate_name("a") is None
        assert named_playlist_store.validate_name("123") is None
        assert named_playlist_store.validate_name("A-z_0") is None

    def test_empty_name(self):
        import named_playlist_store

        assert named_playlist_store.validate_name("") is not None

    def test_special_chars(self):
        import named_playlist_store

        assert named_playlist_store.validate_name("has spaces") is not None
        assert named_playlist_store.validate_name("has/slash") is not None
        assert named_playlist_store.validate_name("has.dot") is not None
        assert named_playlist_store.validate_name("../escape") is not None

    def test_starts_with_hyphen(self):
        import named_playlist_store

        assert named_playlist_store.validate_name("-bad") is not None

    def test_too_long(self):
        import named_playlist_store

        assert named_playlist_store.validate_name("a" * 65) is not None
        assert named_playlist_store.validate_name("a" * 64) is None


class TestNamedPlaylistStore:
    """Test the named_playlist_store module directly."""

    def test_save_and_get(self, tmp_path, monkeypatch):
        import config
        import named_playlist_store

        monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path))
        entries = [
            {"source": "/media/clip1.mp4", "duration": 30.0},
            {"source": "/media/clip2.mp4", "duration": 45.0},
        ]
        result = named_playlist_store.save("test-pl", entries)
        assert result["name"] == "test-pl"
        assert len(result["entries"]) == 2
        assert "created" in result
        assert "updated" in result

        loaded = named_playlist_store.get("test-pl")
        assert loaded is not None
        assert loaded["name"] == "test-pl"
        assert len(loaded["entries"]) == 2

    def test_get_nonexistent(self, tmp_path, monkeypatch):
        import config
        import named_playlist_store

        monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path))
        assert named_playlist_store.get("nope") is None

    def test_update_preserves_created(self, tmp_path, monkeypatch):
        import config
        import named_playlist_store

        monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path))
        entries = [{"source": "/media/clip1.mp4", "duration": 30.0}]
        first = named_playlist_store.save("update-test", entries)

        entries.append({"source": "/media/clip2.mp4", "duration": 20.0})
        second = named_playlist_store.save("update-test", entries)
        assert second["created"] == first["created"]
        assert second["updated"] != first["created"]  # updated should be newer
        assert len(second["entries"]) == 2

    def test_delete(self, tmp_path, monkeypatch):
        import config
        import named_playlist_store

        monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path))
        entries = [{"source": "/media/clip1.mp4", "duration": 30.0}]
        named_playlist_store.save("del-test", entries)
        assert named_playlist_store.delete("del-test") is True
        assert named_playlist_store.get("del-test") is None

    def test_delete_nonexistent(self, tmp_path, monkeypatch):
        import config
        import named_playlist_store

        monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path))
        assert named_playlist_store.delete("nope") is False

    def test_list_all(self, tmp_path, monkeypatch):
        import config
        import named_playlist_store

        monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path))

        named_playlist_store.save(
            "alpha",
            [{"source": "/media/clip1.mp4", "duration": 30.0}],
        )
        named_playlist_store.save(
            "beta",
            [
                {"source": "/media/clip1.mp4", "duration": 30.0},
                {"source": "/media/clip2.mp4", "duration": 60.0},
            ],
        )

        result = named_playlist_store.list_all()
        assert len(result) == 2
        names = [p["name"] for p in result]
        assert "alpha" in names
        assert "beta" in names

        beta = next(p for p in result if p["name"] == "beta")
        assert beta["entry_count"] == 2
        assert beta["total_duration"] == 90.0

    def test_list_all_empty(self, tmp_path, monkeypatch):
        import config
        import named_playlist_store

        monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path))
        assert named_playlist_store.list_all() == []

    def test_channel_isolation(self, tmp_path, monkeypatch):
        import config
        import named_playlist_store

        monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path))
        entries = [{"source": "/media/clip1.mp4", "duration": 30.0}]
        named_playlist_store.save("shared-name", entries, channel_id="ch-a")
        named_playlist_store.save("shared-name", entries, channel_id="ch-b")

        assert named_playlist_store.get("shared-name", channel_id="ch-a") is not None
        assert named_playlist_store.get("shared-name", channel_id="ch-b") is not None
        named_playlist_store.delete("shared-name", channel_id="ch-a")
        assert named_playlist_store.get("shared-name", channel_id="ch-a") is None
        assert named_playlist_store.get("shared-name", channel_id="ch-b") is not None


# ══════════════════════════════════════════════════════════════════
# API endpoint tests
# ══════════════════════════════════════════════════════════════════


@pytest.fixture(autouse=True)
def use_tmp_playlist_dir(tmp_path, monkeypatch):
    """Point named_playlist_store at a temp directory for each test."""
    import config

    monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path))
    yield


def _seed_playlist(name: str, entries: list[dict] | None = None):
    """Helper: save a named playlist directly via the store."""
    import named_playlist_store

    if entries is None:
        entries = [
            {"source": "/media/test/clip_01.mp4", "duration": 30.0},
            {"source": "/media/test/clip_02.mp4", "duration": 45.0},
        ]
    return named_playlist_store.save(name, entries)


@pytest.mark.asyncio
async def test_list_playlists_empty(client):
    """GET /api/playlists with no saved playlists."""
    resp = await client.get("/api/playlists")
    assert resp.status_code == 200
    data = resp.json()
    assert data["playlists"] == []
    assert data["count"] == 0


@pytest.mark.asyncio
async def test_list_playlists(client):
    """GET /api/playlists returns summary of all playlists."""
    _seed_playlist("morning")
    _seed_playlist("evening")
    resp = await client.get("/api/playlists")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 2
    names = [p["name"] for p in data["playlists"]]
    assert "morning" in names
    assert "evening" in names


@pytest.mark.asyncio
async def test_get_playlist(client):
    """GET /api/playlists/{name} returns full entry details."""
    _seed_playlist("test-get")
    resp = await client.get("/api/playlists/test-get")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "test-get"
    assert len(data["entries"]) == 2


@pytest.mark.asyncio
async def test_get_playlist_not_found(client):
    """GET /api/playlists/{name} for missing playlist → 404."""
    resp = await client.get("/api/playlists/nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_create_playlist(client, tmp_path):
    """POST /api/playlists/{name} creates a new playlist."""
    # Create test media files
    media_file = tmp_path / "clip.mp4"
    media_file.touch()

    with (
        patch("routes.playlists.config") as mock_cfg,
        patch("routes.playlists.get_clip_duration", return_value=30.0),
    ):
        mock_cfg.MEDIA_DIR = str(tmp_path)
        resp = await client.post(
            "/api/playlists/new-list",
            json={"files": ["clip.mp4"]},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["name"] == "new-list"
    assert data["entry_count"] == 1


@pytest.mark.asyncio
async def test_create_playlist_invalid_name(client):
    """POST /api/playlists/{name} with invalid name → 400."""
    # Names with dots/special chars are rejected by validate_name
    resp = await client.post(
        "/api/playlists/has.dots",
        json={"files": ["clip.mp4"]},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_create_playlist_empty_files(client):
    """POST /api/playlists/{name} with empty files → 400."""
    resp = await client.post(
        "/api/playlists/empty",
        json={"files": []},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_create_playlist_file_not_found(client):
    """POST /api/playlists/{name} with missing file → 404."""
    with patch("routes.playlists.config") as mock_cfg:
        mock_cfg.MEDIA_DIR = "/nonexistent"
        resp = await client.post(
            "/api/playlists/bad-files",
            json={"files": ["missing.mp4"]},
        )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_playlist(client):
    """DELETE /api/playlists/{name} removes the playlist."""
    _seed_playlist("to-delete")
    resp = await client.delete("/api/playlists/to-delete")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    assert resp.json()["deleted"] == "to-delete"

    # Verify it's gone
    resp = await client.get("/api/playlists/to-delete")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_playlist_not_found(client):
    """DELETE /api/playlists/{name} for missing playlist → 404."""
    resp = await client.delete("/api/playlists/nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_load_playlist(client):
    """POST /api/playlists/{name}/load loads onto the engine."""
    import main

    _seed_playlist("to-load")
    resp = await client.post("/api/playlists/to-load/load")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["name"] == "to-load"
    assert data["layer"] == "input_a"
    assert data["loop"] is True
    # Engine methods should have been called
    main.playout.input_a.load_playlist.assert_called_once()
    main.playout.show.assert_called_with("input_a")


@pytest.mark.asyncio
async def test_load_playlist_custom_layer(client):
    """POST /api/playlists/{name}/load with custom layer."""
    import main

    _seed_playlist("for-blinder")
    resp = await client.post(
        "/api/playlists/for-blinder/load",
        json={"layer": "input_b", "loop": False},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["layer"] == "input_b"
    assert data["loop"] is False


@pytest.mark.asyncio
async def test_load_playlist_invalid_layer(client):
    """POST /api/playlists/{name}/load with invalid layer → 400."""
    _seed_playlist("bad-layer")
    resp = await client.post(
        "/api/playlists/bad-layer/load",
        json={"layer": "nonexistent"},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_load_playlist_not_found(client):
    """POST /api/playlists/{name}/load for missing playlist → 404."""
    resp = await client.post("/api/playlists/missing/load")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_load_playlist_no_engine(client):
    """POST /api/playlists/{name}/load with no engine → 501."""
    import main

    _seed_playlist("no-engine")
    original = main.playout
    main.playout = None
    try:
        resp = await client.post("/api/playlists/no-engine/load")
        assert resp.status_code == 501
    finally:
        main.playout = original


# ══════════════════════════════════════════════════════════════════
# Legacy endpoint integration
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_legacy_playlist_with_name(client, tmp_path):
    """POST /api/playlist with name field also saves as named playlist."""
    import named_playlist_store

    media_file = tmp_path / "clip.mp4"
    media_file.touch()

    with (
        patch("routes.playlist.config") as mock_cfg,
        patch("routes.playlist.get_clip_duration", return_value=30.0),
        patch("routes.playlist.playlist_store"),
    ):
        mock_cfg.MEDIA_DIR = str(tmp_path)
        resp = await client.post(
            "/api/playlist",
            json={"files": ["clip.mp4"], "name": "saved-legacy"},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["saved_as"] == "saved-legacy"

    # Check it was saved
    saved = named_playlist_store.get("saved-legacy")
    assert saved is not None
    assert saved["name"] == "saved-legacy"
