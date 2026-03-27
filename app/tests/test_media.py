"""Tests for media management endpoints — /api/media.

Covers:
    POST   /api/media/upload       — Upload a media file
    DELETE /api/media/{filename}    — Delete (with reference check)
    GET    /api/media/{filename}    — Single file metadata
"""

from __future__ import annotations

import io
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


@pytest.fixture(autouse=True)
def media_tmp_dir(tmp_path, monkeypatch):
    """Point media routes at a temp directory."""
    import config

    monkeypatch.setattr(config, "MEDIA_UPLOAD_DIR", str(tmp_path))
    monkeypatch.setattr(config, "MEDIA_DIR", str(tmp_path))
    yield tmp_path


# ══════════════════════════════════════════════════════════════════
# GET /api/media/{filename}
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_get_media_metadata(client, media_tmp_dir):
    """GET returns metadata for an existing file."""
    # Create a dummy file
    f = media_tmp_dir / "test.mp4"
    f.write_bytes(b"\x00" * 1024)

    with (
        patch("routes.media.get_clip_duration", return_value=30.0),
        patch("routes.media.subprocess") as mock_sub,
    ):
        mock_sub.run.return_value.stdout = (
            '{"format": {"format_name": "mp4"}, "streams": []}'
        )
        resp = await client.get("/api/media/test.mp4")

    assert resp.status_code == 200
    data = resp.json()
    assert data["filename"] == "test.mp4"
    assert data["duration"] == 30.0
    assert data["size"] == 1024


@pytest.mark.asyncio
async def test_get_media_not_found(client):
    """GET for missing file → 404."""
    resp = await client.get("/api/media/missing.mp4")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_media_path_traversal(client):
    """GET with path traversal chars → 400."""
    # URL-level ../ gets normalized by HTTP client, so test with
    # a filename containing .. as a substring
    resp = await client.get("/api/media/..passwd")
    assert resp.status_code == 400


# ══════════════════════════════════════════════════════════════════
# POST /api/media/upload
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_upload_media(client, media_tmp_dir):
    """Upload a valid media file."""
    content = b"\x00" * 2048

    with patch("routes.media.get_clip_duration", return_value=15.0):
        resp = await client.post(
            "/api/media/upload",
            files={"file": ("test_clip.mp4", io.BytesIO(content), "video/mp4")},
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["filename"] == "test_clip.mp4"
    assert data["size"] == 2048
    assert data["duration"] == 15.0
    # Verify file was written
    assert (media_tmp_dir / "test_clip.mp4").exists()


@pytest.mark.asyncio
async def test_upload_bad_extension(client):
    """Upload with unsupported extension → 400."""
    resp = await client.post(
        "/api/media/upload",
        files={"file": ("script.sh", io.BytesIO(b"#!/bin/bash"), "application/x-sh")},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_upload_duplicate(client, media_tmp_dir):
    """Upload when file already exists → 409."""
    (media_tmp_dir / "existing.mp4").write_bytes(b"\x00")
    resp = await client.post(
        "/api/media/upload",
        files={"file": ("existing.mp4", io.BytesIO(b"\x00"), "video/mp4")},
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_upload_size_limit(client, media_tmp_dir, monkeypatch):
    """Upload exceeding size limit → 413."""
    import config

    monkeypatch.setattr(config, "MEDIA_UPLOAD_MAX_SIZE", 100)  # 100 bytes
    content = b"\x00" * 200
    resp = await client.post(
        "/api/media/upload",
        files={"file": ("big.mp4", io.BytesIO(content), "video/mp4")},
    )
    assert resp.status_code == 413
    # Verify partial file was cleaned up
    assert not (media_tmp_dir / "big.mp4").exists()


# ══════════════════════════════════════════════════════════════════
# DELETE /api/media/{filename}
# ══════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_delete_media(client, media_tmp_dir):
    """Delete an unreferenced file."""
    (media_tmp_dir / "delete_me.mp4").write_bytes(b"\x00")

    with patch("routes.media._find_references", return_value=[]):
        resp = await client.delete("/api/media/delete_me.mp4")

    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    assert not (media_tmp_dir / "delete_me.mp4").exists()


@pytest.mark.asyncio
async def test_delete_media_not_found(client):
    """Delete missing file → 404."""
    resp = await client.delete("/api/media/missing.mp4")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_media_in_use(client, media_tmp_dir):
    """Delete file that's referenced → 409."""
    (media_tmp_dir / "in_use.mp4").write_bytes(b"\x00")

    refs = [{"type": "named_playlist", "name": "morning", "detail": "Used in morning"}]
    with patch("routes.media._find_references", return_value=refs):
        resp = await client.delete("/api/media/in_use.mp4")

    assert resp.status_code == 409
    # File should still exist
    assert (media_tmp_dir / "in_use.mp4").exists()


@pytest.mark.asyncio
async def test_delete_media_path_traversal(client):
    """Delete with path traversal chars → 400."""
    resp = await client.delete("/api/media/..secret.mp4")
    assert resp.status_code == 400


# ══════════════════════════════════════════════════════════════════
# Reference checking
# ══════════════════════════════════════════════════════════════════


def test_find_references_in_named_playlist(tmp_path, monkeypatch):
    """_find_references detects files in named playlists."""
    import config
    import named_playlist_store
    import routes.media as media_mod

    monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path / "playlists"))
    monkeypatch.setattr(config, "MEDIA_UPLOAD_DIR", str(tmp_path / "media"))
    monkeypatch.setattr(config, "MEDIA_DIR", str(tmp_path / "media"))

    media_dir = tmp_path / "media"
    media_dir.mkdir()

    named_playlist_store.save(
        "test-pl",
        [{"source": str(media_dir / "clip.mp4"), "duration": 30.0}],
    )

    refs = media_mod._find_references("clip.mp4")
    assert len(refs) >= 1
    assert refs[0]["type"] == "named_playlist"
    assert refs[0]["name"] == "test-pl"
