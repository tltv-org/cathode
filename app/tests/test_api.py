"""Tests for the core REST API endpoints.

Uses httpx AsyncClient with ASGITransport to test the FastAPI app
directly, with all external dependencies replaced by mocks from conftest.py.

Core endpoints: status, playlist, schedule, media listing, system, program.
Plugin endpoint tests live in cathode-plugins.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


# ── Status endpoints ──


@pytest.mark.asyncio
async def test_get_status(client):
    resp = await client.get("/api/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "version" in data
    assert data["backend"] == "gstreamer"
    assert "engine" in data


@pytest.mark.asyncio
async def test_now_playing(client):
    resp = await client.get("/api/now-playing")
    assert resp.status_code == 200
    data = resp.json()
    assert "source" in data


# ── Playlist endpoints ──


@pytest.mark.asyncio
async def test_set_playlist(client, test_media_dir):
    resp = await client.post(
        "/api/playlist",
        json={
            "files": ["clip_01.mp4"],
        },
    )
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


@pytest.mark.asyncio
async def test_set_playlist_file_not_found(client):
    resp = await client.post(
        "/api/playlist",
        json={
            "files": ["nonexistent.mp4"],
        },
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_skip(client):
    resp = await client.post("/api/skip")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_back(client):
    resp = await client.post("/api/back")
    assert resp.status_code == 200


# ── Schedule endpoints ──


@pytest.mark.asyncio
async def test_get_schedule(client):
    resp = await client.get("/api/schedule")
    assert resp.status_code == 200
    data = resp.json()
    assert "schedule" in data
    assert "today" in data


@pytest.mark.asyncio
async def test_get_schedule_date(client):
    """Schedule uses PlaylistStore — save one first."""
    from datetime import date as dt_date
    import playlist_store

    playlist_store.save(
        dt_date(2026, 3, 13),
        [{"source": "/media/clip_01.mp4", "duration": 30.0}],
    )
    resp = await client.get("/api/schedule/2026-03-13")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_get_schedule_date_not_found(client):
    """PlaylistStore returns 404 for missing dates."""
    resp = await client.get("/api/schedule/2099-01-01")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_generate_schedule_auto(client, test_media_dir):
    resp = await client.post(
        "/api/schedule/generate",
        json={
            "date": "2026-03-20",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["method"] == "auto"


@pytest.mark.asyncio
async def test_generate_schedule_with_files(client, test_media_dir):
    resp = await client.post(
        "/api/schedule/generate",
        json={
            "date": "2026-03-20",
            "files": ["clip_01.mp4"],
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["method"] == "files"


@pytest.mark.asyncio
async def test_generate_schedule_invalid_date(client):
    resp = await client.post(
        "/api/schedule/generate",
        json={
            "date": "not-a-date",
        },
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_generate_schedule_from_named_playlist(client, tmp_path, monkeypatch):
    """Generate schedule from a named playlist."""
    import config
    import named_playlist_store

    monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path / "playlists"))
    named_playlist_store.save(
        "test-schedule",
        [
            {"source": "/media/test/clip_01.mp4", "duration": 30.0},
            {"source": "/media/test/clip_02.mp4", "duration": 45.0},
        ],
    )
    resp = await client.post(
        "/api/schedule/generate",
        json={"date": "2026-04-01", "playlist_name": "test-schedule"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["method"] == "named_playlist"
    assert data["playlist_name"] == "test-schedule"
    assert data["clips"] == 2


@pytest.mark.asyncio
async def test_generate_schedule_named_playlist_not_found(
    client, tmp_path, monkeypatch
):
    """Named playlist that doesn't exist → 404."""
    import config

    monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path / "playlists"))
    resp = await client.post(
        "/api/schedule/generate",
        json={"date": "2026-04-01", "playlist_name": "nonexistent"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_schedule(client):
    resp = await client.delete("/api/schedule/2026-03-20")
    assert resp.status_code == 200


# ── Media listing ──


@pytest.mark.asyncio
async def test_list_media(client, test_media_dir):
    resp = await client.get("/api/media")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 3  # clip_01, clip_02, clip_03
    assert len(data["items"]) == 3


# ── System ──


@pytest.mark.asyncio
async def test_system_info(client):
    resp = await client.get("/api/system")
    assert resp.status_code == 200
    data = resp.json()
    assert "cpu" in data
    assert "memory" in data
    assert "disk" in data


# ── Program endpoints ──


@pytest.mark.asyncio
async def test_list_programs(client):
    resp = await client.get("/api/program")
    assert resp.status_code == 200
    data = resp.json()
    assert "programs" in data


@pytest.mark.asyncio
async def test_set_program(client):
    resp = await client.post(
        "/api/program/2026-03-20",
        json={
            "blocks": [
                {
                    "start": "19:00:00",
                    "end": "20:00:00",
                    "type": "playlist",
                    "title": "Test Playlist",
                },
            ],
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["block_count"] == 1


@pytest.mark.asyncio
async def test_set_program_canvas_with_preset(client):
    resp = await client.post(
        "/api/program/2026-03-20",
        json={
            "blocks": [
                {
                    "start": "19:00:00",
                    "end": "19:30:00",
                    "type": "canvas",
                    "preset": "channel-one-intro",
                    "title": "Test Canvas",
                },
            ],
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True


@pytest.mark.asyncio
async def test_set_program_canvas_html_and_preset_rejected(client):
    resp = await client.post(
        "/api/program/2026-03-20",
        json={
            "blocks": [
                {
                    "start": "19:00:00",
                    "end": "19:30:00",
                    "type": "canvas",
                    "html": "<div>Hi</div>",
                    "preset": "channel-one-intro",
                    "title": "Test Canvas",
                },
            ],
        },
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_set_program_invalid_date(client):
    resp = await client.post(
        "/api/program/not-a-date",
        json={
            "blocks": [],
        },
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_set_program_validation_error(client):
    resp = await client.post(
        "/api/program/2026-03-20",
        json={
            "blocks": [
                {
                    "start": "19:00:00",
                    "end": "19:00:00",
                    "type": "playlist",
                    "title": "Test Playlist",
                },
            ],
        },
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_get_program(client):
    await client.post(
        "/api/program/2026-03-20",
        json={
            "blocks": [
                {
                    "start": "19:00:00",
                    "end": "20:00:00",
                    "type": "playlist",
                    "title": "Test Playlist",
                },
            ],
        },
    )
    resp = await client.get("/api/program/2026-03-20")
    assert resp.status_code == 200
    assert len(resp.json()["blocks"]) == 1


@pytest.mark.asyncio
async def test_get_program_not_found(client):
    resp = await client.get("/api/program/2099-01-01")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_program(client):
    await client.post(
        "/api/program/2026-03-20",
        json={
            "blocks": [
                {
                    "start": "19:00:00",
                    "end": "20:00:00",
                    "type": "playlist",
                    "title": "Test Playlist",
                }
            ],
        },
    )
    resp = await client.delete("/api/program/2026-03-20")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


@pytest.mark.asyncio
async def test_delete_program_not_found(client):
    resp = await client.delete("/api/program/2099-01-01")
    assert resp.status_code == 404


# ── Log endpoints ──


@pytest.mark.asyncio
async def test_get_logs(client):
    """GET /api/logs returns log entries with correct fields."""
    resp = await client.get("/api/logs")
    assert resp.status_code == 200
    data = resp.json()
    assert "entries" in data
    assert "count" in data
    assert isinstance(data["entries"], list)
    # Verify entry structure matches phosphor LogEntry type
    if data["entries"]:
        entry = data["entries"][0]
        assert "timestamp" in entry
        assert "level" in entry
        assert "source" in entry
        assert "logger" in entry
        assert "message" in entry
        assert "module" in entry
        assert "lineno" in entry


@pytest.mark.asyncio
async def test_get_logs_with_limit(client):
    """GET /api/logs?limit=5 limits returned entries."""
    resp = await client.get("/api/logs?limit=5")
    assert resp.status_code == 200
    assert resp.json()["count"] <= 5


@pytest.mark.asyncio
async def test_get_logs_with_level_filter(client):
    """GET /api/logs?level=ERROR filters by level."""
    resp = await client.get("/api/logs?level=ERROR")
    assert resp.status_code == 200
    data = resp.json()
    for entry in data["entries"]:
        assert entry["level"] == "ERROR"


@pytest.mark.asyncio
async def test_get_logs_with_source_filter(client):
    """GET /api/logs?source=engine filters by source category."""
    resp = await client.get("/api/logs?source=engine")
    assert resp.status_code == 200
    data = resp.json()
    for entry in data["entries"]:
        assert entry["source"] == "engine"


@pytest.mark.asyncio
async def test_log_stream_endpoint_exists(client):
    """GET /api/logs/stream returns SSE content type."""
    resp = await client.get("/api/logs/stream")
    assert resp.status_code == 200
    assert "text/event-stream" in resp.headers.get("content-type", "")


# ── Backup / restore endpoints ──


@pytest.mark.asyncio
async def test_backup_returns_archive(client):
    """POST /api/backup returns a gzip archive."""
    resp = await client.post("/api/backup")
    assert resp.status_code == 200
    assert "gzip" in resp.headers.get("content-type", "")
    assert "cathode-backup" in resp.headers.get("content-disposition", "")
    # Should be valid gzip (first two bytes)
    assert resp.content[:2] == b"\x1f\x8b"


@pytest.mark.asyncio
async def test_restore_rejects_non_archive(client):
    """POST /api/restore rejects non-tar.gz files."""
    resp = await client.post(
        "/api/restore",
        files={"file": ("bad.txt", b"not an archive", "text/plain")},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_backup_restore_roundtrip(client, tmp_path):
    """Backup and restore produces a valid roundtrip."""
    # Create a backup
    resp = await client.post("/api/backup")
    assert resp.status_code == 200
    archive_data = resp.content

    # Restore it
    resp = await client.post(
        "/api/restore",
        files={"file": ("cathode-backup.tar.gz", archive_data, "application/gzip")},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert isinstance(data["restored"], list)
    assert "engine_restarted" in data
