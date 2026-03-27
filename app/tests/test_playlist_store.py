"""Tests for PlaylistStore — pure Python, no GStreamer required.

Runs in both the standard test container and the playout container.
"""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import playlist_store


# ── Fixtures ──


@pytest.fixture(autouse=True)
def override_playlist_dir(tmp_path, monkeypatch):
    """Override PLAYLIST_DIR for every test."""
    import config

    monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path))
    monkeypatch.setattr(playlist_store, "PLAYLIST_DIR", str(tmp_path))
    yield tmp_path


# ── save ──


def test_save_creates_file(override_playlist_dir):
    """save() creates file with correct JSON."""
    d = date(2026, 3, 18)
    entries = [
        {"source": "/media/clip_01.mp4", "duration": 120.5},
        {"source": "/media/clip_02.mp4", "duration": 45.0},
    ]

    result = playlist_store.save(d, entries, loop=True, channel_id="test-ch")

    assert result["date"] == "2026-03-18"
    assert len(result["entries"]) == 2
    assert result["loop"] is True
    assert "created" in result

    # Verify the file exists
    path = override_playlist_dir / "test-ch" / "2026-03-18.json"
    assert path.exists()

    # Verify JSON content
    data = json.loads(path.read_text())
    assert data["date"] == "2026-03-18"
    assert len(data["entries"]) == 2


def test_save_with_loop_false(override_playlist_dir):
    """save() with loop=False stores loop field correctly."""
    d = date(2026, 3, 18)
    entries = [{"source": "/media/clip.mp4", "duration": 30.0}]

    result = playlist_store.save(d, entries, loop=False, channel_id="test-ch")
    assert result["loop"] is False


def test_save_overwrites_existing(override_playlist_dir):
    """save() overwrites an existing playlist for the same date."""
    d = date(2026, 3, 18)
    playlist_store.save(d, [{"source": "a.mp4", "duration": 10}], channel_id="test-ch")
    playlist_store.save(d, [{"source": "b.mp4", "duration": 20}], channel_id="test-ch")

    result = playlist_store.get(d, channel_id="test-ch")
    assert len(result["entries"]) == 1
    assert result["entries"][0]["source"] == "b.mp4"


def test_save_empty_entries(override_playlist_dir):
    """save() with empty entries list works."""
    d = date(2026, 3, 18)
    result = playlist_store.save(d, [], channel_id="test-ch")
    assert len(result["entries"]) == 0


# ── get ──


def test_get_returns_saved_data(override_playlist_dir):
    """get() returns saved data."""
    d = date(2026, 3, 18)
    entries = [{"source": "/media/clip.mp4", "duration": 60.0}]
    playlist_store.save(d, entries, channel_id="test-ch")

    result = playlist_store.get(d, channel_id="test-ch")
    assert result is not None
    assert result["date"] == "2026-03-18"
    assert len(result["entries"]) == 1
    assert result["entries"][0]["source"] == "/media/clip.mp4"


def test_get_returns_none_for_missing(override_playlist_dir):
    """get() returns None for missing date."""
    result = playlist_store.get(date(2099, 1, 1), channel_id="test-ch")
    assert result is None


def test_get_default_channel(override_playlist_dir):
    """get() uses 'channel-one' as default channel_id."""
    d = date(2026, 3, 18)
    playlist_store.save(d, [{"source": "a.mp4", "duration": 10}])

    result = playlist_store.get(d)
    assert result is not None


# ── delete ──


def test_delete_removes_file(override_playlist_dir):
    """delete() removes file, returns True."""
    d = date(2026, 3, 18)
    playlist_store.save(d, [{"source": "a.mp4", "duration": 10}], channel_id="test-ch")

    assert playlist_store.delete(d, channel_id="test-ch") is True
    assert playlist_store.get(d, channel_id="test-ch") is None


def test_delete_returns_false_for_missing(override_playlist_dir):
    """delete() returns False for missing date."""
    assert playlist_store.delete(date(2099, 1, 1), channel_id="test-ch") is False


# ── exists ──


def test_exists_true(override_playlist_dir):
    """exists() returns True for saved playlist."""
    d = date(2026, 3, 18)
    playlist_store.save(d, [{"source": "a.mp4", "duration": 10}], channel_id="test-ch")

    assert playlist_store.exists(d, channel_id="test-ch") is True


def test_exists_false(override_playlist_dir):
    """exists() returns False for missing date."""
    assert playlist_store.exists(date(2099, 1, 1), channel_id="test-ch") is False


def test_exists_after_delete(override_playlist_dir):
    """exists() returns False after delete."""
    d = date(2026, 3, 18)
    playlist_store.save(d, [{"source": "a.mp4", "duration": 10}], channel_id="test-ch")
    playlist_store.delete(d, channel_id="test-ch")

    assert playlist_store.exists(d, channel_id="test-ch") is False


# ── list_dates ──


def test_list_dates_returns_dict(override_playlist_dir):
    """list_dates() returns dict for N days."""
    result = playlist_store.list_dates(days_ahead=7, channel_id="test-ch")
    assert isinstance(result, dict)
    assert len(result) == 7


def test_list_dates_shows_saved(override_playlist_dir):
    """list_dates() shows True for saved dates."""
    today = date.today()
    playlist_store.save(
        today, [{"source": "a.mp4", "duration": 10}], channel_id="test-ch"
    )

    result = playlist_store.list_dates(days_ahead=3, channel_id="test-ch")
    assert result[today.isoformat()] is True


def test_list_dates_shows_missing(override_playlist_dir):
    """list_dates() shows False for missing dates."""
    result = playlist_store.list_dates(days_ahead=3, channel_id="test-ch")
    # All should be False (nothing saved)
    for val in result.values():
        assert val is False


# ── Channel isolation ──


def test_channel_isolation(override_playlist_dir):
    """Different channel_ids don't interfere."""
    d = date(2026, 3, 18)
    playlist_store.save(
        d, [{"source": "ch1.mp4", "duration": 10}], channel_id="channel-1"
    )
    playlist_store.save(
        d, [{"source": "ch2.mp4", "duration": 20}], channel_id="channel-2"
    )

    r1 = playlist_store.get(d, channel_id="channel-1")
    r2 = playlist_store.get(d, channel_id="channel-2")

    assert r1["entries"][0]["source"] == "ch1.mp4"
    assert r2["entries"][0]["source"] == "ch2.mp4"


def test_delete_one_channel_doesnt_affect_other(override_playlist_dir):
    """Deleting from one channel doesn't affect another."""
    d = date(2026, 3, 18)
    playlist_store.save(
        d, [{"source": "ch1.mp4", "duration": 10}], channel_id="channel-1"
    )
    playlist_store.save(
        d, [{"source": "ch2.mp4", "duration": 20}], channel_id="channel-2"
    )

    playlist_store.delete(d, channel_id="channel-1")
    assert playlist_store.get(d, channel_id="channel-1") is None
    assert playlist_store.get(d, channel_id="channel-2") is not None


# ── Edge cases ──


def test_save_creates_nested_directories(override_playlist_dir):
    """save() creates channel subdirectory if it doesn't exist."""
    d = date(2026, 3, 18)
    playlist_store.save(d, [], channel_id="new-channel")

    path = override_playlist_dir / "new-channel" / "2026-03-18.json"
    assert path.exists()


def test_multiple_dates_same_channel(override_playlist_dir):
    """Multiple dates for the same channel are stored separately."""
    d1 = date(2026, 3, 18)
    d2 = date(2026, 3, 19)

    playlist_store.save(d1, [{"source": "a.mp4", "duration": 10}], channel_id="test-ch")
    playlist_store.save(d2, [{"source": "b.mp4", "duration": 20}], channel_id="test-ch")

    r1 = playlist_store.get(d1, channel_id="test-ch")
    r2 = playlist_store.get(d2, channel_id="test-ch")

    assert r1["entries"][0]["source"] == "a.mp4"
    assert r2["entries"][0]["source"] == "b.mp4"
