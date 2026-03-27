"""PlaylistStore — date-indexed JSON persistence for playlists.

Date-indexed playlist persistence.  Each day's playlist
is stored as a JSON file at {base_dir}/{channel_id}/{YYYY-MM-DD}.json
containing a list of playlist entry dicts.

File format::

    {
        "date": "2026-03-18",
        "entries": [
            {"source": "/media/test/clip_01.mp4", "duration": 120.5},
            {"source": "/media/test/clip_02.mp4", "duration": 45.0}
        ],
        "loop": true,
        "created": "2026-03-18T14:30:00"
    }
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import config

logger = logging.getLogger(__name__)

PLAYLIST_DIR = config.PLAYLIST_DIR


def _playlist_path(d: date, channel_id: str) -> Path:
    """Get the file path for a day's playlist."""
    return Path(PLAYLIST_DIR) / channel_id / f"{d.isoformat()}.json"


def get(d: date, channel_id: str = "channel-one") -> dict | None:
    """Load a playlist for a date.

    Returns the playlist dict or None if no playlist exists.
    """
    path = _playlist_path(d, channel_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        logger.error("Failed to load playlist for %s: %s", d.isoformat(), exc)
        return None


def save(
    d: date,
    entries: list[dict],
    loop: bool = True,
    channel_id: str = "channel-one",
) -> dict:
    """Save a playlist for a date.

    Args:
        d: The date this playlist is for.
        entries: List of entry dicts, each with ``source`` (str) and
                 ``duration`` (float) keys.
        loop: Whether to loop the playlist.
        channel_id: Channel identifier for storage isolation.

    Returns:
        The saved playlist dict.
    """
    playlist = {
        "date": d.isoformat(),
        "entries": entries,
        "loop": loop,
        "created": datetime.now().isoformat(),
    }

    path = _playlist_path(d, channel_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(playlist, indent=2))

    logger.info(
        "Saved playlist for %s: %d entries (loop=%s)",
        d.isoformat(),
        len(entries),
        loop,
    )
    return playlist


def delete(d: date, channel_id: str = "channel-one") -> bool:
    """Delete a playlist for a date. Returns True if deleted."""
    path = _playlist_path(d, channel_id)
    if path.exists():
        path.unlink()
        logger.info("Deleted playlist for %s", d.isoformat())
        return True
    return False


def exists(d: date, channel_id: str = "channel-one") -> bool:
    """Check if a playlist exists for a date."""
    return _playlist_path(d, channel_id).exists()


def list_dates(days_ahead: int = 7, channel_id: str = "channel-one") -> dict[str, bool]:
    """Check which dates have playlists for the next N days.

    Returns a dict mapping ISO date strings to booleans.
    """
    today = date.today()
    result = {}
    for i in range(days_ahead):
        d = today + timedelta(days=i)
        result[d.isoformat()] = _playlist_path(d, channel_id).exists()
    return result
