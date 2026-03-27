"""Named playlist store — reusable named playlists with CRUD.

Named playlists are stored as JSON files at:
    {PLAYLIST_DIR}/named/{channel_id}/{name}.json

File format::

    {
        "name": "morning-show",
        "entries": [
            {"source": "/media/test/clip_01.mp4", "duration": 120.5},
            {"source": "/media/test/clip_02.mp4", "duration": 45.0}
        ],
        "created": "2026-03-18T14:30:00",
        "updated": "2026-03-18T15:00:00"
    }

Names must be alphanumeric with hyphens/underscores, max 64 chars.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def _base_dir() -> Path:
    """Base directory for named playlists."""
    import config

    return Path(config.PLAYLIST_DIR) / "named"


def _playlist_path(name: str, channel_id: str) -> Path:
    return _base_dir() / channel_id / f"{name}.json"


# ── Validation ──

_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$")


def validate_name(name: str) -> str | None:
    """Validate a playlist name.  Returns error message or None."""
    if not name:
        return "Playlist name cannot be empty"
    if not _NAME_RE.match(name):
        return (
            "Playlist name must be 1-64 chars, alphanumeric with hyphens/underscores, "
            "starting with a letter or digit"
        )
    return None


# ── CRUD ──


def get(name: str, channel_id: str = "channel-one") -> dict | None:
    """Load a named playlist.  Returns the playlist dict or None."""
    path = _playlist_path(name, channel_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        logger.error("Failed to load named playlist '%s': %s", name, exc)
        return None


def save(
    name: str,
    entries: list[dict],
    channel_id: str = "channel-one",
) -> dict:
    """Create or update a named playlist.

    Args:
        name: Playlist name (validated by caller).
        entries: List of entry dicts with ``source`` and ``duration``.
        channel_id: Channel identifier for storage isolation.

    Returns:
        The saved playlist dict.
    """
    path = _playlist_path(name, channel_id)
    now = datetime.now().isoformat()

    # Preserve created timestamp on update
    existing = get(name, channel_id)
    created = existing["created"] if existing else now

    playlist = {
        "name": name,
        "entries": entries,
        "created": created,
        "updated": now,
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(playlist, indent=2))

    logger.info("Saved named playlist '%s': %d entries", name, len(entries))
    return playlist


def delete(name: str, channel_id: str = "channel-one") -> bool:
    """Delete a named playlist.  Returns True if deleted."""
    path = _playlist_path(name, channel_id)
    if path.exists():
        path.unlink()
        logger.info("Deleted named playlist '%s'", name)
        return True
    return False


def list_all(channel_id: str = "channel-one") -> list[dict]:
    """List all named playlists with summary info.

    Returns a list of dicts with name, entry_count, total_duration,
    created, and updated.
    """
    channel_dir = _base_dir() / channel_id
    if not channel_dir.is_dir():
        return []

    playlists = []
    for path in sorted(channel_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text())
            entries = data.get("entries", [])
            total_duration = sum(e.get("duration", 0) for e in entries)
            playlists.append(
                {
                    "name": data.get("name", path.stem),
                    "entry_count": len(entries),
                    "total_duration": round(total_duration, 2),
                    "created": data.get("created"),
                    "updated": data.get("updated"),
                }
            )
        except Exception as exc:
            logger.warning("Skipping malformed playlist %s: %s", path, exc)

    return playlists
