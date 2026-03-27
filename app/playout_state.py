"""Playout state persistence — remembers what's playing across restarts.

Stores the active source state per layer so cathode can restore the
last-active playlists after an engine restart or container restart.

State file: {DATA_DIR}/playout-state/{channel_id}.json

Format::

    {
        "input_a": {
            "type": "playlist",
            "playlist_name": "evening-show",
            "loop": true
        },
        "input_b": null,
        "updated": "2026-03-22T14:30:00"
    }

When no state file exists (first boot), cathode runs failover only
until the operator loads a playlist or the slate video is generated.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import config

logger = logging.getLogger(__name__)

_STATE_DIR = config.DATA_DIR


def _state_path(channel_id: str) -> Path:
    return Path(_STATE_DIR) / "playout-state" / f"{channel_id}.json"


def save_layer_state(
    channel_id: str,
    layer: str,
    playlist_name: str | None = None,
    loop: bool = True,
    source_type: str = "playlist",
) -> None:
    """Persist the active source state for a layer.

    Called when a playlist is loaded via API or scheduler.
    """
    path = _state_path(channel_id)
    state = load_state(channel_id) or {}

    state[layer] = {
        "type": source_type,
        "playlist_name": playlist_name,
        "loop": loop,
    }
    state["updated"] = datetime.now().isoformat()

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2))
    logger.debug("Saved playout state for %s layer %s", channel_id, layer)


def clear_layer_state(channel_id: str, layer: str) -> None:
    """Clear the active source state for a layer.

    Called when a source is disconnected.
    """
    path = _state_path(channel_id)
    state = load_state(channel_id)
    if not state:
        return

    state[layer] = None
    state["updated"] = datetime.now().isoformat()

    path.write_text(json.dumps(state, indent=2))
    logger.debug("Cleared playout state for %s layer %s", channel_id, layer)


def load_state(channel_id: str) -> dict | None:
    """Load the persisted playout state for a channel.

    Returns None if no state file exists (first boot).
    """
    path = _state_path(channel_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to load playout state for %s: %s", channel_id, exc)
        return None


def get_layer_state(channel_id: str, layer: str) -> dict | None:
    """Get the persisted state for a specific layer.

    Returns dict with type, playlist_name, loop — or None.
    """
    state = load_state(channel_id)
    if not state:
        return None
    layer_state = state.get(layer)
    if not layer_state or not isinstance(layer_state, dict):
        return None
    return layer_state
