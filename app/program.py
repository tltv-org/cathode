"""Program schedule manager.

A 'program' is a day's broadcast plan with typed blocks.

Core block types:
  playlist  — play a named playlist or file list on a layer
  file      — play a single file on a layer
  image     — show a static image on a layer
  redirect  — play an HLS stream (another channel, external feed)
  flex      — filler placeholder, resolved at airtime from a pool

Plugin-registered block types are also valid — the block type registry
is checked dynamically.  Legacy types 'canvas' and 'generator' are
accepted for backward compat and dispatched through the plugin system.

Storage: JSON files at {PROGRAM_DIR}/{channel_id}/{YYYY}/{MM}/{YYYY-MM-DD}.json
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, time, timedelta
from pathlib import Path

import config

logger = logging.getLogger(__name__)

PROGRAM_DIR = config.PROGRAM_DIR

DEFAULT_CHANNEL_ID = config.DEFAULT_CHANNEL_ID

# Core block types — always available regardless of plugins
CORE_BLOCK_TYPES = {
    "playlist",  # play a named playlist or file list
    "file",  # play a single file (file_loop)
    "image",  # show a static image
    "redirect",  # play an HLS stream (another channel, external)
    "flex",  # filler placeholder, resolved at airtime
    # Legacy types (backward compat, dispatched through plugin system)
    "canvas",  # live HTML via renderer (legacy, prefer html-source)
    "generator",  # generator via renderer (legacy, prefer plugin sources)
}


def _is_valid_block_type(block_type: str) -> bool:
    """Check if a block type is valid (core or plugin-registered)."""
    if block_type in CORE_BLOCK_TYPES:
        return True
    try:
        import plugins

        return block_type in plugins.block_types()
    except ImportError:
        return False


def _all_valid_block_types() -> set[str]:
    """Return all currently valid block types."""
    valid = set(CORE_BLOCK_TYPES)
    try:
        import plugins

        valid.update(plugins.block_types().keys())
    except ImportError:
        pass
    return valid


def _program_path(d: date, channel_id: str = DEFAULT_CHANNEL_ID) -> Path:
    """Get the file path for a day's program."""
    return (
        Path(PROGRAM_DIR)
        / channel_id
        / f"{d.year}"
        / f"{d.month:02d}"
        / f"{d.isoformat()}.json"
    )


def _parse_time(t: str) -> time:
    """Parse HH:MM:SS string to time object."""
    parts = t.split(":")
    return time(int(parts[0]), int(parts[1]), int(parts[2]))


def save_program(
    d: date, blocks: list[dict], channel_id: str = DEFAULT_CHANNEL_ID
) -> dict:
    """Save a program for a date.

    Validates blocks and writes to JSON file.
    Returns the saved program dict.

    Each block must have:
        start: "HH:MM:SS"
        end:   "HH:MM:SS"
        type:  "playlist", "canvas", or "generator"
        title: str — EPG display title (e.g. "Mandelbrot Zoom")

    Canvas blocks must also have:
        html: str — HTML content to render (or preset: str)

    Optional fields:
        files: list[str] — clip filenames for playlist blocks
        name: str — generator name (required for generator blocks)
        params: dict — generator-specific parameters
        width, height, fps: int — canvas render settings
    """
    validated = []
    for i, block in enumerate(blocks):
        # Required fields
        for field in ("start", "end", "type", "title"):
            if field not in block:
                raise ValueError(f"Block {i} missing required field '{field}'")

        block_type = block["type"]
        if not _is_valid_block_type(block_type):
            valid = _all_valid_block_types()
            raise ValueError(
                f"Block {i} has invalid type '{block_type}'. "
                f"Valid: {', '.join(sorted(valid))}."
            )

        # Type-specific validation
        if block_type == "redirect":
            if not block.get("url"):
                raise ValueError(
                    f"Block {i} is type 'redirect' but missing 'url' field."
                )

        if block_type == "flex":
            # Flex blocks need no extra fields — content resolved at airtime
            pass

        if block_type == "file":
            if not block.get("file"):
                raise ValueError(f"Block {i} is type 'file' but missing 'file' field.")

        if block_type == "image":
            if not block.get("file"):
                raise ValueError(f"Block {i} is type 'image' but missing 'file' field.")

        # Legacy canvas/generator validation (backward compat)
        if block_type == "canvas":
            has_html = "html" in block
            has_preset = "preset" in block and block["preset"]
            if not has_html and not has_preset:
                raise ValueError(
                    f"Block {i} is type 'canvas' but missing 'html' or 'preset' field."
                )
            if has_html and has_preset:
                raise ValueError(
                    f"Block {i} has both 'html' and 'preset'. Use one or the other."
                )

        if block_type == "generator" and "name" not in block:
            raise ValueError(f"Block {i} is type 'generator' but missing 'name' field.")

        # Validate optional 'files' field on playlist blocks
        if "files" in block:
            if block["type"] != "playlist":
                raise ValueError(
                    f"Block {i} has 'files' but type is '{block['type']}'. "
                    "'files' is only valid on 'playlist' blocks."
                )
            if not isinstance(block["files"], list):
                raise ValueError(f"Block {i} 'files' must be a list of filenames.")
            if not block["files"]:
                raise ValueError(
                    f"Block {i} 'files' must not be empty. "
                    "Omit 'files' to use the default playlist."
                )
            if not all(isinstance(f, str) for f in block["files"]):
                raise ValueError(f"Block {i} 'files' must contain only strings.")

        # Validate optional 'playlist_name' field
        if "playlist_name" in block and block["playlist_name"]:
            if block["type"] != "playlist":
                raise ValueError(
                    f"Block {i} has 'playlist_name' but type is '{block['type']}'. "
                    "'playlist_name' is only valid on 'playlist' blocks."
                )
            # Verify the named playlist exists
            import named_playlist_store

            if (
                named_playlist_store.get(block["playlist_name"], channel_id=channel_id)
                is None
            ):
                raise ValueError(
                    f"Block {i} references playlist '{block['playlist_name']}' "
                    "which does not exist. Create it first via POST /api/playlists/."
                )

        # Validate optional 'layer' field
        valid_layers = ("failover", "input_a", "input_b", "blinder")
        if "layer" in block and block["layer"] is not None:
            if block["layer"] not in valid_layers:
                raise ValueError(
                    f"Block {i} has invalid layer '{block['layer']}'. "
                    f"Valid: {', '.join(valid_layers)}."
                )

        # Validate time format
        try:
            start = _parse_time(block["start"])
            end = _parse_time(block["end"])
        except (ValueError, IndexError):
            raise ValueError(f"Block {i} has invalid time format. Use HH:MM:SS.")

        if end <= start:
            raise ValueError(
                f"Block {i} end time ({block['end']}) must be after "
                f"start time ({block['start']})."
            )

        validated.append(block)

    # Sort by start time
    validated.sort(key=lambda b: b["start"])

    # Check for per-layer overlaps.
    # Blocks on different layers CAN overlap (that's the point of multi-layer).
    # Only blocks on the SAME layer must not overlap.
    from collections import defaultdict

    layers: dict[str, list[dict]] = defaultdict(list)
    for block in validated:
        layer = block.get("layer") or "input_a"
        layers[layer].append(block)

    for layer, layer_blocks in layers.items():
        for i in range(len(layer_blocks) - 1):
            end_i = _parse_time(layer_blocks[i]["end"])
            start_next = _parse_time(layer_blocks[i + 1]["start"])
            if end_i > start_next:
                raise ValueError(
                    f"Blocks overlap on layer '{layer}': "
                    f"'{layer_blocks[i]['title']}' ends at {layer_blocks[i]['end']} "
                    f"but '{layer_blocks[i + 1]['title']}' starts at "
                    f"{layer_blocks[i + 1]['start']}."
                )

    program = {
        "date": d.isoformat(),
        "blocks": validated,
        "created": datetime.now().isoformat(),
    }

    path = _program_path(d, channel_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(program, indent=2))

    logger.info("Saved program for %s: %d blocks", d.isoformat(), len(validated))
    return program


def load_program(d: date, channel_id: str = DEFAULT_CHANNEL_ID) -> dict | None:
    """Load a program for a date. Returns None if no program exists."""
    path = _program_path(d, channel_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        logger.error("Failed to load program for %s: %s", d.isoformat(), exc)
        return None


def delete_program(d: date, channel_id: str = DEFAULT_CHANNEL_ID) -> bool:
    """Delete a program for a date. Returns True if deleted."""
    path = _program_path(d, channel_id)
    if path.exists():
        path.unlink()
        logger.info("Deleted program for %s", d.isoformat())
        return True
    return False


def list_programs(
    days_ahead: int = 7, channel_id: str = DEFAULT_CHANNEL_ID
) -> dict[str, bool]:
    """Check which dates have programs for the next N days."""
    today = date.today()
    result = {}
    for i in range(days_ahead):
        d = today + timedelta(days=i)
        result[d.isoformat()] = _program_path(d, channel_id).exists()
    return result


def find_current_block(program: dict, now: time) -> dict | None:
    """Find which block should be active at the given time.

    Returns the matching block dict, or None if no block covers
    this time (meaning the playout engine playlist should play).

    For single-block legacy behavior.  Prefer find_active_blocks()
    for multi-layer scheduling.
    """
    for block in program.get("blocks", []):
        start = _parse_time(block["start"])
        end = _parse_time(block["end"])

        if start <= now < end:
            return block

    return None


def find_active_blocks(program: dict, now: time) -> list[dict]:
    """Find ALL blocks active at the given time.

    Returns a list of block dicts (one per layer, potentially).
    With multi-layer scheduling, multiple blocks can be active
    simultaneously on different layers.  Each block's 'layer'
    field determines which compositor layer it targets.
    """
    active = []
    for block in program.get("blocks", []):
        start = _parse_time(block["start"])
        end = _parse_time(block["end"])
        if start <= now < end:
            active.append(block)
    return active


def get_block_remaining_seconds(block: dict, now: datetime) -> int:
    """Calculate seconds remaining in a block from the current time."""
    end = _parse_time(block["end"])
    end_dt = datetime.combine(now.date(), end)
    return max(0, int((end_dt - now).total_seconds()))


def summarize_program(program: dict) -> dict:
    """Create a human-readable summary of a program."""
    blocks = program.get("blocks", [])

    # Count blocks by type (dynamic — not hardcoded to specific types)
    type_counts: dict[str, int] = {}
    type_durations: dict[str, float] = {}
    for block in blocks:
        bt = block["type"]
        type_counts[bt] = type_counts.get(bt, 0) + 1
        start = _parse_time(block["start"])
        end = _parse_time(block["end"])
        dur = (
            datetime.combine(date.today(), end) - datetime.combine(date.today(), start)
        ).total_seconds()
        type_durations[bt] = type_durations.get(bt, 0) + dur

    total_scheduled = sum(type_durations.values())

    result = {
        "date": program["date"],
        "block_count": len(blocks),
        "total_scheduled": total_scheduled,
        "by_type": {
            bt: {"count": type_counts[bt], "duration": type_durations[bt]}
            for bt in sorted(type_counts)
        },
        "created": program.get("created", "unknown"),
    }
    return result
