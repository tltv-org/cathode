"""Shared utility functions — ffprobe and media scanning."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

MEDIA_EXTENSIONS = {".mp4", ".mkv", ".webm", ".avi", ".ogv", ".ogg", ".mov"}


def get_clip_duration(path: str, fallback: float = 30.0) -> float:
    """Get clip duration using ffprobe.

    Args:
        path: Path to the media file.
        fallback: Value to return when probing fails. Defaults to 30.0
            (safe for playlist padding). Callers that list existing files
            callers listing existing files should pass fallback=0.0 instead.
    """
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "quiet",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                path,
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return float(result.stdout.strip())
    except Exception:
        logger.warning("Could not probe duration for %s, using %.1f", path, fallback)
        return fallback


def scan_media(directory: str, prefix: str = "") -> tuple[list[str], list[float]]:
    """Scan directory recursively for media files, return sorted paths and durations.

    Walks subdirectories so files like ``subdir/clip.mp4`` are discovered.
    Results are sorted by relative path (subdirectory files sort naturally).
    If prefix is given, only include files whose name starts with that prefix.
    Returns ([], []) if the directory doesn't exist or is empty.
    """
    dir_path = Path(directory)
    if not dir_path.is_dir():
        return [], []

    files = []
    for p in sorted(dir_path.rglob("*")):
        if p.is_file() and p.suffix.lower() in MEDIA_EXTENSIONS:
            if prefix and not p.name.startswith(prefix):
                continue
            files.append(str(p))

    durations = [get_clip_duration(f) for f in files]
    return files, durations
