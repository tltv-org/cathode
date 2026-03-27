"""Media management endpoints — /api/media.

Upload, delete, list, and inspect media files.

    GET    /api/media              — List all media files (with durations)
    POST   /api/media/upload       — Upload a media file
    POST   /api/media/mkdir        — Create a subfolder
    DELETE /api/media/{filename}   — Delete a media file (with reference check)
    GET    /api/media/{filename}   — Get metadata for a single file
"""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile

import config
import named_playlist_store
import playlist_store
from utils import MEDIA_EXTENSIONS, get_clip_duration, scan_media

logger = logging.getLogger(__name__)

router = APIRouter(tags=["media"])


def _media_dir() -> str:
    return config.MEDIA_UPLOAD_DIR or config.MEDIA_DIR


# ── Upload ──


def _validate_media_path(filename: str) -> str:
    """Validate a relative media path (may include subdirectories).

    Rejects path traversal (``..``) and absolute paths.
    Returns the sanitized relative path.
    """
    if ".." in filename:
        raise HTTPException(400, "Invalid path: '..' not allowed")
    if os.path.isabs(filename):
        raise HTTPException(400, "Invalid path: must be relative")
    # Normalize separators and collapse redundant slashes
    cleaned = os.path.normpath(filename)
    if cleaned.startswith(".."):
        raise HTTPException(400, "Invalid path: escapes media directory")
    return cleaned


@router.post("/api/media/mkdir")
async def create_folder(name: str) -> dict:
    """Create a subfolder in the media library.

    The ``name`` query param is a relative path (e.g. ``bumpers``,
    ``shows/season-1``).  Intermediate directories are created.
    """
    clean = _validate_media_path(name)
    folder_path = os.path.join(_media_dir(), clean)
    os.makedirs(folder_path, exist_ok=True)
    logger.info("Created media folder: %s", clean)
    return {"ok": True, "folder": clean}


@router.post("/api/media/upload")
async def upload_media(file: UploadFile, folder: str | None = None) -> dict:
    """Upload a media file.

    Validates file extension against MEDIA_EXTENSIONS.
    Probes duration after upload.  Rejects files over the size limit.

    Optional ``folder`` query param places the file in a subdirectory
    (e.g. ``?folder=bumpers``).  The folder is created if needed.
    """
    if not file.filename:
        raise HTTPException(400, "No filename provided")

    # Validate extension
    ext = Path(file.filename).suffix.lower()
    if ext not in MEDIA_EXTENSIONS:
        raise HTTPException(
            400,
            f"Unsupported file type '{ext}'. "
            f"Allowed: {', '.join(sorted(MEDIA_EXTENSIONS))}",
        )

    # Sanitize filename (strip any directory components from the upload name)
    safe_name = Path(file.filename).name
    if ".." in safe_name:
        raise HTTPException(400, "Invalid filename")

    # Resolve destination directory
    dest_dir = _media_dir()
    if folder:
        clean_folder = _validate_media_path(folder)
        dest_dir = os.path.join(dest_dir, clean_folder)
        os.makedirs(dest_dir, exist_ok=True)

    dest_path = os.path.join(dest_dir, safe_name)

    # Check if file already exists
    if os.path.exists(dest_path):
        raise HTTPException(
            409,
            f"File '{safe_name}' already exists. Delete it first or use a different name.",
        )

    # Stream to disk with size check
    os.makedirs(dest_dir, exist_ok=True)
    total_size = 0
    try:
        with open(dest_path, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
                total_size += len(chunk)
                if total_size > config.MEDIA_UPLOAD_MAX_SIZE:
                    # Clean up partial file
                    f.close()
                    os.unlink(dest_path)
                    max_mb = config.MEDIA_UPLOAD_MAX_SIZE // (1024 * 1024)
                    raise HTTPException(
                        413, f"File too large. Maximum size: {max_mb}MB"
                    )
                f.write(chunk)
    except HTTPException:
        raise
    except Exception as exc:
        # Clean up on write failure
        if os.path.exists(dest_path):
            os.unlink(dest_path)
        logger.error("Upload failed: %s", exc, exc_info=True)
        raise HTTPException(500, "Upload failed")

    # Probe duration
    duration = get_clip_duration(dest_path, fallback=0.0)

    # Return relative path from MEDIA_DIR root
    rel_path = os.path.relpath(dest_path, _media_dir())
    logger.info(
        "Uploaded media: %s (%.1f MB, %.1fs)", rel_path, total_size / 1e6, duration
    )
    return {
        "ok": True,
        "filename": rel_path,
        "size": total_size,
        "duration": duration,
    }


# ── Delete ──


def _find_references(filename: str) -> list[dict]:
    """Check if a file is referenced by any active playlist or program.

    Returns a list of reference dicts: {type, name, detail}.
    """
    refs: list[dict] = []
    abs_path = os.path.join(_media_dir(), filename)

    # Check named playlists
    for pl in named_playlist_store.list_all():
        pl_data = named_playlist_store.get(pl["name"])
        if pl_data:
            for entry in pl_data.get("entries", []):
                src = entry.get("source", "")
                if src == abs_path or src.endswith(f"/{filename}"):
                    refs.append(
                        {
                            "type": "named_playlist",
                            "name": pl["name"],
                            "detail": f"Used in named playlist '{pl['name']}'",
                        }
                    )
                    break

    # Check date-indexed playlists (today and next 7 days)
    from datetime import date, timedelta

    for i in range(8):
        d = date.today() + timedelta(days=i)
        pl_data = playlist_store.get(d)
        if pl_data:
            for entry in pl_data.get("entries", []):
                src = entry.get("source", "")
                if src == abs_path or src.endswith(f"/{filename}"):
                    refs.append(
                        {
                            "type": "schedule",
                            "name": d.isoformat(),
                            "detail": f"Used in schedule for {d.isoformat()}",
                        }
                    )
                    break

    # Check program blocks (today)
    try:
        import program

        today_prog = program.load_program(date.today())
        if today_prog:
            for block in today_prog.get("blocks", []):
                files = block.get("files", [])
                if filename in files or any(f.endswith(f"/{filename}") for f in files):
                    refs.append(
                        {
                            "type": "program",
                            "name": block.get("title", "unknown"),
                            "detail": f"Used in program block '{block.get('title')}'",
                        }
                    )

    except FileNotFoundError:
        pass  # No program for today — skip program reference check
    except Exception as exc:
        logger.warning("Program reference check failed: %s", exc)

    return refs


@router.delete("/api/media/{filename:path}")
async def delete_media(filename: str) -> dict:
    """Delete a media file.

    Accepts relative paths with subdirectories (e.g. ``bumpers/ident.mp4``).
    Returns 409 if the file is referenced by any active playlist or program.
    """
    clean = _validate_media_path(filename)
    file_path = os.path.join(_media_dir(), clean)
    if not os.path.isfile(file_path):
        raise HTTPException(404, f"File '{clean}' not found")

    # Check references
    refs = _find_references(clean)
    if refs:
        raise HTTPException(
            409,
            {
                "detail": f"Cannot delete '{clean}': file is in use",
                "references": refs,
            },
        )

    try:
        os.unlink(file_path)
    except OSError as exc:
        logger.error("Delete failed: %s", exc, exc_info=True)
        raise HTTPException(500, "Delete failed")

    logger.info("Deleted media: %s", clean)
    return {"ok": True, "deleted": clean}


# ── Single file metadata ──


def _probe_metadata(path: str) -> dict:
    """Get detailed metadata for a media file via ffprobe."""
    result = {
        "duration": get_clip_duration(path, fallback=0.0),
        "size": os.path.getsize(path),
    }

    try:
        probe = subprocess.run(
            [
                "ffprobe",
                "-v",
                "quiet",
                "-print_format",
                "json",
                "-show_format",
                "-show_streams",
                path,
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        import json

        info = json.loads(probe.stdout)

        fmt = info.get("format", {})
        result["format"] = fmt.get("format_long_name", fmt.get("format_name"))

        for stream in info.get("streams", []):
            if stream.get("codec_type") == "video":
                result["video_codec"] = stream.get("codec_name")
                result["width"] = stream.get("width")
                result["height"] = stream.get("height")
                result["fps"] = stream.get("r_frame_rate")
            elif stream.get("codec_type") == "audio":
                result["audio_codec"] = stream.get("codec_name")
                result["audio_channels"] = stream.get("channels")
                result["sample_rate"] = stream.get("sample_rate")
    except Exception as exc:
        logger.debug("Extended probe failed for %s: %s", path, exc)

    return result


@router.get("/api/media/{filename:path}")
async def get_media_metadata(filename: str) -> dict:
    """Get metadata for a single media file.

    Accepts relative paths with subdirectories (e.g. ``cathode/failover.mp4``).
    """
    clean = _validate_media_path(filename)
    file_path = os.path.join(_media_dir(), clean)
    if not os.path.isfile(file_path):
        raise HTTPException(404, f"File '{clean}' not found")

    metadata = _probe_metadata(file_path)
    metadata["filename"] = clean
    return metadata


# ── List all media ──


@router.get("/api/media")
async def list_media() -> dict:
    """List available media files that can be used in schedules.

    Returns relative paths (e.g. ``cathode/failover.mp4``,
    ``bumpers/ident.mp4``) so the frontend can build a folder tree.
    """
    media_root = Path(config.MEDIA_DIR)
    files, durations = scan_media(config.MEDIA_DIR)
    items = []
    for f, dur in zip(files, durations):
        # Relative path from MEDIA_DIR root (preserves subdirectory structure)
        rel = str(Path(f).relative_to(media_root))
        items.append(
            {
                "filename": rel,
                "duration": dur,
            }
        )
    return {
        "media_dir": config.MEDIA_DIR,
        "count": len(items),
        "total_duration": sum(durations),
        "items": items,
    }
