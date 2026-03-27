"""Playlist and schedule endpoints."""

from __future__ import annotations

import logging
import os
from datetime import date, datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException

import config
import main
import playlist_store
from models import GenerateScheduleRequest, PlaylistRequest
from routes.channel_resolve import resolve_channel
from utils import get_clip_duration, scan_media

logger = logging.getLogger(__name__)

router = APIRouter(tags=["playlist"])


@router.post("/api/playlist")
async def set_playlist(req: PlaylistRequest, channel: str | None = None) -> dict:
    """Set a new playlist for today.

    If 'name' is provided, also saves the playlist as a named playlist.
    """
    ctx, engine = resolve_channel(channel)

    durations = []
    for f in req.files:
        if ".." in f:
            raise HTTPException(400, f"Invalid path: {f}")
        local_path = os.path.join(config.MEDIA_DIR, f)
        if not os.path.isfile(local_path):
            raise HTTPException(404, f"File not found: {f}")
        durations.append(get_clip_duration(local_path))

    try:
        from playout.input_layer import PlaylistEntry

        entries = [
            PlaylistEntry(source=os.path.join(config.MEDIA_DIR, f), duration=d)
            for f, d in zip(req.files, durations)
        ]
        engine.input_a.load_playlist(entries, loop=True)
        engine.show("input_a")

        # Persist via PlaylistStore (date-indexed)
        store_entries = [
            {"source": os.path.join(config.MEDIA_DIR, f), "duration": d}
            for f, d in zip(req.files, durations)
        ]
        playlist_store.save(date.today(), store_entries, loop=True)

        # Also save as a named playlist if name is provided
        if req.name:
            import named_playlist_store

            err = named_playlist_store.validate_name(req.name)
            if err:
                raise HTTPException(400, err)
            named_playlist_store.save(req.name, store_entries)

        logger.info("Playlist switched: %s", req.files)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Playlist switch failed: %s", exc, exc_info=True)
        raise HTTPException(500, "Playlist switch failed")

    result: dict = {"ok": True, "files": req.files}
    if req.name:
        result["saved_as"] = req.name
    return result


@router.post("/api/skip")
async def skip_current(channel: str | None = None, layer: str = "input_a") -> dict:
    """Skip to next item in playlist."""
    _ctx, engine = resolve_channel(channel)
    try:
        ch = engine.channel(layer)
        ch.skip()
        np = ch.now_playing
        return {"ok": True, "now_playing": np, "layer": layer}
    except Exception as exc:
        logger.error("Skip failed: %s", exc, exc_info=True)
        raise HTTPException(503, "Playout error")


@router.post("/api/back")
async def go_back(channel: str | None = None, layer: str = "input_a") -> dict:
    """Go back to previous item in playlist."""
    _ctx, engine = resolve_channel(channel)
    try:
        ch = engine.channel(layer)
        ch.back()
        np = ch.now_playing
        return {"ok": True, "now_playing": np, "layer": layer}
    except Exception as exc:
        logger.error("Go back failed: %s", exc, exc_info=True)
        raise HTTPException(503, "Playout error")


# ── Schedule management endpoints ──


@router.get("/api/schedule")
async def get_schedule(days: int = 7, channel: str | None = None) -> dict:
    """Show which dates have playlists for the next N days."""
    try:
        schedule = playlist_store.list_dates(days)
        return {
            "schedule": schedule,
            "today": date.today().isoformat(),
            "days_checked": days,
        }
    except Exception as exc:
        logger.error("Schedule listing failed: %s", exc, exc_info=True)
        raise HTTPException(503, "Playout error")


@router.get("/api/schedule/{schedule_date}")
async def get_schedule_date(schedule_date: str, channel: str | None = None) -> dict:
    """Get the playlist for a specific date."""
    try:
        d = date.fromisoformat(schedule_date)
        playlist = playlist_store.get(d)
        if playlist is None:
            raise HTTPException(404, f"No playlist for {schedule_date}")
        return playlist
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Schedule date lookup failed: %s", exc, exc_info=True)
        raise HTTPException(503, "Playout error")


@router.post("/api/schedule/generate")
async def generate_schedule(
    req: GenerateScheduleRequest, channel: str | None = None
) -> dict:
    """Generate a playlist for a specific date.

    Four modes:
    1. With template: not supported (returns 501).
    2. With playlist_name: uses a named playlist's entries.
    3. With files: creates a playlist from the specified files.
    4. Neither: creates a playlist from all clips in the media library.

    If a playlist already exists for the date, it is replaced.
    """
    target = req.date

    # Validate date format
    try:
        datetime.strptime(target, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD.")

    if req.playlist_name:
        # Use a named playlist
        import named_playlist_store

        pl_data = named_playlist_store.get(req.playlist_name)
        if pl_data is None:
            raise HTTPException(404, f"Named playlist '{req.playlist_name}' not found")

        entries = pl_data.get("entries", [])
        if not entries:
            raise HTTPException(400, f"Named playlist '{req.playlist_name}' is empty")

        try:
            target_date = date.fromisoformat(target)
            playlist_store.save(
                target_date,
                entries,
                loop=True,
                channel_id=config.DEFAULT_CHANNEL_ID,
            )
            logger.info(
                "Generated schedule for %s from named playlist '%s': %d clips",
                target,
                req.playlist_name,
                len(entries),
            )
            return {
                "ok": True,
                "date": target,
                "method": "named_playlist",
                "playlist_name": req.playlist_name,
                "clips": len(entries),
            }
        except Exception as exc:
            logger.error("Schedule generation failed: %s", exc, exc_info=True)
            raise HTTPException(500, "Schedule generation failed")

    elif req.files:
        # Specific files
        durations = []
        for f in req.files:
            local_path = os.path.join(config.MEDIA_DIR, f)
            if not os.path.isfile(local_path):
                raise HTTPException(404, f"File not found: {f}")
            durations.append(get_clip_duration(local_path))

        try:
            target_date = date.fromisoformat(target)
            playlist_store.save(
                target_date,
                [
                    {
                        "source": os.path.join(config.MEDIA_DIR, f),
                        "duration": d,
                    }
                    for f, d in zip(req.files, durations)
                ],
                loop=True,
            )
            logger.info(
                "Generated file-list schedule for %s: %d clips",
                target,
                len(req.files),
            )
            return {
                "ok": True,
                "date": target,
                "method": "files",
                "clips": len(req.files),
            }
        except Exception as exc:
            logger.error("Schedule generation failed: %s", exc, exc_info=True)
            raise HTTPException(500, "Schedule generation failed")

    else:
        # Auto: use all available clips
        local_files, durations = scan_media(config.MEDIA_DIR)
        if not local_files:
            raise HTTPException(404, "No media files found")

        try:
            target_date = date.fromisoformat(target)
            playlist_store.save(
                target_date,
                [{"source": f, "duration": d} for f, d in zip(local_files, durations)],
                loop=True,
            )
            logger.info(
                "Generated auto schedule for %s: %d clips",
                target,
                len(local_files),
            )
            return {
                "ok": True,
                "date": target,
                "method": "auto",
                "clips": len(local_files),
            }
        except Exception as exc:
            logger.error("Schedule generation failed: %s", exc, exc_info=True)
            raise HTTPException(500, "Schedule generation failed")


@router.delete("/api/schedule/{schedule_date}")
async def delete_schedule(schedule_date: str, channel: str | None = None) -> dict:
    """Delete a playlist for a specific date."""
    try:
        d = date.fromisoformat(schedule_date)
        deleted = playlist_store.delete(d)
        if not deleted:
            raise HTTPException(404, f"No playlist for {schedule_date}")
        return {"ok": True, "deleted": schedule_date}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Schedule delete failed: %s", exc, exc_info=True)
        raise HTTPException(500, "Delete failed")
