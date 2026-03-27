"""Status endpoints — /api/status, /api/now-playing, /api/system."""

from __future__ import annotations

import logging
import os
from datetime import date

from fastapi import APIRouter, HTTPException

import config
import main
import program
from routes.channel_resolve import resolve_channel_ctx

logger = logging.getLogger(__name__)

router = APIRouter(tags=["status"])


@router.get("/api/status")
async def get_status(channel: str | None = None) -> dict:
    """Full system status including scheduling info."""
    playout_info = None

    # Resolve channel for program lookup
    try:
        ctx = resolve_channel_ctx(channel)
        ch_id = ctx.id
    except Exception:
        ctx = None
        ch_id = config.DEFAULT_CHANNEL_ID

    # Get engine health from channel context (not the global main.playout)
    engine = ctx.engine if ctx else main.playout
    if engine is not None:
        playout_info = engine.health

    # Check for active program
    try:
        today_program = program.load_program(date.today(), channel_id=ch_id)
        program_info = (
            program.summarize_program(today_program) if today_program else None
        )
    except Exception as exc:
        logger.debug("get_status: program info unavailable: %s", exc)
        program_info = None

    # Station info
    station = None
    if ctx is not None:
        station = {
            "name": ctx.display_name,
            "id": ctx.id,
            "channel_id": getattr(ctx, "channel_id", None),
        }
    else:
        ctxs = main.channels.all()
        if ctxs:
            c = ctxs[0]
            station = {
                "name": c.display_name,
                "id": c.id,
                "channel_id": getattr(c, "channel_id", None),
            }

    result = {
        "version": config.VERSION,
        "station": station,
        "program": program_info,
        "engine": playout_info,
        "backend": "gstreamer",
    }

    return result


@router.get("/api/now-playing")
async def now_playing(channel: str | None = None) -> dict:
    """What's currently on air."""
    try:
        ctx = resolve_channel_ctx(channel)
        engine = ctx.engine
    except Exception:
        engine = main.playout

    if engine is None:
        raise HTTPException(503, "Playout engine not available")

    health = engine.health
    active = health.get("active_channel", "failover")
    ch_info = health.get("channels", {}).get(active, {})
    np = ch_info.get("now_playing") or {}

    return {
        "source": np.get("source", "unknown"),
        "index": np.get("index", -1),
        "played": np.get("played", 0),
        "duration": np.get("duration", 0),
        "mode": active,
        "backend": "gstreamer",
    }


@router.get("/api/system")
async def system_info() -> dict:
    """Get system stats (CPU, RAM, disk) from the container.

    Reads from /proc and shutil directly.
    """
    import shutil

    stats: dict = {}

    # CPU load averages
    try:
        load1, load5, load15 = os.getloadavg()
        stats["cpu"] = {
            "load_1m": round(load1, 2),
            "load_5m": round(load5, 2),
            "load_15m": round(load15, 2),
        }
    except OSError:
        stats["cpu"] = None

    # Memory from /proc/meminfo
    try:
        meminfo: dict[str, int] = {}
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    meminfo[parts[0].rstrip(":")] = int(parts[1]) * 1024  # kB → B
        total = meminfo.get("MemTotal", 0)
        available = meminfo.get("MemAvailable", 0)
        used = total - available
        stats["memory"] = {
            "total_mb": round(total / 1048576, 1),
            "used_mb": round(used / 1048576, 1),
            "available_mb": round(available / 1048576, 1),
            "percent": round(used / total * 100, 1) if total else 0,
        }
    except (OSError, ValueError):
        stats["memory"] = None

    # Disk usage for media directory
    try:
        usage = shutil.disk_usage("/media")
        stats["disk"] = {
            "total_gb": round(usage.total / 1073741824, 2),
            "used_gb": round(usage.used / 1073741824, 2),
            "free_gb": round(usage.free / 1073741824, 2),
            "percent": round(usage.used / usage.total * 100, 1),
        }
    except OSError:
        stats["disk"] = None

    return stats
