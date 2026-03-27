"""Program schedule endpoints — /api/program/*."""

from __future__ import annotations

import logging
from datetime import date

from fastapi import APIRouter, HTTPException

import program
from models import ProgramRequest
from routes.channel_resolve import resolve_channel_ctx

logger = logging.getLogger(__name__)

router = APIRouter(tags=["program"])


def _clear_scheduler_tracking(ctx) -> None:
    """Clear scheduler tracking state for hot-reload.

    Records previously active layers so the scheduler can hide stale
    layers that are no longer in the new program.
    """
    ctx.hot_reload_layers = set(ctx.active_layer_blocks.keys())
    if ctx.active_block_key or ctx.active_playlist_block_key:
        ctx.hot_reload_layers.add("input_a")
    ctx.active_block_key = None
    ctx.active_playlist_block_key = None
    ctx.active_layer_blocks.clear()


@router.get("/api/program")
async def list_programs(days: int = 7, channel: str | None = None) -> dict:
    """List which dates have broadcast programs for the next N days.

    Programs define mixed blocks of playlist, file, redirect, etc.
    The program scheduler runs autonomously, dispatching blocks at
    the scheduled times.
    """
    ctx = resolve_channel_ctx(channel)
    return {
        "programs": program.list_programs(days, channel_id=ctx.id),
        "today": date.today().isoformat(),
        "days_checked": days,
    }


@router.get("/api/program/block-types")
async def list_block_types() -> dict:
    """List all available block types (core + plugin-registered).

    The response includes core types that are always available and
    plugin-registered types that depend on loaded plugins.  If a
    plugin is disabled, its block types disappear from this list.
    """
    import plugins

    core = {
        "playlist": {
            "description": "Play a named playlist or file list",
            "fields": {
                "files": "list[str]",
                "playlist_name": "str",
                "loop": "bool",
                "layer": "str",
            },
            "source": "core",
        },
        "file": {
            "description": "Play a single file",
            "fields": {"file": "str", "layer": "str"},
            "source": "core",
        },
        "image": {
            "description": "Show a static image",
            "fields": {"file": "str", "layer": "str"},
            "source": "core",
        },
        "redirect": {
            "description": "Play an HLS stream (another channel, external feed)",
            "fields": {"url": "str", "layer": "str"},
            "source": "core",
        },
    }

    # Add plugin-registered block types
    plugin_types = {}
    for type_name, info in plugins.block_types().items():
        # Normalize params to simple type strings for the fields key
        params = info.get("params") or {}
        fields = {}
        for field_name, field_info in params.items():
            if isinstance(field_info, dict):
                fields[field_name] = field_info.get("type", "str")
            else:
                fields[field_name] = str(field_info)

        plugin_types[type_name] = {
            "description": info.get("description", ""),
            "fields": fields,
            "source": f"plugin:{info.get('plugin', 'unknown')}",
        }

    return {
        "core": core,
        "plugin": plugin_types,
        "all": list(core.keys()) + list(plugin_types.keys()),
    }


@router.get("/api/program/{program_date}")
async def get_program(program_date: str, channel: str | None = None) -> dict:
    """Get the broadcast program for a specific date.

    Returns all blocks with their types, times, and content.
    """
    ctx = resolve_channel_ctx(channel)
    try:
        d = date.fromisoformat(program_date)
    except ValueError:
        raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD.")

    prog = program.load_program(d, channel_id=ctx.id)
    if not prog:
        raise HTTPException(404, f"No program for {program_date}")
    return prog


@router.post("/api/program/{program_date}")
async def set_program(
    program_date: str, req: ProgramRequest, channel: str | None = None
) -> dict:
    """Set a broadcast program for a specific date.

    A program is a list of time blocks, each with a type and time range.
    Core block types: playlist, file, image, redirect, flex.
    Plugin block types (html, script, etc.) if plugins are loaded.

    The program scheduler dispatches blocks autonomously at their
    scheduled times.  Gaps default to the playout playlist.
    """
    ctx = resolve_channel_ctx(channel)
    try:
        d = date.fromisoformat(program_date)
    except ValueError:
        raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD.")

    blocks = [b.model_dump(exclude_none=True) for b in req.blocks]

    try:
        prog = program.save_program(d, blocks, channel_id=ctx.id)
        logger.info("Program set for %s: %d blocks", program_date, len(blocks))

        # If today's program was changed, invalidate scheduler tracking
        # so blocks are re-evaluated on the next tick (~5s).
        if d == date.today():
            _clear_scheduler_tracking(ctx)
            logger.info("Program: cleared scheduler state for hot reload")

        return {
            "ok": True,
            "date": program_date,
            **program.summarize_program(prog),
        }
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.delete("/api/program/{program_date}")
async def delete_program_route(program_date: str, channel: str | None = None) -> dict:
    """Delete the broadcast program for a specific date.

    Canvas renders scheduled by this program will stop at the next
    scheduler check (~5 seconds).
    """
    ctx = resolve_channel_ctx(channel)
    try:
        d = date.fromisoformat(program_date)
    except ValueError:
        raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD.")

    if program.delete_program(d, channel_id=ctx.id):
        # If today's program was deleted, clear scheduler tracking
        if d == date.today():
            _clear_scheduler_tracking(ctx)
            logger.info("Program: cleared scheduler state after delete")
        return {"ok": True, "deleted": program_date}
    raise HTTPException(404, f"No program for {program_date}")
