"""Channel management endpoints — /api/channels/*.

Multi-channel support: list, create, update, delete channels.
Each channel has its own engine, encoding, scheduling, and identity.
"""

from __future__ import annotations

import logging
import os
import re

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import config
import main
from channel import ChannelContext, ensure_channel_keypair
from routes.channel_resolve import resolve_channel_ctx

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/channels", tags=["channels"])

_CHANNEL_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9-]{0,31}$")


class CreateChannelRequest(BaseModel):
    id: str
    display_name: str
    description: str | None = None
    language: str | None = None
    tags: list[str] = []
    default_output_type: str = "hls"
    hls_dir: str | None = None


class UpdateChannelRequest(BaseModel):
    display_name: str | None = None
    description: str | None = None
    language: str | None = None
    tags: list[str] | None = None
    access: str | None = None
    origins: list[str] | None = None
    timezone: str | None = None
    on_demand: bool | None = None
    status: str | None = None


@router.get("")
async def list_channels():
    """List all channels on this instance."""
    channels = []
    for ctx in main.channels.all():
        ch = {
            "id": ctx.id,
            "channel_id": ctx.channel_id,
            "display_name": ctx.display_name,
            "status": ctx.status,
            "has_engine": ctx.engine is not None,
        }
        if ctx.engine:
            ch["encoding"] = {
                "width": ctx.engine.config.width,
                "height": ctx.engine.config.height,
                "fps": ctx.engine.config.fps,
                "video_bitrate": ctx.engine.config.video_bitrate,
                "audio_bitrate": ctx.engine.config.audio_bitrate,
            }
        channels.append(ch)

    return {"channels": channels, "count": len(channels)}


@router.post("")
async def create_channel(req: CreateChannelRequest):
    """Create a new channel.

    Generates an Ed25519 keypair and registers the channel.
    The engine is NOT started automatically — call POST /api/playout/start
    with ?channel={id} to start it, or configure an RTMP URL.
    """
    # Validate ID
    if not _CHANNEL_ID_RE.match(req.id):
        raise HTTPException(
            400,
            "Channel ID must be 1-32 chars, alphanumeric + hyphens, "
            "starting with a letter or digit.",
        )

    if req.id in main.channels:
        raise HTTPException(409, f"Channel '{req.id}' already exists")

    # Create channel directories
    keys_dir = os.path.join(config.DATA_DIR, "keys")
    program_dir = os.path.join(config.DATA_DIR, "programs")
    generated_dir = os.path.join(config.MEDIA_DIR, "cathode")

    os.makedirs(keys_dir, exist_ok=True)

    # Generate Ed25519 keypair
    federation_id, key_path = ensure_channel_keypair(req.id, key_dir=keys_dir)

    # Create context
    ctx = ChannelContext(
        id=req.id,
        display_name=req.display_name,
        channel_id=federation_id,
        private_key_path=key_path,
        media_dir=config.MEDIA_DIR,
        generated_dir=generated_dir,
        program_dir=program_dir,
        description=req.description or "",
        language=req.language or "en",
        tags=req.tags,
        default_output_type=req.default_output_type,
        hls_dir=req.hls_dir or "",  # Populated with channel_id after keypair gen
    )

    main.channels.register(ctx)

    logger.info("Created channel '%s' (%s)", req.id, federation_id)

    return {
        "ok": True,
        "id": req.id,
        "channel_id": federation_id,
    }


@router.get("/{channel_id}")
async def get_channel(channel_id: str):
    """Get full metadata for a specific channel."""
    ctx = resolve_channel_ctx(channel_id)

    result = {
        "id": ctx.id,
        "channel_id": ctx.channel_id,
        "display_name": ctx.display_name,
        "description": ctx.description,
        "language": ctx.language,
        "tags": ctx.tags,
        "access": ctx.access,
        "origins": ctx.origins,
        "timezone": ctx.timezone,
        "on_demand": ctx.on_demand,
        "status": ctx.status,
        "has_engine": ctx.engine is not None,
        "mirror_mode": ctx.mirror_mode,
    }

    if ctx.engine:
        result["encoding"] = {
            "width": ctx.engine.config.width,
            "height": ctx.engine.config.height,
            "fps": ctx.engine.config.fps,
            "video_bitrate": ctx.engine.config.video_bitrate,
            "audio_bitrate": ctx.engine.config.audio_bitrate,
        }
        result["playout"] = {
            "mode": "loop" if ctx.playlist_loop else "schedule",
            "day_start": ctx.day_start,
            "running": ctx.engine.is_running,
        }

    return result


@router.patch("/{channel_id}")
async def update_channel(channel_id: str, req: UpdateChannelRequest):
    """Update channel metadata.

    Same as PATCH /api/channel but scoped to a specific channel.
    """
    ctx = resolve_channel_ctx(channel_id)

    updated = {}
    for field_name in (
        "display_name",
        "description",
        "language",
        "tags",
        "access",
        "origins",
        "timezone",
        "on_demand",
        "status",
    ):
        value = getattr(req, field_name, None)
        if value is not None:
            setattr(ctx, field_name, value)
            updated[field_name] = value

    if not updated:
        raise HTTPException(400, "No fields to update")

    return {"ok": True, "id": channel_id, "updated": updated}


@router.delete("/{channel_id}")
async def delete_channel(channel_id: str):
    """Delete a channel.

    Stops the engine and removes the channel from the registry.
    Cannot delete the last remaining channel.
    """
    if channel_id not in main.channels:
        raise HTTPException(404, f"Channel '{channel_id}' not found")

    if len(main.channels) <= 1:
        raise HTTPException(
            409,
            "Cannot delete the only channel. Create another channel first.",
        )

    ctx = main.channels.get(channel_id)

    # Stop engine if running
    if ctx.engine and ctx.engine.is_running:
        await ctx.engine.stop()
        logger.info("Stopped engine for channel '%s'", channel_id)

    main.channels.unregister(channel_id)

    return {"ok": True, "deleted": channel_id}
