"""Channel resolution helper for per-channel endpoints.

Provides resolve_channel() which extracts the ?channel= query param
and returns the corresponding ChannelContext + engine.  When no param
is provided, the default (first) channel is used for backward compat.

Usage in route handlers::

    @router.get("/api/playout/health")
    async def health(channel: str | None = None):
        ctx, engine = resolve_channel(channel)
        return engine.health
"""

from __future__ import annotations

from fastapi import HTTPException

import main


def resolve_channel(channel_id: str | None = None):
    """Resolve a channel ID to (ChannelContext, PlayoutEngine).

    Args:
        channel_id: Channel slug from ?channel= query param.
            None means use the default channel.

    Returns:
        (ctx, engine) tuple.

    Raises:
        HTTPException 404 if channel not found.
        HTTPException 501 if channel has no engine.
    """
    if not main.channels:
        raise HTTPException(501, "No channels configured")

    if channel_id:
        ctx = main.channels.get_or_none(channel_id)
        if ctx is None:
            raise HTTPException(404, f"Channel '{channel_id}' not found")
    else:
        ctx = main.channels.default()

    engine = ctx.engine
    if engine is None:
        raise HTTPException(501, f"Playout engine not started for channel '{ctx.id}'")

    return ctx, engine


def resolve_channel_ctx(channel_id: str | None = None):
    """Resolve a channel ID to ChannelContext only (no engine required).

    Used by endpoints that don't need the engine (e.g. metadata, program).
    """
    if not main.channels:
        raise HTTPException(501, "No channels configured")

    if channel_id:
        ctx = main.channels.get_or_none(channel_id)
        if ctx is None:
            raise HTTPException(404, f"Channel '{channel_id}' not found")
    else:
        ctx = main.channels.default()

    return ctx
