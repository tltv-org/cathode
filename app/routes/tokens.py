"""Token management API — operator endpoints for private channel tokens.

Management endpoints (GET/POST/DELETE at /api/tokens/*).
Token validation on protocol endpoints is in protocol/routes.py.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import main

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/tokens", tags=["tokens"])


class TokenCreateRequest(BaseModel):
    """Create a new token for a channel."""

    name: str  # Human-readable label
    expires: str | None = None  # Optional ISO 8601 expiry


def _get_token_store():
    """Get the token store, or None if not initialized."""
    return getattr(main, "token_store", None)


def _find_channel(channel_id: str):
    """Find a channel by either federation ID or slug.

    Accepts TV-prefixed federation IDs or internal slugs (e.g. "channel-one").
    Returns the ChannelContext or None.
    """
    # Try federation ID first
    for ctx in main.channels.all():
        if ctx.channel_id == channel_id:
            return ctx
    # Try slug
    try:
        return main.channels.get(channel_id)
    except KeyError:
        return None


@router.get("/{channel_id}")
async def list_tokens(channel_id: str) -> dict:
    """List tokens for a channel (names and dates, not values).

    Returns token_id, name, created, and expires for each token.
    Token values are NOT returned for security.
    """
    store = _get_token_store()
    if store is None:
        raise HTTPException(503, "Token store not available")

    ctx = _find_channel(channel_id)
    if ctx is None:
        raise HTTPException(404, f"Channel '{channel_id}' not found")

    # Use the federation ID for the store
    fed_id = ctx.channel_id
    if not fed_id:
        raise HTTPException(400, "Channel has no federation ID")

    tokens = store.list_tokens(fed_id)
    return {
        "ok": True,
        "channel_id": fed_id,
        "tokens": [t.to_public_dict() for t in tokens],
        "count": len(tokens),
    }


@router.post("/{channel_id}", status_code=201)
async def create_token(channel_id: str, req: TokenCreateRequest) -> dict:
    """Generate and return a new token for a channel.

    Returns the full token value — this is the only time it's shown.
    """
    store = _get_token_store()
    if store is None:
        raise HTTPException(503, "Token store not available")

    ctx = _find_channel(channel_id)
    if ctx is None:
        raise HTTPException(404, f"Channel '{channel_id}' not found")

    fed_id = ctx.channel_id
    if not fed_id:
        raise HTTPException(400, "Channel has no federation ID")

    entry = store.create(fed_id, name=req.name, expires=req.expires)

    return {
        "ok": True,
        "token": entry.token,
        "token_id": entry.token_id,
        "name": entry.name,
        "created": entry.created,
        "expires": entry.expires,
        "channel_id": fed_id,
    }


@router.delete("/{channel_id}/{token_id}")
async def revoke_token(channel_id: str, token_id: str) -> dict:
    """Revoke a token by its token_id."""
    store = _get_token_store()
    if store is None:
        raise HTTPException(503, "Token store not available")

    ctx = _find_channel(channel_id)
    if ctx is None:
        raise HTTPException(404, f"Channel '{channel_id}' not found")

    fed_id = ctx.channel_id
    if not fed_id:
        raise HTTPException(400, "Channel has no federation ID")

    if store.revoke(fed_id, token_id):
        return {"ok": True, "revoked": token_id, "channel_id": fed_id}
    else:
        raise HTTPException(404, f"Token '{token_id}' not found")
