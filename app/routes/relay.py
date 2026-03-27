"""Relay management API — operator endpoints for relay configuration.

These are management endpoints (POST/GET/DELETE at /api/relay/*).
Protocol-level relay serving (cached metadata, HLS) is in protocol/routes.py.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import config
import main
from protocol.peers import PeerEntry, validate_peer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/relay", tags=["relay"])


class RelayAddRequest(BaseModel):
    """Add a relay target."""

    channel_id: str  # TV-prefixed channel ID to relay
    hint: str  # host:port of the upstream node


@router.get("")
async def list_relays() -> dict:
    """List relayed channels with status."""
    if not hasattr(main, "relay_manager") or main.relay_manager is None:
        return {"ok": True, "relays": [], "count": 0}
    relays = [t.status_dict() for t in main.relay_manager.all()]
    return {"ok": True, "relays": relays, "count": len(relays)}


@router.post("")
async def add_relay(req: RelayAddRequest) -> dict:
    """Add a relay target.  Validates metadata signature before accepting."""
    if not hasattr(main, "relay_manager") or main.relay_manager is None:
        raise HTTPException(503, "Relay manager not available")

    # Validate the upstream first
    metadata = await validate_peer(
        req.hint,
        req.channel_id,
        require_tls=config.PEER_REQUIRE_TLS,
    )
    if not metadata:
        raise HTTPException(
            400,
            f"Could not validate channel {req.channel_id} at {req.hint}",
        )

    # Check on-demand (section 10.2)
    if metadata.get("on_demand"):
        raise HTTPException(
            400,
            "On-demand channels cannot be relayed (spec section 10.2)",
        )

    # Check private (section 10.2/5.7)
    if metadata.get("access") == "token":
        raise HTTPException(400, "Private channels cannot be relayed")

    target = main.relay_manager.add(req.channel_id, [req.hint])

    # Pre-fetch metadata and guide immediately
    await main.relay_manager.fetch_metadata(target)
    await main.relay_manager.fetch_guide(target)

    # Also add to peer store if available
    if hasattr(main, "peer_store") and main.peer_store is not None:
        entry = PeerEntry(
            id=req.channel_id,
            name=metadata.get("name", ""),
            hints=[req.hint],
            verified=True,
        )
        main.peer_store.add(entry)

    return {
        "ok": True,
        "added": req.channel_id,
        "upstream": req.hint,
        "name": metadata.get("name", ""),
    }


@router.delete("/{channel_id}")
async def remove_relay(channel_id: str) -> dict:
    """Stop relaying a channel."""
    if not hasattr(main, "relay_manager") or main.relay_manager is None:
        raise HTTPException(503, "Relay manager not available")

    if await main.relay_manager.remove(channel_id):
        return {"ok": True, "removed": channel_id}
    else:
        raise HTTPException(404, f"Relay {channel_id} not found")
