"""Peer management API — operator endpoints for peer exchange.

These are management endpoints (POST/GET/DELETE at /api/peers/*),
not protocol endpoints.  Protocol peer exchange is in protocol/routes.py.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import main
from protocol.peers import PeerEntry, validate_peer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/peers", tags=["peers"])


class PeerAddRequest(BaseModel):
    """Add a peer by providing a hint and the channel ID at that hint."""

    hint: str  # host:port
    channel_id: str | None = None  # If omitted, discovers all channels at the hint


@router.get("")
async def list_peers() -> dict:
    """Return the raw peer store (for debugging)."""
    if not hasattr(main, "peer_store") or main.peer_store is None:
        return {"ok": True, "peers": [], "count": 0}
    peers = [p.to_dict() for p in main.peer_store.all()]
    return {"ok": True, "peers": peers, "count": len(peers)}


@router.post("")
async def add_peer(req: PeerAddRequest) -> dict:
    """Add a peer by validating it first.

    If channel_id is provided, validates and adds that specific channel.
    If omitted, fetches /.well-known/tltv from the hint and discovers
    all public channels advertised there.
    """
    if not hasattr(main, "peer_store") or main.peer_store is None:
        raise HTTPException(503, "Peer store not available")

    import httpx
    from protocol.peers import PEER_REQUIRE_TLS, PEER_VALIDATE_TIMEOUT

    require_tls = PEER_REQUIRE_TLS
    scheme = "https" if require_tls else "http"
    base_url = f"{scheme}://{req.hint}"

    added = []

    async with httpx.AsyncClient(timeout=PEER_VALIDATE_TIMEOUT) as client:
        if req.channel_id:
            # Validate specific channel
            metadata = await validate_peer(
                req.hint,
                req.channel_id,
                require_tls=require_tls,
                client=client,
            )
            if metadata:
                entry = PeerEntry(
                    id=req.channel_id,
                    name=metadata.get("name", ""),
                    hints=[req.hint],
                    verified=True,
                )
                main.peer_store.add(entry)
                added.append(req.channel_id)
            else:
                raise HTTPException(
                    400,
                    f"Could not validate channel {req.channel_id} at {req.hint}",
                )
        else:
            # Discover all channels at the hint
            try:
                resp = await client.get(f"{base_url}/.well-known/tltv")
                if resp.status_code != 200:
                    raise HTTPException(400, f"Could not reach {req.hint}")
                well_known = resp.json()
                channels = well_known.get("channels", []) + well_known.get(
                    "relaying", []
                )

                for ch in channels:
                    ch_id = ch.get("id")
                    if not ch_id:
                        continue
                    metadata = await validate_peer(
                        req.hint,
                        ch_id,
                        require_tls=require_tls,
                        client=client,
                    )
                    if metadata:
                        entry = PeerEntry(
                            id=ch_id,
                            name=metadata.get("name", ""),
                            hints=[req.hint],
                            verified=True,
                        )
                        main.peer_store.add(entry)
                        added.append(ch_id)

            except HTTPException:
                raise
            except Exception as exc:
                logger.error("Peer discovery failed: %s", exc, exc_info=True)
                raise HTTPException(400, "Discovery failed")

    return {
        "ok": True,
        "added": added,
        "count": len(added),
    }


@router.delete("/{channel_id}")
async def remove_peer(channel_id: str) -> dict:
    """Remove a peer from the store."""
    if not hasattr(main, "peer_store") or main.peer_store is None:
        raise HTTPException(503, "Peer store not available")

    if main.peer_store.remove(channel_id):
        return {"ok": True, "removed": channel_id}
    else:
        raise HTTPException(404, f"Peer {channel_id} not in store")
