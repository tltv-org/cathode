"""Channel metadata endpoints — /api/channel.

Runtime-configurable federation identity fields:
  GET   /api/channel        — return current metadata
  PATCH /api/channel        — update fields in-memory (+ optionally persist to YAML)

Changes take effect immediately: the protocol route at
/tltv/v1/channels/{id} reads ctx fields on every request, so in-memory
updates are reflected instantly.

The seq counter is bumped on every successful PATCH so federation peers
detect the change within the next metadata cache window (~60 s).
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException

import main
from models import ChannelMetadataRequest

logger = logging.getLogger(__name__)

router = APIRouter(tags=["channel"])


def _first_channel():
    ctxs = main.channels.all()
    if not ctxs:
        raise HTTPException(503, "No channels registered")
    return ctxs[0]


# ── GET ──


@router.get("/api/channel")
async def get_channel_metadata() -> dict:
    """Return current channel federation metadata."""
    ctx = _first_channel()
    return {
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
    }


# ── PATCH ──


@router.patch("/api/channel")
async def update_channel_metadata(req: ChannelMetadataRequest) -> dict:
    """Update channel federation metadata (partial update).

    Only supplied fields are changed.  Changes take effect immediately
    in-memory.  The metadata sequence number is bumped so peers detect
    the change.  Optionally persists to the channel YAML file.
    """
    ctx = _first_channel()

    # Validate access value if provided
    if req.access is not None and req.access not in ("public", "private"):
        raise HTTPException(400, "access must be 'public' or 'private'")

    # Validate status if provided
    if req.status is not None and req.status not in ("active", "retired"):
        raise HTTPException(400, "status must be 'active' or 'retired'")

    # Apply updates
    changed: dict = {}
    if req.display_name is not None:
        ctx.display_name = req.display_name
        changed["display_name"] = req.display_name
    if req.description is not None:
        ctx.description = req.description
        changed["description"] = req.description
    if req.language is not None:
        ctx.language = req.language
        changed["language"] = req.language
    if req.tags is not None:
        ctx.tags = req.tags
        changed["tags"] = req.tags
    if req.access is not None:
        ctx.access = req.access
        changed["access"] = req.access
    if req.origins is not None:
        ctx.origins = req.origins
        changed["origins"] = req.origins
    if req.timezone is not None:
        ctx.timezone = req.timezone
        changed["timezone"] = req.timezone
    if req.on_demand is not None:
        ctx.on_demand = req.on_demand
        changed["on_demand"] = req.on_demand
    if req.status is not None:
        ctx.status = req.status
        changed["status"] = req.status

    if not changed:
        raise HTTPException(400, "No fields provided to update")

    # Timezone note: the GStreamer engine runs in-process and inherits
    # the container's TZ environment variable.  No external service to
    # sync.  The ctx.timezone value is used for EPG guide generation
    # and signed metadata only.

    # Bump metadata seq so peers see the change
    _bump_metadata_seq(ctx)

    # Persist to YAML if possible (non-fatal on failure)
    try:
        _persist_channel_yaml(ctx)
    except Exception as exc:
        logger.warning("Could not persist channel YAML: %s", exc)

    logger.info("Channel metadata updated: %s", list(changed.keys()))
    return {"ok": True, **changed}


# ── Helpers ──


def _bump_metadata_seq(ctx) -> None:
    """Increment the channel's metadata sequence counter.

    The protocol signing module tracks seq in /data/seq/{channel_id}.seq.
    We call sign_metadata with a no-op doc to advance the counter, OR
    we directly bump the file if the signing path is accessible.
    """
    try:
        import protocol.signing as signing

        seq_file = Path(signing.SEQ_DIR) / f"{ctx.channel_id}-metadata.seq"
        seq_file.parent.mkdir(parents=True, exist_ok=True)
        current = int(seq_file.read_text().strip()) if seq_file.exists() else 0
        seq_file.write_text(str(current + 1))
        logger.debug("Bumped metadata seq for %s to %d", ctx.id, current + 1)
    except Exception as exc:
        logger.debug("Could not bump metadata seq: %s", exc)


def _persist_channel_yaml(ctx) -> None:
    """Write updated metadata fields back to the channel YAML config file.

    Only updates fields that ChannelMetadataRequest can change.
    Leaves all other keys in the YAML untouched.
    """
    from routes.playout import _update_channel_yaml

    def _update(doc):
        # Top-level fields
        doc["display_name"] = ctx.display_name
        doc["timezone"] = ctx.timezone
        doc["on_demand"] = ctx.on_demand

        # Identity sub-section
        identity = doc.setdefault("identity", {})
        identity["description"] = ctx.description
        identity["language"] = ctx.language
        identity["tags"] = ctx.tags
        identity["status"] = ctx.status
        identity["access"] = ctx.access
        identity["origins"] = ctx.origins

    _update_channel_yaml(ctx, _update)
    logger.debug("Persisted channel metadata for channel '%s'", ctx.id)
