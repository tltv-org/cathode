"""Key migration management API (PROTOCOL.md section 5.14).

POST /api/migration/create — create a migration document for a channel.
GET  /api/migration        — list migration status for all channels.

Migration is permanent and irreversible.  Once a migration document is
created and stored, the channel's metadata endpoint serves the migration
document instead of regular metadata, and stream/guide endpoints return 404.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

import config
import main
from models import MigrationRequest
from protocol.identity import parse_channel_id
from protocol.signing import next_seq, sign_document, verify_migration_document

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/migration", tags=["migration"])

# ── Persistence ──


def _migration_path(channel_slug: str) -> Path:
    """Path to the migration document file for a channel."""
    return Path(config.KEY_DIR) / f"{channel_slug}.migration.json"


def save_migration(channel_slug: str, doc: dict) -> None:
    """Persist a signed migration document to disk."""
    path = _migration_path(channel_slug)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2))
    logger.info("Saved migration document for '%s' at %s", channel_slug, path)


def load_migration(channel_slug: str) -> dict | None:
    """Load a persisted migration document, or None if not migrated."""
    path = _migration_path(channel_slug)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to load migration for '%s': %s", channel_slug, exc)
        return None


# ── Endpoints ──


@router.get("")
async def list_migrations() -> JSONResponse:
    """List migration status for all channels."""
    result = []
    for ctx in main.channels.all():
        entry: dict = {
            "channel_slug": ctx.id,
            "channel_id": ctx.channel_id,
            "migrated": ctx.migration is not None,
        }
        if ctx.migration:
            entry["to"] = ctx.migration.get("to")
            entry["reason"] = ctx.migration.get("reason")
            entry["migrated_at"] = ctx.migration.get("migrated")
        result.append(entry)
    return JSONResponse(content={"channels": result})


@router.post("/create")
async def create_migration(req: MigrationRequest) -> JSONResponse:
    """Create a migration document for a channel.

    Signs the migration document with the channel's private key and
    stores it.  Once created, the migration is permanent — the metadata
    endpoint will serve the migration document instead of regular metadata.

    Validation:
    - 'to' must be a valid V1 channel ID
    - 'to' must differ from the current channel ID
    - Channel must not already be migrated
    - Reason must be <= 256 Unicode code points (if provided)
    """
    # Find the channel (use the first non-migrated channel, or look up by slug)
    # For single-channel setups, use the first channel.
    # For multi-channel, the operator should specify which channel.
    # We'll use the first originated channel for now.
    ctx = None
    for c in main.channels.all():
        if c.channel_id:
            ctx = c
            break

    if ctx is None:
        raise HTTPException(400, "No channel with federation ID")

    # Validate: not already migrated
    if ctx.migration is not None:
        raise HTTPException(
            409,
            f"Channel already migrated to {ctx.migration.get('to')}",
        )

    # Validate: 'to' is a valid V1 channel ID
    try:
        parse_channel_id(req.to)
    except ValueError as exc:
        raise HTTPException(400, f"Invalid target channel ID: {exc}")

    # Validate: 'to' differs from 'from'
    if req.to == ctx.channel_id:
        raise HTTPException(400, "Cannot migrate a channel to itself")

    # Validate: reason length
    if req.reason and len(req.reason) > 256:
        raise HTTPException(400, "Reason must be at most 256 Unicode code points")

    # Validate: private key available
    if not ctx.private_key_path:
        raise HTTPException(503, "Channel private key not available for signing")

    # Build migration document (section 5.14)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    seq = next_seq(ctx.channel_id, "migration")
    doc: dict = {
        "v": 1,
        "seq": seq,
        "type": "migration",
        "from": ctx.channel_id,
        "to": req.to,
        "migrated": now,
    }
    if req.reason:
        doc["reason"] = req.reason

    # Sign with the old key
    sign_document(doc, ctx.private_key_path)

    # Verify our own signature before persisting
    if not verify_migration_document(doc, ctx.channel_id):
        raise HTTPException(500, "Self-verification of migration signature failed")

    # Persist and set on context (permanent, irreversible)
    save_migration(ctx.id, doc)
    ctx.migration = doc

    logger.info(
        "Channel '%s' (%s) migrated to %s",
        ctx.id,
        ctx.channel_id,
        req.to,
    )

    return {
        "ok": True,
        "migrated": True,
        "from": ctx.channel_id,
        "to": req.to,
        "document": doc,
    }
