"""TLTV federation protocol endpoints.

Implements PROTOCOL.md section 8:
- GET /.well-known/tltv           — Node info (section 8.1)
- GET /tltv/v1/channels/{id}      — Signed metadata (section 8.2)
- GET /tltv/v1/channels/{id}/stream.m3u8 — Stream (section 8.3)
- GET /tltv/v1/channels/{id}/segments/{name} — HLS segments (relay)
- GET /tltv/v1/channels/{id}/guide.json  — Signed guide (section 8.4)
- GET /tltv/v1/channels/{id}/guide.xml   — XMLTV guide (section 8.5)
- GET /tltv/v1/peers              — Peer exchange (section 8.6)

For originated channels: metadata is built and signed locally.
For relayed channels: metadata/guide served verbatim from upstream cache,
HLS manifest and segments served from cache (spec section 10.1).

All endpoints are read-only GET. Management API at /api/* is unchanged.
Error responses use {"error": "<code>"} format per section 8.8.
Cache-Control headers per section 8.8 table.

All timestamps in signed documents use UTC with Z suffix (sections 6.1-6.5).
Seq values are Unix epoch timestamps (section 5.5).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response

import config
import main
from protocol.identity import parse_channel_id
from protocol.signing import next_seq, sign_document
from protocol.tokens import extract_token
from routes.guide import (
    _block_category,
    _block_title,
    _collect_guide_entries,
    build_xmltv,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["protocol"])


def _error(
    status_code: int, error_code: str, message: str | None = None
) -> JSONResponse:
    """Return a structured error response per spec section 8.8."""
    body: dict = {"error": error_code}
    if message:
        body["message"] = message
    return JSONResponse(content=body, status_code=status_code)


def _find_channel_by_federation_id(channel_id: str):
    """Look up a ChannelContext by its TV-prefixed federation channel ID.

    Returns the ChannelContext or None.
    """
    for ctx in main.channels.all():
        if ctx.channel_id == channel_id:
            return ctx
    return None


def _find_relay_target(channel_id: str):
    """Look up a RelayTarget by channel ID.

    Returns the RelayTarget or None.
    """
    if hasattr(main, "relay_manager") and main.relay_manager is not None:
        target = main.relay_manager.get(channel_id)
        if target and target.active:
            return target
    return None


def _find_mirror_manager(channel_id: str):
    """Look up a MirrorManager by federation channel ID.

    Returns the MirrorManager or None.
    """
    mirror_managers = getattr(main, "mirror_managers", None)
    if mirror_managers is None:
        return None
    return mirror_managers.get(channel_id)


def _validate_channel_id(channel_id: str) -> JSONResponse | None:
    """Validate a channel ID format. Returns an error response or None."""
    try:
        parse_channel_id(channel_id)
    except ValueError as exc:
        return _error(400, "invalid_request", str(exc))
    return None


def _private_headers(ctx) -> dict[str, str]:
    """Return extra headers for private channel responses.

    Private channels MUST include (sections 5.7, 8.8):
    - Referrer-Policy: no-referrer — prevent token leakage via Referer
    - Cache-Control: private, no-store — prevent caching of private content

    These override any default Cache-Control set by the endpoint.
    """
    if ctx.access == "token":
        return {
            "Referrer-Policy": "no-referrer",
            "Cache-Control": "private, no-store",
        }
    return {}


def _check_token_auth(ctx, request: Request) -> JSONResponse | None:
    """Check token auth for a private channel.

    Returns None if access is allowed, or an error response if denied.
    Public channels always return None (no auth needed).

    For private channels (access == "token"):
    - Missing token → 401 auth_required
    - Invalid/expired token → 403 auth_failed
    - Valid token → None (proceed)
    """
    if ctx.access != "token":
        return None  # Public channel — no auth needed

    token = extract_token(request)
    if not token:
        return _error(
            403,
            "access_denied",
            "This channel requires authentication",
        )

    # Validate against token store
    token_store = getattr(main, "token_store", None)
    if token_store is None:
        logger.error("Token store not initialized but channel %s requires auth", ctx.id)
        return _error(503, "service_unavailable", "Token service not available")

    if not token_store.validate(ctx.channel_id, token):
        return _error(403, "access_denied", "Invalid or expired token")

    return None


# ── GET /.well-known/tltv (section 8.1) ──


@router.get("/.well-known/tltv")
async def well_known_tltv() -> JSONResponse:
    """Node info — lists public channels this node originates and relays.

    No auth required. Private channels excluded.
    Relayed channels listed under 'relaying' (spec section 10.4).
    Cache-Control: max-age=60.
    """
    channel_list = []
    for ctx in main.channels.all():
        if ctx.access == "token":
            continue  # Private channels excluded
        if ctx.status == "retired":
            continue  # Retired channels excluded
        if ctx.migration is not None:
            continue  # Migrated channels excluded (section 5.14)
        if ctx.channel_id:
            channel_list.append(
                {
                    "id": ctx.channel_id,
                    "name": ctx.display_name,
                }
            )

    relaying_list = []
    if hasattr(main, "relay_manager") and main.relay_manager is not None:
        for target in main.relay_manager.active_relays():
            name = ""
            if target.metadata:
                name = target.metadata.get("name", "")
            relaying_list.append(
                {
                    "id": target.channel_id,
                    "name": name,
                }
            )

    body = {
        "protocol": "tltv",
        "versions": [1],
        "channels": channel_list,
        "relaying": relaying_list,
    }
    return JSONResponse(
        content=body,
        headers={"Cache-Control": "max-age=60"},
    )


# ── GET /tltv/v1/channels/{id} (section 8.2) ──


@router.get("/tltv/v1/channels/{channel_id}")
async def get_channel_metadata(channel_id: str, request: Request) -> JSONResponse:
    """Signed channel metadata document.

    For originated channels: builds and signs metadata locally.
    For relayed channels: serves cached upstream metadata verbatim
    (spec section 10.1 — upstream signature remains valid).

    Private channels require token auth (section 5.7).
    404 for unknown. 400 for bad format. Cache-Control: max-age=60.
    """
    # Validate format
    err = _validate_channel_id(channel_id)
    if err:
        return err

    # Check originated channels first
    ctx = _find_channel_by_federation_id(channel_id)
    if ctx:
        # Token auth for private channels — checked before migration
        # because private channel migration docs still require the
        # token (section 5.14: "the token requirement on the metadata
        # endpoint remains in effect").
        auth_err = _check_token_auth(ctx, request)
        if auth_err:
            return auth_err

        # Migration check (section 5.14) — serve migration doc instead
        if ctx.migration is not None:
            headers = {"Cache-Control": "max-age=60"}
            headers.update(_private_headers(ctx))
            return JSONResponse(
                content=ctx.migration,
                headers=headers,
            )

        # Mirror mode (section 10.8): Only the active metadata signer
        # signs fresh metadata.  A mirror in replicating mode re-serves
        # the primary's metadata verbatim — it MUST NOT independently
        # sign fresh metadata while the primary is up.  The signing
        # role transfers only during mirror promotion.
        mirror = _find_mirror_manager(channel_id)
        if mirror is not None and mirror.state == "replicating":
            # Serve cached upstream metadata if available
            cached = getattr(mirror, "cached_upstream_metadata", None)
            if cached is not None:
                headers = {"Cache-Control": "max-age=60"}
                headers.update(_private_headers(ctx))
                return JSONResponse(content=cached, headers=headers)
            # No cached metadata yet — fall through to sign our own
            # (first startup before primary metadata is fetched)

        if not ctx.private_key_path:
            return _error(503, "service_unavailable", "Channel keypair not available")

        # Build metadata document (section 5.1)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        seq = next_seq(channel_id, "metadata")

        # Field length constraints (section 5.2): name 1-64, description max 256
        # code points.  Truncate rather than reject — the operator set these in
        # the channel config and shouldn't get a 500 at runtime.
        name = ctx.display_name or ctx.id
        if len(name) > 64:
            name = name[:64]

        doc: dict = {
            "v": 1,
            "seq": seq,
            "id": channel_id,
            "name": name,
            "stream": f"/tltv/v1/channels/{channel_id}/stream.m3u8",
            "updated": now,
        }

        # Optional fields — only include if set
        if ctx.description:
            desc = ctx.description
            if len(desc) > 256:
                desc = desc[:256]
            doc["description"] = desc
        if ctx.language:
            doc["language"] = ctx.language
        if ctx.tags:
            doc["tags"] = ctx.tags
        if ctx.icon:
            doc["icon"] = f"/tltv/v1/channels/{channel_id}/icon.png"
        if ctx.access and ctx.access != "public":
            doc["access"] = ctx.access
        else:
            doc["access"] = "public"
        if ctx.on_demand:
            doc["on_demand"] = True
        if ctx.origins:
            doc["origins"] = ctx.origins
        if ctx.timezone:
            doc["timezone"] = ctx.timezone
        if ctx.status and ctx.status != "active":
            doc["status"] = ctx.status

        # Guide path
        doc["guide"] = f"/tltv/v1/channels/{channel_id}/guide.json"

        # Sign the document
        sign_document(doc, ctx.private_key_path)

        headers = {"Cache-Control": "max-age=60"}
        headers.update(_private_headers(ctx))
        return JSONResponse(content=doc, headers=headers)

    # Check relayed channels — serve cached metadata verbatim
    target = _find_relay_target(channel_id)
    if target and target.metadata:
        return JSONResponse(
            content=target.metadata,
            headers={"Cache-Control": "max-age=60"},
        )

    return _error(404, "channel_not_found")


# ── GET /tltv/v1/peers (section 8.6) ──


@router.get("/tltv/v1/peers")
async def get_peers() -> JSONResponse:
    """Peer exchange — returns known peers plus our own channels.

    Includes our own public channels with our address as a hint,
    relayed channels with our address as a hint (spec section 10.4),
    plus validated peers from the peer store.
    Private channels excluded (spec section 8.6).

    Cache-Control: max-age=300.
    """
    peers = []
    seen_ids: set[str] = set()

    # Include our own public, active channels as peers
    for ctx in main.channels.all():
        if ctx.access == "token":
            continue  # Private channels excluded
        if ctx.status == "retired":
            continue  # Retired channels excluded from peer exchange
        if ctx.migration is not None:
            continue  # Migrated channels excluded (section 5.14)
        if ctx.channel_id:
            seen_ids.add(ctx.channel_id)
            peers.append(
                {
                    "id": ctx.channel_id,
                    "name": ctx.display_name,
                    "hints": ctx.origins if ctx.origins else [],
                    "last_seen": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                }
            )

    # Include relayed channels with our address as a hint (section 10.4)
    if hasattr(main, "relay_manager") and main.relay_manager is not None:
        for target in main.relay_manager.active_relays():
            if target.channel_id not in seen_ids:
                seen_ids.add(target.channel_id)
                name = ""
                if target.metadata:
                    name = target.metadata.get("name", "")
                # Our own origins serve as hints for relayed channels
                our_hints = []
                for ctx in main.channels.all():
                    if ctx.origins:
                        our_hints = ctx.origins
                        break
                peers.append(
                    {
                        "id": target.channel_id,
                        "name": name,
                        "hints": our_hints,
                        "last_seen": datetime.now(timezone.utc).strftime(
                            "%Y-%m-%dT%H:%M:%SZ"
                        ),
                    }
                )

    # Add peers from the store (excluding already-listed channels)
    if hasattr(main, "peer_store") and main.peer_store is not None:
        store_peers = main.peer_store.public_peers(exclude_ids=seen_ids)
        peers.extend(store_peers)

    return JSONResponse(
        content={"peers": peers},
        headers={"Cache-Control": "max-age=300"},
    )


# ── GET /tltv/v1/channels/{id}/stream.m3u8 (section 8.3) ──


@router.get("/tltv/v1/channels/{channel_id}/stream.m3u8", response_model=None)
async def get_channel_stream(channel_id: str, request: Request) -> Response:
    """Stream endpoint.

    For originated channels: 302 redirect to HLS manifest (spec 8.3).
    For mirror channels in replicating/promoting mode: 200 with cached
    manifest from primary (section 10.8).
    For mirror channels in promoted mode: 200 with sequence-adjusted
    manifest from local nginx (section 10.8).
    For relayed channels: 200 with cached manifest (spec 10.1 —
    relay MUST NOT redirect to origin).

    Private channels require token auth (section 5.7).
    404 for unknown. 503 if relay cache is empty.
    """
    err = _validate_channel_id(channel_id)
    if err:
        return err

    # Originated channel
    ctx = _find_channel_by_federation_id(channel_id)
    if ctx:
        # Migrated channels no longer serve a stream (section 5.14)
        if ctx.migration is not None:
            return _error(404, "channel_migrated", "Channel has migrated")

        # Token auth for private channels
        auth_err = _check_token_auth(ctx, request)
        if auth_err:
            return auth_err

        # Mirror mode — serve from cache instead of redirecting
        # (replicating, promoting, or promoted with sequence adjustment)
        mirror = _find_mirror_manager(channel_id)
        if mirror is not None:
            manifest = mirror.hls_cache.manifest
            if manifest:
                return Response(
                    content=manifest,
                    media_type="application/vnd.apple.mpegurl",
                    headers={
                        "Cache-Control": "max-age=1, no-cache",
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Methods": "GET, OPTIONS",
                    },
                )
            else:
                return _error(
                    503, "stream_unavailable", "Mirror has no cached HLS data"
                )

        # Normal originated channel — redirect to nginx HLS.
        # Private channels (section 5.7): redirect MUST be same-origin
        # with token in query string — no cross-origin redirects.
        redirect_headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
        }
        redirect_headers.update(_private_headers(ctx))
        redirect_url = f"/hls/{channel_id}/stream.m3u8"
        if ctx.access == "token":
            token = extract_token(request)
            if token:
                redirect_url = f"/hls/{channel_id}/stream.m3u8?token={token}"
        return RedirectResponse(
            url=redirect_url,
            status_code=302,
            headers=redirect_headers,
        )

    # Relayed channel — serve cached manifest
    target = _find_relay_target(channel_id)
    if target:
        manifest = target.hls_cache.manifest
        if manifest:
            return Response(
                content=manifest,
                media_type="application/vnd.apple.mpegurl",
                headers={
                    "Cache-Control": "max-age=1, no-cache",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, OPTIONS",
                },
            )
        else:
            return _error(503, "stream_unavailable", "No cached HLS data from upstream")

    return _error(404, "channel_not_found")


# ── GET /tltv/v1/channels/{id}/segments/{name} (relay HLS segments) ──


@router.get(
    "/tltv/v1/channels/{channel_id}/segments/{segment_name}",
    response_model=None,
)
async def get_relay_segment(channel_id: str, segment_name: str) -> Response:
    """Serve a cached HLS segment for a relayed or mirror channel.

    Returns 200 with the segment data, or 404 if not in cache.
    Mirror channels (section 10.8) also serve segments from cache
    in all modes (replicating uses primary's segments, promoted
    uses locally-generated segments cached through the mirror manager).
    """
    # Check mirror channels first (they are also originated channels)
    mirror = _find_mirror_manager(channel_id)
    if mirror is not None:
        data = mirror.hls_cache.get_segment(segment_name)
        if data is not None:
            return Response(
                content=data,
                media_type="video/mp2t",
                headers={
                    "Cache-Control": "max-age=3600",
                    "Access-Control-Allow-Origin": "*",
                },
            )
        return _error(404, "segment_not_found")

    # Check relay targets
    target = _find_relay_target(channel_id)
    if not target:
        return _error(404, "channel_not_found")

    data = target.hls_cache.get_segment(segment_name)
    if data is None:
        return _error(404, "segment_not_found")

    return Response(
        content=data,
        media_type="video/mp2t",
        headers={
            "Cache-Control": "max-age=3600",
            "Access-Control-Allow-Origin": "*",
        },
    )


# ── GET /tltv/v1/channels/{id}/guide.json (section 8.4) ──


def _to_utc_timestamp(d: date, time_str: str, channel_tz: str | None = None) -> str:
    """Convert a local date + HH:MM:SS time to UTC ISO 8601 with Z suffix.

    All timestamps in signed documents use UTC (sections 6.1-6.5).

    Args:
        d: Date in the channel's local timezone.
        time_str: HH:MM:SS time in the channel's local timezone.
        channel_tz: IANA timezone name (e.g. "America/New_York").
                    If None, times are treated as UTC.

    Returns:
        UTC timestamp like "2026-03-15T00:30:00Z".
    """
    parts = time_str.split(":")
    h, m, s = int(parts[0]), int(parts[1]), int(parts[2])

    if channel_tz:
        tz = ZoneInfo(channel_tz)
        local_dt = datetime(d.year, d.month, d.day, h, m, s, tzinfo=tz)
        utc_dt = local_dt.astimezone(timezone.utc)
    else:
        utc_dt = datetime(d.year, d.month, d.day, h, m, s, tzinfo=timezone.utc)

    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _local_midnight_utc(d: date, channel_tz: str | None = None) -> str:
    """Convert local midnight to UTC ISO 8601 timestamp with Z suffix.

    Args:
        d: Date whose midnight to convert.
        channel_tz: IANA timezone name. If None, midnight is UTC.

    Returns:
        UTC timestamp like "2026-03-15T05:00:00Z".
    """
    return _to_utc_timestamp(d, "00:00:00", channel_tz)


@router.get("/tltv/v1/channels/{channel_id}/guide.json")
async def get_channel_guide_json(channel_id: str, request: Request) -> JSONResponse:
    """Signed channel guide document (section 6).

    For originated channels: builds, signs, and returns guide.
    For relayed channels: serves cached upstream guide verbatim.

    Private channels require token auth (section 5.7).
    404 for unknown channel. Cache-Control: max-age=300.
    """
    err = _validate_channel_id(channel_id)
    if err:
        return err

    # Originated channel
    ctx = _find_channel_by_federation_id(channel_id)
    if ctx:
        # Migrated channels no longer serve a guide (section 5.14)
        if ctx.migration is not None:
            return _error(404, "channel_migrated", "Channel has migrated")

        # Token auth for private channels
        auth_err = _check_token_auth(ctx, request)
        if auth_err:
            return auth_err

        if not ctx.private_key_path:
            return _error(503, "service_unavailable", "Channel keypair not available")

        # Compute "today" in the channel's timezone for correct program lookup
        if ctx.timezone:
            today = datetime.now(ZoneInfo(ctx.timezone)).date()
        else:
            today = date.today()

        tomorrow = today + timedelta(days=1)
        entries = _collect_guide_entries(today, channel_id=ctx.id)

        # Convert entries to protocol format (section 6.3)
        # All timestamps are UTC with Z suffix (sections 6.1-6.5)
        protocol_entries = []
        for entry in entries:
            pe: dict = {
                "start": _to_utc_timestamp(entry["date"], entry["start"], ctx.timezone),
                "end": _to_utc_timestamp(entry["date"], entry["stop"], ctx.timezone),
                "title": entry["title"],
            }
            # Optional fields
            category = entry.get("category")
            if category and category != "unknown":
                pe["category"] = category
            protocol_entries.append(pe)

        # Build the guide document envelope (section 6.1)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        seq = next_seq(channel_id, "guide")

        # from/until: coverage window — local midnight boundaries in UTC
        day_after_tomorrow = tomorrow + timedelta(days=1)

        doc: dict = {
            "v": 1,
            "seq": seq,
            "id": channel_id,
            "from": _local_midnight_utc(today, ctx.timezone),
            "until": _local_midnight_utc(day_after_tomorrow, ctx.timezone),
            "entries": protocol_entries,
            "updated": now,
        }

        sign_document(doc, ctx.private_key_path)

        guide_headers = {"Cache-Control": "max-age=300"}
        guide_headers.update(_private_headers(ctx))
        return JSONResponse(content=doc, headers=guide_headers)

    # Relayed channel — serve cached guide verbatim
    target = _find_relay_target(channel_id)
    if target and target.guide:
        return JSONResponse(
            content=target.guide,
            headers={"Cache-Control": "max-age=300"},
        )

    if target:
        return _error(503, "guide_unavailable", "No cached guide from upstream")

    return _error(404, "channel_not_found")


# ── GET /tltv/v1/channels/{id}/guide.xml (section 8.5) ──


@router.get("/tltv/v1/channels/{channel_id}/guide.xml", response_model=None)
async def get_channel_guide_xml(channel_id: str, request: Request) -> Response:
    """XMLTV guide at protocol path (section 8.5).

    For originated channels: generates from local guide data.
    For relayed channels: generates from cached guide entries.

    Private channels require token auth (section 5.7).
    NOT signed — convenience format for IPTV client compatibility.
    Uses TV-prefixed federation ID in <channel id="...">.

    404 for unknown channel. Cache-Control: max-age=300.
    """
    err = _validate_channel_id(channel_id)
    if err:
        return err

    # Originated channel
    ctx = _find_channel_by_federation_id(channel_id)
    if ctx:
        # Migrated channels no longer serve a guide (section 5.14)
        if ctx.migration is not None:
            return _error(404, "channel_migrated", "Channel has migrated")

        # Token auth for private channels
        auth_err = _check_token_auth(ctx, request)
        if auth_err:
            return auth_err
        # Use channel-aware "today" (same as guide.json builder)
        if ctx.timezone:
            today = datetime.now(ZoneInfo(ctx.timezone)).date()
        else:
            today = date.today()
        entries = _collect_guide_entries(today, channel_id=ctx.id)
        xml_bytes = build_xmltv(entries, channel_id, ctx.display_name)

        xml_headers = {"Cache-Control": "max-age=300"}
        xml_headers.update(_private_headers(ctx))
        return Response(
            content=xml_bytes,
            media_type="application/xml",
            headers=xml_headers,
        )

    # Relayed channel — build XMLTV from cached guide entries
    target = _find_relay_target(channel_id)
    if target and target.guide:
        channel_name = ""
        if target.metadata:
            channel_name = target.metadata.get("name", target.channel_id)
        else:
            channel_name = target.channel_id

        xml_bytes = _build_xmltv_from_protocol_guide(
            target.guide, channel_id, channel_name
        )
        return Response(
            content=xml_bytes,
            media_type="application/xml",
            headers={"Cache-Control": "max-age=300"},
        )

    if target:
        return _error(503, "guide_unavailable", "No cached guide from upstream")

    return _error(404, "channel_not_found")


def _build_xmltv_from_protocol_guide(
    guide: dict, channel_id: str, channel_name: str
) -> bytes:
    """Build XMLTV XML from a protocol guide document.

    Converts protocol guide entries (ISO 8601 times) to XMLTV format.
    """
    import xml.etree.ElementTree as ET

    tv = ET.Element("tv", {"generator-info-name": "Cathode"})
    ch = ET.SubElement(tv, "channel", {"id": channel_id})
    display = ET.SubElement(ch, "display-name")
    display.text = channel_name

    for entry in guide.get("entries", []):
        start_iso = entry.get("start", "")
        end_iso = entry.get("end", "")
        title_text = entry.get("title", "")

        # Convert ISO 8601 to XMLTV format: YYYYMMDDHHmmSS +HHMM
        start_xmltv = _iso_to_xmltv_time(start_iso)
        end_xmltv = _iso_to_xmltv_time(end_iso)

        if start_xmltv and end_xmltv:
            prog = ET.SubElement(
                tv,
                "programme",
                {"start": start_xmltv, "stop": end_xmltv, "channel": channel_id},
            )
            t = ET.SubElement(prog, "title")
            t.text = title_text

            category = entry.get("category")
            if category:
                cat = ET.SubElement(prog, "category")
                cat.text = category

    xml_decl = b'<?xml version="1.0" encoding="UTF-8"?>\n'
    return xml_decl + ET.tostring(tv, encoding="unicode").encode("utf-8")


def _iso_to_xmltv_time(iso_str: str) -> str:
    """Convert ISO 8601 timestamp to XMLTV format.

    Input:  "2026-03-14T19:00:00-05:00"
    Output: "20260314190000 -0500"
    """
    if not iso_str:
        return ""
    try:
        # Parse the date/time part (before the timezone)
        # Handle both +HH:MM and -HH:MM timezone offsets
        dt_part = iso_str[:19]  # "2026-03-14T19:00:00"
        tz_part = iso_str[19:]  # "-05:00" or "+00:00" or "Z"

        if tz_part == "Z":
            tz_formatted = "+0000"
        elif len(tz_part) == 6 and tz_part[3] == ":":
            # "-05:00" -> "-0500"
            tz_formatted = tz_part[:3] + tz_part[4:]
        else:
            tz_formatted = tz_part.replace(":", "")

        # Format datetime part
        dt = datetime.fromisoformat(dt_part)
        return dt.strftime("%Y%m%d%H%M%S") + " " + tz_formatted
    except Exception:
        return ""
