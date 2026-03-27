"""HLS serving with private channel token auth.

Replaces the StaticFiles mount at /hls/ with route handlers that:
- Validate access tokens for private channels (PROTOCOL.md section 5.7)
- Rewrite manifests to embed tokens in all segment URIs
- Set privacy headers (Referrer-Policy, Cache-Control) per spec
- Serve public channels with no overhead (direct file response)

Section 5.7: "the node MUST embed the token in every URI within the
HLS playlist graph" and "the node MUST also accept the token on
segment requests."
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

import config
import main
from protocol.tokens import extract_token

logger = logging.getLogger(__name__)

router = APIRouter(tags=["hls"])


def _find_channel_ctx(channel_id: str):
    """Look up a ChannelContext by federation channel_id.

    HLS directories are named by channel_id (the TV-prefixed federation
    ID), not the internal channel name.
    """
    for ctx in main.channels.all():
        if ctx.channel_id == channel_id:
            return ctx
    return None


def _is_private(ctx) -> bool:
    return ctx is not None and ctx.access == "token"


_PRIVACY_HEADERS = {
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "private, no-store",
}


def _check_hls_token(ctx, request: Request) -> None:
    """Validate token for a private channel HLS request.

    Raises HTTPException(403) if the token is missing or invalid.
    Public channels pass through with no check.
    """
    if not _is_private(ctx):
        return

    token = request.query_params.get("token")
    if not token:
        raise HTTPException(403, "Access denied")

    token_store = getattr(main, "token_store", None)
    if token_store is None:
        raise HTTPException(503, "Token service not available")

    if not token_store.validate(ctx.channel_id, token):
        raise HTTPException(403, "Access denied")


def _tokenize_manifest(manifest: str, token: str) -> str:
    """Embed ?token= in all URIs within an HLS manifest.

    Handles segment URIs (bare lines), and URI= attributes in
    EXT-X-MAP, EXT-X-KEY, EXT-X-MEDIA, and EXT-X-STREAM-INF tags.
    Per PROTOCOL.md section 5.7.
    """

    def _append(uri: str) -> str:
        sep = "&" if "?" in uri else "?"
        return f"{uri}{sep}token={token}"

    lines = []
    for line in manifest.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            # URI line (segment, variant playlist, etc.)
            lines.append(_append(stripped))
        elif 'URI="' in stripped and any(
            stripped.startswith(tag)
            for tag in (
                "#EXT-X-MAP:",
                "#EXT-X-KEY:",
                "#EXT-X-MEDIA:",
                "#EXT-X-STREAM-INF:",
            )
        ):
            # Rewrite URI= attribute inside the tag
            def _rewrite_uri_attr(m: re.Match) -> str:
                return f'URI="{_append(m.group(1))}"'

            lines.append(re.sub(r'URI="([^"]*)"', _rewrite_uri_attr, stripped))
        else:
            lines.append(line)
    return "\n".join(lines) + "\n"


# ── GET /hls/{channel_id}/stream.m3u8 ──


@router.get("/hls/{channel_id}/stream.m3u8", response_model=None)
async def get_hls_manifest(channel_id: str, request: Request) -> Response:
    """Serve an HLS manifest, with token embedding for private channels."""
    ctx = _find_channel_ctx(channel_id)
    _check_hls_token(ctx, request)

    manifest_path = Path(config.HLS_OUTPUT_DIR) / channel_id / "stream.m3u8"
    if not manifest_path.is_file():
        raise HTTPException(404, "Stream not available")

    manifest = manifest_path.read_text()

    # For private channels, rewrite the manifest to embed token in all URIs
    if _is_private(ctx):
        token = request.query_params.get("token", "")
        manifest = _tokenize_manifest(manifest, token)

    headers = {
        "Cache-Control": "max-age=1, no-cache",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
    }
    if _is_private(ctx):
        headers.update(_PRIVACY_HEADERS)

    return Response(
        content=manifest,
        media_type="application/vnd.apple.mpegurl",
        headers=headers,
    )


# ── GET /hls/{channel_id}/{filename} ──


@router.get("/hls/{channel_id}/{filename}", response_model=None)
async def get_hls_file(channel_id: str, filename: str, request: Request) -> Response:
    """Serve an HLS segment or other file, with token auth for private channels."""
    ctx = _find_channel_ctx(channel_id)
    _check_hls_token(ctx, request)

    # Prevent path traversal
    if ".." in filename or "/" in filename:
        raise HTTPException(400, "Invalid filename")

    file_path = Path(config.HLS_OUTPUT_DIR) / channel_id / filename
    if not file_path.is_file():
        raise HTTPException(404, "Segment not found")

    # Determine content type from extension
    suffix = file_path.suffix.lower()
    if suffix == ".ts":
        media_type = "video/mp2t"
        cache = "max-age=3600"
    elif suffix in (".m3u8",):
        media_type = "application/vnd.apple.mpegurl"
        cache = "max-age=1, no-cache"
    elif suffix in (".m4s", ".fmp4"):
        media_type = "video/iso.segment"
        cache = "max-age=3600"
    elif suffix == ".mp4":
        media_type = "video/mp4"
        cache = "max-age=3600"
    else:
        media_type = "application/octet-stream"
        cache = "max-age=3600"

    headers = {
        "Cache-Control": cache,
        "Access-Control-Allow-Origin": "*",
    }
    if _is_private(ctx):
        headers.update(_PRIVACY_HEADERS)

    data = file_path.read_bytes()
    return Response(content=data, media_type=media_type, headers=headers)
