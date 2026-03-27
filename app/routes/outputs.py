"""Output pipeline management endpoints — /api/outputs/*."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import main
from playout.output_layer import OutputConfig, OutputType
from routes.channel_resolve import resolve_channel

logger = logging.getLogger(__name__)

router = APIRouter(tags=["outputs"])


# ── Request / response models ──


class CreateOutputRequest(BaseModel):
    type: str  # "hls", "rtmp", "file", "null"
    name: str
    video_bitrate: int | None = None
    audio_bitrate: int | None = None
    keyframe_interval: int | None = None
    preset: str | None = None
    # HLS
    hls_dir: str | None = None
    segment_duration: int | None = None
    playlist_length: int | None = None
    # RTMP
    rtmp_url: str | None = None
    # File
    file_path: str | None = None
    file_format: str | None = None
    max_duration: int | None = None


class UpdateOutputRequest(BaseModel):
    video_bitrate: int | None = None
    audio_bitrate: int | None = None
    keyframe_interval: int | None = None
    preset: str | None = None
    # HLS
    segment_duration: int | None = None
    playlist_length: int | None = None
    # RTMP
    rtmp_url: str | None = None


# ── Endpoints ──


def _engine(channel: str | None = None):
    """Get the engine for a channel, or the default."""
    if channel is not None:
        _, engine = resolve_channel(channel)
        return engine
    if main.playout is not None:
        return main.playout
    _, engine = resolve_channel(None)
    return engine


@router.get("/api/outputs")
async def list_outputs(channel: str | None = None) -> dict:
    """List all active output pipelines."""
    engine = _engine(channel)
    return {"outputs": [output.health for output in engine.outputs.values()]}


@router.post("/api/outputs", status_code=201)
async def create_output(req: CreateOutputRequest, channel: str | None = None) -> dict:
    """Create and start a new output pipeline."""
    engine = _engine(channel)

    try:
        output_type = OutputType(req.type)
    except ValueError:
        valid = [t.value for t in OutputType]
        raise HTTPException(400, f"Invalid output type '{req.type}'. Valid: {valid}")

    # Build OutputConfig from request, using defaults for unset fields
    cfg = OutputConfig(
        type=output_type,
        name=req.name,
    )

    # Apply optional overrides
    if req.video_bitrate is not None:
        cfg.video_bitrate = req.video_bitrate
    if req.audio_bitrate is not None:
        cfg.audio_bitrate = req.audio_bitrate
    if req.keyframe_interval is not None:
        cfg.keyframe_interval = req.keyframe_interval
    if req.preset is not None:
        cfg.preset = req.preset

    # Type-specific fields
    if req.hls_dir is not None:
        cfg.hls_dir = req.hls_dir
    if req.segment_duration is not None:
        cfg.segment_duration = req.segment_duration
    if req.playlist_length is not None:
        cfg.playlist_length = req.playlist_length
    if req.rtmp_url is not None:
        # SSRF protection — reject private/loopback RTMP targets
        from urllib.parse import urlparse

        from protocol.uri import _is_private_or_loopback

        parsed = urlparse(req.rtmp_url)
        if parsed.hostname and _is_private_or_loopback(parsed.hostname):
            raise HTTPException(
                400, "RTMP URL must not target private or loopback addresses"
            )
        cfg.rtmp_url = req.rtmp_url
    if req.file_path is not None:
        # Restrict recording output to /data/ directory
        import os

        norm = os.path.normpath(req.file_path)
        if not norm.startswith("/data/"):
            raise HTTPException(400, "file_path must be within the /data/ directory")
        if ".." in req.file_path:
            raise HTTPException(400, "Path traversal ('..') is not allowed")
        cfg.file_path = req.file_path
    if req.file_format is not None:
        cfg.file_format = req.file_format
    if req.max_duration is not None:
        cfg.max_duration = req.max_duration

    try:
        output = await engine.add_output(cfg)
        logger.info("Output created: %s (%s)", req.name, req.type)
        return {"ok": True, "output": output.health}
    except ValueError as exc:
        raise HTTPException(409, str(exc))
    except Exception as exc:
        logger.error("Failed to create output '%s': %s", req.name, exc, exc_info=True)
        raise HTTPException(500, "Failed to create output")


@router.get("/api/outputs/{name}")
async def get_output(name: str, channel: str | None = None) -> dict:
    """Get status of a specific output pipeline."""
    engine = _engine(channel)
    output = engine.get_output(name)
    if output is None:
        raise HTTPException(404, f"Output '{name}' not found")

    return output.health


@router.patch("/api/outputs/{name}")
async def update_output(
    name: str, req: UpdateOutputRequest, channel: str | None = None
) -> dict:
    """Update an output's config.  Stops, rebuilds, and restarts the output."""
    engine = _engine(channel)
    output = engine.get_output(name)
    if output is None:
        raise HTTPException(404, f"Output '{name}' not found")

    # Build updated config
    cfg = output.config
    if req.video_bitrate is not None:
        cfg.video_bitrate = req.video_bitrate
    if req.audio_bitrate is not None:
        cfg.audio_bitrate = req.audio_bitrate
    if req.keyframe_interval is not None:
        cfg.keyframe_interval = req.keyframe_interval
    if req.preset is not None:
        cfg.preset = req.preset
    if req.segment_duration is not None:
        cfg.segment_duration = req.segment_duration
    if req.playlist_length is not None:
        cfg.playlist_length = req.playlist_length
    if req.rtmp_url is not None:
        cfg.rtmp_url = req.rtmp_url

    try:
        # Remove old, add with updated config
        await engine.remove_output(name)
        new_output = await engine.add_output(cfg)
        return {"ok": True, "output": new_output.health}
    except Exception as exc:
        logger.error("Failed to update output '%s': %s", name, exc, exc_info=True)
        raise HTTPException(500, "Failed to update output")


@router.delete("/api/outputs/{name}")
async def delete_output(name: str, channel: str | None = None) -> dict:
    """Stop and remove an output pipeline."""
    engine = _engine(channel)
    try:
        await engine.remove_output(name)
        logger.info("Output removed: %s", name)
        return {"ok": True, "removed": name}
    except ValueError as exc:
        raise HTTPException(404, str(exc))


@router.post("/api/outputs/{name}/stop")
async def stop_output(name: str, channel: str | None = None) -> dict:
    """Stop an output without removing it."""
    engine = _engine(channel)
    output = engine.get_output(name)
    if output is None:
        raise HTTPException(404, f"Output '{name}' not found")

    output.stop()
    return {"ok": True, "output": output.health}


@router.post("/api/outputs/{name}/start")
async def start_output(name: str, channel: str | None = None) -> dict:
    """Start a stopped output."""
    engine = _engine(channel)
    output = engine.get_output(name)
    if output is None:
        raise HTTPException(404, f"Output '{name}' not found")

    try:
        output.start()
        return {"ok": True, "output": output.health}
    except Exception as exc:
        logger.error("Failed to start output '%s': %s", name, exc, exc_info=True)
        raise HTTPException(500, "Failed to start output")
