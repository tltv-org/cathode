"""Background tasks — watchdog, schedule, program, HLS, peer exchange, relay.

All loops run as asyncio tasks started from the app lifespan.
Per-channel loops receive a ChannelContext.  Global loops (peer exchange,
relay metadata, relay HLS) operate across all channels and access state
through the main module.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path

import random

import config
import main
import plugins
import program
from channel import ChannelContext
from utils import get_clip_duration, scan_media

logger = logging.getLogger(__name__)


# ── Startup helpers ──


def _generate_video_gstreamer(
    output_path: str,
    title: str = "Cathode",
    subtitle: str = "",
    duration: int = 60,
    width: int = 1920,
    height: int = 1080,
    fps: int = 30,
    pattern: str = "smpte",
) -> bool:
    """Generate a test card video file using GStreamer.

    Renders SMPTE bars (or another pattern) with centered text overlay
    and silent audio to an MP4 file.  Uses the same GStreamer elements
    as the live failover pipeline but outputs to filesink.

    Pure GStreamer — no ffmpeg, no plugins.

    Args:
        output_path: Where to write the mp4.
        title: Large centered text (channel name).
        subtitle: Smaller text below the title.
        duration: Video length in seconds.
        width: Video width.
        height: Video height.
        fps: Frame rate.
        pattern: videotestsrc pattern name (smpte, black, white, etc.).

    Returns:
        True on success, False on failure.
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    try:
        import gi

        gi.require_version("Gst", "1.0")
        from gi.repository import Gst

        if not Gst.is_initialized():
            Gst.init(None)
    except (ImportError, ValueError) as exc:
        logger.error("GStreamer not available for video generation: %s", exc)
        return False

    video_buffers = duration * fps
    # 1024 samples per buffer at 48kHz — standard GStreamer default
    audio_buffers = (duration * 48000) // 1024 + 1

    # Build text lines
    lines = []
    if title:
        lines.append(title)
    if subtitle:
        lines.append(subtitle)
    text = "\n".join(lines) if lines else ""

    # Escape for Gst.parse_launch (double-quotes inside single-quoted strings)
    safe_text = text.replace('"', '\\"')

    # Video branch: testsrc → text overlay → encode
    # Audio branch: silence → encode
    # Both feed into mp4mux → filesink
    pipeline_str = (
        f"videotestsrc pattern={pattern} num-buffers={video_buffers} "
        f"! video/x-raw,width={width},height={height},framerate={fps}/1,format=I420 "
    )

    if safe_text:
        pipeline_str += (
            f'! textoverlay text="{safe_text}" '
            f"  valignment=center halignment=center "
            f'  font-desc="Sans Bold 48" '
            f"  shaded-background=true line-alignment=center "
        )

    pipeline_str += (
        f"! videoconvert ! x264enc speed-preset=ultrafast tune=zerolatency "
        f'! h264parse ! mp4mux name=mux ! filesink location="{output_path}" '
        f"audiotestsrc wave=silence volume=0 num-buffers={audio_buffers} "
        f"! audio/x-raw,rate=48000,channels=2,format=S16LE "
        f"! audioconvert ! audioresample ! avenc_aac ! mux. "
    )

    logger.info("Generating video: %s (%ds)...", Path(output_path).name, duration)
    pipeline = None
    try:
        pipeline = Gst.parse_launch(pipeline_str)
        pipeline.set_state(Gst.State.PLAYING)

        bus = pipeline.get_bus()
        msg = bus.timed_pop_filtered(
            120 * Gst.SECOND,  # 120s timeout
            Gst.MessageType.EOS | Gst.MessageType.ERROR,
        )

        if msg is None:
            logger.error("GStreamer timed out generating %s", output_path)
            return False
        if msg.type == Gst.MessageType.ERROR:
            err, debug = msg.parse_error()
            logger.error("GStreamer error: %s (%s)", err.message, debug)
            return False

    except Exception as exc:
        logger.error("GStreamer generation failed: %s", exc)
        return False
    finally:
        if pipeline is not None:
            pipeline.set_state(Gst.State.NULL)

    # Validate output — catch malformed/truncated files before they
    # get loaded on a layer and cause visual glitches.
    out = Path(output_path)
    if not out.exists():
        logger.error("Generated file missing: %s", output_path)
        return False

    size = out.stat().st_size
    # A valid 30s 1080p MP4 should be at least a few MB.  Under 100KB
    # means the encoder produced garbage (common on low-memory VPS).
    min_size = max(50_000, duration * 1000)  # ~1KB/s absolute minimum
    if size < min_size:
        logger.error(
            "Generated video too small (%d bytes for %ds) — likely corrupt: %s",
            size,
            duration,
            output_path,
        )
        out.unlink(missing_ok=True)
        return False

    # Probe duration to confirm the container is valid
    actual_dur = get_clip_duration(str(out))
    if actual_dur <= 0:
        logger.error(
            "Generated video has no detectable duration — corrupt: %s",
            output_path,
        )
        out.unlink(missing_ok=True)
        return False

    logger.info("Generated: %s (%d bytes, %.1fs)", output_path, size, actual_dur)
    return True


def ensure_failover_video(ctx: ChannelContext) -> None:
    """Generate failover video if it doesn't exist.

    Creates SMPTE bars with the channel name using GStreamer.
    No plugin dependencies — pure core functionality.
    """
    failover_path = Path(ctx.generated_dir) / config.FAILOVER_FILENAME
    if failover_path.exists():
        logger.debug("Failover video exists: %s", failover_path)
        return

    logger.info("Generating failover video...")
    _generate_video_gstreamer(
        str(failover_path),
        title=ctx.failover_title or ctx.display_name,
        subtitle=ctx.failover_subtitle or "",
        duration=ctx.failover_duration,
        pattern=ctx.failover_pattern,
    )


def ensure_slate_video(ctx: ChannelContext) -> None:
    """Generate channel slate video if it doesn't exist.

    The slate is the default content on input_a — what viewers see
    when no playlist or program block is active.  Always created
    at boot.  Customizable via the channel API.
    """
    slate_path = Path(ctx.generated_dir) / config.SLATE_FILENAME
    if slate_path.exists():
        logger.debug("Slate video exists: %s", slate_path)
        return

    logger.info("Generating slate video...")
    _generate_video_gstreamer(
        str(slate_path),
        title=ctx.slate_title or ctx.display_name,
        subtitle=ctx.slate_subtitle,
        duration=ctx.slate_duration,
        pattern=ctx.slate_pattern,
    )


# ── Playlist helpers ──


def _resolve_local_files(
    filenames: list[str],
    ctx: ChannelContext,
) -> tuple[list[str], list[float]]:
    """Resolve block filenames to local paths and durations.

    Searches media_dir first, then generated_dir (cathode/) as
    fallback for bare system filenames like failover.mp4.
    Returns local filesystem paths used by the playout engine.
    """
    local_paths: list[str] = []
    durations: list[float] = []

    for filename in filenames:
        for local_dir in (ctx.media_dir, ctx.generated_dir):
            local_path = os.path.join(local_dir, filename)
            if os.path.isfile(local_path):
                local_paths.append(local_path)
                durations.append(get_clip_duration(local_path))
                break
        else:
            logger.warning("Block file not found in media directory: %s", filename)

    return local_paths, durations


async def _activate_failover(
    date_str: str, ctx: ChannelContext, layer: str = "input_a"
) -> None:
    """Activate failover as the safety net under live content.

    Hides the specified layer so the always-running failover layer
    shows through — no playlist push needed.
    """
    engine = ctx.engine or main.playout
    if engine is None:
        logger.warning("Failover: engine not active, cannot activate failover")
        return

    # Failover is always running on layer 0.
    # Just hide the target layer so it shows through.
    engine.hide(layer)
    logger.warning("Failover activated (hid %s)", layer)


async def _push_and_reset_playlist(
    files: list[str],
    durations: list[float],
    date_str: str,
    ctx: ChannelContext,
    layer: str = "input_a",
    loop: bool | None = None,
) -> None:
    """Load a new playlist and activate it.

    Builds a PlaylistEntry list, loads on the specified layer,
    shows the layer, and saves via PlaylistStore.  Hot-swap — no gap.

    Args:
        layer: Target compositor layer (default "input_a").
        loop: Whether to loop. None means use ctx.playlist_loop.
    """
    engine = ctx.engine or main.playout
    if engine is None:
        logger.warning("Cannot push playlist: engine not active")
        return

    should_loop = loop if loop is not None else ctx.playlist_loop

    from playout.input_layer import PlaylistEntry

    entries = [PlaylistEntry(source=f, duration=d) for f, d in zip(files, durations)]
    ch = engine.channel(layer)
    ch.load_playlist(entries, loop=should_loop)
    engine.show(layer)

    # Persist via PlaylistStore (only for input_a to maintain legacy behavior)
    if layer == "input_a":
        import playlist_store

        playlist_store.save(
            date.fromisoformat(date_str),
            [{"source": f, "duration": d} for f, d in zip(files, durations)],
            loop=should_loop,
            channel_id=ctx.id,
        )
    logger.info(
        "Loaded playlist on %s for %s: %d clips (loop=%s)",
        layer,
        date_str,
        len(files),
        should_loop,
    )


async def _restore_default_playlist(date_str: str, ctx: ChannelContext) -> None:
    """Restore a playlist from ALL available media for a date.

    Recursively scans the media library (includes subdirs like
    cathode/, plugin dirs, etc.).  Shuffles so fallback isn't
    just files in order.  Called when a block ends or when no
    program block is active.
    """
    all_files: list[str] = []
    all_durations: list[float] = []

    # Single recursive scan of media_dir (includes subdirs like cathode/, plugin dirs)
    if Path(ctx.media_dir).is_dir():
        all_files, all_durations = scan_media(ctx.media_dir)

    if not all_files:
        logger.warning("No media files found to restore default playlist")
        return

    combined = list(zip(all_files, all_durations))
    random.shuffle(combined)
    shuffled_files = [f for f, _ in combined]
    shuffled_durations = [d for _, d in combined]

    await _push_and_reset_playlist(shuffled_files, shuffled_durations, date_str, ctx)


# ── Engine source reload ──


async def _reload_sources_after_restart(ctx: ChannelContext) -> None:
    """Reload failover video and playlist after an engine restart.

    After stop()/start(), the engine comes up with black test patterns
    on all channels.  This reloads the failover video on layer 0 and
    restores input_a from persisted playout state (same priority as boot).
    """
    engine = ctx.engine or main.playout
    if engine is None:
        return

    # Reload failover video
    failover_path = os.path.join(ctx.generated_dir, config.FAILOVER_FILENAME)
    if os.path.isfile(failover_path):
        engine.failover.load_file_loop(failover_path)
        engine.show("failover")
        logger.debug("Watchdog: reloaded failover video")

    # Restore input_a from persisted state (named playlist or slate)
    import playout_state
    import named_playlist_store

    layer_state = playout_state.get_layer_state(ctx.id, "input_a")
    restored = False

    if layer_state and layer_state.get("playlist_name"):
        pl_name = layer_state["playlist_name"]
        pl_data = named_playlist_store.get(pl_name, channel_id=ctx.id)
        if pl_data and pl_data.get("entries"):
            try:
                from playout.input_layer import PlaylistEntry

                entries = [
                    PlaylistEntry(source=e["source"], duration=e.get("duration", 0))
                    for e in pl_data["entries"]
                ]
                loop = layer_state.get("loop", True)
                engine.input_a.load_playlist(entries, loop=loop, name=pl_name)
                engine.show("input_a")
                logger.info(
                    "Watchdog: restored playlist '%s' (%d clips)",
                    pl_name,
                    len(entries),
                )
                restored = True
            except ImportError:
                logger.warning("Watchdog: GStreamer not available for playlist reload")

    if not restored:
        # Fall back to slate
        slate_path = os.path.join(ctx.generated_dir, config.SLATE_FILENAME)
        if os.path.isfile(slate_path):
            engine.input_a.load_file_loop(slate_path)
            engine.show("input_a")
            logger.debug("Watchdog: loaded channel slate")


# ── Background loops ──


async def watchdog_loop(ctx: ChannelContext) -> None:
    """Periodically check that the stream pipeline is healthy.

    Polls the playout engine health for pipeline state and logs
    ingest transitions.
    """
    await asyncio.sleep(config.WATCHDOG_INITIAL_DELAY)

    while True:
        try:
            engine = ctx.engine or main.playout
            if engine is None:
                await asyncio.sleep(config.WATCHDOG_INTERVAL)
                continue

            health = engine.health
            if not health.get("running"):
                logger.error("Watchdog: engine not running, attempting restart")
                try:
                    await engine.restart()
                    await _reload_sources_after_restart(ctx)
                    logger.info("Watchdog: engine restarted with sources")
                except Exception as restart_exc:
                    logger.error("Watchdog: engine restart failed: %s", restart_exc)
            else:
                active = health.get("active_channel", "failover")
                ch_info = health.get("channels", {}).get(active, {})
                now_playing = ch_info.get("now_playing")
                source = "unknown"
                played = 0.0
                if now_playing:
                    source = now_playing.get("source", "unknown")
                    played = now_playing.get("played", 0.0)

                logger.debug(
                    "Watchdog: %s playing %s at %.1fs (uptime %.0fs)",
                    active,
                    source,
                    played,
                    health.get("uptime", 0),
                )

        except Exception as exc:
            logger.exception("Watchdog: unexpected error: %s", exc)

        await asyncio.sleep(config.WATCHDOG_INTERVAL)


async def schedule_loop(ctx: ChannelContext) -> None:
    """Pre-generate tomorrow's playlist if it doesn't exist.

    Runs every hour. If tomorrow's playlist is missing, creates one from
    the current media library. An AI agent can override this by pushing
    its own playlist for the date before this task runs.

    In loop mode (playlist_loop=true), this task is a no-op — the playout
    backend loops the current playlist forever.
    """
    await asyncio.sleep(config.SCHEDULE_INITIAL_DELAY)

    while True:
        if ctx.playlist_loop:
            await asyncio.sleep(config.SCHEDULE_INTERVAL)
            continue

        try:
            tomorrow_date = date.today() + timedelta(days=1)
            tomorrow = tomorrow_date.isoformat()

            import playlist_store

            if not playlist_store.exists(tomorrow_date, channel_id=ctx.id):
                local_files, durations = scan_media(ctx.media_dir)
                if local_files:
                    playlist_store.save(
                        tomorrow_date,
                        [
                            {"source": f, "duration": d}
                            for f, d in zip(local_files, durations)
                        ],
                        loop=ctx.playlist_loop,
                        channel_id=ctx.id,
                    )
                    logger.info(
                        "Scheduler: pre-generated playlist for %s",
                        tomorrow,
                    )
                else:
                    logger.warning("Scheduler: no media files for %s", tomorrow)
            else:
                logger.debug("Scheduler: playlist for %s already exists", tomorrow)
        except Exception as exc:
            logger.error("Scheduler: failed to ensure playlist: %s", exc)

        await asyncio.sleep(config.SCHEDULE_INTERVAL)


async def hls_watchdog_loop(ctx: ChannelContext) -> None:
    """Monitor HLS output by tracking manifest sequence numbers.

    Reads the HLS m3u8 file directly from disk (written by hlssink2)
    and parses EXT-X-MEDIA-SEQUENCE.  If it stops incrementing, the
    output pipeline is dead (encoder stall, output error, etc).

    Escalation:
      1. Stale 10s -> no-op (wait for output auto-restart)
      2. Stale 25s -> full engine restart
      3. Recovery -> log and reset counters
    """
    await asyncio.sleep(config.HLS_WATCHDOG_INITIAL_DELAY)

    last_sequence: int | None = None
    stale_since: datetime | None = None
    restart_attempted = False

    while True:
        try:
            sequence = _read_hls_sequence(ctx)

            if sequence is not None:
                if last_sequence is not None and sequence <= last_sequence:
                    # Sequence didn't advance — stream may be dead
                    if stale_since is None:
                        stale_since = datetime.now()

                    stale_secs = (datetime.now() - stale_since).total_seconds()

                    if (
                        stale_secs >= config.HLS_STALE_RESTART_THRESHOLD
                        and not restart_attempted
                    ):
                        logger.warning(
                            "HLS stale for %.0fs, restarting engine",
                            stale_secs,
                        )
                        try:
                            engine = ctx.engine or main.playout
                            if engine is not None:
                                await engine.restart()
                                await _reload_sources_after_restart(ctx)
                        except Exception as exc:
                            logger.error("HLS watchdog: restart failed: %s", exc)
                        restart_attempted = True

                else:
                    # Sequence advanced — stream is healthy
                    if stale_since is not None:
                        recovered_after = (datetime.now() - stale_since).total_seconds()
                        logger.info(
                            "HLS recovered after %.0fs (seq %d)",
                            recovered_after,
                            sequence,
                        )
                    stale_since = None
                    restart_attempted = False
                    last_sequence = sequence

            else:
                # Couldn't read manifest — HLS output may not be running
                if stale_since is None:
                    stale_since = datetime.now()
                    logger.warning("HLS watchdog: manifest unavailable")

        except Exception as exc:
            logger.error("HLS watchdog error: %s", exc)

        await asyncio.sleep(config.HLS_WATCHDOG_INTERVAL)


def _read_hls_sequence(ctx: ChannelContext) -> int | None:
    """Read and parse EXT-X-MEDIA-SEQUENCE from the HLS m3u8 on disk.

    Returns the sequence number, or None if the file doesn't exist.
    """
    hls_dir = ctx.hls_dir or os.path.join(
        config.HLS_OUTPUT_DIR, ctx.channel_id or ctx.id
    )
    m3u8_path = Path(hls_dir) / "stream.m3u8"

    try:
        if not m3u8_path.exists():
            return None
        text = m3u8_path.read_text()
        match = re.search(r"#EXT-X-MEDIA-SEQUENCE:(\d+)", text)
        return int(match.group(1)) if match else None
    except OSError:
        return None


# ── Program scheduler — per-block-type handlers ──


async def _handle_playlist_block(
    current: dict,
    block_key: str,
    now: datetime,
    ctx: ChannelContext,
) -> None:
    """Handle a playlist block with specific files or a named playlist.

    Supports multi-layer targeting: the block's 'layer' field determines
    which compositor layer to load the playlist on.  Default is "input_a"
    for backward compatibility.
    """
    layer = current.get("layer") or "input_a"
    block_loop = current.get("loop", True)
    layer_key = f"{layer}:{block_key}"

    # Check per-layer tracking (new) then fall back to legacy tracking
    already_active = ctx.active_layer_blocks.get(layer) == layer_key
    if not already_active and layer == "input_a":
        already_active = ctx.active_playlist_block_key == block_key

    if not already_active:
        try:
            # Resolve files: prefer playlist_name, then files
            paths: list[str] = []
            durs: list[float] = []

            playlist_name = current.get("playlist_name")
            if playlist_name:
                import named_playlist_store

                pl_data = named_playlist_store.get(playlist_name)
                if pl_data:
                    entries = pl_data.get("entries", [])
                    paths = [e["source"] for e in entries]
                    durs = [e.get("duration", 0) for e in entries]
                else:
                    logger.warning(
                        "Program: named playlist '%s' not found for block [%s -> %s]",
                        playlist_name,
                        current["start"],
                        current["end"],
                    )
            elif current.get("files"):
                paths, durs = _resolve_local_files(current["files"], ctx)

            if paths:
                await _push_and_reset_playlist(
                    paths,
                    durs,
                    now.date().isoformat(),
                    ctx,
                    layer=layer,
                    loop=block_loop,
                )
                ctx.active_layer_blocks[layer] = layer_key
                # Legacy tracking for backward compat
                if layer == "input_a":
                    ctx.active_playlist_block_key = block_key
                logger.info(
                    "Program: pushed playlist on %s for block [%s -> %s] (%d clips, loop=%s)",
                    layer,
                    current["start"],
                    current["end"],
                    len(paths),
                    block_loop,
                )
            else:
                logger.warning(
                    "Program: no valid files for playlist block [%s -> %s]",
                    current["start"],
                    current["end"],
                )
        except Exception as exc:
            logger.error("Program: failed to push block playlist: %s", exc)


async def _handle_redirect_block(
    current: dict,
    block_key: str,
    now: datetime,
    ctx: ChannelContext,
) -> None:
    """Handle a redirect block: load an HLS stream on a layer.

    Redirect blocks point to another channel's HLS URL or an external
    HLS feed.  The engine loads it using load_hls() on the target layer.
    """
    layer = current.get("layer") or "input_a"
    layer_key = f"{layer}:{block_key}"

    if ctx.active_layer_blocks.get(layer) == layer_key:
        return  # already active

    url = current.get("url")
    if not url:
        logger.warning(
            "Program: redirect block [%s -> %s] has no URL, skipping",
            current["start"],
            current["end"],
        )
        return

    try:
        engine = ctx.engine or main.playout
        if engine and engine.is_running:
            ch = engine.channel(layer)
            ch.load_hls(url)
            engine.show(layer)
            ctx.active_layer_blocks[layer] = layer_key
            logger.info(
                "Program: redirect on %s [%s -> %s] → %s",
                layer,
                current["start"],
                current["end"],
                url,
            )
    except Exception as exc:
        logger.error("Program: redirect block failed: %s", exc)


async def _handle_file_block(
    current: dict,
    block_key: str,
    now: datetime,
    ctx: ChannelContext,
) -> None:
    """Handle a file block: load a single file on a layer."""
    layer = current.get("layer") or "input_a"
    layer_key = f"{layer}:{block_key}"

    if ctx.active_layer_blocks.get(layer) == layer_key:
        return

    filepath = current.get("file")
    if not filepath:
        logger.warning(
            "Program: file block [%s -> %s] has no file, skipping",
            current["start"],
            current["end"],
        )
        return

    try:
        engine = ctx.engine or main.playout
        if engine and engine.is_running:
            full_path = os.path.join(config.MEDIA_DIR, filepath)
            ch = engine.channel(layer)
            ch.load_file_loop(full_path)
            engine.show(layer)
            ctx.active_layer_blocks[layer] = layer_key
            logger.info(
                "Program: file on %s [%s -> %s] → %s",
                layer,
                current["start"],
                current["end"],
                filepath,
            )
    except Exception as exc:
        logger.error("Program: file block failed: %s", exc)


async def _handle_image_block(
    current: dict,
    block_key: str,
    now: datetime,
    ctx: ChannelContext,
) -> None:
    """Handle an image block: show a static image on a layer."""
    layer = current.get("layer") or "input_a"
    layer_key = f"{layer}:{block_key}"

    if ctx.active_layer_blocks.get(layer) == layer_key:
        return

    filepath = current.get("file")
    if not filepath:
        logger.warning(
            "Program: image block [%s -> %s] has no file, skipping",
            current["start"],
            current["end"],
        )
        return

    try:
        engine = ctx.engine or main.playout
        if engine and engine.is_running:
            full_path = os.path.join(config.MEDIA_DIR, filepath)
            ch = engine.channel(layer)
            ch.load_image(full_path)
            engine.show(layer)
            ctx.active_layer_blocks[layer] = layer_key
            logger.info(
                "Program: image on %s [%s -> %s] → %s",
                layer,
                current["start"],
                current["end"],
                filepath,
            )
    except Exception as exc:
        logger.error("Program: image block failed: %s", exc)


async def _handle_plugin_block(
    current: dict,
    block_key: str,
    now: datetime,
    ctx: ChannelContext,
) -> None:
    """Handle a plugin-registered block type.

    Delegates to the plugin's block type handler.  If the plugin is
    not loaded, the block is skipped with a warning.
    """
    block_type = current["type"]
    layer = current.get("layer") or "input_a"
    layer_key = f"{layer}:{block_key}"

    if ctx.active_layer_blocks.get(layer) == layer_key:
        return

    import plugins

    type_info = plugins.block_types().get(block_type)
    if type_info is None:
        logger.warning(
            "Program: block type '%s' not available (plugin not loaded), "
            "skipping block [%s -> %s]",
            block_type,
            current["start"],
            current["end"],
        )
        return

    handler = type_info.get("handler")
    if handler is None:
        logger.warning(
            "Program: block type '%s' has no handler, skipping",
            block_type,
        )
        return

    try:
        await handler.dispatch(current, block_key, now, ctx)
        ctx.active_layer_blocks[layer] = layer_key
        logger.info(
            "Program: plugin block '%s' on %s [%s -> %s]",
            block_type,
            layer,
            current["start"],
            current["end"],
        )
    except Exception as exc:
        logger.error(
            "Program: plugin block '%s' failed: %s",
            block_type,
            exc,
        )


async def _handle_no_block(
    now: datetime,
    ctx: ChannelContext,
) -> None:
    """Handle gap between blocks: restore default playlist on input_a."""
    # Restore default playlist if we were in any non-default state,
    # OR if a hot-reload just cleared tracking (input_a was active
    # before the clear but the new program has no block for it).
    needs_restore = (
        ctx.active_block_key is not None
        or ctx.active_playlist_block_key is not None
        or "input_a" in ctx.hot_reload_layers
    )
    if needs_restore:
        try:
            await _restore_default_playlist(now.date().isoformat(), ctx)
            logger.info("Program: restored default playlist")
        except Exception as exc:
            logger.error(
                "Program: failed to restore default playlist: %s",
                exc,
            )
        ctx.active_block_key = None
        ctx.active_playlist_block_key = None
        ctx.hot_reload_layers.discard("input_a")


async def _deactivate_ended_layers(
    active_blocks: list[dict], ctx: ChannelContext
) -> None:
    """Hide layers whose blocks have ended.

    Compares currently active blocks against ctx.active_layer_blocks
    AND ctx.hot_reload_layers (layers that were active before a
    hot-reload clear).  Any layer that was active but is NOT in the
    current active block set gets hidden.
    """
    engine = ctx.engine or main.playout
    if engine is None:
        return

    # Build set of layers that have active blocks right now
    active_layers = set()
    for block in active_blocks:
        layer = block.get("layer") or "input_a"
        active_layers.add(layer)

    # Check tracked layers (normal path)
    ended = []
    for layer in list(ctx.active_layer_blocks.keys()):
        if layer not in active_layers and layer != "input_a":
            try:
                engine.hide(layer)
                logger.info("Program: block ended on %s, hiding layer", layer)
            except Exception as exc:
                logger.debug("Program: hide layer %s failed: %s", layer, exc)
            ended.append(layer)

    for layer in ended:
        del ctx.active_layer_blocks[layer]

    # Hot-reload path: hide layers that were active before the clear
    # but are no longer in the new program's active blocks.
    if ctx.hot_reload_layers:
        for layer in ctx.hot_reload_layers:
            if layer not in active_layers and layer != "input_a":
                try:
                    engine.hide(layer)
                    logger.info("Program: hot-reload hiding stale layer %s", layer)
                except Exception as exc:
                    logger.debug("Program: hide layer %s failed: %s", layer, exc)
        ctx.hot_reload_layers.clear()


async def program_scheduler_loop(ctx: ChannelContext) -> None:
    """Watch the program schedule and manage blocks at boundaries.

    Checks every 5 seconds. Supports multi-layer scheduling: multiple
    blocks can be active simultaneously on different layers.

    Core block types:
    - playlist: pushes files or named playlist to target layer
    - file: loads a single file on a layer
    - image: shows a static image on a layer
    - redirect: loads an HLS stream on a layer
    - Gaps (no block): default playlist on input_a

    Plugin-registered block types are dispatched to their handler.
    """
    await asyncio.sleep(config.PROGRAM_INITIAL_DELAY)

    while True:
        try:
            now = datetime.now()
            prog = program.load_program(now.date(), channel_id=ctx.id)

            if prog:
                active_blocks = program.find_active_blocks(prog, now.time())

                # ── Deactivate layers whose blocks have ended ──
                await _deactivate_ended_layers(active_blocks, ctx)

                # ── Handle each active block ──
                has_input_a_block = False
                for block in active_blocks:
                    bkey = f"{block['start']}-{block['end']}"
                    block_type = block["type"]
                    block_layer = block.get("layer") or "input_a"

                    if block_layer == "input_a":
                        has_input_a_block = True

                    if block_type == "playlist" and (
                        block.get("files") or block.get("playlist_name")
                    ):
                        await _handle_playlist_block(block, bkey, now, ctx)
                    elif block_type == "redirect":
                        await _handle_redirect_block(block, bkey, now, ctx)
                    elif block_type == "file":
                        await _handle_file_block(block, bkey, now, ctx)
                    elif block_type == "image":
                        await _handle_image_block(block, bkey, now, ctx)
                    else:
                        # Plugin-registered block type
                        await _handle_plugin_block(block, bkey, now, ctx)

                # If no block targets input_a, handle as gap
                if not has_input_a_block:
                    await _handle_no_block(now, ctx)

            else:
                # No program today — clean up playlist block state
                if ctx.active_playlist_block_key is not None:
                    try:
                        await _restore_default_playlist(now.date().isoformat(), ctx)
                    except Exception as exc:
                        logger.debug(
                            "Program: restore default playlist failed: %s", exc
                        )
                    ctx.active_playlist_block_key = None
                # Clean up multi-layer state
                if ctx.active_layer_blocks:
                    await _deactivate_ended_layers([], ctx)

        except Exception as exc:
            logger.error("Program scheduler error: %s", exc)

        await asyncio.sleep(config.PROGRAM_CHECK_INTERVAL)


# ── Peer exchange (global, not per-channel) ──


async def peer_exchange_loop() -> None:
    """Periodically fetch and validate peers from known nodes.

    Runs every 30 minutes (spec section 10.3).
    For each known peer hint, fetches GET /tltv/v1/peers.
    For each returned peer, validates before adding to our store.
    Also evicts stale entries.
    """
    await asyncio.sleep(config.PEER_EXCHANGE_INITIAL_DELAY)

    while True:
        try:
            if main.peer_store is None:
                await asyncio.sleep(config.PEER_EXCHANGE_INTERVAL)
                continue

            # Evict stale entries first
            main.peer_store.evict_stale()

            # Collect all unique hints from existing peers
            hints: set[str] = set()
            for peer in main.peer_store.all():
                for hint in peer.hints:
                    hints.add(hint)

            if not hints:
                logger.debug("Peer exchange: no known hints to query")
                await asyncio.sleep(config.PEER_EXCHANGE_INTERVAL)
                continue

            from protocol.peers import fetch_remote_peers, validate_peer, PeerEntry

            import httpx

            async with httpx.AsyncClient(timeout=5.0) as client:
                for hint in hints:
                    try:
                        remote_peers = await fetch_remote_peers(
                            hint,
                            require_tls=config.PEER_REQUIRE_TLS,
                            client=client,
                        )
                        for rp in remote_peers:
                            rp_id = rp.get("id")
                            rp_hints = rp.get("hints", [])
                            if not rp_id:
                                continue

                            # Skip our own channels
                            if any(
                                ctx.channel_id == rp_id for ctx in main.channels.all()
                            ):
                                continue

                            # Skip if already known and recently seen
                            existing = main.peer_store.get(rp_id)
                            if existing and existing.verified:
                                # Just refresh last_seen and merge hints
                                entry = PeerEntry(
                                    id=rp_id,
                                    name=rp.get("name", existing.name),
                                    hints=rp_hints or existing.hints,
                                    verified=True,
                                )
                                main.peer_store.add(entry)
                                continue

                            # Validate new peer using its hints
                            for rp_hint in rp_hints:
                                metadata = await validate_peer(
                                    rp_hint,
                                    rp_id,
                                    require_tls=config.PEER_REQUIRE_TLS,
                                    client=client,
                                )
                                if metadata:
                                    entry = PeerEntry(
                                        id=rp_id,
                                        name=metadata.get("name", ""),
                                        hints=rp_hints,
                                        verified=True,
                                    )
                                    main.peer_store.add(entry)
                                    logger.info(
                                        "Peer exchange: discovered %s (%s)",
                                        rp_id[:16],
                                        metadata.get("name", ""),
                                    )
                                    break  # One valid hint is enough

                    except Exception as exc:
                        logger.debug("Peer exchange: error querying %s: %s", hint, exc)

            logger.debug(
                "Peer exchange complete: %d peers in store", len(main.peer_store)
            )

        except Exception as exc:
            logger.error("Peer exchange loop error: %s", exc)

        await asyncio.sleep(config.PEER_EXCHANGE_INTERVAL)


# ── Relay background tasks (global, not per-channel) ──


async def relay_metadata_loop() -> None:
    """Refresh metadata and guide for all relayed channels.

    Metadata: every 60 seconds (spec section 5.8 — at least once per
    cache lifetime). Origins from superseded (lower-seq) metadata are
    discarded automatically by the seq ordering check in fetch_metadata.

    Guide: every 15 minutes (less time-sensitive).
    """
    await asyncio.sleep(config.RELAY_METADATA_INITIAL_DELAY)

    guide_counter = 0
    metadata_interval = config.ORIGIN_METADATA_REFRESH_INTERVAL

    while True:
        try:
            if main.relay_manager is None:
                await asyncio.sleep(metadata_interval)
                continue

            await main.relay_manager.refresh_all_metadata()

            # Refresh guides less frequently (every 15 min)
            guide_counter += 1
            if guide_counter >= (config.RELAY_GUIDE_INTERVAL // metadata_interval):
                await main.relay_manager.refresh_all_guides()
                guide_counter = 0

        except Exception as exc:
            logger.error("Relay metadata loop error: %s", exc)

        await asyncio.sleep(metadata_interval)


async def relay_hls_loop() -> None:
    """Fetch HLS manifests and segments for all relayed channels.

    Runs every ~2 seconds (segment duration, spec section 10.3).
    """
    await asyncio.sleep(config.RELAY_HLS_INITIAL_DELAY)

    while True:
        try:
            if main.relay_manager is None:
                await asyncio.sleep(config.RELAY_HLS_INTERVAL)
                continue

            await main.relay_manager.refresh_all_hls()

        except Exception as exc:
            logger.error("Relay HLS loop error: %s", exc)

        await asyncio.sleep(config.RELAY_HLS_INTERVAL)


# ── Mirror background task (per-channel) ──


async def mirror_loop(ctx: ChannelContext) -> None:
    """Mirror replication, health monitoring, and promotion/demotion.

    Implements PROTOCOL.md section 10.8 (Mirror Nodes):
    - In replicating mode: poll primary HLS every ~2 seconds
    - On primary failure: promote to self-generation (continue sequence)
    - In promoted mode: poll local nginx HLS, adjust sequence numbers,
      periodically check if primary has recovered
    - On primary recovery: demote back to replication after delay

    Runs every ~2 seconds (segment interval).
    """
    from protocol.mirror import MirrorState

    await asyncio.sleep(config.RELAY_HLS_INITIAL_DELAY)

    mirror_managers = getattr(main, "mirror_managers", None)
    if mirror_managers is None:
        return

    mirror = mirror_managers.get(ctx.channel_id)
    if mirror is None:
        return

    import time

    health_check_counter = 0

    while True:
        try:
            if mirror.state == MirrorState.REPLICATING:
                # Pull HLS from primary
                await mirror.poll_primary_hls()

                # Check if we should promote
                if mirror.should_promote():
                    logger.warning(
                        "Mirror: primary unreachable for %s after %d failures, "
                        "promoting to self-generation",
                        ctx.id,
                        mirror.consecutive_failures,
                    )
                    mirror.begin_promotion()

                    # Start playout for this channel
                    try:
                        if ctx.engine is not None:
                            # Load sources on existing engine
                            ensure_failover_video(ctx)
                            failover_path = os.path.join(
                                ctx.generated_dir, config.FAILOVER_FILENAME
                            )
                            if os.path.isfile(failover_path):
                                ctx.engine.failover.load_file_loop(failover_path)
                            local_files, durations = scan_media(ctx.media_dir)
                            if local_files:
                                from playout.input_layer import PlaylistEntry

                                entries = [
                                    PlaylistEntry(source=f, duration=d)
                                    for f, d in zip(local_files, durations)
                                ]
                                ctx.engine.input_a.load_playlist(entries, loop=True)
                                ctx.engine.show("input_a")
                        await asyncio.sleep(config.STARTUP_DELAY)
                        # Start per-channel tasks that were skipped
                        ctx.watchdog_task = asyncio.create_task(watchdog_loop(ctx))
                        ctx.hls_watchdog_task = asyncio.create_task(
                            hls_watchdog_loop(ctx)
                        )
                    except Exception as exc:
                        logger.error(
                            "Mirror: playout startup failed for %s: %s",
                            ctx.id,
                            exc,
                        )
                        # Revert to replicating — playout failed
                        mirror.complete_demotion()

            elif mirror.state in (MirrorState.PROMOTING, MirrorState.PROMOTED):
                # Poll local HLS and adjust sequence numbers.
                # Mirror uses HTTP internally to fetch the manifest,
                # so point it at cathode's own HLS endpoint.
                local_hls_url = f"http://localhost:{config.PORT}/hls/{ctx.channel_id or ctx.id}/stream.m3u8"
                await mirror.poll_local_hls(local_hls_url)

                # Periodically check if primary has recovered
                # (every ~10 seconds = 5 poll cycles)
                health_check_counter += 1
                if health_check_counter >= 5:
                    health_check_counter = 0
                    primary_healthy = await mirror.check_primary_health()
                    mono_now = time.monotonic()

                    if primary_healthy:
                        mirror.mark_primary_recovered(mono_now)

                        # Check if demotion delay has elapsed
                        if mirror.should_demote(mono_now):
                            logger.info(
                                "Mirror: primary recovered for %s, demoting",
                                ctx.id,
                            )
                            mirror.begin_demotion()

                            # Stop engine and per-channel tasks
                            for task in (ctx.watchdog_task, ctx.hls_watchdog_task):
                                if task:
                                    task.cancel()
                                    try:
                                        await task
                                    except asyncio.CancelledError:
                                        pass
                            ctx.watchdog_task = None
                            ctx.hls_watchdog_task = None

                            try:
                                if ctx.engine is not None:
                                    ctx.engine.input_a.disconnect()
                                    ctx.engine.hide("input_a")
                            except Exception as exc:
                                logger.debug(
                                    "Mirror: playout stop during demotion: %s", exc
                                )

                            mirror.complete_demotion()
                    else:
                        mirror.mark_primary_unreachable()

            elif mirror.state == MirrorState.DEMOTING:
                # Transition state — should have been completed above
                mirror.complete_demotion()

        except Exception as exc:
            logger.error("Mirror loop error for %s: %s", ctx.id, exc)

        await asyncio.sleep(config.MIRROR_POLL_INTERVAL)
