"""Cathode — TLTV reference server.

App creation, shared service instances, channel registry, lifespan,
middleware, router includes, and uvicorn entrypoint.  All endpoint
handlers live in routes/, background tasks in scheduler.py.

Plugin routes and services are loaded dynamically by plugins.py.
Core routes (status, playlist, program, guide, federation)
are included directly.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

sys.modules.setdefault("main", sys.modules[__name__])

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

import config
import plugins
from channel import ChannelContext, ChannelRegistry
from protocol.identity import ensure_channel_keypair
import scheduler

logging.basicConfig(
    level=config.LOG_LEVEL,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Channel registry ──
# Holds all channel contexts. Populated during lifespan startup.
channels = ChannelRegistry()

# ── Playout engine (GStreamer) ──
# Set to PlayoutEngine instance during lifespan.  Route handlers and
# scheduler check `main.playout is not None` as a safety guard.
playout = None

# ── Federation state ──
# Peer store, relay manager, and token store — initialized during lifespan.
# Set to None here so route modules can import and check.
peer_store = None
relay_manager = None
token_store = None

# ── Mirror state ──
# Dict of federation_channel_id -> MirrorManager.
# Populated during lifespan for channels with mirror_mode=True.
mirror_managers: dict = {}


# ── Channel loading ──


def _load_channel_configs() -> list[dict]:
    """Load channel configuration from YAML files.

    Reads *.yaml from CHANNEL_CONFIG_DIR (skips *.example.yaml).
    On first start, if no .yaml files exist but .example.yaml files do,
    copies them as a starting point.

    Returns a list of parsed channel config dicts.
    """
    config_dir = Path(config.CHANNEL_CONFIG_DIR)
    if not config_dir.is_dir():
        logger.debug("Channel config dir not found: %s", config_dir)
        return []

    try:
        import yaml
    except ImportError:
        logger.warning("PyYAML not installed, skipping channel config loading")
        return []

    import shutil

    # Find real configs (not examples)
    yaml_files = [
        f
        for f in sorted(config_dir.glob("*.yaml"))
        if not f.name.endswith(".example.yaml")
    ]

    # First start: copy examples if no real configs exist
    if not yaml_files:
        for example in sorted(config_dir.glob("*.example.yaml")):
            target = example.with_name(example.name.replace(".example.yaml", ".yaml"))
            shutil.copy2(example, target)
            logger.info("First start: created %s from %s", target.name, example.name)
        yaml_files = [
            f
            for f in sorted(config_dir.glob("*.yaml"))
            if not f.name.endswith(".example.yaml")
        ]

    configs = []
    for cfg_file in yaml_files:
        try:
            with open(cfg_file) as f:
                cfg = yaml.safe_load(f)
            if cfg and isinstance(cfg, dict) and "id" in cfg:
                configs.append(cfg)
                logger.info("Loaded channel config: %s", cfg_file.name)
        except Exception as exc:
            logger.error("Failed to load channel config %s: %s", cfg_file, exc)

    return configs


def _create_channel_context(cfg: dict) -> ChannelContext:
    """Create a ChannelContext from a parsed YAML config dict."""
    output_cfg = cfg.get("output", {})
    media_cfg = cfg.get("media", {})
    identity_cfg = cfg.get("identity", {})
    failover_cfg = cfg.get("failover", {})
    slate_cfg = cfg.get("slate", {})

    ctx = ChannelContext(
        id=cfg["id"],
        display_name=cfg.get("display_name", cfg["id"]),
        # Output config
        default_output_type=output_cfg.get("type", "hls"),
        hls_dir=output_cfg.get(
            "hls_dir", ""
        ),  # Resolved with channel_id at engine start
        # Paths
        media_dir=media_cfg.get("base_dir", config.MEDIA_DIR),
        generated_dir=os.path.join(
            media_cfg.get("base_dir", config.MEDIA_DIR), "cathode"
        ),
        program_dir=media_cfg.get("program_dir", config.PROGRAM_DIR),
        # Identity (key path — channel_id populated later by ensure_channel_keypair)
        private_key_path=identity_cfg.get("private_key_path"),
        # Federation metadata
        description=identity_cfg.get("description"),
        language=identity_cfg.get("language"),
        tags=identity_cfg.get("tags", []),
        access=identity_cfg.get("access", "public"),
        origins=identity_cfg.get("origins", []),
        icon=identity_cfg.get("icon"),
        on_demand=cfg.get("on_demand", False),
        # Timezone — top-level in channel YAML, IANA name
        timezone=cfg.get("timezone"),
        # Playlist loop — channel config overrides env var
        playlist_loop=cfg.get("playlist_loop", config.PLAYLIST_LOOP),
        # Day start — when schedule mode's broadcast day begins
        day_start=cfg.get("day_start", "00:00:00"),
        # Channel status — "active" or "retired"
        status=identity_cfg.get("status", "active"),
        # Mirror mode (section 10.8) — replicates HLS from primary
        mirror_mode=cfg.get("mirror_mode", False),
        mirror_primary=cfg.get("mirror_primary"),
        # Failover/slate customization
        failover_title=failover_cfg.get("title"),
        failover_subtitle=failover_cfg.get("subtitle"),
        failover_duration=failover_cfg.get("duration", 60),
        failover_pattern=failover_cfg.get("pattern", "smpte"),
        slate_title=slate_cfg.get("title"),
        slate_subtitle=slate_cfg.get("subtitle", "No content scheduled"),
        slate_duration=slate_cfg.get("duration", 300),
        slate_pattern=slate_cfg.get("pattern", "black"),
    )
    return ctx


def _create_default_channel() -> ChannelContext:
    """Create a default channel-one context from env vars / config.py."""
    return ChannelContext(
        id=config.DEFAULT_CHANNEL_ID,
        display_name="TLTV Channel One",
        default_output_type="hls",
        hls_dir="",  # Resolved with channel_id at engine start
        media_dir=config.MEDIA_DIR,
        generated_dir=os.path.join(config.MEDIA_DIR, "cathode"),
        program_dir=config.PROGRAM_DIR,
    )


# ── App lifecycle ──


@asynccontextmanager
async def lifespan(app: FastAPI):
    global peer_store, relay_manager, token_store
    global playout

    # Install log buffer handler (captures all subsequent log output)
    from routes.logs import install as install_log_buffer

    install_log_buffer()

    # Load channel configs from YAML, fall back to default
    configs = _load_channel_configs()
    if configs:
        for cfg in configs:
            ctx = _create_channel_context(cfg)
            channels.register(ctx)
    else:
        ctx = _create_default_channel()
        channels.register(ctx)

    # Generate Ed25519 keypairs for channels that don't have one
    for ctx in channels.all():
        try:
            federation_id, key_path = ensure_channel_keypair(
                ctx.id, key_dir=config.KEY_DIR
            )
            ctx.channel_id = federation_id
            ctx.private_key_path = key_path
            logger.info("Channel '%s' federation ID: %s", ctx.id, federation_id)
        except Exception as exc:
            logger.warning(
                "Could not ensure keypair for %s: %s (federation will not work)",
                ctx.id,
                exc,
            )

    # Load persisted migration documents (section 5.14)
    from routes.migration import load_migration

    for ctx in channels.all():
        migration = load_migration(ctx.id)
        if migration is not None:
            ctx.migration = migration
            logger.info(
                "Channel '%s' has migrated to %s",
                ctx.id,
                migration.get("to"),
            )

    # Start the GStreamer playout engine.
    # Mirror channels don't generate their own stream initially —
    # they replicate from the primary (section 10.8).
    try:
        from playout import PlayoutEngine, OutputConfig, OutputType

        playout = PlayoutEngine()
        # Use the first non-mirror channel for the primary engine.
        primary_ctx = next((c for c in channels.all() if not c.mirror_mode), None)
        if primary_ctx:
            # Build default output config from channel context.
            # Use channel_id (federation ID) for the HLS path so
            # phosphor can find the stream by channel ID.
            hls_dir = primary_ctx.hls_dir or os.path.join(
                config.HLS_OUTPUT_DIR, primary_ctx.channel_id or primary_ctx.id
            )
            primary_ctx.hls_dir = hls_dir
            default_output = OutputConfig(
                type=OutputType.HLS,
                name="primary",
                hls_dir=hls_dir,
                segment_duration=config.HLS_SEGMENT_DURATION,
                playlist_length=config.HLS_PLAYLIST_LENGTH,
            )
            await playout.start(default_output=default_output)
            primary_ctx.engine = playout
            logger.info("GStreamer playout engine started → HLS at %s", hls_dir)
        else:
            logger.warning("No non-mirror channels — engine started with no sources")
    except ImportError:
        logger.error(
            "GStreamer/PyGObject not available. "
            "Use Dockerfile.playout for the GStreamer image."
        )
        playout = None
    except Exception as exc:
        logger.error("Failed to start GStreamer engine: %s", exc)
        playout = None

    # Post-startup: generate system videos and restore sources.
    # These run AFTER the engine is confirmed started — failures here
    # must NOT null out playout (the engine is already running).
    if playout is not None and primary_ctx is not None:
        try:
            scheduler.ensure_failover_video(primary_ctx)
            scheduler.ensure_slate_video(primary_ctx)
        except Exception as exc:
            logger.warning("Failed to generate system videos: %s", exc)

        # Load failover on safety layer
        failover_path = os.path.join(
            primary_ctx.generated_dir, config.FAILOVER_FILENAME
        )
        try:
            if os.path.isfile(failover_path):
                playout.failover.load_file_loop(failover_path)
                playout.show("failover")
                logger.info("Failover video loaded: %s", failover_path)
        except Exception as exc:
            logger.warning("Failed to load failover video: %s", exc)

        # Restore input_a from persisted playout state.
        #
        # Boot priority:
        # 1. Persisted state (named playlist from last session)
        # 2. Channel slate (always generated — default content)
        try:
            import playout_state
            import named_playlist_store
            from playout.input_layer import PlaylistEntry

            layer_state = playout_state.get_layer_state(primary_ctx.id, "input_a")

            if layer_state and layer_state.get("playlist_name"):
                pl_name = layer_state["playlist_name"]
                pl_data = named_playlist_store.get(pl_name, channel_id=primary_ctx.id)
                if pl_data and pl_data.get("entries"):
                    entries = [
                        PlaylistEntry(
                            source=e["source"],
                            duration=e.get("duration", 0),
                        )
                        for e in pl_data["entries"]
                    ]
                    loop = layer_state.get("loop", True)
                    playout.input_a.load_playlist(entries, loop=loop, name=pl_name)
                    playout.show("input_a")
                    logger.info(
                        "Restored playlist '%s': %d clips",
                        pl_name,
                        len(entries),
                    )
                else:
                    logger.warning(
                        "Persisted playlist '%s' not found — falling back to slate",
                        pl_name,
                    )
                    layer_state = None  # fall through to slate

            if not layer_state or not layer_state.get("playlist_name"):
                slate_path = os.path.join(
                    primary_ctx.generated_dir, config.SLATE_FILENAME
                )
                if os.path.isfile(slate_path):
                    playout.input_a.load_file_loop(slate_path)
                    playout.show("input_a")
                    logger.info("Channel slate loaded: %s", slate_path)
        except Exception as exc:
            logger.warning("Failed to restore sources after engine start: %s", exc)

    logger.debug("Waiting for playout to start streaming...")
    await asyncio.sleep(config.STARTUP_DELAY)

    # Initialize mirror managers for mirror-mode channels
    from protocol.mirror import MirrorManager

    for ctx in channels.all():
        if ctx.mirror_mode and ctx.mirror_primary and ctx.channel_id:
            mm = MirrorManager(
                channel_id=ctx.channel_id,
                primary_hint=ctx.mirror_primary,
                require_tls=config.PEER_REQUIRE_TLS,
            )
            mirror_managers[ctx.channel_id] = mm
            logger.info(
                "Mirror manager initialized for '%s' (primary: %s)",
                ctx.id,
                ctx.mirror_primary,
            )

    # Start background tasks for each channel
    for ctx in channels.all():
        if ctx.mirror_mode:
            # Mirror channels get mirror_loop instead of the normal tasks.
            # watchdog, schedule, program, hls_watchdog are started on
            # promotion by the mirror_loop itself.
            ctx.watchdog_task = asyncio.create_task(scheduler.mirror_loop(ctx))
            continue
        ctx.watchdog_task = asyncio.create_task(scheduler.watchdog_loop(ctx))
        ctx.scheduler_task = asyncio.create_task(scheduler.schedule_loop(ctx))
        ctx.program_task = asyncio.create_task(scheduler.program_scheduler_loop(ctx))
        ctx.hls_watchdog_task = asyncio.create_task(scheduler.hls_watchdog_loop(ctx))

    # Initialize federation subsystems (global, not per-channel)
    from protocol.peers import PeerStore

    peer_store = PeerStore(path=config.PEER_FILE)
    logger.info("Peer store initialized (%d peers loaded)", len(peer_store))

    from protocol.relay import RelayManager

    relay_manager = RelayManager(path=config.RELAY_FILE)
    logger.info("Relay manager initialized (%d relays)", len(relay_manager))

    from protocol.tokens import TokenStore

    token_store = TokenStore()
    logger.info("Token store initialized")

    # Start global background tasks (peer exchange + relay)
    _global_tasks = []
    _global_tasks.append(asyncio.create_task(scheduler.peer_exchange_loop()))
    _global_tasks.append(asyncio.create_task(scheduler.relay_metadata_loop()))
    _global_tasks.append(asyncio.create_task(scheduler.relay_hls_loop()))

    # ── Load plugins ──
    # Plugins register their routes and services.  This happens AFTER
    # federation init so plugins can depend on federation state.
    plugins.load_plugins(app, config)
    plugins.start_tasks()

    # Tag any plugin-registered routes that don't have tags.
    # Plugins add routes directly via app.include_router() during
    # register(), so we retroactively tag untagged routes as "plugins".
    for route in app.routes:
        if hasattr(route, "tags") and not route.tags:
            route.tags = ["plugins"]

    logger.info(
        "Cathode started with %d channel(s): %s",
        len(channels),
        ", ".join(ctx.id for ctx in channels.all()),
    )

    yield

    # ── Shutdown ──

    # Shutdown plugins first (cancels tasks, runs hooks)
    await plugins.shutdown_plugins()

    # Cancel global background tasks
    for task in _global_tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # Cancel all per-channel background tasks
    for ctx in channels.all():
        for task in (
            ctx.watchdog_task,
            ctx.scheduler_task,
            ctx.program_task,
            ctx.hls_watchdog_task,
        ):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    # Clean up mirror manager HTTP clients
    for mm in mirror_managers.values():
        await mm.close()

    # Clean up relay manager HTTP clients
    if relay_manager:
        await relay_manager.close()

    # Stop playout engine
    if playout is not None:
        try:
            await playout.stop()
            logger.info("GStreamer playout engine stopped")
        except Exception as exc:
            logger.debug("Playout engine stop during shutdown: %s", exc)

    logger.info("Cathode shutdown complete")


# ── App + middleware + routers ──

# Disable Swagger UI and OpenAPI schema in production (when API_KEY is set).
# Exposing them maps the entire API surface for attackers.
_docs_url = "/docs" if not config.API_KEY else None
_openapi_url = "/openapi.json" if not config.API_KEY else None

app = FastAPI(
    title="Cathode",
    version=config.VERSION,
    lifespan=lifespan,
    docs_url=_docs_url,
    openapi_url=_openapi_url,
    openapi_tags=[
        {"name": "status", "description": "System status, now-playing, media listing"},
        {"name": "playlist", "description": "Playlist control, skip, back, schedule"},
        {"name": "program", "description": "Day-based program schedule management"},
        {
            "name": "playout",
            "description": "Playout engine control, settings, and text overlay",
        },
        {"name": "channel", "description": "Channel federation metadata"},
        {"name": "guide", "description": "EPG guide (JSON and XMLTV)"},
        {"name": "peers", "description": "Federation peer management"},
        {"name": "relay", "description": "HLS relay management"},
        {"name": "tokens", "description": "Private channel access tokens"},
        {"name": "migration", "description": "Channel key migration"},
        {
            "name": "protocol",
            "description": "TLTV federation protocol endpoints (public)",
        },
        {
            "name": "channels",
            "description": "Multi-channel management (list, create, update, delete)",
        },
        {
            "name": "outputs",
            "description": "Output pipeline management (HLS, RTMP, recording)",
        },
        {
            "name": "plugins",
            "description": "Plugin management and plugin-provided endpoints",
        },
        {"name": "logs", "description": "Log buffer and real-time log stream"},
        {"name": "backup", "description": "Backup and restore configuration and state"},
    ],
)

# ── Global exception handler ──
# Catch unhandled exceptions and return a generic error instead of
# leaking stack traces, file paths, or internal state to clients.
from fastapi import Request as _Request
from fastapi.responses import JSONResponse as _JSONResponse


@app.exception_handler(Exception)
async def _unhandled_exception_handler(_request: _Request, exc: Exception):
    logger.error("Unhandled exception: %s", exc, exc_info=True)
    return _JSONResponse(
        content={"error": "internal_error", "message": "Internal server error"},
        status_code=500,
        media_type="application/json; charset=utf-8",
    )


# Middleware — order matters (last added = outermost = runs first)
from middleware import (
    APIKeyMiddleware,
    BodySizeLimitMiddleware,
    CharsetMiddleware,
    RateLimitMiddleware,
    SecurityHeadersMiddleware,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*", "X-API-Key"],
)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(CharsetMiddleware)
app.add_middleware(BodySizeLimitMiddleware)
if config.API_KEY:
    # Only enable rate limiting and API key auth when a key is configured.
    # Without a key (dev/test), both are skipped.
    app.add_middleware(RateLimitMiddleware)
    app.add_middleware(APIKeyMiddleware, api_key=config.API_KEY)

# ── HLS serving ──
# HLS served through route handlers (not StaticFiles) to support
# private channel token auth per PROTOCOL.md section 5.7.
os.makedirs(config.HLS_OUTPUT_DIR, exist_ok=True)

# Core route modules — always included
from routes.status import router as status_router  # noqa: E402
from routes.playlist import router as playlist_router  # noqa: E402
from routes.program import router as program_router  # noqa: E402
from routes.guide import router as guide_router  # noqa: E402
from routes.peers import router as peers_mgmt_router  # noqa: E402
from routes.relay import router as relay_mgmt_router  # noqa: E402
from routes.tokens import router as tokens_mgmt_router  # noqa: E402
from routes.migration import router as migration_mgmt_router  # noqa: E402
from routes.playout import router as playout_router  # noqa: E402
from routes.playlists import router as playlists_router  # noqa: E402
from routes.media import router as media_router  # noqa: E402
from routes.channel import router as channel_router  # noqa: E402
from routes.channels import router as channels_router  # noqa: E402
from routes.plugins import router as plugins_mgmt_router  # noqa: E402
from routes.outputs import router as outputs_router  # noqa: E402
from routes.logs import router as logs_router  # noqa: E402
from routes.backup import router as backup_router  # noqa: E402
from routes.hls import router as hls_router  # noqa: E402
from protocol.routes import router as protocol_router  # noqa: E402

app.include_router(hls_router)
app.include_router(status_router)
app.include_router(playlist_router)
app.include_router(playlists_router)
app.include_router(program_router)
app.include_router(playout_router)
app.include_router(media_router)
app.include_router(channel_router)
app.include_router(guide_router)
app.include_router(peers_mgmt_router)
app.include_router(relay_mgmt_router)
app.include_router(tokens_mgmt_router)
app.include_router(migration_mgmt_router)
app.include_router(channels_router)
app.include_router(plugins_mgmt_router)
app.include_router(outputs_router)
app.include_router(logs_router)
app.include_router(backup_router)
app.include_router(protocol_router)

# Plugin routes are loaded by plugins.load_plugins() during lifespan startup.


if __name__ == "__main__":
    import uvicorn

    logger.info("Starting Cathode on %s:%d", config.HOST, config.PORT)
    uvicorn.run(app, host=config.HOST, port=config.PORT, log_level="info")
