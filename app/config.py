"""Cathode configuration — all env vars, constants, and paths.

Every module imports from here instead of reading os.getenv directly.
"""

from __future__ import annotations

import os

VERSION = "1.0.0"

# ── Media paths ──

MEDIA_DIR = os.getenv("MEDIA_DIR", "/media")

# ── HLS output ──

HLS_OUTPUT_DIR = os.getenv("HLS_OUTPUT_DIR", "/data/hls")
HLS_SEGMENT_DURATION = int(os.getenv("HLS_SEGMENT_DURATION", "4"))
HLS_PLAYLIST_LENGTH = int(os.getenv("HLS_PLAYLIST_LENGTH", "5"))

# ── Management API authentication ──

# API key for management endpoints (/api/*).  When set, all /api/* requests
# must include X-API-Key header or ?api_key= query param with this value.
# When unset (empty), management API is unprotected (development only).
API_KEY: str = os.getenv("API_KEY", "")

# ── CORS ──

# Comma-separated list of allowed origins.  Empty by default — only same-origin
# requests are allowed.  Set CORS_ORIGINS for cross-origin access:
#   CORS_ORIGINS="https://tv.example.com,https://admin.example.com"
# Use "*" only for development (allows all origins).
_cors_raw = os.getenv("CORS_ORIGINS", "")
CORS_ORIGINS: list[str] = [o.strip() for o in _cors_raw.split(",") if o.strip()]

# ── System-generated video filenames ──
# Stored in {MEDIA_DIR}/cathode/.  Auto-generated at boot via GStreamer.
# Customizable via /api/playout/failover and /api/playout/slate.

FAILOVER_FILENAME = "failover.mp4"  # Safety layer (layer 0)
SLATE_FILENAME = "slate.mp4"  # Default content (input_a)

# ── Background task timing (seconds) ──

STARTUP_DELAY = 5  # Wait after engine start before launching background tasks
WATCHDOG_INITIAL_DELAY = 30  # Wait before first watchdog health check
WATCHDOG_INTERVAL = 15  # Time between watchdog health checks
SCHEDULE_INITIAL_DELAY = 60  # Wait before first schedule pre-generation check
SCHEDULE_INTERVAL = 3600  # Time between schedule pre-generation checks
PROGRAM_INITIAL_DELAY = 15  # Wait before program scheduler starts checking blocks
PROGRAM_CHECK_INTERVAL = 5  # Time between program block boundary checks
HLS_WATCHDOG_INITIAL_DELAY = 60  # Wait for pipeline to stabilize before monitoring
HLS_WATCHDOG_INTERVAL = 5  # Time between HLS sequence checks
HLS_STALE_RESTART_THRESHOLD = 12  # Seconds stale before full restart

# ── HLS monitoring ──
# HLS watchdog reads the m3u8 file directly from disk (no HTTP needed).
# HLS_OUTPUT_DIR defined above is the default location.

# ── Channel config ──

CHANNEL_CONFIG_DIR = os.getenv("CHANNEL_CONFIG_DIR", "/config/channels")
KEY_DIR = os.getenv("KEY_DIR", "/data/keys")
DEFAULT_CHANNEL_ID = "channel-one"

# ── Playlist mode ──

# When true, the playout engine loops the current playlist forever instead
# of switching to the next day's playlist at midnight.  Auto-enabled
# on first start (no media files).
PLAYLIST_LOOP = os.getenv("PLAYLIST_LOOP", "false").lower() in ("true", "1", "yes")

# ── Playlist storage ──

PLAYLIST_DIR = os.getenv("PLAYLIST_DIR", "/data/playlists")

# ── Playout engine ──
# GStreamer playout engine is always active.  No external service needed.

# ── Default video resolution ──

DEFAULT_WIDTH = 1920
DEFAULT_HEIGHT = 1080
DEFAULT_FPS = 30

# ── Peer exchange (PROTOCOL.md section 8.6, 11) ──

PEER_FILE = os.getenv("PEER_FILE", "/data/peers.json")
PEER_REQUIRE_TLS = os.getenv("PEER_REQUIRE_TLS", "true").lower() in ("true", "1", "yes")
PEER_EXCHANGE_INTERVAL = 1800  # 30 minutes (spec section 10.3)
PEER_EXCHANGE_INITIAL_DELAY = 30  # Wait before first peer exchange run

# ── Relay (PROTOCOL.md section 10) ──

RELAY_FILE = os.getenv("RELAY_FILE", "/data/relays.json")
# Relay MUST NOT poll metadata less frequently than the metadata cache
# lifetime (section 10.3).  Cache lifetime is 60s (section 8.8), so
# this MUST be <= 60.
RELAY_METADATA_INTERVAL = 60  # Metadata cache lifetime (spec section 10.3)
RELAY_GUIDE_INTERVAL = 900  # 15 minutes (spec section 10.3)
RELAY_HLS_INTERVAL = 2  # ~segment duration (spec section 10.3)
RELAY_METADATA_INITIAL_DELAY = 10  # Wait before first relay metadata fetch
RELAY_HLS_INITIAL_DELAY = 10  # Wait before first HLS fetch
RELAY_MAX_SEGMENTS = 10  # Segment cache per channel
RELAY_UPSTREAM_TIMEOUT = 5.0  # HTTP timeout for upstream requests

# ── Mirror mode (PROTOCOL.md section 10.8) ──

MIRROR_POLL_INTERVAL = 2  # Seconds between HLS polls from primary (~segment duration)
MIRROR_FAILURE_THRESHOLD = 3  # Consecutive HLS failures before promoting
MIRROR_DEMOTION_DELAY = 30  # Seconds primary must be healthy before demoting back
MIRROR_UPSTREAM_TIMEOUT = 5.0  # HTTP timeout for primary requests

# ── Origin metadata refresh (PROTOCOL.md section 5.8) ──
# Relay/client MUST refresh metadata at least once per cache lifetime (60s).
# Relays SHOULD NOT poll more frequently than every 10 seconds (section 10.3).
# Separate from RELAY_METADATA_INTERVAL which governs the scheduler loop.
RELAY_METADATA_POLL_FLOOR = 10  # Minimum seconds between metadata polls (section 10.3)
_RAW_METADATA_INTERVAL = int(os.getenv("ORIGIN_METADATA_REFRESH_INTERVAL", "60"))
ORIGIN_METADATA_REFRESH_INTERVAL = max(
    _RAW_METADATA_INTERVAL, RELAY_METADATA_POLL_FLOOR
)

# ── Data directories ──

DATA_DIR = os.getenv("DATA_DIR", "/data")
PROGRAM_DIR = os.getenv("PROGRAM_DIR", "/data/programs")
SEQ_DIR = os.getenv("SEQ_DIR", "/data/seq")
TOKEN_DIR = os.getenv("TOKEN_DIR", "/data/tokens")
MIGRATION_DIR = os.getenv("MIGRATION_DIR", "/data/migrations")
PLAYOUT_STATE_DIR = os.getenv("PLAYOUT_STATE_DIR", "/data/playout-state")

# ── Media upload ──

MEDIA_UPLOAD_MAX_SIZE = int(
    os.getenv("MEDIA_UPLOAD_MAX_SIZE", str(2 * 1024 * 1024 * 1024))
)
MEDIA_UPLOAD_DIR = os.getenv("MEDIA_UPLOAD_DIR", "")

# ── Server ──

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
