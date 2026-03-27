"""Shared fixtures for Cathode tests.

Provides:
1. Mocked FastAPI test client for integration/route tests (standard container)
2. Real GStreamer engine fixtures for playout tests (playout container)

Tests run inside Docker containers where deps are installed.

Integration tests: docker compose -f docker-compose.test.yml run --rm cathode pytest tests/ -v
Playout tests:     docker compose -f docker-compose.playout-test.yml run --rm cathode pytest tests/ -v
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
import pytest_asyncio

# Ensure the app directory is on sys.path (mirrors container layout)
APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


# ── GStreamer is always available (single Dockerfile) ──

import gi

gi.require_version("Gst", "1.0")
from gi.repository import Gst  # noqa: F401

HAS_GSTREAMER = True


# ══════════════════════════════════════════════════════════════════
# Custom markers
# ══════════════════════════════════════════════════════════════════


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "rtmp: test requires nginx-rtmp service")


# ══════════════════════════════════════════════════════════════════
# Mock engine data (integration tests)
# ══════════════════════════════════════════════════════════════════


# Mock engine health dict (matches PlayoutEngine.health format)
MOCK_ENGINE_HEALTH = {
    "running": True,
    "state": "PLAYING",
    "uptime": 42.0,
    "errors": 0,
    "last_error": None,
    "active_channel": "failover",
    "channels": {
        "failover": {
            "source_type": "test",
            "now_playing": None,
            "visible": True,
            "alpha": 1.0,
            "volume": 1.0,
            "playlist_name": None,
        },
        "input_a": {
            "source_type": "playlist",
            "now_playing": {
                "source": "/tv-media/1/clip_01.mp4",
                "index": 0,
                "duration": 120.5,
                "played": 12.5,
            },
            "visible": False,
            "alpha": 0.0,
            "volume": 0.0,
            "playlist_name": None,
        },
        "input_b": {
            "source_type": "test",
            "now_playing": None,
            "visible": False,
            "alpha": 0.0,
            "volume": 0.0,
            "playlist_name": None,
        },
        "blinder": {
            "source_type": "test",
            "now_playing": None,
            "visible": False,
            "alpha": 0.0,
            "volume": 0.0,
            "playlist_name": None,
        },
    },
}


@pytest.fixture
def mock_engine():
    """Create a mocked PlayoutEngine for integration tests.

    Mimics the real engine's API surface so route handlers that
    check `main.playout is not None` exercise the engine code path.
    """
    engine = MagicMock()
    engine.is_running = True
    engine.health = MOCK_ENGINE_HEALTH.copy()
    engine.active_channel = "failover"
    engine.watchdog_timeout = 3.0

    # Named channels
    for ch_name in ("failover", "input_a", "input_b", "blinder"):
        ch = MagicMock()
        ch.source_type = MagicMock()
        ch.source_type.value = "test"
        ch.now_playing = None
        ch.played = 0.0
        ch.data_age = 0.0
        ch._playlist_name = None
        ch._playlist_entries = []
        ch._display_index = 0
        ch._playlist_loop = True
        setattr(engine, ch_name, ch)

    engine.channel = MagicMock(side_effect=lambda name: getattr(engine, name))
    engine.layer_visibility = MagicMock(
        return_value={"visible": False, "alpha": 0.0, "volume": 0.0}
    )
    engine.show = MagicMock()
    engine.hide = MagicMock()
    engine.hide_blinder = MagicMock()
    engine.set_position = MagicMock()
    engine.reset_position = MagicMock()
    engine.start = AsyncMock()
    engine.stop = AsyncMock()
    engine.restart = AsyncMock()
    engine.on_source_lost = None
    engine._outputs = {}  # No outputs in mock engine by default
    engine.outputs = {}  # Property access

    # add_output / remove_output mocks
    engine.add_output = AsyncMock()
    engine.remove_output = AsyncMock()
    engine.get_output = MagicMock(return_value=None)

    # config with real defaults for encoding endpoint tests
    from playout import PlayoutConfig

    _cfg = PlayoutConfig()
    engine._config = _cfg
    type(engine).config = PropertyMock(return_value=_cfg)

    return engine


@pytest.fixture
def mock_renderer():
    """Create a mocked HTMLRenderer."""
    renderer = AsyncMock()
    renderer.start = AsyncMock(return_value={"status": "streaming"})
    renderer.stop = AsyncMock(return_value={"status": "idle"})
    renderer.status = AsyncMock(return_value={"status": "idle"})
    renderer.health = AsyncMock(return_value={"healthy": True})
    renderer.close = AsyncMock()
    return renderer


@pytest.fixture
def mock_media_gen(tmp_path):
    """Create a mocked MediaGenerator."""
    gen = MagicMock()
    gen.list_plugins = MagicMock(
        return_value=[
            {
                "name": "slate",
                "description": "Channel slate",
                "params": {},
                "defaults": {},
            },
        ]
    )
    gen.plugins = {"slate": {"name": "slate"}}
    gen.get_plugin = MagicMock(return_value={"name": "slate"})
    gen.generate = MagicMock(
        return_value={
            "filename": "slate_test.mp4",
            "path": str(tmp_path / "slate_test.mp4"),
            "duration": 120.0,
        }
    )
    gen.list_generated = MagicMock(return_value=[])
    gen.load_plugins = MagicMock()
    return gen


@pytest.fixture
def test_media_dir(tmp_path):
    """Create a temporary media directory with fake clip files."""
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    # Create small dummy files (not real video — ffprobe will fail gracefully)
    for i in range(1, 4):
        (media_dir / f"clip_{i:02d}.mp4").write_bytes(b"\x00" * 1024)
    return media_dir


# Track whether plugin routes have been included on the shared app.
# Plugin routes are normally loaded by plugins.load_plugins() during
# lifespan, which tests bypass.  We include them once here.
_plugin_routes_loaded = False


@pytest.fixture
def app_with_mocks(
    mock_engine,
    mock_renderer,
    mock_media_gen,
    test_media_dir,
    tmp_path,
):
    """Create a FastAPI app with all external deps mocked.

    Patches module-level globals in main.py and config.py, and skips
    the real lifespan (no engine start, no watchdog/scheduler tasks).

    Plugin services are registered via the plugins module so that
    plugins.has_plugin() and plugins.get_service() work correctly.

    Sets main.playout to mock_engine so engine code paths are exercised.
    """
    global _plugin_routes_loaded

    # generated_dir is now {media_dir}/cathode/
    generated_dir = str(test_media_dir / "cathode")

    # Patch env vars before importing main
    env_patches = {
        "MEDIA_DIR": str(test_media_dir),
        "PROGRAM_DIR": str(tmp_path / "programs"),
    }
    (generated_dir and os.makedirs(generated_dir, exist_ok=True))
    (tmp_path / "programs").mkdir(exist_ok=True)

    with patch.dict(os.environ, env_patches):
        # Import main fresh with patched env
        import main
        import config
        import plugins

        # Patch config module constants BEFORE loading plugins — plugins
        # read config during register() for service construction paths.
        config.MEDIA_DIR = str(test_media_dir)

        # Load plugins once — discovers volume-mounted cathode-plugins
        # from app/plugins/ and registers their routes on the app.
        # On subsequent fixture calls, routes are already on the app;
        # we just override services with mocks below.
        if not _plugin_routes_loaded:
            plugins.reset()
            plugins.load_plugins(main.app, config)
            _plugin_routes_loaded = True

        # Override plugin services with mocks
        plugins.register_service("renderer", mock_renderer, plugin_name="renderer")
        plugins.register_service("media_gen", mock_media_gen, plugin_name="media-gen")
        plugins.mark_loaded("generators")
        plugins.mark_loaded("canvas")
        plugins.mark_loaded("pyvideo")

        # Core services — engine is always the playout backend
        main.playout = mock_engine

        # Patch program module's PROGRAM_DIR
        import program as prog_mod

        prog_mod.PROGRAM_DIR = str(tmp_path / "programs")

        # Register a test channel with federation identity for protocol tests
        from channel import ChannelContext, ChannelRegistry
        from protocol.identity import make_channel_id

        # Use a deterministic test keypair (RFC 8032 test vector 1)
        test_seed = bytes.fromhex(
            "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
        )
        test_pubkey = bytes.fromhex(
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
        )
        test_channel_id = make_channel_id(test_pubkey)
        test_key_path = tmp_path / "test.key"
        test_key_path.write_bytes(test_seed)

        # Create a fresh registry with our test channel
        main.channels = ChannelRegistry()
        test_ctx = ChannelContext(
            id="channel-one",
            display_name="TLTV Channel One",
            channel_id=test_channel_id,
            private_key_path=str(test_key_path),
            media_dir=str(test_media_dir),
            generated_dir=generated_dir,
            program_dir=str(tmp_path / "programs"),
            description="24/7 experimental television",
            language="en",
            tags=["experimental", "generative"],
            access="public",
            origins=[],
            timezone="America/New_York",
        )
        test_ctx.engine = mock_engine
        main.channels.register(test_ctx)

        # Patch seq dir so signing doesn't use /data/seq
        import protocol.signing as signing_mod

        signing_mod.SEQ_DIR = str(tmp_path / "seq")

        # Initialize peer store, relay manager, and token store for federation tests
        from protocol.peers import PeerStore
        from protocol.relay import RelayManager
        from protocol.tokens import TokenStore

        main.peer_store = PeerStore(path=str(tmp_path / "peers.json"), max_peers=100)
        main.relay_manager = RelayManager(path=str(tmp_path / "relays.json"))
        main.token_store = TokenStore(token_dir=str(tmp_path / "tokens"))

        yield main.app

        # Restore playout to None so other test runs don't leak
        main.playout = None


@pytest_asyncio.fixture
async def client(app_with_mocks):
    """Async test client that bypasses the lifespan."""
    from httpx import ASGITransport, AsyncClient

    transport = ASGITransport(app=app_with_mocks)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ══════════════════════════════════════════════════════════════════
# GStreamer playout engine fixtures (playout container only)
# ══════════════════════════════════════════════════════════════════

CLIP_DIR = "/tmp/test-clips"
HLS_OUTPUT_DIR = "/tmp/hls-test"
NUM_CLIPS = 3
CLIP_DURATION = 3  # seconds — keep short for speed


def _generate_clip(
    index: int, color: str, freq: int, duration: int, outdir: str
) -> str:
    """Generate a single test clip with ffmpeg. Returns the file path."""
    path = f"{outdir}/clip_{index:02d}.mp4"
    if os.path.exists(path) and os.path.getsize(path) > 1000:
        return path

    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "lavfi",
        "-i",
        (
            f"color=c={color}:s=640x360:d={duration}:r=15,"
            f"drawtext=text='CLIP {index:02d}':fontsize=48:fontcolor=white"
            f":x=(w-text_w)/2:y=(h-text_h)/2"
        ),
        "-f",
        "lavfi",
        "-i",
        f"sine=frequency={freq}:duration={duration}:sample_rate=48000",
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        "-tune",
        "zerolatency",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-shortest",
        path,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed for clip {index}: {result.stderr[-300:]}")

    return path


def _probe_duration(path: str, fallback: float = 3.0) -> float:
    """Get file duration via ffprobe."""
    try:
        r = subprocess.run(
            [
                "ffprobe",
                "-v",
                "quiet",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                path,
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return float(r.stdout.strip())
    except Exception:
        return fallback


@pytest.fixture(scope="session")
def test_clips() -> list[str]:
    """Generate test clips once per session. Returns paths to 3 short MP4s.

    Requires ffmpeg — skips if not available (standard test container).
    """
    try:
        subprocess.run(
            ["ffmpeg", "-version"], capture_output=True, timeout=5, check=True
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        pytest.skip("ffmpeg not available")

    os.makedirs(CLIP_DIR, exist_ok=True)

    clips_config = [
        (0, "red", 440),
        (1, "blue", 660),
        (2, "green", 880),
    ]

    paths = []
    for index, color, freq in clips_config:
        path = _generate_clip(index, color, freq, CLIP_DURATION, CLIP_DIR)
        paths.append(path)

    return paths


@pytest.fixture
def hls_output_dir(tmp_path):
    """Temp directory for HLS segment output during tests."""
    d = tmp_path / "hls"
    d.mkdir()
    return str(d)


@pytest.fixture
def playout_config():
    """Low-res config for fast tests."""
    from playout import PlayoutConfig

    return PlayoutConfig(
        width=640,
        height=360,
        fps=15,
        video_bitrate=800,
        audio_bitrate=128,
        keyframe_interval=30,
    )


@pytest_asyncio.fixture
async def engine(playout_config, hls_output_dir):
    """A started PlayoutEngine with HLS output to temp dir.

    Stopped automatically in teardown.  HLS output verifiable
    by checking for segment files in hls_output_dir.
    """
    from playout import PlayoutEngine, OutputConfig, OutputType

    eng = PlayoutEngine()
    output_cfg = OutputConfig(
        type=OutputType.HLS,
        name="test",
        hls_dir=hls_output_dir,
        video_bitrate=playout_config.video_bitrate,
        audio_bitrate=playout_config.audio_bitrate,
        keyframe_interval=playout_config.keyframe_interval,
    )
    await eng.start(config=playout_config, default_output=output_cfg)

    yield eng

    await eng.stop()


@pytest_asyncio.fixture
async def engine_null_output(playout_config):
    """A started PlayoutEngine with null output (no disk writes).

    For tests that don't need to verify output — layer switching,
    failover, playlist transport, etc.
    """
    from playout import PlayoutEngine, OutputConfig, OutputType

    eng = PlayoutEngine()
    output_cfg = OutputConfig(
        type=OutputType.NULL,
        name="test-null",
        video_bitrate=playout_config.video_bitrate,
        audio_bitrate=playout_config.audio_bitrate,
        keyframe_interval=playout_config.keyframe_interval,
    )
    await eng.start(config=playout_config, default_output=output_cfg)

    yield eng

    await eng.stop()


@pytest.fixture
def engine_no_start():
    """A bare PlayoutEngine — not started."""
    from playout import PlayoutEngine

    return PlayoutEngine()


@pytest.fixture
def playlist_entries(test_clips):
    """PlaylistEntry list from test clips (with probed durations)."""
    from playout import PlaylistEntry

    entries = []
    for path in test_clips:
        dur = _probe_duration(path)
        entries.append(PlaylistEntry(source=path, duration=dur))
    return entries
