"""Tests for mirror subsystem (PROTOCOL.md section 10.8).

Tests the MirrorManager: HLS replication, media sequence tracking,
promotion/demotion state machine, sequence adjustment, health
detection, and protocol endpoint serving for mirror channels.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure the app directory is on sys.path
APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from protocol.mirror import (
    MirrorManager,
    MirrorState,
    adjust_media_sequence,
    parse_media_sequence,
)
from protocol.relay import HLSCache


# ── Sample HLS manifests ──

SAMPLE_MANIFEST = """\
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:2
#EXT-X-MEDIA-SEQUENCE:500
#EXTINF:2.000,
stream498.ts
#EXTINF:2.000,
stream499.ts
#EXTINF:2.000,
stream500.ts
"""

SAMPLE_LOCAL_MANIFEST = """\
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:2
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:2.000,
stream0.ts
"""

SAMPLE_LOCAL_MANIFEST_2 = """\
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:2
#EXT-X-MEDIA-SEQUENCE:1
#EXTINF:2.000,
stream0.ts
#EXTINF:2.000,
stream1.ts
"""

TEST_CHANNEL_ID = "TVMkVHiXF9W1NgM9KLgs7tcBMvC1YtF4Daj4yfTrJercs3"


# ── parse_media_sequence tests ──


class TestParseMediaSequence:
    def test_parses_sequence(self):
        assert parse_media_sequence(SAMPLE_MANIFEST) == 500

    def test_parses_zero(self):
        assert parse_media_sequence(SAMPLE_LOCAL_MANIFEST) == 0

    def test_returns_none_for_missing(self):
        manifest = "#EXTM3U\n#EXTINF:2.000,\nstream.ts\n"
        assert parse_media_sequence(manifest) is None

    def test_parses_large_sequence(self):
        manifest = "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:999999\n"
        assert parse_media_sequence(manifest) == 999999


# ── adjust_media_sequence tests ──


class TestAdjustMediaSequence:
    def test_adds_positive_offset(self):
        result = adjust_media_sequence(SAMPLE_LOCAL_MANIFEST, 503)
        assert "#EXT-X-MEDIA-SEQUENCE:503" in result

    def test_preserves_other_lines(self):
        result = adjust_media_sequence(SAMPLE_LOCAL_MANIFEST, 503)
        assert "#EXTM3U" in result
        assert "#EXT-X-TARGETDURATION:2" in result
        assert "stream0.ts" in result

    def test_zero_offset_unchanged(self):
        result = adjust_media_sequence(SAMPLE_LOCAL_MANIFEST, 0)
        assert "#EXT-X-MEDIA-SEQUENCE:0" in result

    def test_offset_applied_to_nonzero_sequence(self):
        result = adjust_media_sequence(SAMPLE_LOCAL_MANIFEST_2, 500)
        assert "#EXT-X-MEDIA-SEQUENCE:501" in result

    def test_manifest_without_sequence_unchanged(self):
        manifest = "#EXTM3U\n#EXTINF:2.000,\nstream.ts\n"
        result = adjust_media_sequence(manifest, 100)
        assert result == manifest


# ── MirrorManager unit tests ──


class TestMirrorManagerInit:
    def test_initial_state(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary.example.com:443")
        assert mm.state == MirrorState.REPLICATING
        assert mm.channel_id == TEST_CHANNEL_ID
        assert mm.primary_hint == "primary.example.com:443"
        assert mm.consecutive_failures == 0
        assert mm.last_primary_media_sequence == 0
        assert mm.sequence_offset == 0

    def test_primary_url_with_tls(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary.example.com:443", require_tls=True)
        assert mm.primary_url == "https://primary.example.com:443"

    def test_primary_url_without_tls(self):
        mm = MirrorManager(
            TEST_CHANNEL_ID, "primary.example.com:8080", require_tls=False
        )
        assert mm.primary_url == "http://primary.example.com:8080"


class TestMirrorManagerHealth:
    def test_should_promote_after_threshold(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443")
        assert not mm.should_promote()

        # Record failures up to threshold
        for _ in range(3):
            mm._record_failure()
        assert mm.should_promote()

    def test_should_not_promote_when_already_promoted(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443")
        mm.state = MirrorState.PROMOTED
        for _ in range(10):
            mm._record_failure()
        assert not mm.should_promote()

    def test_success_resets_failures(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443")
        for _ in range(2):
            mm._record_failure()
        mm._record_success()
        assert mm.consecutive_failures == 0
        assert not mm.should_promote()

    def test_should_demote_after_delay(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443")
        mm.state = MirrorState.PROMOTED
        now = time.monotonic()

        # Not recovered yet
        assert not mm.should_demote(now)

        # Mark recovered
        mm.mark_primary_recovered(now - 31)
        assert mm.should_demote(now)

    def test_should_not_demote_before_delay(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443")
        mm.state = MirrorState.PROMOTED
        now = time.monotonic()
        mm.mark_primary_recovered(now - 10)  # Only 10s, need 30
        assert not mm.should_demote(now)

    def test_unreachable_resets_demotion_timer(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443")
        mm.state = MirrorState.PROMOTED
        now = time.monotonic()
        mm.mark_primary_recovered(now - 25)
        mm.mark_primary_unreachable()
        assert mm.primary_recovered_at is None
        assert not mm.should_demote(now)


class TestMirrorStateTransitions:
    def test_begin_promotion(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443")
        mm.last_primary_media_sequence = 500
        mm.last_primary_segment_count = 3
        mm.begin_promotion()
        assert mm.state == MirrorState.PROMOTING

    def test_begin_demotion(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443")
        mm.state = MirrorState.PROMOTED
        mm.sequence_offset = 503
        mm.begin_demotion()
        assert mm.state == MirrorState.DEMOTING
        assert mm.sequence_offset == 0
        assert mm.primary_recovered_at is None

    def test_complete_demotion(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443")
        mm.state = MirrorState.DEMOTING
        mm.consecutive_failures = 5
        mm.complete_demotion()
        assert mm.state == MirrorState.REPLICATING
        assert mm.consecutive_failures == 0


class TestMirrorStatusDict:
    def test_status_dict(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary.example.com:443")
        mm.last_primary_media_sequence = 500
        mm.last_primary_segment_count = 3
        status = mm.status_dict()
        assert status["channel_id"] == TEST_CHANNEL_ID
        assert status["primary_hint"] == "primary.example.com:443"
        assert status["state"] == "replicating"
        assert status["last_primary_media_sequence"] == 500
        assert status["last_primary_segment_count"] == 3
        assert status["sequence_offset"] == 0


# ── MirrorManager async tests (poll_primary_hls) ──


@pytest.mark.asyncio
class TestMirrorPollPrimary:
    async def test_successful_poll(self):
        """Test HLS polling from primary."""
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443", require_tls=False)

        mock_client = AsyncMock()

        # Mock manifest response
        manifest_resp = MagicMock()
        manifest_resp.status_code = 200
        manifest_resp.text = SAMPLE_MANIFEST
        manifest_resp.url = "http://primary:443/hls/stream.m3u8"

        # Mock segment responses
        seg_resp = MagicMock()
        seg_resp.status_code = 200
        seg_resp.content = b"\x00" * 100

        mock_client.get = AsyncMock(
            side_effect=[manifest_resp, seg_resp, seg_resp, seg_resp]
        )
        mm._client = mock_client

        result = await mm.poll_primary_hls()
        assert result is True
        assert mm.last_primary_media_sequence == 500
        assert mm.last_primary_segment_count == 3
        assert mm.consecutive_failures == 0
        assert mm.hls_cache.manifest is not None
        assert mm.hls_cache.segment_count == 3

    async def test_poll_failure_increments_counter(self):
        """Test that failed polls increment failure counter."""
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443", require_tls=False)

        mock_client = AsyncMock()
        resp = MagicMock()
        resp.status_code = 503
        mock_client.get = AsyncMock(return_value=resp)
        mm._client = mock_client

        result = await mm.poll_primary_hls()
        assert result is False
        assert mm.consecutive_failures == 1

    async def test_poll_exception_increments_counter(self):
        """Test that network errors increment failure counter."""
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443", require_tls=False)

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=Exception("connection refused"))
        mm._client = mock_client

        result = await mm.poll_primary_hls()
        assert result is False
        assert mm.consecutive_failures == 1


@pytest.mark.asyncio
class TestMirrorPollLocal:
    async def test_promoted_state_transition(self):
        """Test that first local poll transitions PROMOTING -> PROMOTED."""
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443", require_tls=False)
        mm.state = MirrorState.PROMOTING
        mm.last_primary_media_sequence = 500
        mm.last_primary_segment_count = 3

        mock_client = AsyncMock()

        # Mock local nginx manifest
        manifest_resp = MagicMock()
        manifest_resp.status_code = 200
        manifest_resp.text = SAMPLE_LOCAL_MANIFEST

        seg_resp = MagicMock()
        seg_resp.status_code = 200
        seg_resp.content = b"\x00" * 50

        mock_client.get = AsyncMock(side_effect=[manifest_resp, seg_resp])
        mm._client = mock_client

        result = await mm.poll_local_hls("http://nginx:8080/hls/stream.m3u8")
        assert result is True
        assert mm.state == MirrorState.PROMOTED
        # Offset: primary ended at seq 500 + 3 segments = 503 - 0 = 503
        assert mm.sequence_offset == 503

    async def test_sequence_offset_in_manifest(self):
        """Test that served manifest has adjusted sequence number."""
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443", require_tls=False)
        mm.state = MirrorState.PROMOTED
        mm.sequence_offset = 503

        mock_client = AsyncMock()

        manifest_resp = MagicMock()
        manifest_resp.status_code = 200
        manifest_resp.text = SAMPLE_LOCAL_MANIFEST

        seg_resp = MagicMock()
        seg_resp.status_code = 200
        seg_resp.content = b"\x00" * 50

        mock_client.get = AsyncMock(side_effect=[manifest_resp, seg_resp])
        mm._client = mock_client

        await mm.poll_local_hls("http://nginx:8080/hls/stream.m3u8")
        # The cached manifest should have sequence 503
        cached = mm.hls_cache.manifest
        assert cached is not None
        # The manifest is rewritten to protocol paths, and sequence adjusted
        assert "503" in cached or "MEDIA-SEQUENCE:503" in cached

    async def test_sequence_continuity_on_promotion(self):
        """Test the critical requirement: media sequence continuity.

        Primary at sequence 500 with 3 segments (500, 501, 502).
        Mirror promotes, local nginx starts at sequence 0.
        Served sequence should be 503 (next after 502).
        """
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443", require_tls=False)
        mm.state = MirrorState.PROMOTING
        mm.last_primary_media_sequence = 500
        mm.last_primary_segment_count = 3  # segments 500, 501, 502

        mock_client = AsyncMock()
        manifest_resp = MagicMock()
        manifest_resp.status_code = 200
        manifest_resp.text = SAMPLE_LOCAL_MANIFEST  # starts at seq 0

        seg_resp = MagicMock()
        seg_resp.status_code = 200
        seg_resp.content = b"\x00" * 50

        mock_client.get = AsyncMock(side_effect=[manifest_resp, seg_resp])
        mm._client = mock_client

        await mm.poll_local_hls("http://nginx:8080/hls/stream.m3u8")

        assert mm.sequence_offset == 503  # 500 + 3 - 0
        assert mm.state == MirrorState.PROMOTED

        # Verify manifest contains the adjusted sequence
        cached = mm.hls_cache.manifest
        assert cached is not None
        seq = parse_media_sequence(cached)
        assert seq == 503


@pytest.mark.asyncio
class TestMirrorHealthCheck:
    async def test_primary_healthy(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443", require_tls=False)

        mock_client = AsyncMock()
        resp = MagicMock()
        resp.status_code = 200
        mock_client.get = AsyncMock(return_value=resp)
        mm._client = mock_client

        result = await mm.check_primary_health()
        assert result is True

    async def test_primary_unhealthy(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443", require_tls=False)

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=Exception("timeout"))
        mm._client = mock_client

        result = await mm.check_primary_health()
        assert result is False


# ── Protocol endpoint tests for mirror channels ──


@pytest.mark.asyncio
class TestMirrorProtocolEndpoints:
    async def test_stream_serves_from_cache_when_replicating(self, client):
        """Mirror in replicating mode serves cached manifest."""
        import main
        from protocol.mirror import MirrorManager

        ctx = main.channels.all()[0]
        ctx.mirror_mode = True
        ctx.mirror_primary = "primary:443"

        mm = MirrorManager(ctx.channel_id, "primary:443", require_tls=False)
        mm.hls_cache.update_manifest("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:100\nstream.ts\n")
        main.mirror_managers[ctx.channel_id] = mm

        try:
            resp = await client.get(f"/tltv/v1/channels/{ctx.channel_id}/stream.m3u8")
            assert resp.status_code == 200
            assert "MEDIA-SEQUENCE:100" in resp.text
        finally:
            del main.mirror_managers[ctx.channel_id]
            ctx.mirror_mode = False

    async def test_stream_returns_503_when_cache_empty(self, client):
        """Mirror with empty cache returns 503."""
        import main
        from protocol.mirror import MirrorManager

        ctx = main.channels.all()[0]
        ctx.mirror_mode = True

        mm = MirrorManager(ctx.channel_id, "primary:443", require_tls=False)
        main.mirror_managers[ctx.channel_id] = mm

        try:
            resp = await client.get(f"/tltv/v1/channels/{ctx.channel_id}/stream.m3u8")
            assert resp.status_code == 503
        finally:
            del main.mirror_managers[ctx.channel_id]
            ctx.mirror_mode = False

    async def test_segment_serves_from_mirror_cache(self, client):
        """Mirror segments served from HLS cache."""
        import main
        from protocol.mirror import MirrorManager

        ctx = main.channels.all()[0]
        ctx.mirror_mode = True

        mm = MirrorManager(ctx.channel_id, "primary:443", require_tls=False)
        mm.hls_cache.add_segment("stream100.ts", b"\x47\x40" + b"\x00" * 186)
        main.mirror_managers[ctx.channel_id] = mm

        try:
            resp = await client.get(
                f"/tltv/v1/channels/{ctx.channel_id}/segments/stream100.ts"
            )
            assert resp.status_code == 200
            assert resp.headers["content-type"] == "video/mp2t"
        finally:
            del main.mirror_managers[ctx.channel_id]
            ctx.mirror_mode = False

    async def test_segment_404_when_not_cached(self, client):
        """Missing mirror segment returns 404."""
        import main
        from protocol.mirror import MirrorManager

        ctx = main.channels.all()[0]

        mm = MirrorManager(ctx.channel_id, "primary:443", require_tls=False)
        main.mirror_managers[ctx.channel_id] = mm

        try:
            resp = await client.get(
                f"/tltv/v1/channels/{ctx.channel_id}/segments/missing.ts"
            )
            assert resp.status_code == 404
        finally:
            del main.mirror_managers[ctx.channel_id]

    async def test_metadata_still_signed_locally(self, client):
        """Mirror channel metadata is built and signed locally."""
        import main
        from protocol.mirror import MirrorManager
        from protocol.signing import verify_document

        ctx = main.channels.all()[0]
        ctx.mirror_mode = True

        mm = MirrorManager(ctx.channel_id, "primary:443", require_tls=False)
        main.mirror_managers[ctx.channel_id] = mm

        try:
            resp = await client.get(f"/tltv/v1/channels/{ctx.channel_id}")
            assert resp.status_code == 200
            doc = resp.json()
            # Metadata is locally signed (same key)
            assert doc["id"] == ctx.channel_id
            assert "signature" in doc
            assert verify_document(doc, ctx.channel_id)
        finally:
            del main.mirror_managers[ctx.channel_id]
            ctx.mirror_mode = False

    async def test_non_mirror_channel_redirects(self, client):
        """Non-mirror originated channel still does 302 redirect."""
        import main

        ctx = main.channels.all()[0]
        ctx.mirror_mode = False

        # Ensure no mirror manager for this channel
        main.mirror_managers.pop(ctx.channel_id, None)

        resp = await client.get(
            f"/tltv/v1/channels/{ctx.channel_id}/stream.m3u8",
            follow_redirects=False,
        )
        assert resp.status_code == 302
        assert f"/hls/{ctx.channel_id}/stream.m3u8" in resp.headers.get("location", "")


# ── HLSCache integration tests ──


class TestMirrorHLSCache:
    def test_cache_segment_storage_and_retrieval(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443")
        mm.hls_cache.add_segment("seg001.ts", b"data1")
        mm.hls_cache.add_segment("seg002.ts", b"data2")
        assert mm.hls_cache.get_segment("seg001.ts") == b"data1"
        assert mm.hls_cache.get_segment("seg002.ts") == b"data2"
        assert mm.hls_cache.segment_count == 2

    def test_cache_eviction(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443")
        # Default max is 10 from config
        for i in range(15):
            mm.hls_cache.add_segment(f"seg{i:03d}.ts", f"data{i}".encode())
        assert mm.hls_cache.segment_count <= 10

    def test_manifest_update(self):
        mm = MirrorManager(TEST_CHANNEL_ID, "primary:443")
        mm.hls_cache.update_manifest(SAMPLE_MANIFEST)
        assert mm.hls_cache.manifest == SAMPLE_MANIFEST
        assert mm.hls_cache.manifest_updated is not None
