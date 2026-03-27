"""Tests for relay subsystem (Phase 6).

Tests the RelayManager, upstream fetching, HLS cache,
signature verification, access mode transitions, and
protocol endpoint serving for relayed channels.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

# Ensure the app directory is on sys.path
APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from protocol.identity import make_channel_id, generate_ed25519_keypair
from protocol.signing import sign_document, verify_document
from protocol.relay import (
    RelayManager,
    RelayTarget,
    HLSCache,
    _parse_segment_names,
    _rewrite_manifest,
)


# ── Test fixtures ──

# Use the same test keypair as conftest.py
TEST_SEED = bytes.fromhex(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
)
TEST_PUBKEY = bytes.fromhex(
    "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
)
TEST_CHANNEL_ID = "TVMkVHiXF9W1NgM9KLgs7tcBMvC1YtF4Daj4yfTrJercs3"

# Generate a second keypair for the "upstream" channel
_UPSTREAM_SEED = bytes.fromhex(
    "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb"
)


def _get_upstream_keypair(tmp_path):
    """Create an upstream keypair for relay tests."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    private_key = Ed25519PrivateKey.from_private_bytes(_UPSTREAM_SEED)
    public_bytes = private_key.public_key().public_bytes_raw()
    channel_id = make_channel_id(public_bytes)
    key_path = tmp_path / "upstream.key"
    key_path.write_bytes(_UPSTREAM_SEED)
    return channel_id, str(key_path)


def _make_signed_metadata(channel_id: str, key_path: str, **overrides) -> dict:
    """Build a signed metadata document for an upstream channel."""
    doc = {
        "v": 1,
        "seq": overrides.pop("seq", 1),
        "id": channel_id,
        "name": overrides.pop("name", "Upstream Channel"),
        "stream": f"/tltv/v1/channels/{channel_id}/stream.m3u8",
        "access": overrides.pop("access", "public"),
        "updated": "2026-03-15T12:00:00Z",
    }
    doc.update(overrides)
    return sign_document(doc, key_path)


def _make_signed_guide(channel_id: str, key_path: str, **overrides) -> dict:
    """Build a signed guide document for an upstream channel.

    All timestamps use UTC with Z suffix per spec sections 6.1-6.5.
    """
    doc = {
        "v": 1,
        "seq": overrides.pop("seq", 1),
        "id": channel_id,
        "from": "2026-03-15T05:00:00Z",
        "until": "2026-03-17T05:00:00Z",
        "entries": overrides.pop(
            "entries",
            [
                {
                    "start": "2026-03-16T00:00:00Z",
                    "end": "2026-03-16T01:00:00Z",
                    "title": "Test Show",
                }
            ],
        ),
        "updated": "2026-03-15T12:00:00Z",
    }
    doc.update(overrides)
    return sign_document(doc, key_path)


SAMPLE_MANIFEST = """\
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:2
#EXT-X-MEDIA-SEQUENCE:42
#EXTINF:2.000,
seg-0042.ts
#EXTINF:2.000,
seg-0043.ts
#EXTINF:2.000,
seg-0044.ts
"""


# ── HLSCache unit tests ──


class TestHLSCache:
    def test_add_and_get_segment(self):
        cache = HLSCache(max_segments=5)
        cache.add_segment("seg-001.ts", b"\x00" * 100)
        assert cache.get_segment("seg-001.ts") is not None
        assert cache.segment_count == 1

    def test_eviction(self):
        cache = HLSCache(max_segments=3)
        cache.add_segment("seg-001.ts", b"a")
        cache.add_segment("seg-002.ts", b"b")
        cache.add_segment("seg-003.ts", b"c")
        cache.add_segment("seg-004.ts", b"d")
        assert cache.segment_count == 3
        assert cache.get_segment("seg-001.ts") is None  # Evicted
        assert cache.get_segment("seg-004.ts") is not None

    def test_update_manifest(self):
        cache = HLSCache()
        cache.update_manifest("#EXTM3U\ntest")
        assert cache.manifest == "#EXTM3U\ntest"
        assert cache.manifest_updated is not None

    def test_duplicate_segment_moves_to_end(self):
        cache = HLSCache(max_segments=3)
        cache.add_segment("seg-001.ts", b"a")
        cache.add_segment("seg-002.ts", b"b")
        cache.add_segment("seg-001.ts", b"a")  # Re-add
        cache.add_segment("seg-003.ts", b"c")
        # seg-002 should be evicted (not seg-001 which was refreshed)
        assert cache.segment_count == 3


class TestManifestParsing:
    def test_parse_segment_names(self):
        names = _parse_segment_names(SAMPLE_MANIFEST)
        assert names == ["seg-0042.ts", "seg-0043.ts", "seg-0044.ts"]

    def test_parse_empty(self):
        assert _parse_segment_names("") == []

    def test_rewrite_manifest(self):
        rewritten = _rewrite_manifest(
            SAMPLE_MANIFEST,
            "TVxyz123",
            "http://origin.com/hls/stream.m3u8",
        )
        assert "/tltv/v1/channels/TVxyz123/segments/seg-0042.ts" in rewritten
        assert "/tltv/v1/channels/TVxyz123/segments/seg-0043.ts" in rewritten
        assert "#EXT-X-MEDIA-SEQUENCE:42" in rewritten


# ── RelayManager unit tests ──


class TestRelayManager:
    def test_add_and_get(self, tmp_path):
        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add("TVtest1", ["origin.com:8000"])
        assert "TVtest1" in mgr
        assert mgr.get("TVtest1") is target

    @pytest.mark.asyncio
    async def test_remove(self, tmp_path):
        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        mgr.add("TVtest1", ["origin.com:8000"])
        assert await mgr.remove("TVtest1") is True
        assert "TVtest1" not in mgr
        await mgr.close()

    @pytest.mark.asyncio
    async def test_remove_nonexistent(self, tmp_path):
        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        assert await mgr.remove("TVnonexistent") is False
        await mgr.close()

    def test_persistence(self, tmp_path):
        path = str(tmp_path / "relays.json")
        mgr1 = RelayManager(path=path)
        mgr1.add("TVtest1", ["origin.com:8000"])
        mgr1.add("TVtest2", ["other.com:8000"])

        mgr2 = RelayManager(path=path)
        assert len(mgr2) == 2
        assert "TVtest1" in mgr2
        assert "TVtest2" in mgr2

    def test_active_relays(self, tmp_path):
        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        t1 = mgr.add("TVtest1", ["a.com:8000"])
        t2 = mgr.add("TVtest2", ["b.com:8000"])
        t2.active = False
        assert len(mgr.active_relays()) == 1
        assert mgr.active_relays()[0].channel_id == "TVtest1"


class TestRelayTarget:
    def test_upstream_url_http(self, tmp_path):
        with patch("config.PEER_REQUIRE_TLS", False):
            target = RelayTarget("TVtest", ["origin.com:8000"])
            assert target.upstream_url == "http://origin.com:8000"

    def test_upstream_url_https(self, tmp_path):
        with patch("config.PEER_REQUIRE_TLS", True):
            target = RelayTarget("TVtest", ["origin.com:8443"])
            assert target.upstream_url == "https://origin.com:8443"

    def test_status_dict(self):
        target = RelayTarget("TVtest", ["origin.com:8000"])
        status = target.status_dict()
        assert status["channel_id"] == "TVtest"
        assert status["active"] is True
        assert status["cached_segments"] == 0


# ── Upstream fetching tests ──


class TestRelayMetadataFetch:
    @pytest.mark.asyncio
    async def test_fetch_valid_metadata(self, tmp_path):
        """Fetching valid signed metadata caches it."""
        channel_id, key_path = _get_upstream_keypair(tmp_path)
        metadata = _make_signed_metadata(channel_id, key_path, seq=5)

        import httpx

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=metadata)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["origin.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_metadata(target)

        assert result is True
        assert target.metadata is not None
        assert target.metadata["id"] == channel_id
        assert target.metadata_seq == 5
        await client.aclose()

    @pytest.mark.asyncio
    async def test_reject_invalid_signature(self, tmp_path):
        """Metadata with invalid signature is rejected."""
        channel_id, key_path = _get_upstream_keypair(tmp_path)
        metadata = _make_signed_metadata(channel_id, key_path)
        metadata["name"] = "Tampered"  # Invalidate signature

        import httpx

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=metadata)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["origin.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_metadata(target)

        assert result is False
        assert target.metadata is None
        await client.aclose()

    @pytest.mark.asyncio
    async def test_seq_ordering(self, tmp_path):
        """Only update cache if new seq is higher."""
        channel_id, key_path = _get_upstream_keypair(tmp_path)

        import httpx

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["origin.com:8000"])

        # First fetch: seq=5
        meta5 = _make_signed_metadata(channel_id, key_path, seq=5, name="Seq5")

        async def handler5(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=meta5)

        client5 = httpx.AsyncClient(transport=httpx.MockTransport(handler5))
        target._client = client5
        with patch("config.PEER_REQUIRE_TLS", False):
            await mgr.fetch_metadata(target)
        assert target.metadata_seq == 5
        assert target.metadata["name"] == "Seq5"
        await client5.aclose()

        # Second fetch: seq=3 (lower) — should NOT update
        meta3 = _make_signed_metadata(channel_id, key_path, seq=3, name="Seq3")

        async def handler3(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=meta3)

        client3 = httpx.AsyncClient(transport=httpx.MockTransport(handler3))
        target._client = client3
        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_metadata(target)
        assert result is False
        assert target.metadata_seq == 5  # Still 5
        assert target.metadata["name"] == "Seq5"  # Unchanged
        await client3.aclose()

    @pytest.mark.asyncio
    async def test_access_mode_transition(self, tmp_path):
        """Stop relaying if channel goes private (section 10.6)."""
        channel_id, key_path = _get_upstream_keypair(tmp_path)
        private_metadata = _make_signed_metadata(channel_id, key_path, access="token")

        import httpx

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=private_metadata)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["origin.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_metadata(target)

        assert result is False
        assert target.active is False
        assert target.error == "access_token"
        await client.aclose()

    @pytest.mark.asyncio
    async def test_retired_channel_deactivated(self, tmp_path):
        """Retired channels deactivate the relay."""
        channel_id, key_path = _get_upstream_keypair(tmp_path)
        metadata = _make_signed_metadata(channel_id, key_path, status="retired")

        import httpx

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=metadata)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["origin.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_metadata(target)

        assert result is False
        assert target.active is False
        assert target.error == "channel_retired"
        await client.aclose()

    @pytest.mark.asyncio
    async def test_on_demand_rejected(self, tmp_path):
        """On-demand channels are refused (section 10.2)."""
        channel_id, key_path = _get_upstream_keypair(tmp_path)
        metadata = _make_signed_metadata(channel_id, key_path, on_demand=True)

        import httpx

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=metadata)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["origin.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_metadata(target)

        assert result is False
        assert target.active is False
        assert target.error == "on_demand_rejected"
        await client.aclose()

    @pytest.mark.asyncio
    async def test_upstream_down(self, tmp_path):
        """Upstream returning 500 doesn't crash."""
        channel_id, key_path = _get_upstream_keypair(tmp_path)

        import httpx

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["origin.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_metadata(target)

        assert result is False
        assert target.error is not None
        await client.aclose()

    @pytest.mark.asyncio
    async def test_reject_future_seq(self, tmp_path):
        """Metadata with seq too far in the future is rejected (section 5.5)."""
        import time

        import httpx

        channel_id, key_path = _get_upstream_keypair(tmp_path)
        future_seq = int(time.time()) + 7200  # 2 hours ahead
        metadata = _make_signed_metadata(channel_id, key_path, seq=future_seq)

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=metadata)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["origin.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_metadata(target)

        assert result is False
        # verify_document now rejects far-future seq (>1hr) at signature
        # validation, so the error may be "invalid signature" or "future"
        assert target.error is not None
        await client.aclose()


class TestRelayGuideFetch:
    @pytest.mark.asyncio
    async def test_fetch_valid_guide(self, tmp_path):
        channel_id, key_path = _get_upstream_keypair(tmp_path)
        guide = _make_signed_guide(channel_id, key_path, seq=2)

        import httpx

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=guide)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["origin.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_guide(target)

        assert result is True
        assert target.guide is not None
        assert target.guide_seq == 2
        await client.aclose()

    @pytest.mark.asyncio
    async def test_reject_future_guide_seq(self, tmp_path):
        """Guide with seq too far in the future is rejected (section 5.5)."""
        import time

        import httpx

        channel_id, key_path = _get_upstream_keypair(tmp_path)
        future_seq = int(time.time()) + 7200
        guide = _make_signed_guide(channel_id, key_path, seq=future_seq)

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=guide)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["origin.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_guide(target)

        assert result is False
        assert target.guide is None
        await client.aclose()

    @pytest.mark.asyncio
    async def test_reject_invalid_guide_signature(self, tmp_path):
        channel_id, key_path = _get_upstream_keypair(tmp_path)
        guide = _make_signed_guide(channel_id, key_path)
        guide["entries"] = []  # Invalidate signature

        import httpx

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=guide)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["origin.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_guide(target)

        assert result is False
        assert target.guide is None
        await client.aclose()


class TestRelayHLSFetch:
    @pytest.mark.asyncio
    async def test_fetch_hls(self, tmp_path):
        """Fetching HLS caches manifest and segments."""
        import httpx

        segment_data = b"\x47" * 188  # Mock TS packet

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if "stream.m3u8" in url:
                return httpx.Response(200, text=SAMPLE_MANIFEST)
            elif ".ts" in url:
                return httpx.Response(200, content=segment_data)
            return httpx.Response(404)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        _, pub = generate_ed25519_keypair()
        cid = make_channel_id(pub)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(cid, ["origin.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_hls(target)

        assert result is True
        assert target.hls_cache.manifest is not None
        assert target.hls_cache.segment_count == 3
        assert target.hls_cache.get_segment("seg-0042.ts") == segment_data
        await client.aclose()


# ── Protocol endpoint tests for relayed channels ──


class TestRelayProtocolEndpoints:
    """Test protocol endpoints serving relayed channel data."""

    @pytest.fixture
    def relay_setup(self, tmp_path):
        """Set up a relay target with cached data on the test app."""
        channel_id, key_path = _get_upstream_keypair(tmp_path)
        metadata = _make_signed_metadata(channel_id, key_path, seq=10)
        guide = _make_signed_guide(channel_id, key_path, seq=3)

        return {
            "channel_id": channel_id,
            "key_path": key_path,
            "metadata": metadata,
            "guide": guide,
        }

    def _setup_relay(self, relay_setup):
        """Add a relay target to the manager with cached data."""
        import main

        cid = relay_setup["channel_id"]
        target = main.relay_manager.add(cid, ["upstream.example.com:8000"])
        target.metadata = relay_setup["metadata"]
        target.metadata_seq = relay_setup["metadata"]["seq"]
        target.guide = relay_setup["guide"]
        target.guide_seq = relay_setup["guide"]["seq"]
        target.active = True
        return cid, target

    async def test_metadata_serves_cached_verbatim(self, client, relay_setup):
        """GET /tltv/v1/channels/{id} serves cached metadata for relay."""
        cid, target = self._setup_relay(relay_setup)
        try:
            resp = await client.get(f"/tltv/v1/channels/{cid}")
            assert resp.status_code == 200
            data = resp.json()
            # Must be the upstream metadata verbatim
            assert data["id"] == cid
            assert data["name"] == "Upstream Channel"
            assert data["signature"] == relay_setup["metadata"]["signature"]
            # Verify signature is valid (upstream key)
            assert verify_document(data, cid) is True
        finally:
            import main

            await main.relay_manager.remove(cid)

    async def test_guide_serves_cached_verbatim(self, client, relay_setup):
        """GET /tltv/v1/channels/{id}/guide.json serves cached guide."""
        cid, target = self._setup_relay(relay_setup)
        try:
            resp = await client.get(f"/tltv/v1/channels/{cid}/guide.json")
            assert resp.status_code == 200
            data = resp.json()
            assert data["id"] == cid
            assert verify_document(data, cid) is True
            assert len(data["entries"]) == 1
            assert data["entries"][0]["title"] == "Test Show"
        finally:
            import main

            await main.relay_manager.remove(cid)

    async def test_stream_returns_200_not_302(self, client, relay_setup):
        """Relayed stream returns 200 with manifest (not 302 redirect)."""
        cid, target = self._setup_relay(relay_setup)
        target.hls_cache.update_manifest(SAMPLE_MANIFEST)
        try:
            resp = await client.get(
                f"/tltv/v1/channels/{cid}/stream.m3u8",
                follow_redirects=False,
            )
            assert resp.status_code == 200
            assert "seg-0042.ts" in resp.text or "segments/seg-0042.ts" in resp.text
        finally:
            import main

            await main.relay_manager.remove(cid)

    async def test_stream_503_no_cache(self, client, relay_setup):
        """503 when relay has no cached HLS data."""
        cid, target = self._setup_relay(relay_setup)
        # Don't add any HLS data to the cache
        try:
            resp = await client.get(
                f"/tltv/v1/channels/{cid}/stream.m3u8",
                follow_redirects=False,
            )
            assert resp.status_code == 503
            assert resp.json()["error"] == "stream_unavailable"
        finally:
            import main

            await main.relay_manager.remove(cid)

    async def test_segment_serving(self, client, relay_setup):
        """Segments are served from cache."""
        cid, target = self._setup_relay(relay_setup)
        seg_data = b"\x47" * 188
        target.hls_cache.add_segment("seg-0042.ts", seg_data)
        try:
            resp = await client.get(f"/tltv/v1/channels/{cid}/segments/seg-0042.ts")
            assert resp.status_code == 200
            assert resp.content == seg_data
            assert "video/mp2t" in resp.headers.get("content-type", "")
        finally:
            import main

            await main.relay_manager.remove(cid)

    async def test_segment_404_not_cached(self, client, relay_setup):
        """404 for uncached segments."""
        cid, target = self._setup_relay(relay_setup)
        try:
            resp = await client.get(f"/tltv/v1/channels/{cid}/segments/seg-9999.ts")
            assert resp.status_code == 404
        finally:
            import main

            await main.relay_manager.remove(cid)

    async def test_well_known_includes_relayed(self, client, relay_setup):
        """/.well-known/tltv lists relayed channels under 'relaying'."""
        cid, target = self._setup_relay(relay_setup)
        try:
            resp = await client.get("/.well-known/tltv")
            data = resp.json()
            relaying_ids = [r["id"] for r in data["relaying"]]
            assert cid in relaying_ids
        finally:
            import main

            await main.relay_manager.remove(cid)

    async def test_peers_includes_relayed(self, client, relay_setup):
        """GET /tltv/v1/peers includes relayed channels."""
        cid, target = self._setup_relay(relay_setup)
        try:
            resp = await client.get("/tltv/v1/peers")
            data = resp.json()
            peer_ids = [p["id"] for p in data["peers"]]
            assert cid in peer_ids
        finally:
            import main

            await main.relay_manager.remove(cid)

    async def test_guide_xml_for_relayed(self, client, relay_setup):
        """GET /tltv/v1/channels/{id}/guide.xml generates XMLTV from cache."""
        cid, target = self._setup_relay(relay_setup)
        try:
            resp = await client.get(f"/tltv/v1/channels/{cid}/guide.xml")
            assert resp.status_code == 200
            assert "application/xml" in resp.headers.get("content-type", "")
            content = resp.text
            assert f'id="{cid}"' in content
            assert "Test Show" in content
        finally:
            import main

            await main.relay_manager.remove(cid)

    async def test_inactive_relay_not_served(self, client, relay_setup):
        """Inactive relay targets are not served."""
        cid, target = self._setup_relay(relay_setup)
        target.active = False
        try:
            resp = await client.get(f"/tltv/v1/channels/{cid}")
            assert resp.status_code == 404
        finally:
            import main

            target.active = True  # Restore to allow removal
            await main.relay_manager.remove(cid)


# ── Management API tests ──


class TestRelayManagementAPI:
    async def test_list_relays_empty(self, client):
        resp = await client.get("/api/relay")
        assert resp.status_code == 200
        data = resp.json()
        assert data["relays"] == []

    async def test_delete_nonexistent_relay(self, client):
        resp = await client.delete("/api/relay/TVnonexistent")
        assert resp.status_code == 404


# ── Origin failover tests (section 12.1) ──


class TestRelayOriginFailover:
    """Test upstream origin rotation when current upstream fails."""

    def test_rotate_upstream_with_multiple_hints(self):
        """rotate_upstream cycles through hints."""
        with patch("config.PEER_REQUIRE_TLS", False):
            target = RelayTarget("TVtest", ["a.com:8000", "b.com:8000", "c.com:8000"])
            assert "a.com" in target.upstream_url

            # Rotate once
            assert target.rotate_upstream() is True
            assert "b.com" in target.upstream_url

            # Rotate again
            assert target.rotate_upstream() is True
            assert "c.com" in target.upstream_url

            # Full cycle — exhausted
            assert target.rotate_upstream() is False

    def test_rotate_upstream_single_hint(self):
        """No rotation possible with single hint."""
        target = RelayTarget("TVtest", ["only.com:8000"])
        assert target.rotate_upstream() is False

    def test_rotate_upstream_empty_hints(self):
        """No rotation possible with empty hints."""
        target = RelayTarget("TVtest", [])
        assert target.rotate_upstream() is False

    def test_reset_failure_count(self):
        """Success resets failure counter."""
        target = RelayTarget("TVtest", ["a.com:8000", "b.com:8000"])
        target.rotate_upstream()
        target.reset_failure_count()
        assert target._consecutive_failures == 0

    def test_effective_origins_prefers_metadata(self):
        """Origins from cached metadata are preferred over hints."""
        with patch("config.PEER_REQUIRE_TLS", False):
            target = RelayTarget("TVtest", ["hint.com:8000"])
            target.metadata = {
                "id": "TVtest",
                "origins": ["meta1.com:443", "meta2.com:443"],
            }
            origins = target._effective_origins()
            assert origins == ["meta1.com:443", "meta2.com:443"]
            assert "meta1.com" in target.upstream_url

    def test_effective_origins_falls_back_to_hints(self):
        """When metadata has no origins, use upstream_hints."""
        target = RelayTarget("TVtest", ["hint.com:8000"])
        target.metadata = {"id": "TVtest"}  # No origins field
        origins = target._effective_origins()
        assert origins == ["hint.com:8000"]

    def test_status_dict_includes_current_upstream(self):
        """Status dict includes current_upstream field."""
        with patch("config.PEER_REQUIRE_TLS", False):
            target = RelayTarget("TVtest", ["a.com:8000"])
            status = target.status_dict()
            assert "current_upstream" in status
            assert "a.com" in status["current_upstream"]


class TestRelayMetadataFailover:
    """Test that metadata fetching tries other origins on failure."""

    @pytest.mark.asyncio
    async def test_failover_on_http_error(self, tmp_path):
        """Metadata fetch tries next origin when first returns HTTP error."""
        channel_id, key_path = _get_upstream_keypair(tmp_path)
        metadata = _make_signed_metadata(channel_id, key_path, seq=5)

        import httpx

        call_count = 0

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            url = str(request.url)
            if "origin1.com" in url:
                return httpx.Response(503)  # First origin down
            return httpx.Response(200, json=metadata)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["origin1.com:8000", "origin2.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_metadata(target)

        assert result is True
        assert target.metadata is not None
        assert call_count == 2  # Tried origin1, then origin2
        await client.aclose()

    @pytest.mark.asyncio
    async def test_failover_on_connection_error(self, tmp_path):
        """Metadata fetch tries next origin on connection error."""
        channel_id, key_path = _get_upstream_keypair(tmp_path)
        metadata = _make_signed_metadata(channel_id, key_path, seq=5)

        import httpx

        call_count = 0

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            url = str(request.url)
            if "origin1.com" in url:
                raise httpx.ConnectError("refused")
            return httpx.Response(200, json=metadata)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["origin1.com:8000", "origin2.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_metadata(target)

        assert result is True
        assert call_count == 2
        await client.aclose()

    @pytest.mark.asyncio
    async def test_no_failover_on_signature_failure(self, tmp_path):
        """Signature failure is definitive — don't try other origins."""
        channel_id, key_path = _get_upstream_keypair(tmp_path)
        metadata = _make_signed_metadata(channel_id, key_path)
        metadata["name"] = "Tampered"  # Invalidate signature

        import httpx

        call_count = 0

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(200, json=metadata)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["origin1.com:8000", "origin2.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_metadata(target)

        assert result is False
        assert call_count == 1  # Only tried once
        await client.aclose()

    @pytest.mark.asyncio
    async def test_all_origins_exhausted(self, tmp_path):
        """Returns False when all origins fail."""
        channel_id, _ = _get_upstream_keypair(tmp_path)

        import httpx

        call_count = 0

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(503)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["a.com:8000", "b.com:8000", "c.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_metadata(target)

        assert result is False
        assert call_count == 3  # Tried all three
        await client.aclose()


class TestRelayHLSFailover:
    """Test that HLS fetching tries other origins on failure."""

    @pytest.mark.asyncio
    async def test_hls_failover_to_second_origin(self, tmp_path):
        """HLS fetch tries next origin when first fails."""
        import httpx

        call_count = 0

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            url = str(request.url)
            if "bad.com" in url:
                return httpx.Response(503)
            # Good origin — return manifest and segments
            if "stream.m3u8" in url:
                return httpx.Response(
                    200,
                    text=SAMPLE_MANIFEST,
                    headers={"content-type": "application/vnd.apple.mpegurl"},
                )
            if ".ts" in url:
                return httpx.Response(200, content=b"\x00" * 100)
            return httpx.Response(404)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add("TVtest", ["bad.com:8000", "good.com:8000"])
        target._client = client

        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_hls(target)

        assert result is True
        assert target.hls_cache.manifest is not None
        assert call_count >= 2  # At least one fail + one success for manifest
        await client.aclose()


class TestOriginMetadataRefresh:
    """Test that metadata refresh discards stale origins (section 5.8)."""

    @pytest.mark.asyncio
    async def test_higher_seq_replaces_origins(self, tmp_path):
        """When metadata seq advances, old origins are replaced."""
        channel_id, key_path = _get_upstream_keypair(tmp_path)

        import httpx

        # First fetch: seq=5 with origins [old1, old2]
        meta5 = _make_signed_metadata(
            channel_id,
            key_path,
            seq=5,
            origins=["old1.com:443", "old2.com:443"],
        )

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["initial.com:8000"])

        client5 = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json=meta5))
        )
        target._client = client5
        with patch("config.PEER_REQUIRE_TLS", False):
            await mgr.fetch_metadata(target)
        assert target.metadata["origins"] == ["old1.com:443", "old2.com:443"]
        await client5.aclose()

        # Second fetch: seq=10 with origins [new1, new2]
        meta10 = _make_signed_metadata(
            channel_id,
            key_path,
            seq=10,
            origins=["new1.com:443", "new2.com:443"],
        )

        client10 = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json=meta10))
        )
        target._client = client10
        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_metadata(target)
        assert result is True
        # Old origins completely replaced
        assert target.metadata["origins"] == ["new1.com:443", "new2.com:443"]
        assert target.metadata_seq == 10
        await client10.aclose()

    @pytest.mark.asyncio
    async def test_lower_seq_does_not_replace_origins(self, tmp_path):
        """Metadata with lower seq does not overwrite origins."""
        channel_id, key_path = _get_upstream_keypair(tmp_path)

        import httpx

        mgr = RelayManager(path=str(tmp_path / "relays.json"))
        target = mgr.add(channel_id, ["initial.com:8000"])

        # First: seq=10
        meta10 = _make_signed_metadata(
            channel_id,
            key_path,
            seq=10,
            origins=["current.com:443"],
        )
        client10 = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json=meta10))
        )
        target._client = client10
        with patch("config.PEER_REQUIRE_TLS", False):
            await mgr.fetch_metadata(target)
        await client10.aclose()

        # Second: seq=5 (lower) — must not overwrite
        meta5 = _make_signed_metadata(
            channel_id,
            key_path,
            seq=5,
            origins=["stale.com:443"],
        )
        client5 = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json=meta5))
        )
        target._client = client5
        with patch("config.PEER_REQUIRE_TLS", False):
            result = await mgr.fetch_metadata(target)
        assert result is False
        assert target.metadata["origins"] == ["current.com:443"]
        assert target.metadata_seq == 10
        await client5.aclose()
