"""Tests for peer exchange subsystem (Phase 5).

Tests the peer store, validation, persistence, protocol endpoint,
and management API endpoints.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

# Ensure the app directory is on sys.path
APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from protocol.identity import make_channel_id, generate_ed25519_keypair
from protocol.signing import sign_document
from protocol.peers import PeerEntry, PeerStore, validate_peer, fetch_remote_peers


# ── Test fixtures ──

# Use the same test keypair as conftest.py
TEST_SEED = bytes.fromhex(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
)
TEST_PUBKEY = bytes.fromhex(
    "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
)
TEST_CHANNEL_ID = "TVMkVHiXF9W1NgM9KLgs7tcBMvC1YtF4Daj4yfTrJercs3"


def _make_peer_entry(suffix: int = 0, **kwargs) -> PeerEntry:
    """Create a PeerEntry with a unique channel ID."""
    _, pubkey = generate_ed25519_keypair()
    channel_id = kwargs.pop("id", make_channel_id(pubkey))
    return PeerEntry(
        id=channel_id,
        name=kwargs.pop("name", f"Test Channel {suffix}"),
        hints=kwargs.pop("hints", [f"node{suffix}.example.com:8000"]),
        last_seen=kwargs.pop(
            "last_seen",
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        ),
        verified=kwargs.pop("verified", True),
    )


# ── PeerStore unit tests ──


class TestPeerStore:
    """Tests for the in-memory peer store."""

    def test_add_and_retrieve(self, tmp_path):
        store = PeerStore(path=str(tmp_path / "peers.json"), max_peers=100)
        entry = _make_peer_entry(1)
        store.add(entry)
        assert entry.id in store
        assert len(store) == 1
        retrieved = store.get(entry.id)
        assert retrieved is not None
        assert retrieved.name == entry.name

    def test_remove(self, tmp_path):
        store = PeerStore(path=str(tmp_path / "peers.json"))
        entry = _make_peer_entry(1)
        store.add(entry)
        assert store.remove(entry.id) is True
        assert entry.id not in store
        assert len(store) == 0

    def test_remove_nonexistent(self, tmp_path):
        store = PeerStore(path=str(tmp_path / "peers.json"))
        assert store.remove("TVnonexistent") is False

    def test_update_existing_merges_hints(self, tmp_path):
        store = PeerStore(path=str(tmp_path / "peers.json"))
        _, pubkey = generate_ed25519_keypair()
        cid = make_channel_id(pubkey)
        entry1 = PeerEntry(id=cid, name="Ch", hints=["a.com:8000"])
        entry2 = PeerEntry(id=cid, name="Ch Updated", hints=["b.com:8000"])
        store.add(entry1)
        store.add(entry2)
        assert len(store) == 1
        retrieved = store.get(cid)
        assert "a.com:8000" in retrieved.hints
        assert "b.com:8000" in retrieved.hints
        assert retrieved.name == "Ch Updated"

    def test_eviction_at_capacity(self, tmp_path):
        """When full (max_peers=5), adding a new entry evicts the oldest."""
        store = PeerStore(path=str(tmp_path / "peers.json"), max_peers=5)
        entries = []
        for i in range(5):
            ts = (datetime.now(timezone.utc) - timedelta(hours=5 - i)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            e = _make_peer_entry(i, last_seen=ts)
            entries.append(e)
            store.add(e)

        assert len(store) == 5

        # Add one more — oldest (entry 0) should be evicted
        new_entry = _make_peer_entry(99)
        store.add(new_entry)
        assert len(store) == 5
        assert entries[0].id not in store
        assert new_entry.id in store

    def test_100_cap(self, tmp_path):
        """Store caps at 100 entries."""
        store = PeerStore(path=str(tmp_path / "peers.json"), max_peers=100)
        for i in range(105):
            store.add(_make_peer_entry(i))
        assert len(store) == 100

    def test_evict_stale(self, tmp_path):
        """Entries older than 7 days are evicted."""
        store = PeerStore(path=str(tmp_path / "peers.json"))
        fresh = _make_peer_entry(1)
        stale_ts = (datetime.now(timezone.utc) - timedelta(days=8)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        stale = _make_peer_entry(2, last_seen=stale_ts)

        store.add(fresh)
        store.add(stale)
        assert len(store) == 2

        evicted = store.evict_stale()
        assert evicted == 1
        assert len(store) == 1
        assert fresh.id in store
        assert stale.id not in store

    def test_public_peers_excludes_stale(self, tmp_path):
        """public_peers() omits stale entries."""
        store = PeerStore(path=str(tmp_path / "peers.json"))
        fresh = _make_peer_entry(1)
        stale_ts = (datetime.now(timezone.utc) - timedelta(days=8)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        stale = _make_peer_entry(2, last_seen=stale_ts)

        store.add(fresh)
        store.add(stale)

        public = store.public_peers()
        assert len(public) == 1
        assert public[0]["id"] == fresh.id

    def test_public_peers_excludes_specified_ids(self, tmp_path):
        store = PeerStore(path=str(tmp_path / "peers.json"))
        e1 = _make_peer_entry(1)
        e2 = _make_peer_entry(2)
        store.add(e1)
        store.add(e2)

        public = store.public_peers(exclude_ids={e1.id})
        assert len(public) == 1
        assert public[0]["id"] == e2.id

    def test_public_peers_excludes_unverified(self, tmp_path):
        """public_peers() omits unverified peers (spec section 8.6)."""
        store = PeerStore(path=str(tmp_path / "peers.json"))
        verified = _make_peer_entry(1, verified=True)
        unverified = _make_peer_entry(2, verified=False)
        store.add(verified)
        store.add(unverified)

        public = store.public_peers()
        assert len(public) == 1
        assert public[0]["id"] == verified.id

    def test_public_peers_all_unverified_returns_empty(self, tmp_path):
        """If all peers are unverified, public_peers() returns nothing."""
        store = PeerStore(path=str(tmp_path / "peers.json"))
        store.add(_make_peer_entry(1, verified=False))
        store.add(_make_peer_entry(2, verified=False))

        public = store.public_peers()
        assert len(public) == 0


class TestPeerPersistence:
    """Tests for save/load roundtrip."""

    def test_save_and_load(self, tmp_path):
        path = str(tmp_path / "peers.json")
        store1 = PeerStore(path=path)
        e1 = _make_peer_entry(1)
        e2 = _make_peer_entry(2)
        store1.add(e1)
        store1.add(e2)

        # Load into a new store
        store2 = PeerStore(path=path)
        assert len(store2) == 2
        assert e1.id in store2
        assert e2.id in store2
        assert store2.get(e1.id).name == e1.name

    def test_load_empty_file(self, tmp_path):
        """Loading from a non-existent path starts empty."""
        store = PeerStore(path=str(tmp_path / "nonexistent.json"))
        assert len(store) == 0

    def test_load_corrupt_file(self, tmp_path):
        """Loading corrupt JSON starts empty."""
        path = tmp_path / "peers.json"
        path.write_text("not json!!!")
        store = PeerStore(path=str(path))
        assert len(store) == 0

    def test_save_creates_directories(self, tmp_path):
        path = str(tmp_path / "deep" / "nested" / "peers.json")
        store = PeerStore(path=path)
        store.add(_make_peer_entry(1))
        assert Path(path).exists()


class TestPeerEntry:
    """Tests for PeerEntry serialization."""

    def test_to_dict_roundtrip(self):
        entry = _make_peer_entry(1)
        d = entry.to_dict()
        restored = PeerEntry.from_dict(d)
        assert restored.id == entry.id
        assert restored.name == entry.name
        assert restored.hints == entry.hints
        assert restored.last_seen == entry.last_seen
        assert restored.verified == entry.verified

    def test_default_last_seen(self):
        entry = PeerEntry(id="TVtest", name="Test", hints=[])
        assert entry.last_seen is not None
        # Should be a valid ISO timestamp
        assert "T" in entry.last_seen


# ── Peer validation tests ──


class TestPeerValidation:
    """Tests for validate_peer() — section 11.5."""

    @pytest.fixture
    def key_dir(self, tmp_path):
        key_path = tmp_path / "test.key"
        key_path.write_bytes(TEST_SEED)
        return tmp_path

    def _make_signed_metadata(self, key_dir) -> dict:
        """Build a properly signed metadata document."""
        doc = {
            "v": 1,
            "seq": 1,
            "id": TEST_CHANNEL_ID,
            "name": "Test Channel",
            "stream": f"/tltv/v1/channels/{TEST_CHANNEL_ID}/stream.m3u8",
            "access": "public",
            "updated": "2026-03-14T12:00:00Z",
        }
        return sign_document(doc, str(key_dir / "test.key"))

    @pytest.mark.asyncio
    async def test_validate_success(self, key_dir):
        """Successful validation returns metadata dict."""
        import httpx

        metadata = self._make_signed_metadata(key_dir)
        well_known = {
            "protocol": "tltv",
            "versions": [1],
            "channels": [{"id": TEST_CHANNEL_ID, "name": "Test Channel"}],
            "relaying": [],
        }

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            if "/.well-known/tltv" in str(request.url):
                return httpx.Response(200, json=well_known)
            elif f"/tltv/v1/channels/{TEST_CHANNEL_ID}" in str(request.url):
                return httpx.Response(200, json=metadata)
            return httpx.Response(404)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        result = await validate_peer(
            "node.example.com:8000",
            TEST_CHANNEL_ID,
            require_tls=False,
            client=client,
        )
        assert result is not None
        assert result["id"] == TEST_CHANNEL_ID
        await client.aclose()

    @pytest.mark.asyncio
    async def test_validate_channel_not_in_wellknown(self, key_dir):
        """Validation fails if channel not listed in well-known."""
        import httpx

        well_known = {
            "protocol": "tltv",
            "versions": [1],
            "channels": [],
            "relaying": [],
        }

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=well_known)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        result = await validate_peer(
            "node.example.com:8000",
            TEST_CHANNEL_ID,
            require_tls=False,
            client=client,
        )
        assert result is None
        await client.aclose()

    @pytest.mark.asyncio
    async def test_validate_bad_signature(self, key_dir):
        """Validation fails if metadata signature is invalid."""
        import httpx

        well_known = {
            "protocol": "tltv",
            "versions": [1],
            "channels": [{"id": TEST_CHANNEL_ID, "name": "Test Channel"}],
            "relaying": [],
        }
        bad_metadata = {
            "v": 1,
            "seq": 1,
            "id": TEST_CHANNEL_ID,
            "name": "Test Channel",
            "access": "public",
            "signature": "invalidsignaturedata",
        }

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            if "/.well-known/tltv" in str(request.url):
                return httpx.Response(200, json=well_known)
            elif f"/tltv/v1/channels/" in str(request.url):
                return httpx.Response(200, json=bad_metadata)
            return httpx.Response(404)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        result = await validate_peer(
            "node.example.com:8000",
            TEST_CHANNEL_ID,
            require_tls=False,
            client=client,
        )
        assert result is None
        await client.aclose()

    @pytest.mark.asyncio
    async def test_validate_retired_channel_rejected(self, key_dir):
        """Retired channels are not added to peer store."""
        import httpx

        doc = {
            "v": 1,
            "seq": 1,
            "id": TEST_CHANNEL_ID,
            "name": "Retired Channel",
            "stream": f"/tltv/v1/channels/{TEST_CHANNEL_ID}/stream.m3u8",
            "access": "public",
            "status": "retired",
            "updated": "2026-03-14T12:00:00Z",
        }
        retired_metadata = sign_document(doc, str(key_dir / "test.key"))

        well_known = {
            "protocol": "tltv",
            "versions": [1],
            "channels": [{"id": TEST_CHANNEL_ID, "name": "Retired Channel"}],
            "relaying": [],
        }

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            if "/.well-known/tltv" in str(request.url):
                return httpx.Response(200, json=well_known)
            elif f"/tltv/v1/channels/" in str(request.url):
                return httpx.Response(200, json=retired_metadata)
            return httpx.Response(404)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        result = await validate_peer(
            "node.example.com:8000",
            TEST_CHANNEL_ID,
            require_tls=False,
            client=client,
        )
        assert result is None
        await client.aclose()

    @pytest.mark.asyncio
    async def test_validate_private_channel_rejected(self, key_dir):
        """Private channels are not added to peer store."""
        import httpx

        metadata = self._make_signed_metadata(key_dir)
        # Re-sign with access: token
        doc = {
            "v": 1,
            "seq": 1,
            "id": TEST_CHANNEL_ID,
            "name": "Private Channel",
            "stream": f"/tltv/v1/channels/{TEST_CHANNEL_ID}/stream.m3u8",
            "access": "token",
            "updated": "2026-03-14T12:00:00Z",
        }
        private_metadata = sign_document(doc, str(key_dir / "test.key"))

        well_known = {
            "protocol": "tltv",
            "versions": [1],
            "channels": [{"id": TEST_CHANNEL_ID, "name": "Private Channel"}],
            "relaying": [],
        }

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            if "/.well-known/tltv" in str(request.url):
                return httpx.Response(200, json=well_known)
            elif f"/tltv/v1/channels/" in str(request.url):
                return httpx.Response(200, json=private_metadata)
            return httpx.Response(404)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        result = await validate_peer(
            "node.example.com:8000",
            TEST_CHANNEL_ID,
            require_tls=False,
            client=client,
        )
        assert result is None
        await client.aclose()

    @pytest.mark.asyncio
    async def test_validate_unreachable_silently_fails(self):
        """Unreachable nodes return None without raising."""
        import httpx

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("Connection refused")

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        result = await validate_peer(
            "unreachable.example.com:8000",
            TEST_CHANNEL_ID,
            require_tls=False,
            client=client,
        )
        assert result is None
        await client.aclose()

    @pytest.mark.asyncio
    async def test_validate_wellknown_404(self):
        """404 from well-known returns None."""
        import httpx

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        result = await validate_peer(
            "node.example.com:8000",
            TEST_CHANNEL_ID,
            require_tls=False,
            client=client,
        )
        assert result is None
        await client.aclose()


class TestFetchRemotePeers:
    """Tests for fetch_remote_peers()."""

    @pytest.mark.asyncio
    async def test_fetch_returns_peers(self):
        import httpx

        peer_data = {
            "peers": [
                {
                    "id": TEST_CHANNEL_ID,
                    "name": "Test",
                    "hints": ["a.com:8000"],
                    "last_seen": "2026-03-14T12:00:00Z",
                }
            ]
        }

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=peer_data)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        result = await fetch_remote_peers(
            "node.example.com:8000",
            require_tls=False,
            client=client,
        )
        assert len(result) == 1
        assert result[0]["id"] == TEST_CHANNEL_ID
        await client.aclose()

    @pytest.mark.asyncio
    async def test_fetch_failure_returns_empty(self):
        import httpx

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500)

        transport = httpx.MockTransport(mock_handler)
        client = httpx.AsyncClient(transport=transport)

        result = await fetch_remote_peers(
            "node.example.com:8000",
            require_tls=False,
            client=client,
        )
        assert result == []
        await client.aclose()


# ── Protocol endpoint tests (GET /tltv/v1/peers) ──


class TestPeersProtocolEndpoint:
    """Tests for GET /tltv/v1/peers with live peer store."""

    async def test_peers_includes_own_channels(self, client):
        """Our own public channels appear in the peer list."""
        resp = await client.get("/tltv/v1/peers")
        assert resp.status_code == 200
        data = resp.json()
        peer_ids = [p["id"] for p in data["peers"]]
        assert TEST_CHANNEL_ID in peer_ids

    async def test_peers_includes_store_peers(self, client):
        """Verified peers from the store appear in the response."""
        import main

        from protocol.peers import PeerStore, PeerEntry

        # Create and attach a peer store — peer must be verified (section 8.6)
        store = PeerStore(path="/dev/null", max_peers=100)
        _, pub = generate_ed25519_keypair()
        other_id = make_channel_id(pub)
        store.add(
            PeerEntry(
                id=other_id, name="Other", hints=["other.com:8000"], verified=True
            )
        )

        old_store = main.peer_store
        main.peer_store = store
        try:
            resp = await client.get("/tltv/v1/peers")
            data = resp.json()
            peer_ids = [p["id"] for p in data["peers"]]
            assert other_id in peer_ids
            assert TEST_CHANNEL_ID in peer_ids
        finally:
            main.peer_store = old_store

    async def test_peers_excludes_private_channels(self, client):
        """Private channels don't appear in peer list."""
        import main

        # Temporarily make our channel private
        ctx = main.channels.all()[0]
        old_access = ctx.access
        ctx.access = "token"
        try:
            resp = await client.get("/tltv/v1/peers")
            data = resp.json()
            peer_ids = [p["id"] for p in data["peers"]]
            assert TEST_CHANNEL_ID not in peer_ids
        finally:
            ctx.access = old_access

    async def test_peers_cache_control(self, client):
        """Cache-Control: max-age=300."""
        resp = await client.get("/tltv/v1/peers")
        assert resp.headers.get("cache-control") == "max-age=300"


# ── Management API tests ──


class TestPeerManagementAPI:
    """Tests for /api/peers/* management endpoints."""

    async def test_get_peers_empty(self, client):
        """GET /api/peers returns empty when no store."""
        resp = await client.get("/api/peers")
        assert resp.status_code == 200
        data = resp.json()
        assert "peers" in data

    async def test_get_peers_with_store(self, client):
        """GET /api/peers returns stored peers."""
        import main
        from protocol.peers import PeerStore, PeerEntry

        store = PeerStore(path="/dev/null")
        entry = _make_peer_entry(1)
        store.add(entry)

        old_store = main.peer_store
        main.peer_store = store
        try:
            resp = await client.get("/api/peers")
            data = resp.json()
            assert data["count"] == 1
            assert data["peers"][0]["id"] == entry.id
        finally:
            main.peer_store = old_store

    async def test_delete_peer(self, client):
        """DELETE /api/peers/{id} removes the peer."""
        import main
        from protocol.peers import PeerStore, PeerEntry

        store = PeerStore(path="/dev/null")
        entry = _make_peer_entry(1)
        store.add(entry)

        old_store = main.peer_store
        main.peer_store = store
        try:
            resp = await client.delete(f"/api/peers/{entry.id}")
            assert resp.status_code == 200
            assert resp.json()["removed"] == entry.id
            assert len(store) == 0
        finally:
            main.peer_store = old_store

    async def test_delete_nonexistent_peer(self, client):
        """DELETE /api/peers/{id} returns 404 for unknown peer."""
        import main
        from protocol.peers import PeerStore

        store = PeerStore(path="/dev/null")
        old_store = main.peer_store
        main.peer_store = store
        try:
            resp = await client.delete("/api/peers/TVnonexistent")
            assert resp.status_code == 404
        finally:
            main.peer_store = old_store
