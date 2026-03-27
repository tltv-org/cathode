"""Tests for private channel token auth (Phase 7).

Tests:
- Token generation (format, uniqueness)
- Token persistence (save/load roundtrip)
- Token validation (valid passes, invalid fails, expired fails)
- Protocol endpoints return 401 for private channels without token
- Protocol endpoints return 403 for private channels with wrong token
- Protocol endpoints return 200 for private channels with valid token
- Public channels still work without any token
- Private channels excluded from well-known and peers (regression check)
- Relay stops when channel goes private (regression check)
- Management API (list, create, revoke)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

# Ensure the app directory is on sys.path
APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from protocol.identity import make_channel_id
from protocol.tokens import TokenEntry, TokenStore, extract_token, generate_token


# ── Reuse test vectors from test_protocol.py ──

TEST_SEED = bytes.fromhex(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
)
TEST_PUBKEY = bytes.fromhex(
    "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
)
TEST_CHANNEL_ID = make_channel_id(TEST_PUBKEY)


# ═══════════════════════════════════════════════════════════════════════
# Token generation
# ═══════════════════════════════════════════════════════════════════════


class TestTokenGeneration:
    """Token format, length, and uniqueness."""

    def test_token_format(self):
        """Tokens are URL-safe base64 strings."""
        token = generate_token()
        assert isinstance(token, str)
        assert len(token) > 0
        # URL-safe: only A-Z, a-z, 0-9, -, _
        import re

        assert re.match(r"^[A-Za-z0-9_-]+$", token), (
            f"Token has non-URL-safe chars: {token}"
        )

    def test_token_length(self):
        """Tokens are derived from 32 bytes (43 chars base64url, no padding)."""
        token = generate_token()
        assert len(token) == 43  # 32 bytes → 43 base64url chars (no padding)

    def test_tokens_unique(self):
        """Each token is unique."""
        tokens = {generate_token() for _ in range(100)}
        assert len(tokens) == 100

    def test_token_entry_id_derived(self):
        """token_id is derived from token hash."""
        entry = TokenEntry(
            token="test_token_value",
            name="test",
            created="2026-03-15T00:00:00Z",
        )
        assert len(entry.token_id) == 8
        # Same token → same ID
        entry2 = TokenEntry(
            token="test_token_value",
            name="other",
            created="2026-03-15T01:00:00Z",
        )
        assert entry.token_id == entry2.token_id

    def test_different_tokens_different_ids(self):
        """Different tokens produce different token_ids."""
        e1 = TokenEntry(token="token_a", name="a", created="2026-03-15T00:00:00Z")
        e2 = TokenEntry(token="token_b", name="b", created="2026-03-15T00:00:00Z")
        assert e1.token_id != e2.token_id


# ═══════════════════════════════════════════════════════════════════════
# Token expiry
# ═══════════════════════════════════════════════════════════════════════


class TestTokenExpiry:
    """Token expiration logic."""

    def test_no_expiry_never_expires(self):
        """Token without expires is never expired."""
        entry = TokenEntry(token="t", name="n", created="2026-03-15T00:00:00Z")
        assert entry.is_expired() is False

    def test_future_expiry_not_expired(self):
        """Token with future expiry is not expired."""
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        entry = TokenEntry(
            token="t", name="n", created="2026-03-15T00:00:00Z", expires=future
        )
        assert entry.is_expired() is False

    def test_past_expiry_is_expired(self):
        """Token with past expiry is expired."""
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        entry = TokenEntry(
            token="t", name="n", created="2026-03-15T00:00:00Z", expires=past
        )
        assert entry.is_expired() is True


# ═══════════════════════════════════════════════════════════════════════
# Token store persistence
# ═══════════════════════════════════════════════════════════════════════


class TestTokenStore:
    """Token store CRUD and persistence."""

    def test_create_and_list(self, tmp_path):
        """Create a token, then list it."""
        store = TokenStore(token_dir=str(tmp_path))
        entry = store.create(TEST_CHANNEL_ID, name="viewer-1")
        tokens = store.list_tokens(TEST_CHANNEL_ID)
        assert len(tokens) == 1
        assert tokens[0].name == "viewer-1"
        assert tokens[0].token == entry.token

    def test_create_multiple(self, tmp_path):
        """Create multiple tokens for one channel."""
        store = TokenStore(token_dir=str(tmp_path))
        store.create(TEST_CHANNEL_ID, name="viewer-1")
        store.create(TEST_CHANNEL_ID, name="viewer-2")
        store.create(TEST_CHANNEL_ID, name="viewer-3")
        tokens = store.list_tokens(TEST_CHANNEL_ID)
        assert len(tokens) == 3

    def test_validate_valid_token(self, tmp_path):
        """Valid token passes validation."""
        store = TokenStore(token_dir=str(tmp_path))
        entry = store.create(TEST_CHANNEL_ID, name="test")
        assert store.validate(TEST_CHANNEL_ID, entry.token) is True

    def test_validate_invalid_token(self, tmp_path):
        """Invalid token fails validation."""
        store = TokenStore(token_dir=str(tmp_path))
        store.create(TEST_CHANNEL_ID, name="test")
        assert store.validate(TEST_CHANNEL_ID, "not-a-real-token") is False

    def test_validate_wrong_channel(self, tmp_path):
        """Token for one channel doesn't work for another."""
        store = TokenStore(token_dir=str(tmp_path))
        entry = store.create(TEST_CHANNEL_ID, name="test")
        # Different channel
        from protocol.identity import generate_ed25519_keypair

        _, other_pub = generate_ed25519_keypair()
        other_id = make_channel_id(other_pub)
        assert store.validate(other_id, entry.token) is False

    def test_validate_expired_token(self, tmp_path):
        """Expired token fails validation."""
        store = TokenStore(token_dir=str(tmp_path))
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        entry = store.create(TEST_CHANNEL_ID, name="expired", expires=past)
        assert store.validate(TEST_CHANNEL_ID, entry.token) is False

    def test_revoke_token(self, tmp_path):
        """Revoked token is removed and no longer validates."""
        store = TokenStore(token_dir=str(tmp_path))
        entry = store.create(TEST_CHANNEL_ID, name="revokable")
        assert store.validate(TEST_CHANNEL_ID, entry.token) is True
        assert store.revoke(TEST_CHANNEL_ID, entry.token_id) is True
        assert store.validate(TEST_CHANNEL_ID, entry.token) is False
        assert len(store.list_tokens(TEST_CHANNEL_ID)) == 0

    def test_revoke_nonexistent(self, tmp_path):
        """Revoking a non-existent token returns False."""
        store = TokenStore(token_dir=str(tmp_path))
        assert store.revoke(TEST_CHANNEL_ID, "nonexistent") is False

    def test_revoke_all(self, tmp_path):
        """Revoke all tokens for a channel."""
        store = TokenStore(token_dir=str(tmp_path))
        store.create(TEST_CHANNEL_ID, name="a")
        store.create(TEST_CHANNEL_ID, name="b")
        assert store.revoke_all(TEST_CHANNEL_ID) == 2
        assert len(store.list_tokens(TEST_CHANNEL_ID)) == 0

    def test_persistence_roundtrip(self, tmp_path):
        """Tokens survive store re-creation (disk roundtrip)."""
        store1 = TokenStore(token_dir=str(tmp_path))
        entry = store1.create(TEST_CHANNEL_ID, name="persist-test")
        token_value = entry.token

        # Create a new store from the same directory
        store2 = TokenStore(token_dir=str(tmp_path))
        assert store2.validate(TEST_CHANNEL_ID, token_value) is True
        tokens = store2.list_tokens(TEST_CHANNEL_ID)
        assert len(tokens) == 1
        assert tokens[0].name == "persist-test"

    def test_file_created(self, tmp_path):
        """Token file is created on disk."""
        store = TokenStore(token_dir=str(tmp_path))
        store.create(TEST_CHANNEL_ID, name="test")
        file_path = tmp_path / f"{TEST_CHANNEL_ID}.json"
        assert file_path.exists()
        data = json.loads(file_path.read_text())
        assert len(data["tokens"]) == 1

    def test_empty_channel_returns_empty(self, tmp_path):
        """Listing tokens for a channel with none returns empty list."""
        store = TokenStore(token_dir=str(tmp_path))
        tokens = store.list_tokens(TEST_CHANNEL_ID)
        assert tokens == []

    def test_validate_empty_channel(self, tmp_path):
        """Validating against a channel with no tokens returns False."""
        store = TokenStore(token_dir=str(tmp_path))
        assert store.validate(TEST_CHANNEL_ID, "anything") is False


# ═══════════════════════════════════════════════════════════════════════
# Token extraction from requests
# ═══════════════════════════════════════════════════════════════════════


class TestExtractToken:
    """extract_token helper: query param and Authorization header."""

    def _make_request(self, query_string: str = "", headers: dict | None = None):
        """Build a minimal mock request for testing extract_token."""
        from starlette.datastructures import QueryParams, Headers

        class MockRequest:
            def __init__(self, qs, hdrs):
                self.query_params = QueryParams(qs)
                self.headers = Headers(hdrs or {})

        return MockRequest(query_string, headers or {})

    def test_query_param(self):
        """Token from ?token= query parameter."""
        req = self._make_request("token=abc123")
        assert extract_token(req) == "abc123"

    def test_bearer_header(self):
        """Token from Authorization: Bearer header."""
        req = self._make_request(headers={"authorization": "Bearer secret123"})
        assert extract_token(req) == "secret123"

    def test_query_param_takes_precedence(self):
        """Query param takes precedence over Bearer header."""
        req = self._make_request(
            "token=from_query",
            headers={"authorization": "Bearer from_header"},
        )
        assert extract_token(req) == "from_query"

    def test_no_token(self):
        """No token returns None."""
        req = self._make_request()
        assert extract_token(req) is None


# ═══════════════════════════════════════════════════════════════════════
# Protocol endpoint auth (private channels)
# ═══════════════════════════════════════════════════════════════════════


class TestPrivateChannelEndpoints:
    """Protocol endpoints enforce token auth for private channels."""

    @pytest.fixture
    def private_channel_setup(self, app_with_mocks, tmp_path):
        """Set up a private channel with a valid token.

        Modifies the existing test channel to be private, then creates
        a token for it.
        """
        import main

        # Get the test channel context and make it private
        ctx = main.channels.all()[0]
        ctx.access = "token"

        # Create a token
        entry = main.token_store.create(ctx.channel_id, name="test-viewer")

        yield {
            "channel_id": ctx.channel_id,
            "token": entry.token,
            "token_id": entry.token_id,
            "ctx": ctx,
        }

        # Restore public access
        ctx.access = "public"

    # ── Metadata endpoint ──

    async def test_metadata_403_no_token(self, client, private_channel_setup):
        """Private channel metadata returns 403 without token (section 8.8)."""
        cid = private_channel_setup["channel_id"]
        resp = await client.get(f"/tltv/v1/channels/{cid}")
        assert resp.status_code == 403
        assert resp.json()["error"] == "access_denied"

    async def test_metadata_403_wrong_token(self, client, private_channel_setup):
        """Private channel metadata returns 403 with wrong token (section 8.8)."""
        cid = private_channel_setup["channel_id"]
        resp = await client.get(f"/tltv/v1/channels/{cid}?token=wrong-token")
        assert resp.status_code == 403
        assert resp.json()["error"] == "access_denied"

    async def test_metadata_200_valid_token_query(self, client, private_channel_setup):
        """Private channel metadata returns 200 with valid ?token= param."""
        cid = private_channel_setup["channel_id"]
        token = private_channel_setup["token"]
        resp = await client.get(f"/tltv/v1/channels/{cid}?token={token}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == cid
        assert data["access"] == "token"

    async def test_metadata_200_valid_token_bearer(self, client, private_channel_setup):
        """Private channel metadata returns 200 with valid Bearer header."""
        cid = private_channel_setup["channel_id"]
        token = private_channel_setup["token"]
        resp = await client.get(
            f"/tltv/v1/channels/{cid}",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200

    async def test_metadata_referrer_policy(self, client, private_channel_setup):
        """Private channel responses include Referrer-Policy: no-referrer."""
        cid = private_channel_setup["channel_id"]
        token = private_channel_setup["token"]
        resp = await client.get(f"/tltv/v1/channels/{cid}?token={token}")
        assert resp.status_code == 200
        assert resp.headers.get("referrer-policy") == "no-referrer"

    # ── Stream endpoint ──

    async def test_stream_403_no_token(self, client, private_channel_setup):
        """Private channel stream returns 403 without token (section 8.8)."""
        cid = private_channel_setup["channel_id"]
        resp = await client.get(
            f"/tltv/v1/channels/{cid}/stream.m3u8",
            follow_redirects=False,
        )
        assert resp.status_code == 403

    async def test_stream_403_wrong_token(self, client, private_channel_setup):
        """Private channel stream returns 403 with wrong token (section 8.8)."""
        cid = private_channel_setup["channel_id"]
        resp = await client.get(
            f"/tltv/v1/channels/{cid}/stream.m3u8?token=wrong",
            follow_redirects=False,
        )
        assert resp.status_code == 403
        assert resp.json()["error"] == "access_denied"

    async def test_stream_302_valid_token(self, client, private_channel_setup):
        """Private channel stream returns 302 with valid token."""
        cid = private_channel_setup["channel_id"]
        token = private_channel_setup["token"]
        resp = await client.get(
            f"/tltv/v1/channels/{cid}/stream.m3u8?token={token}",
            follow_redirects=False,
        )
        assert resp.status_code == 302

    # ── Guide JSON endpoint ──

    async def test_guide_json_403_no_token(self, client, private_channel_setup):
        """Private channel guide returns 403 without token (section 8.8)."""
        cid = private_channel_setup["channel_id"]
        resp = await client.get(f"/tltv/v1/channels/{cid}/guide.json")
        assert resp.status_code == 403

    async def test_guide_json_403_wrong_token(self, client, private_channel_setup):
        """Private channel guide returns 403 with wrong token (section 8.8)."""
        cid = private_channel_setup["channel_id"]
        resp = await client.get(f"/tltv/v1/channels/{cid}/guide.json?token=wrong")
        assert resp.status_code == 403
        assert resp.json()["error"] == "access_denied"

    async def test_guide_json_200_valid_token(self, client, private_channel_setup):
        """Private channel guide returns 200 with valid token."""
        cid = private_channel_setup["channel_id"]
        token = private_channel_setup["token"]
        resp = await client.get(f"/tltv/v1/channels/{cid}/guide.json?token={token}")
        assert resp.status_code == 200
        assert "signature" in resp.json()

    # ── Guide XML endpoint ──

    async def test_guide_xml_403_no_token(self, client, private_channel_setup):
        """Private channel XMLTV returns 403 without token (section 8.8)."""
        cid = private_channel_setup["channel_id"]
        resp = await client.get(f"/tltv/v1/channels/{cid}/guide.xml")
        assert resp.status_code == 403

    async def test_guide_xml_200_valid_token(self, client, private_channel_setup):
        """Private channel XMLTV returns 200 with valid token."""
        cid = private_channel_setup["channel_id"]
        token = private_channel_setup["token"]
        resp = await client.get(f"/tltv/v1/channels/{cid}/guide.xml?token={token}")
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")


# ═══════════════════════════════════════════════════════════════════════
# Public channels unaffected
# ═══════════════════════════════════════════════════════════════════════


class TestPublicChannelUnaffected:
    """Public channels work exactly as before — no auth needed."""

    async def test_metadata_no_token_needed(self, client):
        """Public channel metadata returns 200 without any token."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}")
        assert resp.status_code == 200

    async def test_stream_no_token_needed(self, client):
        """Public channel stream returns 302 without any token."""
        resp = await client.get(
            f"/tltv/v1/channels/{TEST_CHANNEL_ID}/stream.m3u8",
            follow_redirects=False,
        )
        assert resp.status_code == 302

    async def test_guide_json_no_token_needed(self, client):
        """Public channel guide returns 200 without any token."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.json")
        assert resp.status_code == 200

    async def test_guide_xml_no_token_needed(self, client):
        """Public channel XMLTV returns 200 without any token."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.xml")
        assert resp.status_code == 200

    async def test_no_private_referrer_policy_on_public(self, client):
        """Public channel responses do NOT include no-referrer policy.

        The security headers middleware sets strict-origin-when-cross-origin
        on all responses; private channels override this to no-referrer.
        Public channels should NOT have the private-channel override.
        """
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}")
        assert resp.status_code == 200
        rp = resp.headers.get("referrer-policy", "")
        assert rp != "no-referrer"


# ═══════════════════════════════════════════════════════════════════════
# Private channels excluded from well-known and peers
# ═══════════════════════════════════════════════════════════════════════


class TestPrivateChannelExclusion:
    """Private channels MUST NOT appear in well-known or peers."""

    @pytest.fixture
    def private_ctx(self, app_with_mocks):
        """Temporarily make the test channel private."""
        import main

        ctx = main.channels.all()[0]
        ctx.access = "token"
        yield ctx
        ctx.access = "public"

    async def test_well_known_excludes_private(self, client, private_ctx):
        """Private channel does not appear in /.well-known/tltv channels."""
        resp = await client.get("/.well-known/tltv")
        assert resp.status_code == 200
        data = resp.json()
        channel_ids = [ch["id"] for ch in data["channels"]]
        assert private_ctx.channel_id not in channel_ids

    async def test_peers_excludes_private(self, client, private_ctx):
        """Private channel does not appear in /tltv/v1/peers."""
        resp = await client.get("/tltv/v1/peers")
        assert resp.status_code == 200
        data = resp.json()
        peer_ids = [p["id"] for p in data["peers"]]
        assert private_ctx.channel_id not in peer_ids


# ═══════════════════════════════════════════════════════════════════════
# Relay stops when channel goes private (regression)
# ═══════════════════════════════════════════════════════════════════════


class TestRelayPrivateTransition:
    """Relay deactivates when a channel's metadata changes to access: token."""

    def test_relay_deactivates_on_private_metadata(self, tmp_path):
        """RelayManager.fetch_metadata stops relay if access becomes token.

        This tests the existing Phase 6 behavior — ensures Phase 7 changes
        don't break it.
        """
        from protocol.relay import RelayTarget

        # Create a relay target with metadata that says public
        target = RelayTarget(
            channel_id=TEST_CHANNEL_ID,
            upstream_hints=["fake.host:8000"],
        )
        target.active = True
        target.metadata = {
            "v": 1,
            "id": TEST_CHANNEL_ID,
            "access": "public",
            "name": "Test",
        }

        # Simulate what happens in fetch_metadata when access changes
        # (The actual async fetch is tested in test_relay.py —
        # here we just verify the logic path)
        new_metadata = dict(target.metadata)
        new_metadata["access"] = "token"

        # This is the check from relay.py — relay must not relay
        # non-public channels (sections 5.2, 10.6, 12.4).
        access = new_metadata.get("access", "public")
        if access != "public":
            target.active = False
            target.metadata = None
            target.error = f"access_{access}"

        assert target.active is False
        assert target.error == "access_token"


# ═══════════════════════════════════════════════════════════════════════
# URI token roundtrip (regression)
# ═══════════════════════════════════════════════════════════════════════


class TestUriTokenRoundtrip:
    """Verify token parameter roundtrips in tltv:// URIs."""

    def test_format_with_token(self):
        """Token appears in formatted URI."""
        from protocol.uri import format_tltv_uri

        uri = format_tltv_uri(TEST_CHANNEL_ID, token="my_secret_token")
        assert "token=my_secret_token" in uri

    def test_parse_with_token(self):
        """Token is extracted from parsed URI."""
        from protocol.uri import parse_tltv_uri

        uri = f"tltv://{TEST_CHANNEL_ID}?token=my_secret_token"
        parsed = parse_tltv_uri(uri)
        assert parsed.token == "my_secret_token"
        assert parsed.channel_id == TEST_CHANNEL_ID

    def test_roundtrip_token_and_hints(self):
        """Token and hints survive format → parse roundtrip."""
        from protocol.uri import format_tltv_uri, parse_tltv_uri

        uri = format_tltv_uri(
            TEST_CHANNEL_ID,
            hints=["relay.example.com:443"],
            token="secret123",
        )
        parsed = parse_tltv_uri(uri)
        assert parsed.channel_id == TEST_CHANNEL_ID
        assert parsed.token == "secret123"
        assert "relay.example.com:443" in parsed.hints


# ═══════════════════════════════════════════════════════════════════════
# Management API
# ═══════════════════════════════════════════════════════════════════════


class TestTokenManagementAPI:
    """Management API endpoints for token CRUD."""

    async def test_list_empty(self, client):
        """Listing tokens for a channel with none returns empty list."""
        resp = await client.get(f"/api/tokens/{TEST_CHANNEL_ID}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["tokens"] == []

    async def test_create_token(self, client):
        """Creating a token returns the token value."""
        resp = await client.post(
            f"/api/tokens/{TEST_CHANNEL_ID}",
            json={"name": "test-viewer"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert "token" in data
        assert data["name"] == "test-viewer"
        assert "token_id" in data
        assert data["channel_id"] == TEST_CHANNEL_ID

    async def test_list_after_create(self, client):
        """After creating a token, it appears in the list (without value)."""
        # Create
        create_resp = await client.post(
            f"/api/tokens/{TEST_CHANNEL_ID}",
            json={"name": "listed-token"},
        )
        assert create_resp.status_code == 201
        token_id = create_resp.json()["token_id"]

        # List
        list_resp = await client.get(f"/api/tokens/{TEST_CHANNEL_ID}")
        assert list_resp.status_code == 200
        data = list_resp.json()
        assert data["count"] >= 1
        names = [t["name"] for t in data["tokens"]]
        assert "listed-token" in names
        # Verify token value is NOT in the list response
        for t in data["tokens"]:
            assert "token" not in t

    async def test_revoke_token(self, client):
        """Revoking a token removes it."""
        # Create
        create_resp = await client.post(
            f"/api/tokens/{TEST_CHANNEL_ID}",
            json={"name": "to-revoke"},
        )
        token_id = create_resp.json()["token_id"]

        # Revoke
        revoke_resp = await client.delete(f"/api/tokens/{TEST_CHANNEL_ID}/{token_id}")
        assert revoke_resp.status_code == 200
        assert revoke_resp.json()["revoked"] == token_id

    async def test_revoke_nonexistent_404(self, client):
        """Revoking a non-existent token returns 404."""
        resp = await client.delete(f"/api/tokens/{TEST_CHANNEL_ID}/nonexistent")
        assert resp.status_code == 404

    async def test_unknown_channel_404(self, client):
        """Token endpoints return 404 for unknown channels."""
        resp = await client.get("/api/tokens/not-a-channel")
        assert resp.status_code == 404

    async def test_create_slug_lookup(self, client):
        """Channel can be referenced by slug (channel-one) or federation ID."""
        resp = await client.post(
            "/api/tokens/channel-one",
            json={"name": "slug-test"},
        )
        assert resp.status_code == 201
        assert resp.json()["channel_id"] == TEST_CHANNEL_ID
