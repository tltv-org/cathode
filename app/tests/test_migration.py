"""Tests for key migration (PROTOCOL.md section 5.14).

Tests migration document signing and verification, the management API,
protocol endpoint behavior for migrated channels, and the spec test vector.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Ensure the app directory is on sys.path
APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from protocol.identity import make_channel_id
from protocol.signing import (
    canonical_json,
    sign_document,
    verify_document,
    verify_migration_document,
)


# ── Test keypairs (same as conftest.py) ──

# Old key: RFC 8032 test vector 1
OLD_SEED = bytes.fromhex(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
)
OLD_PUBKEY = bytes.fromhex(
    "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
)
OLD_CHANNEL_ID = "TVMkVHiXF9W1NgM9KLgs7tcBMvC1YtF4Daj4yfTrJercs3"

# New key (from test vector c7)
NEW_PUBKEY = bytes.fromhex(
    "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c"
)
NEW_CHANNEL_ID = "TVBNw4nHBzAaBWr8b17Sd2sGYcvMc1utersd6tceC6WmBZ"


def _old_key_path(tmp_path) -> str:
    """Write old private key seed to tmp file."""
    key_path = tmp_path / "old.key"
    key_path.write_bytes(OLD_SEED)
    return str(key_path)


def _make_migration_doc(key_path: str, **overrides) -> dict:
    """Build and sign a migration document."""
    doc = {
        "v": 1,
        "seq": overrides.pop("seq", 1742000000),
        "type": "migration",
        "from": overrides.pop("from_id", OLD_CHANNEL_ID),
        "to": overrides.pop("to_id", NEW_CHANNEL_ID),
        "migrated": overrides.pop("migrated", "2026-03-14T12:00:00Z"),
    }
    reason = overrides.pop("reason", None)
    if reason is not None:
        doc["reason"] = reason
    doc.update(overrides)
    return sign_document(doc, key_path)


# ── Test vector from c7-key-migration.json ──


class TestMigrationTestVector:
    """Verify against the official TLTV test vector (c7-key-migration.json)."""

    def test_canonical_json_length(self, tmp_path):
        """Canonical JSON of unsigned doc matches expected length."""
        doc = {
            "v": 1,
            "seq": 1742000000,
            "type": "migration",
            "from": OLD_CHANNEL_ID,
            "to": NEW_CHANNEL_ID,
            "reason": "key compromise",
            "migrated": "2026-03-14T12:00:00Z",
        }
        cj = canonical_json(doc)
        assert len(cj) == 213

    def test_signature_matches_test_vector(self, tmp_path):
        """Signature matches the expected base58 value from test vector."""
        key_path = _old_key_path(tmp_path)
        doc = {
            "v": 1,
            "seq": 1742000000,
            "type": "migration",
            "from": OLD_CHANNEL_ID,
            "to": NEW_CHANNEL_ID,
            "reason": "key compromise",
            "migrated": "2026-03-14T12:00:00Z",
        }
        signed = sign_document(doc, key_path)
        expected_sig = (
            "3Shcvdqrgb6Voi3mfPhKc77vbksDxcLGAbxKfDugQ3on"
            "q4DdagYeFPhb98DhLwCwrSrW7wtrxZF4GE8BxjHUinWA"
        )
        assert signed["signature"] == expected_sig

    def test_verify_test_vector(self, tmp_path):
        """Signed test vector document verifies correctly."""
        signed_doc = {
            "v": 1,
            "seq": 1742000000,
            "type": "migration",
            "from": OLD_CHANNEL_ID,
            "to": NEW_CHANNEL_ID,
            "reason": "key compromise",
            "migrated": "2026-03-14T12:00:00Z",
            "signature": (
                "3Shcvdqrgb6Voi3mfPhKc77vbksDxcLGAbxKfDugQ3on"
                "q4DdagYeFPhb98DhLwCwrSrW7wtrxZF4GE8BxjHUinWA"
            ),
        }
        assert verify_migration_document(signed_doc, OLD_CHANNEL_ID) is True


# ── verify_migration_document tests ──


class TestVerifyMigrationDocument:
    def test_valid_migration(self, tmp_path):
        key_path = _old_key_path(tmp_path)
        doc = _make_migration_doc(key_path, reason="key rotation")
        assert verify_migration_document(doc, OLD_CHANNEL_ID) is True

    def test_wrong_from_rejected(self, tmp_path):
        key_path = _old_key_path(tmp_path)
        doc = _make_migration_doc(key_path)
        assert verify_migration_document(doc, NEW_CHANNEL_ID) is False

    def test_tampered_to_rejected(self, tmp_path):
        key_path = _old_key_path(tmp_path)
        doc = _make_migration_doc(key_path)
        doc["to"] = OLD_CHANNEL_ID  # Tamper
        assert verify_migration_document(doc, OLD_CHANNEL_ID) is False

    def test_missing_signature_rejected(self, tmp_path):
        doc = {
            "v": 1,
            "seq": 1742000000,
            "type": "migration",
            "from": OLD_CHANNEL_ID,
            "to": NEW_CHANNEL_ID,
            "migrated": "2026-03-14T12:00:00Z",
        }
        assert verify_migration_document(doc, OLD_CHANNEL_ID) is False

    def test_wrong_type_rejected(self, tmp_path):
        key_path = _old_key_path(tmp_path)
        doc = _make_migration_doc(key_path)
        doc["type"] = "metadata"
        assert verify_migration_document(doc, OLD_CHANNEL_ID) is False

    def test_no_type_field_rejected(self, tmp_path):
        key_path = _old_key_path(tmp_path)
        doc = _make_migration_doc(key_path)
        del doc["type"]
        assert verify_migration_document(doc, OLD_CHANNEL_ID) is False

    def test_migration_without_reason(self, tmp_path):
        """Reason is optional — doc without reason is valid."""
        key_path = _old_key_path(tmp_path)
        doc = _make_migration_doc(key_path)
        assert "reason" not in doc  # No reason was passed
        assert verify_migration_document(doc, OLD_CHANNEL_ID) is True

    def test_missing_v_rejected(self, tmp_path):
        """Migration doc without v field is rejected."""
        key_path = _old_key_path(tmp_path)
        doc = {
            "seq": 1742000000,
            "type": "migration",
            "from": OLD_CHANNEL_ID,
            "to": NEW_CHANNEL_ID,
            "migrated": "2026-03-14T12:00:00Z",
        }
        sign_document(doc, key_path)
        assert verify_migration_document(doc, OLD_CHANNEL_ID) is False

    def test_wrong_v_rejected(self, tmp_path):
        """Migration doc with unsupported v is rejected."""
        key_path = _old_key_path(tmp_path)
        doc = _make_migration_doc(key_path)
        doc["v"] = 99
        assert verify_migration_document(doc, OLD_CHANNEL_ID) is False

    def test_missing_seq_rejected(self, tmp_path):
        """Migration doc without seq field is rejected."""
        key_path = _old_key_path(tmp_path)
        doc = {
            "v": 1,
            "type": "migration",
            "from": OLD_CHANNEL_ID,
            "to": NEW_CHANNEL_ID,
            "migrated": "2026-03-14T12:00:00Z",
        }
        sign_document(doc, key_path)
        assert verify_migration_document(doc, OLD_CHANNEL_ID) is False

    def test_regular_metadata_not_accepted(self, tmp_path):
        """verify_migration_document rejects regular metadata."""
        key_path = _old_key_path(tmp_path)
        metadata = {
            "v": 1,
            "seq": 1,
            "id": OLD_CHANNEL_ID,
            "name": "Test",
            "stream": "/tltv/v1/channels/x/stream.m3u8",
            "updated": "2026-03-14T12:00:00Z",
        }
        sign_document(metadata, key_path)
        assert verify_migration_document(metadata, OLD_CHANNEL_ID) is False


# ── Migration persistence tests ──


class TestMigrationPersistence:
    def test_save_and_load(self, tmp_path):
        from routes.migration import save_migration, load_migration

        with patch("config.KEY_DIR", str(tmp_path)):
            doc = {"type": "migration", "from": "A", "to": "B"}
            save_migration("channel-one", doc)
            loaded = load_migration("channel-one")
            assert loaded == doc

    def test_load_nonexistent(self, tmp_path):
        from routes.migration import load_migration

        with patch("config.KEY_DIR", str(tmp_path)):
            assert load_migration("nonexistent") is None

    def test_save_creates_directory(self, tmp_path):
        from routes.migration import save_migration

        nested = tmp_path / "keys" / "subdir"
        with patch("config.KEY_DIR", str(nested)):
            save_migration("test", {"type": "migration"})
            assert (nested / "test.migration.json").exists()


# ── Management API tests ──


@pytest.mark.asyncio
class TestMigrationManagementAPI:
    async def test_create_migration(self, client):
        """POST /api/migration/create succeeds with valid target."""
        import main

        resp = await client.post(
            "/api/migration/create",
            json={"to": NEW_CHANNEL_ID, "reason": "key rotation"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["migrated"] is True
        assert data["to"] == NEW_CHANNEL_ID
        doc = data["document"]
        assert doc["type"] == "migration"
        assert doc["v"] == 1
        assert isinstance(doc["seq"], int)
        assert doc["from"] == OLD_CHANNEL_ID
        assert doc["to"] == NEW_CHANNEL_ID
        assert doc["reason"] == "key rotation"
        assert "signature" in doc
        assert "migrated" in doc

        # Verify the signature
        assert verify_migration_document(doc, OLD_CHANNEL_ID) is True

        # Clean up — reset migration state
        ctx = main.channels.all()[0]
        ctx.migration = None

    async def test_create_migration_without_reason(self, client):
        """Reason is optional."""
        import main

        resp = await client.post(
            "/api/migration/create",
            json={"to": NEW_CHANNEL_ID},
        )
        assert resp.status_code == 200
        doc = resp.json()["document"]
        assert "reason" not in doc

        ctx = main.channels.all()[0]
        ctx.migration = None

    async def test_create_migration_already_migrated(self, client):
        """Cannot migrate a channel that's already migrated."""
        import main

        ctx = main.channels.all()[0]
        ctx.migration = {"type": "migration", "to": "TVxxx"}
        try:
            resp = await client.post(
                "/api/migration/create",
                json={"to": NEW_CHANNEL_ID},
            )
            assert resp.status_code == 409
            assert "already migrated" in resp.json()["detail"].lower()
        finally:
            ctx.migration = None

    async def test_create_migration_invalid_target(self, client):
        """Invalid target channel ID is rejected."""
        resp = await client.post(
            "/api/migration/create",
            json={"to": "not-a-valid-id"},
        )
        assert resp.status_code == 400
        assert "invalid target" in resp.json()["detail"].lower()

    async def test_create_migration_same_channel(self, client):
        """Cannot migrate to self."""
        import main

        ctx = main.channels.all()[0]
        resp = await client.post(
            "/api/migration/create",
            json={"to": ctx.channel_id},
        )
        assert resp.status_code == 400
        assert "itself" in resp.json()["detail"].lower()

    async def test_create_migration_reason_too_long(self, client):
        """Reason over 256 chars is rejected."""
        resp = await client.post(
            "/api/migration/create",
            json={"to": NEW_CHANNEL_ID, "reason": "x" * 257},
        )
        assert resp.status_code == 400
        assert "reason" in resp.json()["detail"].lower()

    async def test_list_migrations_empty(self, client):
        """GET /api/migration lists channels with migration status."""
        resp = await client.get("/api/migration")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["channels"]) >= 1
        assert data["channels"][0]["migrated"] is False

    async def test_list_migrations_after_migration(self, client):
        """Migrated channel shows in list."""
        import main

        ctx = main.channels.all()[0]
        ctx.migration = {
            "type": "migration",
            "from": OLD_CHANNEL_ID,
            "to": NEW_CHANNEL_ID,
            "migrated": "2026-03-14T12:00:00Z",
            "reason": "test",
        }
        try:
            resp = await client.get("/api/migration")
            data = resp.json()
            ch = data["channels"][0]
            assert ch["migrated"] is True
            assert ch["to"] == NEW_CHANNEL_ID
            assert ch["reason"] == "test"
        finally:
            ctx.migration = None


# ── Protocol endpoint behavior for migrated channels ──


def _migrate_channel(main_module):
    """Helper: set a migration document on the test channel."""
    ctx = main_module.channels.all()[0]
    key_path = ctx.private_key_path
    doc = {
        "v": 1,
        "seq": 1742000000,
        "type": "migration",
        "from": ctx.channel_id,
        "to": NEW_CHANNEL_ID,
        "reason": "key rotation",
        "migrated": "2026-03-14T12:00:00Z",
    }
    sign_document(doc, key_path)
    ctx.migration = doc
    return ctx


@pytest.mark.asyncio
class TestMigratedChannelMetadata:
    async def test_metadata_returns_migration_document(self, client):
        """GET /tltv/v1/channels/{id} returns migration doc, not metadata."""
        import main

        ctx = _migrate_channel(main)
        try:
            resp = await client.get(f"/tltv/v1/channels/{ctx.channel_id}")
            assert resp.status_code == 200
            doc = resp.json()
            assert doc["type"] == "migration"
            assert doc["from"] == ctx.channel_id
            assert doc["to"] == NEW_CHANNEL_ID
            assert "signature" in doc
            assert doc["v"] == 1
            assert "seq" in doc
            # Regular metadata fields should NOT be present
            assert "name" not in doc
            assert "stream" not in doc
        finally:
            ctx.migration = None

    async def test_migration_doc_is_signed(self, client):
        """Migration doc served by endpoint has valid signature."""
        import main

        ctx = _migrate_channel(main)
        try:
            resp = await client.get(f"/tltv/v1/channels/{ctx.channel_id}")
            doc = resp.json()
            assert verify_migration_document(doc, ctx.channel_id) is True
        finally:
            ctx.migration = None

    async def test_migration_has_cache_control(self, client):
        """Migration response has Cache-Control: max-age=60."""
        import main

        ctx = _migrate_channel(main)
        try:
            resp = await client.get(f"/tltv/v1/channels/{ctx.channel_id}")
            assert "max-age=60" in resp.headers.get("cache-control", "")
        finally:
            ctx.migration = None

    async def test_migration_requires_token_on_private(self, client):
        """Private channel migration still requires token (section 5.14).

        The spec says: "the token requirement on the metadata endpoint
        remains in effect" for private channels with migration docs.
        """
        import main

        ctx = _migrate_channel(main)
        ctx.access = "token"
        try:
            # Without token — should be 403
            resp = await client.get(f"/tltv/v1/channels/{ctx.channel_id}")
            assert resp.status_code == 403
            assert resp.json()["error"] == "access_denied"
        finally:
            ctx.migration = None
            ctx.access = "public"


@pytest.mark.asyncio
class TestMigratedChannelStream:
    async def test_stream_404_for_migrated(self, client):
        """GET stream.m3u8 returns 404 for migrated channel."""
        import main

        ctx = _migrate_channel(main)
        try:
            resp = await client.get(
                f"/tltv/v1/channels/{ctx.channel_id}/stream.m3u8",
                follow_redirects=False,
            )
            assert resp.status_code == 404
            assert resp.json()["error"] == "channel_migrated"
        finally:
            ctx.migration = None


@pytest.mark.asyncio
class TestMigratedChannelGuide:
    async def test_guide_json_404_for_migrated(self, client):
        """GET guide.json returns 404 for migrated channel."""
        import main

        ctx = _migrate_channel(main)
        try:
            resp = await client.get(f"/tltv/v1/channels/{ctx.channel_id}/guide.json")
            assert resp.status_code == 404
            assert resp.json()["error"] == "channel_migrated"
        finally:
            ctx.migration = None

    async def test_guide_xml_404_for_migrated(self, client):
        """GET guide.xml returns 404 for migrated channel."""
        import main

        ctx = _migrate_channel(main)
        try:
            resp = await client.get(f"/tltv/v1/channels/{ctx.channel_id}/guide.xml")
            assert resp.status_code == 404
            assert resp.json()["error"] == "channel_migrated"
        finally:
            ctx.migration = None


@pytest.mark.asyncio
class TestMigratedChannelExclusion:
    async def test_well_known_excludes_migrated(self, client):
        """/.well-known/tltv should not list a migrated channel."""
        import main

        ctx = _migrate_channel(main)
        try:
            resp = await client.get("/.well-known/tltv")
            data = resp.json()
            channel_ids = [ch["id"] for ch in data["channels"]]
            assert ctx.channel_id not in channel_ids
        finally:
            ctx.migration = None

    async def test_well_known_includes_non_migrated(self, client):
        """Non-migrated channel still appears in /.well-known/tltv."""
        import main

        ctx = main.channels.all()[0]
        assert ctx.migration is None
        resp = await client.get("/.well-known/tltv")
        data = resp.json()
        channel_ids = [ch["id"] for ch in data["channels"]]
        assert ctx.channel_id in channel_ids

    async def test_peers_excludes_migrated(self, client):
        """GET /tltv/v1/peers should not list a migrated channel."""
        import main

        ctx = _migrate_channel(main)
        try:
            resp = await client.get("/tltv/v1/peers")
            data = resp.json()
            peer_ids = [p["id"] for p in data["peers"]]
            assert ctx.channel_id not in peer_ids
        finally:
            ctx.migration = None


# ── Migration irreversibility ──


@pytest.mark.asyncio
class TestMigrationIrreversibility:
    async def test_cannot_double_migrate(self, client):
        """A second migration attempt returns 409."""
        import main

        ctx = _migrate_channel(main)
        try:
            resp = await client.post(
                "/api/migration/create",
                json={"to": NEW_CHANNEL_ID},
            )
            assert resp.status_code == 409
            assert "already migrated" in resp.json()["detail"].lower()
        finally:
            ctx.migration = None

    async def test_regular_metadata_not_served_after_migration(self, client):
        """After migration, metadata endpoint does not serve regular metadata."""
        import main

        ctx = _migrate_channel(main)
        try:
            resp = await client.get(f"/tltv/v1/channels/{ctx.channel_id}")
            doc = resp.json()
            # Must be migration doc, not regular metadata
            assert doc.get("type") == "migration"
            assert "name" not in doc
            # Migration docs now have seq (section 5.14) but not
            # regular metadata fields like name/stream
            assert "stream" not in doc
        finally:
            ctx.migration = None
