"""Tests for TLTV federation protocol implementation.

Verifies against Appendix C test vectors from PROTOCOL.md.
Tests identity encoding, canonical JSON, signing/verification,
URI utilities, and protocol endpoints.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Ensure the app directory is on sys.path
APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from protocol.identity import (
    b58decode,
    b58encode,
    make_channel_id,
    parse_channel_id,
)
from protocol.signing import canonical_json, sign_document, verify_document
from protocol.uri import TltvUri, format_tltv_uri, parse_tltv_uri


# ── Appendix C.1 Test Vector: Channel Identity ──

# RFC 8032 section 7.1, test vector 1
TEST_SEED = bytes.fromhex(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
)
TEST_PUBKEY = bytes.fromhex(
    "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
)
TEST_CHANNEL_ID = "TVMkVHiXF9W1NgM9KLgs7tcBMvC1YtF4Daj4yfTrJercs3"

# Appendix C.2 test document
TEST_DOCUMENT = {
    "v": 1,
    "seq": 1,
    "id": TEST_CHANNEL_ID,
    "name": "Test Channel",
    "description": "A test channel for protocol verification",
    "stream": f"/tltv/v1/channels/{TEST_CHANNEL_ID}/stream.m3u8",
    "guide": f"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.json",
    "access": "public",
    "updated": "2026-03-14T12:00:00Z",
}

TEST_CANONICAL_JSON_LENGTH = 373
TEST_SIGNATURE_B58 = "4ga7qsWoJmM4dp8t8YbQoaCHFXAYpRCxfMmkz1s2UaC55quqV9pioXfnGRkxGhzcLZVRgYFr5bKV6F8oJV9U2esc"


class TestChannelIdentity:
    """Tests for PROTOCOL.md Appendix C.1 — channel ID encoding."""

    def test_make_channel_id_test_vector(self):
        """Appendix C.1: known pubkey produces known channel ID."""
        channel_id = make_channel_id(TEST_PUBKEY)
        assert channel_id == TEST_CHANNEL_ID

    def test_channel_id_starts_with_tv(self):
        """All V1 channel IDs start with 'TV'."""
        channel_id = make_channel_id(TEST_PUBKEY)
        assert channel_id.startswith("TV")

    def test_parse_channel_id_roundtrip(self):
        """Encoding then decoding returns the original pubkey."""
        channel_id = make_channel_id(TEST_PUBKEY)
        recovered = parse_channel_id(channel_id)
        assert recovered == TEST_PUBKEY

    def test_parse_channel_id_test_vector(self):
        """Appendix C.1: decode the known channel ID to the known pubkey."""
        pubkey = parse_channel_id(TEST_CHANNEL_ID)
        assert pubkey == TEST_PUBKEY

    def test_parse_channel_id_raw_bytes(self):
        """Decoding the channel ID gives 34 bytes (2 prefix + 32 pubkey)."""
        raw = b58decode(TEST_CHANNEL_ID)
        assert len(raw) == 34
        assert raw[:2] == b"\x14\x33"
        assert raw[2:] == TEST_PUBKEY

    def test_make_channel_id_wrong_length(self):
        """Reject pubkeys that aren't 32 bytes."""
        with pytest.raises(ValueError, match="32 bytes"):
            make_channel_id(b"\x00" * 31)
        with pytest.raises(ValueError, match="32 bytes"):
            make_channel_id(b"\x00" * 33)

    def test_parse_channel_id_bad_prefix(self):
        """Reject channel IDs with wrong version prefix."""
        # Encode with a wrong prefix
        bad_id = b58encode(b"\x14\x34" + TEST_PUBKEY)
        with pytest.raises(ValueError, match="Unknown version prefix"):
            parse_channel_id(bad_id)

    def test_parse_channel_id_wrong_length(self):
        """Reject channel IDs that decode to wrong number of bytes."""
        short_id = b58encode(b"\x14\x33" + b"\x00" * 16)
        with pytest.raises(ValueError, match="34 bytes"):
            parse_channel_id(short_id)

    def test_parse_channel_id_invalid_base58(self):
        """Reject strings with characters outside base58 alphabet."""
        with pytest.raises(ValueError, match="Invalid base58"):
            parse_channel_id("TV0OlI" + "x" * 40)  # 0, O, l, I not in base58


class TestCanonicalJson:
    """Tests for PROTOCOL.md section 4 — canonical JSON (JCS)."""

    def test_appendix_c2_canonical_length(self):
        """Appendix C.2: canonical form of test document is exactly 373 bytes."""
        result = canonical_json(TEST_DOCUMENT)
        assert len(result) == TEST_CANONICAL_JSON_LENGTH

    def test_appendix_c2_canonical_content(self):
        """Appendix C.2: canonical JSON matches expected byte-for-byte."""
        expected = (
            '{"access":"public",'
            '"description":"A test channel for protocol verification",'
            f'"guide":"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.json",'
            f'"id":"{TEST_CHANNEL_ID}",'
            '"name":"Test Channel",'
            '"seq":1,'
            f'"stream":"/tltv/v1/channels/{TEST_CHANNEL_ID}/stream.m3u8",'
            '"updated":"2026-03-14T12:00:00Z",'
            '"v":1}'
        )
        result = canonical_json(TEST_DOCUMENT)
        assert result == expected.encode("utf-8")

    def test_sorted_keys(self):
        """Keys are sorted lexicographically."""
        doc = {"z": 1, "a": 2, "m": 3}
        result = canonical_json(doc)
        assert result == b'{"a":2,"m":3,"z":1}'

    def test_no_whitespace(self):
        """No spaces or newlines in output."""
        doc = {"key": "value", "num": 42}
        result = canonical_json(doc)
        assert b" " not in result
        assert b"\n" not in result

    def test_nested_objects_sorted(self):
        """Nested object keys are also sorted."""
        doc = {"outer": {"z": 1, "a": 2}}
        result = canonical_json(doc)
        assert result == b'{"outer":{"a":2,"z":1}}'

    def test_utf8_encoding(self):
        """Output is UTF-8 bytes."""
        doc = {"text": "hello"}
        result = canonical_json(doc)
        assert isinstance(result, bytes)
        # Verify it's valid UTF-8
        result.decode("utf-8")

    def test_integers_no_quotes(self):
        """Integer values are not quoted."""
        doc = {"n": 42}
        result = canonical_json(doc)
        assert result == b'{"n":42}'

    def test_deterministic(self):
        """Same input always produces same output."""
        doc = {"b": 2, "a": 1, "c": 3}
        r1 = canonical_json(doc)
        r2 = canonical_json(doc)
        assert r1 == r2


class TestSigning:
    """Tests for PROTOCOL.md section 7 — Ed25519 signing."""

    @pytest.fixture
    def key_dir(self, tmp_path):
        """Create a temp directory with the test private key."""
        key_path = tmp_path / "test.key"
        key_path.write_bytes(TEST_SEED)
        return tmp_path

    def test_sign_document_adds_signature(self, key_dir):
        """sign_document adds a 'signature' field."""
        doc = dict(TEST_DOCUMENT)
        key_path = str(key_dir / "test.key")
        result = sign_document(doc, key_path)
        assert "signature" in result
        assert isinstance(result["signature"], str)

    def test_appendix_c2_signature(self, key_dir):
        """Appendix C.2: signing the test document produces the known signature."""
        doc = dict(TEST_DOCUMENT)
        key_path = str(key_dir / "test.key")
        result = sign_document(doc, key_path)
        assert result["signature"] == TEST_SIGNATURE_B58

    def test_verify_valid_signature(self, key_dir):
        """verify_document accepts a correctly signed document."""
        doc = dict(TEST_DOCUMENT)
        doc["signature"] = TEST_SIGNATURE_B58
        assert verify_document(doc, TEST_CHANNEL_ID) is True

    def test_verify_rejects_tampered_document(self, key_dir):
        """verify_document rejects a document with modified fields."""
        doc = dict(TEST_DOCUMENT)
        doc["signature"] = TEST_SIGNATURE_B58
        doc["name"] = "Tampered Name"  # Modify after signing
        assert verify_document(doc, TEST_CHANNEL_ID) is False

    def test_verify_rejects_wrong_channel_id(self, key_dir):
        """verify_document rejects when channel_id doesn't match doc.id."""
        doc = dict(TEST_DOCUMENT)
        doc["signature"] = TEST_SIGNATURE_B58
        # Use a different channel ID for verification
        from protocol.identity import generate_ed25519_keypair, make_channel_id

        _, other_pubkey = generate_ed25519_keypair()
        other_id = make_channel_id(other_pubkey)
        assert verify_document(doc, other_id) is False

    def test_verify_rejects_missing_signature(self):
        """verify_document rejects documents without a signature."""
        doc = dict(TEST_DOCUMENT)
        # No signature field
        assert verify_document(doc, TEST_CHANNEL_ID) is False

    def test_verify_rejects_bad_signature(self):
        """verify_document rejects documents with garbage signature."""
        doc = dict(TEST_DOCUMENT)
        doc["signature"] = "notavalidsignature"
        assert verify_document(doc, TEST_CHANNEL_ID) is False

    def test_sign_then_verify_roundtrip(self, key_dir):
        """sign + verify round-trip works with any document."""
        doc = {
            "v": 1,
            "seq": 42,
            "id": TEST_CHANNEL_ID,
            "name": "Round Trip Test",
            "updated": "2026-03-15T00:00:00Z",
        }
        key_path = str(key_dir / "test.key")
        signed = sign_document(doc, key_path)
        assert verify_document(signed, TEST_CHANNEL_ID) is True

    def test_sign_removes_existing_signature(self, key_dir):
        """sign_document ignores any pre-existing signature field."""
        doc = dict(TEST_DOCUMENT)
        doc["signature"] = "old_garbage_signature"
        key_path = str(key_dir / "test.key")
        result = sign_document(doc, key_path)
        # Should produce the correct signature, not based on old one
        assert result["signature"] == TEST_SIGNATURE_B58


class TestSeqCounter:
    """Tests for sequence counter persistence.

    Seq values are Unix epoch timestamps (spec section 5.5).
    next_seq() uses max(current_unix_time, last_seq + 1).
    """

    def test_next_seq_returns_epoch_timestamp(self, tmp_path):
        """First call returns approximately the current Unix timestamp."""
        import time

        import protocol.signing as signing_mod

        old_dir = signing_mod.SEQ_DIR
        signing_mod.SEQ_DIR = str(tmp_path)
        try:
            before = int(time.time())
            seq = signing_mod.next_seq("TVtest123", "metadata")
            after = int(time.time())
            assert before <= seq <= after
        finally:
            signing_mod.SEQ_DIR = old_dir

    def test_next_seq_monotonic(self, tmp_path):
        """Successive calls return non-decreasing timestamps."""
        import protocol.signing as signing_mod

        old_dir = signing_mod.SEQ_DIR
        signing_mod.SEQ_DIR = str(tmp_path)
        try:
            s1 = signing_mod.next_seq("TVtest123", "metadata")
            s2 = signing_mod.next_seq("TVtest123", "metadata")
            s3 = signing_mod.next_seq("TVtest123", "metadata")
            assert s1 <= s2 <= s3
            # Within the same second, each call must still increment
            assert s2 >= s1 + 1 or s2 > s1
        finally:
            signing_mod.SEQ_DIR = old_dir

    def test_seq_persists_to_file(self, tmp_path):
        """Counter is written to a file on disk as an epoch timestamp."""
        import time

        import protocol.signing as signing_mod

        old_dir = signing_mod.SEQ_DIR
        signing_mod.SEQ_DIR = str(tmp_path)
        try:
            signing_mod.next_seq("TVtest123", "guide")
            seq2 = signing_mod.next_seq("TVtest123", "guide")
            seq_file = tmp_path / "TVtest123-guide.seq"
            assert seq_file.exists()
            persisted = int(seq_file.read_text().strip())
            assert persisted == seq2
            # Must be a reasonable epoch timestamp (after 2020)
            assert persisted > 1577836800
        finally:
            signing_mod.SEQ_DIR = old_dir

    def test_separate_counters_per_type(self, tmp_path):
        """metadata and guide have independent counters."""
        import protocol.signing as signing_mod

        old_dir = signing_mod.SEQ_DIR
        signing_mod.SEQ_DIR = str(tmp_path)
        try:
            m1 = signing_mod.next_seq("TVtest123", "metadata")
            m2 = signing_mod.next_seq("TVtest123", "metadata")
            g1 = signing_mod.next_seq("TVtest123", "guide")
            # Both should be epoch timestamps
            assert m1 > 1577836800
            assert g1 > 1577836800
            # metadata counter progressed, guide is independent
            assert m2 >= m1
        finally:
            signing_mod.SEQ_DIR = old_dir

    def test_validate_seq_accepts_current(self, tmp_path):
        """validate_seq accepts timestamps near current time."""
        import time

        from protocol.signing import validate_seq

        now = int(time.time())
        assert validate_seq(now) is True
        assert validate_seq(now - 1000) is True
        assert validate_seq(now + 3599) is True

    def test_validate_seq_rejects_future(self, tmp_path):
        """validate_seq rejects timestamps > 3600s in the future."""
        import time

        from protocol.signing import validate_seq

        now = int(time.time())
        assert validate_seq(now + 3601) is False
        assert validate_seq(now + 7200) is False


# ── Phase 2: URI Utilities ──


class TestTltvUri:
    """Tests for PROTOCOL.md section 3 — tltv:// URI scheme."""

    def test_format_basic(self):
        """Basic URI with just a channel ID."""
        uri = format_tltv_uri(TEST_CHANNEL_ID)
        assert uri == f"tltv://{TEST_CHANNEL_ID}"

    def test_format_with_hints(self):
        """URI with single hint uses @hint syntax (spec section 3.1)."""
        uri = format_tltv_uri(TEST_CHANNEL_ID, hints=["relay.example.com:8443"])
        assert uri == f"tltv://{TEST_CHANNEL_ID}@relay.example.com:8443"

    def test_format_with_multiple_hints(self):
        """URI with multiple hints: first in @, rest in via=."""
        uri = format_tltv_uri(
            TEST_CHANNEL_ID, hints=["relay1.example.com:443", "192.168.1.100:8000"]
        )
        assert f"@relay1.example.com:443" in uri
        assert "via=192.168.1.100:8000" in uri

    def test_format_with_token(self):
        """URI with access token."""
        uri = format_tltv_uri(TEST_CHANNEL_ID, token="abc123")
        assert "token=abc123" in uri

    def test_format_with_token_and_hints(self):
        """URI with token — all hints in via= (no @hint when token present)."""
        uri = format_tltv_uri(
            TEST_CHANNEL_ID,
            hints=["relay.example.com:443"],
            token="secret",
        )
        assert "token=secret" in uri
        assert "via=relay.example.com:443" in uri
        # When token is present, no @hint syntax
        assert "@" not in uri.split("?")[0]

    def test_parse_basic(self):
        """Parse a basic tltv:// URI."""
        result = parse_tltv_uri(f"tltv://{TEST_CHANNEL_ID}")
        assert result.channel_id == TEST_CHANNEL_ID
        assert result.hints == []
        assert result.token is None

    def test_parse_with_at_hint(self):
        """Parse URI with @host:port hint (public address)."""
        result = parse_tltv_uri(f"tltv://{TEST_CHANNEL_ID}@relay.example.com:8000")
        assert result.channel_id == TEST_CHANNEL_ID
        assert result.hints == ["relay.example.com:8000"]

    def test_parse_private_hint_filtered(self):
        """Private-network hints excluded by default (section 3.1)."""
        result = parse_tltv_uri(f"tltv://{TEST_CHANNEL_ID}@192.168.1.100:8000")
        assert result.channel_id == TEST_CHANNEL_ID
        assert result.hints == []

    def test_parse_private_hint_allowed(self):
        """Private-network hints kept when allow_private_hints=True."""
        result = parse_tltv_uri(
            f"tltv://{TEST_CHANNEL_ID}@192.168.1.100:8000",
            allow_private_hints=True,
        )
        assert result.channel_id == TEST_CHANNEL_ID
        assert result.hints == ["192.168.1.100:8000"]

    def test_parse_duplicate_query_param_first_wins(self):
        """Duplicate query params use first occurrence (section 3.1)."""
        result = parse_tltv_uri(f"tltv://{TEST_CHANNEL_ID}?token=first&token=second")
        assert result.token == "first"

    def test_parse_ipv6_hint_bracketed(self):
        """IPv6 hints must be bracketed (section 3.1)."""
        result = parse_tltv_uri(
            f"tltv://{TEST_CHANNEL_ID}?via=[2001:db8::1]:8443",
            allow_private_hints=True,
        )
        assert "[2001:db8::1]:8443" in result.hints

    def test_parse_loopback_hint_filtered(self):
        """Loopback hints excluded by default (section 3.1)."""
        result = parse_tltv_uri(f"tltv://{TEST_CHANNEL_ID}?via=127.0.0.1:8000")
        assert result.hints == []

    def test_parse_with_via_hints(self):
        """Parse URI with ?via= hints."""
        uri = f"tltv://{TEST_CHANNEL_ID}?via=relay1.example.com:443,relay2.example.com:8443"
        result = parse_tltv_uri(uri)
        assert result.channel_id == TEST_CHANNEL_ID
        assert "relay1.example.com:443" in result.hints
        assert "relay2.example.com:8443" in result.hints

    def test_parse_with_token(self):
        """Parse URI with ?token=."""
        result = parse_tltv_uri(f"tltv://{TEST_CHANNEL_ID}?token=abc123")
        assert result.channel_id == TEST_CHANNEL_ID
        assert result.token == "abc123"

    def test_parse_preserves_case(self):
        """Channel ID case is preserved (section 2.5)."""
        # Use an ID with mixed case
        result = parse_tltv_uri(f"tltv://{TEST_CHANNEL_ID}")
        assert result.channel_id == TEST_CHANNEL_ID
        # Verify exact case matches
        assert "MkVH" in result.channel_id

    def test_parse_wrong_scheme(self):
        """Reject non-tltv:// schemes."""
        with pytest.raises(ValueError, match="tltv://"):
            parse_tltv_uri("https://example.com")

    def test_parse_empty_channel_id(self):
        """Reject URI with no channel ID."""
        with pytest.raises(ValueError, match="[Mm]issing"):
            parse_tltv_uri("tltv://")

    def test_roundtrip(self):
        """Format then parse gives back the same components."""
        original = format_tltv_uri(
            TEST_CHANNEL_ID,
            hints=["relay.example.com:8443"],
            token="tok123",
        )
        parsed = parse_tltv_uri(original)
        assert parsed.channel_id == TEST_CHANNEL_ID
        assert "relay.example.com:8443" in parsed.hints
        assert parsed.token == "tok123"


# ── Phase 2: Protocol Endpoint Tests ──


class TestWellKnown:
    """Tests for GET /.well-known/tltv (section 8.1)."""

    async def test_well_known_returns_protocol_info(self, client):
        """Response includes protocol, versions, channels, relaying."""
        resp = await client.get("/.well-known/tltv")
        assert resp.status_code == 200
        data = resp.json()
        assert data["protocol"] == "tltv"
        assert 1 in data["versions"]
        assert isinstance(data["channels"], list)
        assert isinstance(data["relaying"], list)

    async def test_well_known_lists_channel(self, client):
        """Our test channel appears in the channels list."""
        resp = await client.get("/.well-known/tltv")
        data = resp.json()
        assert len(data["channels"]) >= 1
        ch = data["channels"][0]
        assert ch["id"] == TEST_CHANNEL_ID
        assert ch["name"] == "TLTV Channel One"

    async def test_well_known_cache_control(self, client):
        """Cache-Control: max-age=60."""
        resp = await client.get("/.well-known/tltv")
        assert resp.headers.get("cache-control") == "max-age=60"

    async def test_well_known_relaying_empty(self, client):
        """relaying is empty (we don't relay anything)."""
        resp = await client.get("/.well-known/tltv")
        assert resp.json()["relaying"] == []

    async def test_well_known_excludes_retired(self, client):
        """Retired channels excluded from well-known."""
        import main

        ctx = main.channels.all()[0]
        old_status = ctx.status
        ctx.status = "retired"
        try:
            resp = await client.get("/.well-known/tltv")
            data = resp.json()
            channel_ids = [c["id"] for c in data["channels"]]
            assert TEST_CHANNEL_ID not in channel_ids
        finally:
            ctx.status = old_status


class TestChannelMetadata:
    """Tests for GET /tltv/v1/channels/{id} (section 8.2)."""

    async def test_metadata_returns_signed_document(self, client):
        """Response is a signed metadata document."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["v"] == 1
        assert data["id"] == TEST_CHANNEL_ID
        assert data["name"] == "TLTV Channel One"
        assert "signature" in data
        assert "seq" in data
        assert "updated" in data
        assert "stream" in data

    async def test_metadata_signature_valid(self, client):
        """Signature verifies against the channel ID."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}")
        data = resp.json()
        assert verify_document(data, TEST_CHANNEL_ID) is True

    async def test_metadata_includes_optional_fields(self, client):
        """Optional metadata fields from config are included."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}")
        data = resp.json()
        assert data.get("description") == "24/7 experimental television"
        assert data.get("language") == "en"
        assert data.get("tags") == ["experimental", "generative"]
        assert data.get("access") == "public"

    async def test_metadata_stream_path(self, client):
        """stream field points to the protocol stream path."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}")
        data = resp.json()
        assert data["stream"] == f"/tltv/v1/channels/{TEST_CHANNEL_ID}/stream.m3u8"

    async def test_metadata_guide_path(self, client):
        """guide field points to the protocol guide path."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}")
        data = resp.json()
        assert data["guide"] == f"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.json"

    async def test_metadata_cache_control(self, client):
        """Cache-Control: max-age=60."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}")
        assert resp.headers.get("cache-control") == "max-age=60"

    async def test_metadata_404_unknown_channel(self, client):
        """Unknown channel returns 404 with structured error."""
        # Generate a valid but unknown channel ID
        from protocol.identity import generate_ed25519_keypair, make_channel_id

        _, pubkey = generate_ed25519_keypair()
        unknown_id = make_channel_id(pubkey)
        resp = await client.get(f"/tltv/v1/channels/{unknown_id}")
        assert resp.status_code == 404
        assert resp.json()["error"] == "channel_not_found"

    async def test_metadata_includes_timezone(self, client):
        """Metadata includes timezone field from channel config (section 5.12)."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}")
        data = resp.json()
        assert data.get("timezone") == "America/New_York"
        # Timezone must be included in signed document (signature still valid)
        assert verify_document(data, TEST_CHANNEL_ID) is True

    async def test_metadata_seq_is_epoch(self, client):
        """Metadata seq is a Unix epoch timestamp (section 5.5)."""
        import time

        before = int(time.time())
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}")
        after = int(time.time())
        data = resp.json()
        seq = data["seq"]
        assert before <= seq <= after, f"seq {seq} not between {before} and {after}"

    async def test_metadata_status_retired(self, client):
        """Retired channel includes status field in signed metadata."""
        import main

        ctx = main.channels.all()[0]
        old_status = ctx.status
        ctx.status = "retired"
        try:
            resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}")
            data = resp.json()
            assert data["status"] == "retired"
            assert verify_document(data, TEST_CHANNEL_ID) is True
        finally:
            ctx.status = old_status

    async def test_metadata_status_active_omitted(self, client):
        """Active channels don't include status field (it's the default)."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}")
        data = resp.json()
        assert "status" not in data

    async def test_metadata_400_bad_format(self, client):
        """Malformed channel ID returns 400 with structured error."""
        resp = await client.get("/tltv/v1/channels/not-a-valid-id")
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_request"


class TestPeers:
    """Tests for GET /tltv/v1/peers (section 8.6)."""

    async def test_peers_includes_own_channels(self, client):
        """Our own public channels appear in the peer list."""
        resp = await client.get("/tltv/v1/peers")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data["peers"], list)
        peer_ids = [p["id"] for p in data["peers"]]
        assert TEST_CHANNEL_ID in peer_ids

    async def test_peers_cache_control(self, client):
        """Cache-Control: max-age=300."""
        resp = await client.get("/tltv/v1/peers")
        assert resp.headers.get("cache-control") == "max-age=300"

    async def test_peers_excludes_retired(self, client):
        """Retired channels excluded from peer exchange."""
        import main

        ctx = main.channels.all()[0]
        old_status = ctx.status
        ctx.status = "retired"
        try:
            resp = await client.get("/tltv/v1/peers")
            data = resp.json()
            peer_ids = [p["id"] for p in data["peers"]]
            assert TEST_CHANNEL_ID not in peer_ids
        finally:
            ctx.status = old_status


# ── Phase 3: Guide Federation ──


class TestSignedGuideJson:
    """Tests for GET /tltv/v1/channels/{id}/guide.json (section 8.4)."""

    async def test_guide_returns_signed_document(self, client):
        """Response is a signed guide document with protocol envelope."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.json")
        assert resp.status_code == 200
        data = resp.json()
        assert data["v"] == 1
        assert data["id"] == TEST_CHANNEL_ID
        assert "seq" in data
        assert "from" in data
        assert "until" in data
        assert "entries" in data
        assert "updated" in data
        assert "signature" in data

    async def test_guide_signature_valid(self, client):
        """Guide signature verifies against the channel ID."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.json")
        data = resp.json()
        assert verify_document(data, TEST_CHANNEL_ID) is True

    async def test_guide_entries_use_end_not_stop(self, client):
        """Entries use 'end' field (not 'stop') per spec section 6.3."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.json")
        data = resp.json()
        for entry in data.get("entries", []):
            assert "end" in entry, "Entry should have 'end' field"
            assert "stop" not in entry, "Entry should not have 'stop' field"

    async def test_guide_times_utc(self, client):
        """from/until fields are UTC ISO 8601 with Z suffix (sections 6.1-6.5)."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.json")
        data = resp.json()
        from_str = data["from"]
        until_str = data["until"]
        assert "T" in from_str
        assert from_str.endswith("Z"), f"from must use Z suffix, got: {from_str}"
        assert until_str.endswith("Z"), f"until must use Z suffix, got: {until_str}"

    async def test_guide_cache_control(self, client):
        """Cache-Control: max-age=300."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.json")
        assert resp.headers.get("cache-control") == "max-age=300"

    async def test_guide_404_unknown_channel(self, client):
        """Unknown channel returns 404."""
        from protocol.identity import generate_ed25519_keypair, make_channel_id

        _, pubkey = generate_ed25519_keypair()
        unknown_id = make_channel_id(pubkey)
        resp = await client.get(f"/tltv/v1/channels/{unknown_id}/guide.json")
        assert resp.status_code == 404
        assert resp.json()["error"] == "channel_not_found"

    async def test_guide_seq_is_epoch(self, client):
        """Guide seq is a Unix epoch timestamp (section 5.5)."""
        import time

        before = int(time.time())
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.json")
        after = int(time.time())
        data = resp.json()
        seq = data["seq"]
        assert before <= seq <= after, f"seq {seq} not between {before} and {after}"

    async def test_guide_entry_times_utc(self, client):
        """Guide entry start/end times use Z suffix (sections 6.1-6.5)."""
        # Write a program so entries exist
        import json as json_mod

        import program as prog_mod
        from datetime import date as date_cls
        from pathlib import Path as P
        from zoneinfo import ZoneInfo

        today = date_cls.today()
        prog_dir = (
            P(prog_mod.PROGRAM_DIR)
            / "channel-one"
            / str(today.year)
            / f"{today.month:02d}"
        )
        prog_dir.mkdir(parents=True, exist_ok=True)
        prog = {
            "date": today.isoformat(),
            "blocks": [
                {
                    "start": "19:00:00",
                    "end": "20:00:00",
                    "type": "playlist",
                    "title": "Test",
                },
            ],
            "created": "test",
        }
        (prog_dir / f"{today.isoformat()}.json").write_text(json_mod.dumps(prog))

        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.json")
        data = resp.json()
        assert len(data["entries"]) >= 1
        entry = data["entries"][0]
        assert entry["start"].endswith("Z"), (
            f"entry start must use Z suffix: {entry['start']}"
        )
        assert entry["end"].endswith("Z"), (
            f"entry end must use Z suffix: {entry['end']}"
        )

    async def test_guide_400_bad_format(self, client):
        """Bad channel ID format returns 400."""
        resp = await client.get("/tltv/v1/channels/bad-id/guide.json")
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_request"


class TestProtocolGuideXml:
    """Tests for GET /tltv/v1/channels/{id}/guide.xml (section 8.5)."""

    async def test_xmltv_at_protocol_path(self, client):
        """XMLTV is served at the protocol path."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.xml")
        assert resp.status_code == 200
        assert "application/xml" in resp.headers.get("content-type", "")

    async def test_xmltv_uses_federation_channel_id(self, client):
        """XMLTV <channel id="..."> uses TV-prefixed federation ID."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.xml")
        content = resp.text
        assert f'id="{TEST_CHANNEL_ID}"' in content

    async def test_xmltv_cache_control(self, client):
        """Cache-Control: max-age=300."""
        resp = await client.get(f"/tltv/v1/channels/{TEST_CHANNEL_ID}/guide.xml")
        assert resp.headers.get("cache-control") == "max-age=300"

    async def test_xmltv_404_unknown_channel(self, client):
        """Unknown channel returns 404."""
        from protocol.identity import generate_ed25519_keypair, make_channel_id

        _, pubkey = generate_ed25519_keypair()
        unknown_id = make_channel_id(pubkey)
        resp = await client.get(f"/tltv/v1/channels/{unknown_id}/guide.xml")
        assert resp.status_code == 404


class TestManagementGuideXmltv:
    """Tests for B2 — existing /api/guide.xml uses federation channel ID."""

    async def test_api_xmltv_uses_federation_id(self, client):
        """The management API XMLTV also uses the TV-prefixed ID."""
        resp = await client.get("/api/guide.xml")
        assert resp.status_code == 200
        content = resp.text
        # Should use the TV-prefixed federation ID, not "channel-one"
        assert f'id="{TEST_CHANNEL_ID}"' in content
        assert 'id="channel-one"' not in content


# ── Phase 4: Stream Redirect ──


class TestStreamRedirect:
    """Tests for GET /tltv/v1/channels/{id}/stream.m3u8 (section 8.3)."""

    async def test_stream_returns_302(self, client):
        """Stream endpoint returns 302 redirect."""
        resp = await client.get(
            f"/tltv/v1/channels/{TEST_CHANNEL_ID}/stream.m3u8",
            follow_redirects=False,
        )
        assert resp.status_code == 302

    async def test_stream_redirects_to_hls(self, client):
        """Redirect target is /hls/{channel_id}/stream.m3u8."""
        resp = await client.get(
            f"/tltv/v1/channels/{TEST_CHANNEL_ID}/stream.m3u8",
            follow_redirects=False,
        )
        location = resp.headers.get("location", "")
        assert f"/hls/{TEST_CHANNEL_ID}/stream.m3u8" in location

    async def test_stream_cors_headers(self, client):
        """Redirect includes CORS headers."""
        resp = await client.get(
            f"/tltv/v1/channels/{TEST_CHANNEL_ID}/stream.m3u8",
            follow_redirects=False,
        )
        assert resp.headers.get("access-control-allow-origin") == "*"

    async def test_stream_404_unknown_channel(self, client):
        """Unknown channel returns 404."""
        from protocol.identity import generate_ed25519_keypair, make_channel_id

        _, pubkey = generate_ed25519_keypair()
        unknown_id = make_channel_id(pubkey)
        resp = await client.get(
            f"/tltv/v1/channels/{unknown_id}/stream.m3u8",
            follow_redirects=False,
        )
        assert resp.status_code == 404
        assert resp.json()["error"] == "channel_not_found"

    async def test_stream_400_bad_format(self, client):
        """Bad channel ID returns 400."""
        resp = await client.get(
            "/tltv/v1/channels/bad-id/stream.m3u8",
            follow_redirects=False,
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_request"
