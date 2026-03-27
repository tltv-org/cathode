"""Canonical JSON serialization and Ed25519 document signing.

Implements PROTOCOL.md sections 4 (Canonical JSON / JCS) and 7 (Signatures).

Canonical JSON rules (RFC 8785, simplified for TLTV):
- Sort keys at each nesting level
- No whitespace between tokens
- No null values, no floats (integers only)
- UTF-8 encoding, no BOM

Signing procedure (section 7.1):
1. Remove 'signature' field from document
2. Serialize to canonical JSON bytes
3. Sign with Ed25519 private key
4. Base58-encode the 64-byte signature
5. Add 'signature' field to document

Sequence counters are file-backed at /data/seq/{channel_id}-{type}.seq.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import config

from protocol.identity import b58decode, b58encode, parse_channel_id

logger = logging.getLogger(__name__)

# Default directory for sequence counter files
SEQ_DIR = config.SEQ_DIR

# Maximum clock drift allowed for incoming seq values (seconds)
MAX_SEQ_DRIFT = 3600


def _check_no_nulls_or_floats(obj: object, path: str = "") -> None:
    """Validate that a value contains no None or float types.

    TLTV signed documents MUST NOT contain null values or floating-point
    numbers (PROTOCOL.md section 4.1). This function enforces that
    constraint before serialization.

    Raises:
        ValueError: If a null or float is found, with the path to the value.
    """
    if obj is None:
        raise ValueError(
            f"null value at {path or 'root'}: TLTV documents must not contain null"
        )
    if isinstance(obj, float):
        raise ValueError(
            f"float value at {path or 'root'}: TLTV documents must use integers only"
        )
    if isinstance(obj, dict):
        for k, v in obj.items():
            _check_no_nulls_or_floats(v, f"{path}.{k}" if path else k)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            _check_no_nulls_or_floats(v, f"{path}[{i}]")


def canonical_json(obj: dict) -> bytes:
    """Serialize a dict to canonical JSON bytes (RFC 8785 / JCS).

    For TLTV documents (ASCII keys, integers only, no nulls),
    json.dumps with sort_keys=True is sufficient. The key sort order
    matches RFC 8785 section 3.2.3 for ASCII-only keys.

    Args:
        obj: Dictionary to serialize. Must not contain None values
            or float numbers.

    Returns:
        UTF-8 encoded canonical JSON bytes.

    Raises:
        ValueError: If the document contains null values or floats.
    """
    _check_no_nulls_or_floats(obj)
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _read_seq(channel_id: str, doc_type: str) -> int:
    """Read the current sequence number from disk.

    Returns 0 if no counter file exists yet.
    """
    seq_path = Path(SEQ_DIR) / f"{channel_id}-{doc_type}.seq"
    if seq_path.exists():
        try:
            return int(seq_path.read_text().strip())
        except (ValueError, OSError) as exc:
            logger.warning("Failed to read seq counter %s: %s", seq_path, exc)
    return 0


def _write_seq(channel_id: str, doc_type: str, seq: int) -> None:
    """Write a sequence number to disk."""
    seq_dir = Path(SEQ_DIR)
    seq_dir.mkdir(parents=True, exist_ok=True)
    seq_path = seq_dir / f"{channel_id}-{doc_type}.seq"
    seq_path.write_text(str(seq))


def next_seq(channel_id: str, doc_type: str) -> int:
    """Get the next sequence number for a document type.

    Seq values are Unix epoch timestamps in seconds (spec section 5.5).
    Uses max(current_unix_time, last_seq + 1) to ensure monotonicity
    even if the clock jumps backward or multiple documents are signed
    within the same second.

    Args:
        channel_id: TV-prefixed channel ID.
        doc_type: "metadata" or "guide".

    Returns:
        The new sequence number (Unix epoch timestamp).
    """
    last_seq = _read_seq(channel_id, doc_type)
    now = int(time.time())
    new_seq = max(now, last_seq + 1)
    _write_seq(channel_id, doc_type, new_seq)
    return new_seq


def validate_seq(seq: int) -> bool:
    """Check that a seq value is valid.

    Per spec section 5.5:
    - seq MUST be a positive integer (reject seq <= 0).
    - Reject documents with seq more than 3600 seconds ahead of
      the receiver's current time.

    Args:
        seq: The seq value from a received document.

    Returns:
        True if seq is acceptable, False otherwise.
    """
    if not isinstance(seq, int) or seq <= 0:
        return False
    now = int(time.time())
    return seq <= now + MAX_SEQ_DRIFT


def validate_updated(updated: str | None) -> bool:
    """Check that an 'updated' or 'migrated' timestamp is not too far in the future.

    Per spec section 5.5: reject any signed document with an updated
    or migrated timestamp more than 1 hour in the future.

    Args:
        updated: ISO 8601 UTC timestamp string (e.g. "2026-03-14T12:00:00Z"),
                 or None if the field is absent.

    Returns:
        True if acceptable (or absent), False if too far in the future.
    """
    if not updated:
        return True
    try:
        dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        max_future = now + timedelta(seconds=MAX_SEQ_DRIFT)
        return dt <= max_future
    except (ValueError, TypeError):
        logger.debug("Unparseable timestamp: %s", updated)
        return False  # Malformed timestamps are rejected


def sign_document(doc: dict, private_key_path: str) -> dict:
    """Sign a TLTV document with an Ed25519 private key.

    Implements PROTOCOL.md section 7.1:
    1. Remove 'signature' field if present
    2. Serialize to canonical JSON
    3. Sign with Ed25519
    4. Base58-encode signature
    5. Add 'signature' to document

    Args:
        doc: Document dict (will be modified in place).
        private_key_path: Path to 32-byte Ed25519 private key (seed).

    Returns:
        The document with 'signature' field added.
    """
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    # Step 1: Remove signature if present
    doc_without_sig = {k: v for k, v in doc.items() if k != "signature"}

    # Step 2: Canonical JSON
    payload = canonical_json(doc_without_sig)

    # Step 3: Sign
    private_bytes = Path(private_key_path).read_bytes()
    private_key = Ed25519PrivateKey.from_private_bytes(private_bytes)
    signature_bytes = private_key.sign(payload)

    # Step 4: Base58-encode
    signature_b58 = b58encode(signature_bytes)

    # Step 5: Add to document
    doc["signature"] = signature_b58
    return doc


def verify_document(doc: dict, channel_id: str) -> bool:
    """Verify a signed TLTV document.

    Implements PROTOCOL.md section 7.2:
    1. Remove 'signature' field
    2. Serialize to canonical JSON
    3. Decode channel ID to get pubkey
    4. Decode signature from base58
    5. Verify Ed25519 signature

    Also checks identity binding (section 5.4): the document's 'id'
    field must match the expected channel_id.

    Args:
        doc: Signed document dict (must have 'signature' and 'id' fields).
        channel_id: Expected channel ID (for identity binding check).

    Returns:
        True if signature is valid and identity matches.
        False otherwise.
    """
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    # Identity binding check
    if doc.get("id") != channel_id:
        logger.debug(
            "Identity binding failed: doc id=%s != expected %s",
            doc.get("id"),
            channel_id,
        )
        return False

    # Version check (section 14.3) — reject unsupported document versions
    v = doc.get("v")
    if v is not None and v != 1:
        logger.debug("Unsupported document version: v=%s", v)
        return False

    # Seq validation (section 5.5) — reject invalid or far-future seq
    seq = doc.get("seq")
    if seq is not None:
        if not validate_seq(seq):
            logger.debug("Invalid or far-future seq: %s", seq)
            return False

    # Timestamp validation (section 5.5) — reject far-future timestamps
    if not validate_updated(doc.get("updated")):
        logger.debug("Far-future updated timestamp rejected")
        return False

    signature_b58 = doc.get("signature")
    if not signature_b58:
        logger.debug("Document has no signature field")
        return False

    # Step 1: Remove signature
    doc_without_sig = {k: v for k, v in doc.items() if k != "signature"}

    # Step 2: Canonical JSON
    payload = canonical_json(doc_without_sig)

    try:
        # Step 3: Extract pubkey from channel ID
        pubkey_bytes = parse_channel_id(channel_id)

        # Step 4: Decode signature
        signature_bytes = b58decode(signature_b58)
        if len(signature_bytes) != 64:
            logger.debug(
                "Signature wrong length: %d bytes (expected 64)",
                len(signature_bytes),
            )
            return False

        # Step 5: Verify
        public_key = Ed25519PublicKey.from_public_bytes(pubkey_bytes)
        public_key.verify(signature_bytes, payload)
        return True

    except Exception as exc:
        logger.debug("Signature verification failed: %s", exc)
        return False


def verify_migration_document(doc: dict, channel_id: str) -> bool:
    """Verify a signed migration document (section 5.14).

    Migration documents use the 'from' field for identity binding
    (instead of 'id') — the old channel's key signs the document.

    Verification steps (section 5.14):
    1. Confirm type is "migration"
    2. Confirm v is a supported version (must be 1)
    3. Apply seq replay protection (caller's responsibility for
       stateful tracking; we validate seq exists and is an int)
    4. Verify Ed25519 signature against the 'from' field's public key

    Args:
        doc: Signed migration document dict. Must have 'v', 'seq',
            'type', 'from', 'to', 'migrated', and 'signature' fields.
        channel_id: Expected old channel ID (must match 'from').

    Returns:
        True if type is 'migration', v is 1, signature is valid,
        and 'from' matches channel_id.
        False otherwise.
    """
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    # Step 1: Type check
    if doc.get("type") != "migration":
        logger.debug("Not a migration document: type=%s", doc.get("type"))
        return False

    # Step 2: Version check
    if doc.get("v") != 1:
        logger.debug("Unsupported migration version: v=%s", doc.get("v"))
        return False

    # Seq field must exist and be an integer (replay protection is
    # the caller's responsibility for stateful tracking)
    seq = doc.get("seq")
    if not isinstance(seq, int):
        logger.debug("Migration document missing or invalid seq: %s", seq)
        return False

    # Identity binding via 'from' field (section 5.14)
    if doc.get("from") != channel_id:
        logger.debug(
            "Migration identity binding failed: from=%s != expected %s",
            doc.get("from"),
            channel_id,
        )
        return False

    signature_b58 = doc.get("signature")
    if not signature_b58:
        logger.debug("Migration document has no signature field")
        return False

    doc_without_sig = {k: v for k, v in doc.items() if k != "signature"}
    payload = canonical_json(doc_without_sig)

    try:
        pubkey_bytes = parse_channel_id(channel_id)
        signature_bytes = b58decode(signature_b58)
        if len(signature_bytes) != 64:
            logger.debug(
                "Migration signature wrong length: %d bytes (expected 64)",
                len(signature_bytes),
            )
            return False

        public_key = Ed25519PublicKey.from_public_bytes(pubkey_bytes)
        public_key.verify(signature_bytes, payload)
        return True

    except Exception as exc:
        logger.debug("Migration signature verification failed: %s", exc)
        return False
