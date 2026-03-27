"""TLTV channel identity — Ed25519 keypair, base58, version prefix.

Implements PROTOCOL.md sections 2.1-2.3:
- Channel ID = base58(0x1433 || Ed25519_pubkey)
- Always starts with "TV" (46 characters)
- Base58 uses Bitcoin alphabet (no 0, O, l, I)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Base58 alphabet (Bitcoin variant — no 0, O, l, I)
_B58_ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

# V1 version prefix — produces "TV" leading characters in base58
VERSION_PREFIX = b"\x14\x33"


def b58encode(data: bytes) -> str:
    """Encode bytes to base58 (Bitcoin alphabet)."""
    n = int.from_bytes(data, "big")
    result = bytearray()
    while n > 0:
        n, r = divmod(n, 58)
        result.append(_B58_ALPHABET[r])
    # Preserve leading zero bytes
    for b in data:
        if b == 0:
            result.append(_B58_ALPHABET[0])
        else:
            break
    return bytes(reversed(result)).decode("ascii")


def b58decode(s: str) -> bytes:
    """Decode base58 string to bytes."""
    n = 0
    for c in s.encode("ascii"):
        n = n * 58 + _B58_ALPHABET.index(c)
    # Count leading '1's (zero bytes)
    pad = 0
    for c in s:
        if c == "1":
            pad += 1
        else:
            break
    result = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return b"\x00" * pad + result


def make_channel_id(pubkey: bytes) -> str:
    """Encode a 32-byte Ed25519 public key as a TLTV channel ID.

    Prepends the V1 version prefix (0x1433) before base58 encoding.
    The result always starts with "TV" and is 46 characters long.

    Args:
        pubkey: 32-byte Ed25519 public key.

    Returns:
        Channel ID string (e.g. "TVMkVHiXF9W1NgM9KLgs7tcBMvC1YtF4Daj4yfTrJercs3").
    """
    if len(pubkey) != 32:
        raise ValueError(f"Ed25519 public key must be 32 bytes, got {len(pubkey)}")
    return b58encode(VERSION_PREFIX + pubkey)


def parse_channel_id(channel_id: str) -> bytes:
    """Decode a TLTV channel ID to extract the 32-byte Ed25519 public key.

    Verifies the V1 version prefix (0x1433).

    Args:
        channel_id: Base58-encoded channel ID starting with "TV".

    Returns:
        32-byte Ed25519 public key.

    Raises:
        ValueError: If the channel ID is invalid (bad base58, wrong prefix,
            wrong length).
    """
    try:
        raw = b58decode(channel_id)
    except Exception as exc:
        raise ValueError(f"Invalid base58 in channel ID: {exc}") from exc

    if len(raw) != 34:
        raise ValueError(
            f"Channel ID must decode to 34 bytes (2 prefix + 32 pubkey), got {len(raw)}"
        )

    prefix = raw[:2]
    if prefix != VERSION_PREFIX:
        raise ValueError(
            f"Unknown version prefix 0x{prefix.hex()} "
            f"(expected 0x{VERSION_PREFIX.hex()})"
        )

    return raw[2:]


def generate_ed25519_keypair() -> tuple[bytes, bytes]:
    """Generate an Ed25519 keypair.

    Returns (private_key_seed, public_key_bytes).
    Private key is 32 bytes (seed), public key is 32 bytes.
    """
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    private_key = Ed25519PrivateKey.generate()
    private_bytes = private_key.private_bytes_raw()
    public_bytes = private_key.public_key().public_bytes_raw()
    return private_bytes, public_bytes


def _read_seed(path: Path) -> bytes:
    """Read an Ed25519 seed from a file.

    Accepts either format:
    - 64-char hex text (with optional trailing newline) — tltv-cli format
    - 32 raw bytes — cathode native format

    Raises ValueError if the file is neither format.
    """
    data = path.read_bytes()

    # Try hex first (tltv-cli writes hex + newline)
    trimmed = data.strip()
    if len(trimmed) == 64:
        try:
            return bytes.fromhex(trimmed.decode("ascii"))
        except (ValueError, UnicodeDecodeError):
            pass

    # Raw binary
    if len(data) == 32:
        return data

    raise ValueError(
        f"invalid key file: expected 64 hex chars or 32 raw bytes, got {len(data)} bytes"
    )


def ensure_channel_keypair(
    channel_slug: str,
    key_dir: str = "/data/keys",
    key_path_override: str | None = None,
) -> tuple[str, str]:
    """Ensure an Ed25519 keypair exists for a channel.

    Loads from key_path_override (if set in channel YAML) or falls back
    to {key_dir}/{channel_slug}.key.  Generates a new keypair only if
    no key file exists at either path.

    Accepts both hex-encoded seeds (tltv-cli format) and raw 32-byte
    seeds (cathode format).

    Args:
        channel_slug: Human-friendly channel slug (e.g. "channel-one").
        key_dir: Default directory for key files.
        key_path_override: Explicit path from channel YAML
            (identity.private_key_path).  Takes priority over default.

    Returns:
        (channel_id, key_path) where channel_id is the TV-prefixed
        federation ID and key_path is the filesystem path to the
        private key file.
    """
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    # Determine key file path: explicit config > default convention
    if key_path_override:
        key_path = Path(key_path_override)
    else:
        key_path = Path(key_dir) / f"{channel_slug}.key"

    if key_path.exists():
        try:
            seed = _read_seed(key_path)
            private_key = Ed25519PrivateKey.from_private_bytes(seed)
            public_bytes = private_key.public_key().public_bytes_raw()
            channel_id = make_channel_id(public_bytes)
            logger.info("Loaded Ed25519 keypair for %s: %s", channel_slug, channel_id)
            return channel_id, str(key_path)
        except Exception as exc:
            if key_path_override:
                # Explicit path was configured — don't silently replace it
                raise ValueError(
                    f"Cannot load key from configured path {key_path}: {exc}"
                ) from exc
            logger.error(
                "Failed to load key for %s from %s: %s — generating new keypair",
                channel_slug,
                key_path,
                exc,
            )

    # Generate new keypair
    private_bytes, public_bytes = generate_ed25519_keypair()
    channel_id = make_channel_id(public_bytes)

    key_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.write_bytes(private_bytes)
    os.chmod(str(key_path), 0o600)

    logger.warning(
        "Generated NEW Ed25519 keypair for %s: %s (key saved to %s)",
        channel_slug,
        channel_id,
        key_path,
    )
    return channel_id, str(key_path)
