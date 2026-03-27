"""Private channel token management.

Implements PROTOCOL.md section 5.7 (Private Channels / Token Auth).

Tokens are opaque URL-safe strings (32 bytes, base64url-encoded).
Per-channel token lists stored at /data/tokens/{channel-id}.json.
Tokens are validated on protocol endpoints for channels with access: "token".
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import Request

import config

logger = logging.getLogger(__name__)

# Default directory for token files
TOKEN_DIR = config.TOKEN_DIR


@dataclass
class TokenEntry:
    """A single access token for a private channel."""

    token: str  # base64url-encoded opaque string
    name: str  # human-readable label (e.g. "my-viewer")
    created: str  # ISO 8601 UTC timestamp
    expires: str | None = None  # Optional expiry timestamp
    token_id: str = ""  # Short ID for revocation (derived from token hash)

    def __post_init__(self) -> None:
        if not self.token_id:
            self.token_id = _token_id(self.token)

    def is_expired(self) -> bool:
        """Check if this token has expired."""
        if not self.expires:
            return False
        try:
            exp = datetime.fromisoformat(self.expires.replace("Z", "+00:00"))
            return datetime.now(timezone.utc) > exp
        except (ValueError, TypeError):
            return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "token": self.token,
            "name": self.name,
            "created": self.created,
            "expires": self.expires,
            "token_id": self.token_id,
        }

    def to_public_dict(self) -> dict[str, Any]:
        """Return a dict safe for listing (no token value)."""
        return {
            "token_id": self.token_id,
            "name": self.name,
            "created": self.created,
            "expires": self.expires,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> TokenEntry:
        return cls(
            token=d["token"],
            name=d.get("name", ""),
            created=d.get("created", ""),
            expires=d.get("expires"),
            token_id=d.get("token_id", ""),
        )


def _token_id(token: str) -> str:
    """Derive a short ID from a token (first 8 chars of SHA-256 hex)."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:8]


def generate_token() -> str:
    """Generate a new opaque access token.

    32 random bytes, base64url-encoded (no padding).
    Uses only URL-safe characters per spec section 5.7:
    A-Z a-z 0-9 - _
    """
    return secrets.token_urlsafe(32)


class TokenStore:
    """Per-channel token storage with file-backed persistence.

    Each channel's tokens are stored in a separate JSON file at
    {token_dir}/{channel_id}.json.
    """

    def __init__(self, token_dir: str | None = None) -> None:
        self._token_dir = token_dir or TOKEN_DIR
        # In-memory cache: channel_id -> list of TokenEntry
        self._cache: dict[str, list[TokenEntry]] = {}

    def _file_path(self, channel_id: str) -> Path:
        return Path(self._token_dir) / f"{channel_id}.json"

    def _load(self, channel_id: str) -> list[TokenEntry]:
        """Load tokens for a channel from disk."""
        if channel_id in self._cache:
            return self._cache[channel_id]

        path = self._file_path(channel_id)
        if not path.exists():
            self._cache[channel_id] = []
            return []

        try:
            data = json.loads(path.read_text())
            entries = [TokenEntry.from_dict(e) for e in data.get("tokens", [])]
            self._cache[channel_id] = entries
            return entries
        except (json.JSONDecodeError, OSError, KeyError) as exc:
            logger.warning("Failed to load tokens for %s: %s", channel_id[:16], exc)
            self._cache[channel_id] = []
            return []

    def _save(self, channel_id: str) -> None:
        """Persist tokens for a channel to disk."""
        path = self._file_path(channel_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            entries = self._cache.get(channel_id, [])
            data = {"tokens": [e.to_dict() for e in entries]}
            path.write_text(json.dumps(data, indent=2))
        except OSError as exc:
            logger.error("Failed to save tokens for %s: %s", channel_id[:16], exc)

    def create(
        self, channel_id: str, name: str, expires: str | None = None
    ) -> TokenEntry:
        """Generate and store a new token for a channel.

        Args:
            channel_id: TV-prefixed federation channel ID.
            name: Human-readable label for this token.
            expires: Optional ISO 8601 expiry timestamp.

        Returns:
            The new TokenEntry (including the token value).
        """
        token = generate_token()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        entry = TokenEntry(token=token, name=name, created=now, expires=expires)

        entries = self._load(channel_id)
        entries.append(entry)
        self._cache[channel_id] = entries
        self._save(channel_id)

        logger.info("Created token '%s' for channel %s", name, channel_id[:16])
        return entry

    def list_tokens(self, channel_id: str) -> list[TokenEntry]:
        """List all tokens for a channel (including expired)."""
        return self._load(channel_id)

    def validate(self, channel_id: str, token: str) -> bool:
        """Check if a token is valid for a channel.

        Returns True if the token exists and is not expired.
        """
        entries = self._load(channel_id)
        for entry in entries:
            if hmac.compare_digest(entry.token, token):
                if entry.is_expired():
                    logger.debug(
                        "Token '%s' for %s is expired", entry.name, channel_id[:16]
                    )
                    return False
                return True
        return False

    def revoke(self, channel_id: str, token_id: str) -> bool:
        """Revoke a token by its token_id.

        Returns True if the token was found and removed.
        """
        entries = self._load(channel_id)
        before = len(entries)
        entries = [e for e in entries if e.token_id != token_id]
        if len(entries) == before:
            return False
        self._cache[channel_id] = entries
        self._save(channel_id)
        logger.info("Revoked token %s for channel %s", token_id, channel_id[:16])
        return True

    def revoke_all(self, channel_id: str) -> int:
        """Revoke all tokens for a channel. Returns count removed."""
        entries = self._load(channel_id)
        count = len(entries)
        if count > 0:
            self._cache[channel_id] = []
            self._save(channel_id)
            logger.info("Revoked all %d tokens for channel %s", count, channel_id[:16])
        return count


def extract_token(request: Request) -> str | None:
    """Extract access token from a request.

    Checks (in order):
    1. ?token= query parameter (spec section 5.7)
    2. Authorization: Bearer header (HTTP convention)

    Returns the token string, or None if not provided.
    """
    # Query parameter (spec-defined)
    token = request.query_params.get("token")
    if token:
        return token

    # Authorization header (HTTP convention)
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()

    return None
