"""Peer exchange — track and validate known TLTV channels.

Implements PROTOCOL.md sections 8.6 (Peer Exchange), 11 (Discovery),
11.5 (Peer Validation), and 13.2 (Peer List Poisoning mitigation).

The peer store is an in-memory dict keyed by channel ID with file-backed
persistence at /data/peers.json.  Capped at 100 entries (spec section 8.6).
Stale entries (not contacted in 7 days) are evicted (section 11.5).
Private channels are never stored.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

import config
from protocol.signing import verify_document

logger = logging.getLogger(__name__)

# Defaults
MAX_PEERS = 100
STALE_DAYS = 7
PEER_VALIDATE_TIMEOUT = 5.0  # seconds
PEER_FILE = config.PEER_FILE
PEER_REQUIRE_TLS = config.PEER_REQUIRE_TLS


class PeerEntry:
    """A single peer in the store."""

    __slots__ = ("id", "name", "hints", "last_seen", "verified")

    def __init__(
        self,
        id: str,
        name: str,
        hints: list[str],
        last_seen: str | None = None,
        verified: bool = False,
    ) -> None:
        self.id = id
        self.name = name
        self.hints = hints
        self.last_seen = last_seen or datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        self.verified = verified

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "hints": self.hints,
            "last_seen": self.last_seen,
            "verified": self.verified,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> PeerEntry:
        return cls(
            id=d["id"],
            name=d.get("name", ""),
            hints=d.get("hints", []),
            last_seen=d.get("last_seen"),
            verified=d.get("verified", False),
        )


class PeerStore:
    """In-memory peer store with file-backed persistence.

    Thread-safe for asyncio (single-threaded event loop).
    Cap: 100 entries.  Evicts oldest last_seen first when full.
    """

    def __init__(self, path: str | None = None, max_peers: int = MAX_PEERS) -> None:
        self._peers: dict[str, PeerEntry] = {}
        self._path = path or PEER_FILE
        self._max_peers = max_peers
        self._load()

    # ── Public API ──

    def add(self, entry: PeerEntry) -> None:
        """Add or update a peer.  Evicts oldest if at capacity."""
        if entry.id in self._peers:
            # Update existing — merge hints, refresh last_seen
            existing = self._peers[entry.id]
            merged_hints = list(dict.fromkeys(existing.hints + entry.hints))
            existing.hints = merged_hints
            existing.last_seen = entry.last_seen
            existing.name = entry.name or existing.name
            existing.verified = entry.verified or existing.verified
        else:
            # Evict if at capacity
            if len(self._peers) >= self._max_peers:
                self._evict_oldest()
            self._peers[entry.id] = entry
        self._save()

    def remove(self, channel_id: str) -> bool:
        """Remove a peer by channel ID.  Returns True if it existed."""
        if channel_id in self._peers:
            del self._peers[channel_id]
            self._save()
            return True
        return False

    def get(self, channel_id: str) -> PeerEntry | None:
        return self._peers.get(channel_id)

    def all(self) -> list[PeerEntry]:
        """Return all peers (sorted by last_seen descending)."""
        return sorted(
            self._peers.values(),
            key=lambda p: p.last_seen,
            reverse=True,
        )

    def public_peers(self, exclude_ids: set[str] | None = None) -> list[dict]:
        """Return peer list for the protocol response (section 8.6).

        Excludes stale entries, unverified peers, and optionally specific
        channel IDs.  Per spec section 8.6: nodes MUST NOT include a peer
        unless they have verified that peer's signed metadata.
        """
        exclude = exclude_ids or set()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=STALE_DAYS)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        result = []
        for p in self._peers.values():
            if p.id in exclude:
                continue
            if p.last_seen < cutoff:
                continue
            if not p.verified:
                continue
            result.append(
                {
                    "id": p.id,
                    "name": p.name,
                    "hints": p.hints,
                    "last_seen": p.last_seen,
                }
            )
        return result

    def evict_stale(self) -> int:
        """Remove entries not contacted in STALE_DAYS.  Returns count removed."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=STALE_DAYS)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        stale_ids = [cid for cid, p in self._peers.items() if p.last_seen < cutoff]
        for cid in stale_ids:
            del self._peers[cid]
        if stale_ids:
            self._save()
            logger.info("Evicted %d stale peers", len(stale_ids))
        return len(stale_ids)

    def __len__(self) -> int:
        return len(self._peers)

    def __contains__(self, channel_id: str) -> bool:
        return channel_id in self._peers

    # ── Persistence ──

    def _load(self) -> None:
        """Load peers from disk."""
        path = Path(self._path)
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text())
            for entry_dict in data.get("peers", []):
                try:
                    entry = PeerEntry.from_dict(entry_dict)
                    self._peers[entry.id] = entry
                except (KeyError, TypeError) as exc:
                    logger.debug("Skipping malformed peer entry: %s", exc)
            logger.info("Loaded %d peers from %s", len(self._peers), self._path)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load peer store from %s: %s", self._path, exc)

    def _save(self) -> None:
        """Persist peers to disk."""
        path = Path(self._path)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            data = {"peers": [p.to_dict() for p in self._peers.values()]}
            path.write_text(json.dumps(data, indent=2))
        except OSError as exc:
            logger.error("Failed to save peer store to %s: %s", self._path, exc)

    def _evict_oldest(self) -> None:
        """Remove the peer with the oldest last_seen."""
        if not self._peers:
            return
        oldest_id = min(self._peers, key=lambda cid: self._peers[cid].last_seen)
        del self._peers[oldest_id]
        logger.debug("Evicted oldest peer: %s", oldest_id)


# ── Peer validation (section 11.5) ──


async def validate_peer(
    hint: str,
    channel_id: str,
    require_tls: bool | None = None,
    client: httpx.AsyncClient | None = None,
) -> dict | None:
    """Validate a peer by fetching its well-known and metadata.

    Returns the verified metadata dict (with 'name' etc.) on success,
    or None on failure.  Never raises — all errors are caught and logged.

    Steps (section 11.5):
    1. Fetch /.well-known/tltv from the hint.
    2. Verify the response contains the expected channel ID.
    3. Fetch and verify the channel's signed metadata.
    4. Only then return the metadata for adding to the peer store.
    """
    if require_tls is None:
        require_tls = PEER_REQUIRE_TLS

    scheme = "https" if require_tls else "http"
    base_url = f"{scheme}://{hint}"

    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=PEER_VALIDATE_TIMEOUT)

    try:
        # Step 1: Fetch well-known
        resp = await client.get(f"{base_url}/.well-known/tltv")
        if resp.status_code != 200:
            logger.debug(
                "Peer validation failed for %s: well-known returned %d",
                hint,
                resp.status_code,
            )
            return None

        well_known = resp.json()

        # Step 2: Check channel ID is listed
        all_channel_ids = set()
        for ch in well_known.get("channels", []):
            all_channel_ids.add(ch.get("id"))
        for ch in well_known.get("relaying", []):
            all_channel_ids.add(ch.get("id"))

        if channel_id not in all_channel_ids:
            logger.debug(
                "Peer validation failed for %s: channel %s not in well-known",
                hint,
                channel_id,
            )
            return None

        # Step 3: Fetch and verify signed metadata.
        # Use negotiated version from well-known (section 3.3).
        versions = well_known.get("versions", [1])
        api_version = max(v for v in versions if isinstance(v, int)) if versions else 1
        resp = await client.get(f"{base_url}/tltv/v{api_version}/channels/{channel_id}")
        if resp.status_code != 200:
            logger.debug(
                "Peer validation failed for %s: metadata returned %d",
                hint,
                resp.status_code,
            )
            return None

        metadata = resp.json()

        # Verify signature
        if not verify_document(metadata, channel_id):
            logger.debug(
                "Peer validation failed for %s: invalid metadata signature",
                hint,
            )
            return None

        # Private channels must not be added
        if metadata.get("access") == "token":
            logger.debug(
                "Peer validation skipped for %s: channel %s is private",
                hint,
                channel_id,
            )
            return None

        # Retired channels must not be added to peer store
        if metadata.get("status") == "retired":
            logger.debug(
                "Peer validation skipped for %s: channel %s is retired",
                hint,
                channel_id,
            )
            return None

        # On-demand channels noted (relay will reject, but peers are ok)
        return metadata

    except Exception as exc:
        logger.debug("Peer validation failed for %s: %s", hint, exc)
        return None

    finally:
        if own_client:
            await client.aclose()


async def fetch_remote_peers(
    hint: str,
    require_tls: bool | None = None,
    client: httpx.AsyncClient | None = None,
) -> list[dict]:
    """Fetch the peer list from a remote node.

    Returns a list of peer entries from the remote's /tltv/v1/peers,
    or an empty list on failure.
    """
    if require_tls is None:
        require_tls = PEER_REQUIRE_TLS

    scheme = "https" if require_tls else "http"
    base_url = f"{scheme}://{hint}"

    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=PEER_VALIDATE_TIMEOUT)

    try:
        resp = await client.get(f"{base_url}/tltv/v1/peers")
        if resp.status_code != 200:
            return []
        data = resp.json()
        return data.get("peers", [])
    except Exception as exc:
        logger.debug("Failed to fetch peers from %s: %s", hint, exc)
        return []
    finally:
        if own_client:
            await client.aclose()
