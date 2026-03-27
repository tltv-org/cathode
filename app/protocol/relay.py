"""Relay subsystem — relay other channels' streams and metadata.

Implements PROTOCOL.md section 10 (Relay Model):
- Fetch, verify, and cache upstream metadata verbatim (10.1)
- Fetch and cache upstream guide documents (10.1)
- Fetch HLS manifests and segments, serve from cache (10.1)
- Announce relayed channels in /.well-known/tltv (10.4)
- Stop relaying if channel goes private (10.6)
- Reject on-demand channels (10.2)

Client/relay failover (section 12.1):
- Try all origins from cached metadata before reporting failure
- Track current upstream and rotate on failure

Origin metadata refresh (section 5.8):
- Refresh at least once per cache lifetime (60s)
- Discard origins from superseded (lower-seq) metadata

Persist relay configuration to /data/relays.json.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

import config
from protocol.signing import validate_seq, validate_updated, verify_document

logger = logging.getLogger(__name__)


class HLSCache:
    """In-memory cache for HLS manifest and segments for one channel."""

    def __init__(self, max_segments: int = 10) -> None:
        self.manifest: str | None = None
        self.manifest_updated: str | None = None
        # OrderedDict preserves insertion order for LRU-style eviction
        self.segments: OrderedDict[str, bytes] = OrderedDict()
        self._max_segments = max_segments

    def update_manifest(self, manifest: str) -> None:
        self.manifest = manifest
        self.manifest_updated = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    def add_segment(self, name: str, data: bytes) -> None:
        if name in self.segments:
            self.segments.move_to_end(name)
            return
        self.segments[name] = data
        while len(self.segments) > self._max_segments:
            self.segments.popitem(last=False)

    def get_segment(self, name: str) -> bytes | None:
        return self.segments.get(name)

    @property
    def segment_count(self) -> int:
        return len(self.segments)


class RelayTarget:
    """State for one relayed channel.

    Supports upstream origin failover (section 12.1): when the current
    upstream is unreachable, tries other origins from cached metadata
    before reporting failure.
    """

    def __init__(
        self,
        channel_id: str,
        upstream_hints: list[str],
    ) -> None:
        self.channel_id = channel_id
        self.upstream_hints = upstream_hints
        self.metadata: dict | None = None
        self.metadata_seq: int = 0
        self.metadata_signature: str | None = (
            None  # Cached signature for equal-seq check
        )
        self.guide: dict | None = None
        self.guide_seq: int = 0
        self.guide_signature: str | None = None  # Cached signature for equal-seq check
        self.hls_cache = HLSCache(max_segments=config.RELAY_MAX_SEGMENTS)
        self.last_metadata_fetch: str | None = None
        self.last_guide_fetch: str | None = None
        self.last_hls_fetch: str | None = None
        self.active = True
        self.error: str | None = None
        self._client: httpx.AsyncClient | None = None

        # Origin failover state (section 12.1)
        self._current_origin_index: int = 0
        self._consecutive_failures: int = 0

    @property
    def upstream_url(self) -> str:
        """Current upstream base URL, with origin failover.

        Uses origins from cached metadata if available, falling
        back to the configured upstream_hints.
        """
        origins = self._effective_origins()
        if not origins:
            return ""
        idx = self._current_origin_index % len(origins)
        hint = origins[idx]
        require_tls = config.PEER_REQUIRE_TLS
        scheme = "https" if require_tls else "http"
        return f"{scheme}://{hint}"

    def _effective_origins(self) -> list[str]:
        """Return the best available origins list.

        Prefers origins from cached metadata (section 5.8 — most
        recent seq always wins).  Falls back to upstream_hints if
        metadata has no origins.
        """
        if self.metadata and self.metadata.get("origins"):
            return self.metadata["origins"]
        return self.upstream_hints

    def rotate_upstream(self) -> bool:
        """Rotate to the next available upstream origin.

        Returns True if there was another origin to try, False if
        all origins have been exhausted in this failure cycle.

        Called by fetch methods on upstream failure (section 12.1).
        """
        origins = self._effective_origins()
        if len(origins) <= 1:
            return False
        self._current_origin_index = (self._current_origin_index + 1) % len(origins)
        self._consecutive_failures += 1
        # If we've cycled through all origins, signal exhaustion
        if self._consecutive_failures >= len(origins):
            self._consecutive_failures = 0
            return False
        logger.debug(
            "Relay: rotating upstream for %s to origin %d/%d",
            self.channel_id[:16],
            self._current_origin_index + 1,
            len(origins),
        )
        return True

    def reset_failure_count(self) -> None:
        """Reset the failure counter after a successful fetch."""
        self._consecutive_failures = 0

    async def get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=config.RELAY_UPSTREAM_TIMEOUT)
        return self._client

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def to_dict(self) -> dict:
        return {
            "channel_id": self.channel_id,
            "upstream_hints": self.upstream_hints,
        }

    def status_dict(self) -> dict:
        return {
            "channel_id": self.channel_id,
            "upstream_hints": self.upstream_hints,
            "active": self.active,
            "metadata_name": self.metadata.get("name") if self.metadata else None,
            "metadata_seq": self.metadata_seq,
            "guide_seq": self.guide_seq,
            "last_metadata_fetch": self.last_metadata_fetch,
            "last_guide_fetch": self.last_guide_fetch,
            "last_hls_fetch": self.last_hls_fetch,
            "cached_segments": self.hls_cache.segment_count,
            "current_upstream": self.upstream_url,
            "error": self.error,
        }


class RelayManager:
    """Manages all relay targets with persistence and upstream fetching."""

    def __init__(self, path: str | None = None) -> None:
        self._relays: dict[str, RelayTarget] = {}
        self._path = path or config.RELAY_FILE
        self._load()

    # ── CRUD ──

    def add(self, channel_id: str, upstream_hints: list[str]) -> RelayTarget:
        """Add or update a relay target."""
        if channel_id in self._relays:
            target = self._relays[channel_id]
            target.upstream_hints = upstream_hints
            target.active = True
            target.error = None
        else:
            target = RelayTarget(channel_id, upstream_hints)
            self._relays[channel_id] = target
        self._save()
        return target

    async def remove(self, channel_id: str) -> bool:
        """Remove a relay target.  Closes its HTTP client."""
        if channel_id in self._relays:
            await self._relays[channel_id].close()
            del self._relays[channel_id]
            self._save()
            return True
        return False

    def get(self, channel_id: str) -> RelayTarget | None:
        return self._relays.get(channel_id)

    def all(self) -> list[RelayTarget]:
        return list(self._relays.values())

    def active_relays(self) -> list[RelayTarget]:
        return [r for r in self._relays.values() if r.active]

    def __len__(self) -> int:
        return len(self._relays)

    def __contains__(self, channel_id: str) -> bool:
        return channel_id in self._relays

    # ── Upstream fetching ──

    async def fetch_metadata(self, target: RelayTarget) -> bool:
        """Fetch and verify upstream metadata for a relay target.

        Returns True if metadata was updated, False otherwise.
        Serves verbatim (spec 10.1) — we cache the raw upstream document.

        On failure, tries other origins from cached metadata before
        giving up (section 12.1 — client/relay failover).

        When metadata is updated with a higher seq, origins from the
        previous (superseded) metadata are discarded automatically
        because we replace the entire metadata dict (section 5.8).
        """
        result = await self._try_fetch_metadata(target)
        if result is not None:
            return result

        # First attempt failed — try other origins (section 12.1)
        while target.rotate_upstream():
            if not target.upstream_url:
                break
            result = await self._try_fetch_metadata(target)
            if result is not None:
                return result

        return False

    async def _try_fetch_metadata(self, target: RelayTarget) -> bool | None:
        """Single attempt to fetch metadata from current upstream.

        Returns True if updated, False if not updated (same seq),
        None on failure (caller should try next origin).
        """
        if not target.upstream_url:
            return None

        client = await target.get_client()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            resp = await client.get(
                f"{target.upstream_url}/tltv/v1/channels/{target.channel_id}"
            )
            if resp.status_code != 200:
                target.error = f"metadata HTTP {resp.status_code}"
                return None  # Try next origin

            # Size limit (section 5.6): reject documents > 64 KB
            if len(resp.content) > 65536:
                target.error = "metadata too large"
                logger.warning(
                    "Relay: metadata for %s exceeds 64 KB (%d bytes)",
                    target.channel_id[:16],
                    len(resp.content),
                )
                return False

            metadata = resp.json()

            # Verify signature (section 10.1)
            if not verify_document(metadata, target.channel_id):
                target.error = "invalid metadata signature"
                logger.warning(
                    "Relay: invalid signature from upstream for %s",
                    target.channel_id[:16],
                )
                return False  # Signature failure is definitive, don't retry

            # Access check (sections 5.2, 10.6, 12.4): relays must only
            # relay public channels.  Unrecognized access values are
            # treated as non-public — the relay must not relay them.
            access = metadata.get("access", "public")
            if access != "public":
                logger.warning(
                    "Relay: channel %s has access=%s, stopping relay",
                    target.channel_id[:16],
                    access,
                )
                target.active = False
                target.metadata = None
                target.error = f"access_{access}"
                self._save()
                return False

            # Retired channel — stop polling
            if metadata.get("status") == "retired":
                logger.warning(
                    "Relay: channel %s is retired, stopping relay",
                    target.channel_id[:16],
                )
                target.active = False
                target.metadata = metadata  # Keep last metadata for reference
                target.error = "channel_retired"
                self._save()
                return False

            # On-demand transition (section 10.6): stop relaying
            # immediately when updated metadata shows on_demand.
            # Clear cached metadata/segments — they must not be served.
            if metadata.get("on_demand"):
                logger.warning(
                    "Relay: channel %s is on-demand, stopping relay",
                    target.channel_id[:16],
                )
                target.active = False
                target.metadata = None
                target.error = "on_demand_rejected"
                self._save()
                return False

            # Future-rejection (spec section 5.5): reject if seq is
            # non-positive or more than 3600 seconds ahead of our time.
            # Also reject if updated timestamp is >1 hour in the future.
            new_seq = metadata.get("seq", 0)
            if not validate_seq(new_seq):
                target.error = "metadata seq invalid or too far in future"
                logger.warning(
                    "Relay: metadata seq %s invalid for %s",
                    new_seq,
                    target.channel_id[:16],
                )
                return False
            if not validate_updated(metadata.get("updated")):
                target.error = "metadata updated too far in future"
                logger.warning(
                    "Relay: metadata updated too far in future for %s",
                    target.channel_id[:16],
                )
                return False

            # Sequence ordering (section 5.5):
            # - Reject if seq is strictly lower than cached.
            # - For equal seq, accept only if the signature field is
            #   identical to the cached copy's signature (Ed25519 is
            #   deterministic — same key + same content = same sig).
            # When seq advances, the entire metadata dict (including
            # origins) is replaced — origins from superseded metadata
            # are discarded (section 5.8).
            new_sig = metadata.get("signature")
            if target.metadata is not None:
                if new_seq < target.metadata_seq:
                    target.last_metadata_fetch = now
                    target.reset_failure_count()
                    return False
                if new_seq == target.metadata_seq:
                    if new_sig != target.metadata_signature:
                        # Different signature at same seq — discard
                        # incoming, retain cache (section 5.5).
                        target.last_metadata_fetch = now
                        target.reset_failure_count()
                        return False
                    # Same signature — document is equivalent, no update needed
                    target.last_metadata_fetch = now
                    target.reset_failure_count()
                    return False

            target.metadata = metadata
            target.metadata_seq = new_seq
            target.metadata_signature = new_sig
            target.last_metadata_fetch = now
            target.error = None
            target.reset_failure_count()
            return True

        except Exception as exc:
            target.error = f"metadata fetch: {exc}"
            logger.debug(
                "Relay: metadata fetch failed for %s: %s", target.channel_id[:16], exc
            )
            return None  # Try next origin

    async def fetch_guide(self, target: RelayTarget) -> bool:
        """Fetch and cache upstream guide document.

        On failure, tries other origins (section 12.1).
        """
        result = await self._try_fetch_guide(target)
        if result is not None:
            return result

        # First attempt failed — try other origins
        while target.rotate_upstream():
            if not target.upstream_url:
                break
            result = await self._try_fetch_guide(target)
            if result is not None:
                return result

        return False

    async def _try_fetch_guide(self, target: RelayTarget) -> bool | None:
        """Single attempt to fetch guide from current upstream.

        Returns True if updated, False if not updated, None on failure.
        """
        if not target.upstream_url:
            return None

        client = await target.get_client()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            resp = await client.get(
                f"{target.upstream_url}/tltv/v1/channels/{target.channel_id}/guide.json"
            )
            if resp.status_code != 200:
                return None  # Try next origin

            # Size limit (section 5.6)
            if len(resp.content) > 65536:
                logger.debug(
                    "Relay: guide for %s exceeds 64 KB", target.channel_id[:16]
                )
                return False

            guide = resp.json()

            # Verify signature
            if not verify_document(guide, target.channel_id):
                logger.debug(
                    "Relay: invalid guide signature for %s", target.channel_id[:16]
                )
                return False  # Signature failure is definitive

            # Future-rejection (spec section 5.5): seq and updated
            new_seq = guide.get("seq", 0)
            if not validate_seq(new_seq):
                logger.debug(
                    "Relay: guide seq %s invalid for %s",
                    new_seq,
                    target.channel_id[:16],
                )
                return False
            if not validate_updated(guide.get("updated")):
                logger.debug(
                    "Relay: guide updated too far in future for %s",
                    target.channel_id[:16],
                )
                return False

            # Sequence ordering (section 5.5):
            # - Reject if seq is strictly lower than cached.
            # - For equal seq, accept only if signature field matches.
            new_sig = guide.get("signature")
            if target.guide is not None:
                if new_seq < target.guide_seq:
                    target.last_guide_fetch = now
                    target.reset_failure_count()
                    return False
                if new_seq == target.guide_seq:
                    if new_sig != target.guide_signature:
                        target.last_guide_fetch = now
                        target.reset_failure_count()
                        return False
                    target.last_guide_fetch = now
                    target.reset_failure_count()
                    return False

            target.guide = guide
            target.guide_seq = new_seq
            target.guide_signature = new_sig
            target.last_guide_fetch = now
            target.reset_failure_count()
            return True

        except Exception as exc:
            logger.debug(
                "Relay: guide fetch failed for %s: %s", target.channel_id[:16], exc
            )
            return None  # Try next origin

    async def fetch_hls(self, target: RelayTarget) -> bool:
        """Fetch upstream HLS manifest and any new segments.

        The relay serves segments directly (spec 10.1: MUST NOT redirect).
        On failure, tries other origins from cached metadata before
        giving up (section 12.1 — client/relay failover).
        """
        result = await self._try_fetch_hls(target)
        if result:
            return True

        # First attempt failed — try other origins (section 12.1)
        while target.rotate_upstream():
            if not target.upstream_url or not target.active:
                break
            result = await self._try_fetch_hls(target)
            if result:
                return True

        return False

    async def _try_fetch_hls(self, target: RelayTarget) -> bool:
        """Single attempt to fetch HLS from current upstream.

        Returns True on success, False on failure.
        """
        if not target.upstream_url or not target.active:
            return False

        client = await target.get_client()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            # Fetch manifest
            resp = await client.get(
                f"{target.upstream_url}/tltv/v1/channels/{target.channel_id}/stream.m3u8",
                follow_redirects=True,
            )
            if resp.status_code != 200:
                return False

            manifest_text = resp.text

            # Parse segment URLs from the manifest
            segment_names = _parse_segment_names(manifest_text)

            # Rewrite manifest to use our local segment paths
            rewritten = _rewrite_manifest(
                manifest_text,
                target.channel_id,
                str(resp.url),
            )
            target.hls_cache.update_manifest(rewritten)

            # Fetch any segments we don't already have
            base_url = str(resp.url).rsplit("/", 1)[0]
            for seg_name in segment_names:
                if target.hls_cache.get_segment(seg_name) is not None:
                    continue
                # Build segment URL
                if seg_name.startswith("http"):
                    seg_url = seg_name
                else:
                    seg_url = f"{base_url}/{seg_name}"
                try:
                    seg_resp = await client.get(seg_url)
                    if seg_resp.status_code == 200:
                        target.hls_cache.add_segment(seg_name, seg_resp.content)
                except Exception as exc:
                    logger.debug("Relay: segment fetch failed: %s", exc)

            target.last_hls_fetch = now
            target.reset_failure_count()
            return True

        except Exception as exc:
            logger.debug(
                "Relay: HLS fetch failed for %s: %s", target.channel_id[:16], exc
            )
            return False

    # ── Bulk operations (called by scheduler loops) ──

    async def refresh_all_metadata(self) -> None:
        for target in self.active_relays():
            result = await self.fetch_metadata(target)
            if not result and target.error:
                logger.warning(
                    "Relay: metadata refresh failed for %s: %s",
                    target.channel_id[:16],
                    target.error,
                )

    async def refresh_all_guides(self) -> None:
        for target in self.active_relays():
            await self.fetch_guide(target)

    async def refresh_all_hls(self) -> None:
        for target in self.active_relays():
            await self.fetch_hls(target)

    # ── Cleanup ──

    async def close(self) -> None:
        """Close all HTTP clients."""
        for target in self._relays.values():
            await target.close()

    # ── Persistence ──

    def _load(self) -> None:
        path = Path(self._path)
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text())
            for entry in data.get("relays", []):
                cid = entry.get("channel_id")
                hints = entry.get("upstream_hints", [])
                if cid and hints:
                    self._relays[cid] = RelayTarget(cid, hints)
            logger.info(
                "Loaded %d relay targets from %s", len(self._relays), self._path
            )
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load relay config from %s: %s", self._path, exc)

    def _save(self) -> None:
        path = Path(self._path)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            data = {"relays": [t.to_dict() for t in self._relays.values()]}
            path.write_text(json.dumps(data, indent=2))
        except OSError as exc:
            logger.error("Failed to save relay config to %s: %s", self._path, exc)


# ── HLS manifest helpers ──


def _parse_segment_names(manifest: str) -> list[str]:
    """Extract segment filenames/URLs from an HLS manifest."""
    segments = []
    for line in manifest.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            segments.append(line)
    return segments


def _rewrite_manifest(
    manifest: str,
    channel_id: str,
    source_url: str,
    token: str | None = None,
) -> str:
    """Rewrite URLs in an HLS manifest to point to our relay paths.

    Replaces relative or absolute segment references with our protocol path.

    For private channels (section 5.7): token must be embedded in every
    URI in the HLS playlist graph — segment URIs, variant playlist URIs
    (EXT-X-STREAM-INF), rendition playlist URIs (EXT-X-MEDIA), encryption
    key URIs (EXT-X-KEY), and map URIs (EXT-X-MAP).

    Args:
        manifest: Raw HLS manifest text.
        channel_id: TV-prefixed channel ID for path construction.
        source_url: Original URL the manifest was fetched from.
        token: Optional access token for private channels.
    """

    def _append_token(uri: str) -> str:
        """Append ?token=... to a URI if token is set."""
        if not token:
            return uri
        sep = "&" if "?" in uri else "?"
        return f"{uri}{sep}token={token}"

    lines = []
    for line in manifest.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            # This is a URI line (segment, variant playlist, etc.)
            seg_name = stripped.rsplit("/", 1)[-1]
            uri = f"/tltv/v1/channels/{channel_id}/segments/{seg_name}"
            lines.append(_append_token(uri))
        elif stripped.startswith("#EXT-X-MAP:"):
            # EXT-X-MAP URI attribute (section 5.7)
            lines.append(_rewrite_tag_uri(stripped, channel_id, token))
        elif stripped.startswith("#EXT-X-KEY:"):
            # EXT-X-KEY URI attribute (section 5.7)
            lines.append(_rewrite_tag_uri(stripped, channel_id, token))
        elif stripped.startswith("#EXT-X-MEDIA:") and "URI=" in stripped:
            # EXT-X-MEDIA URI attribute (section 5.7)
            lines.append(_rewrite_tag_uri(stripped, channel_id, token))
        elif stripped.startswith("#EXT-X-STREAM-INF:"):
            # The URI for EXT-X-STREAM-INF is on the NEXT line (handled
            # as a non-comment line above), but if there are URI= attributes
            # in the tag itself, rewrite those too.
            if "URI=" in stripped:
                lines.append(_rewrite_tag_uri(stripped, channel_id, token))
            else:
                lines.append(line)
        else:
            lines.append(line)
    return "\n".join(lines) + "\n"


def _rewrite_tag_uri(
    tag_line: str,
    channel_id: str,
    token: str | None = None,
) -> str:
    """Rewrite URI= attribute within an HLS tag line.

    Handles tags like #EXT-X-MAP:URI="init.mp4", #EXT-X-KEY:URI="key.bin",
    #EXT-X-MEDIA:...,URI="alt.m3u8".
    """
    match = re.search(r'URI="([^"]*)"', tag_line)
    if not match:
        return tag_line

    old_uri = match.group(1)
    name = old_uri.rsplit("/", 1)[-1]
    new_uri = f"/tltv/v1/channels/{channel_id}/segments/{name}"
    if token:
        sep = "&" if "?" in new_uri else "?"
        new_uri = f"{new_uri}{sep}token={token}"
    return tag_line[: match.start()] + f'URI="{new_uri}"' + tag_line[match.end() :]
