"""Mirror subsystem — replicate a channel's live stream from a primary origin.

Implements PROTOCOL.md section 10.8 (Mirror Nodes):
- Pull HLS manifest from primary every ~2 seconds
- Fetch new segments, cache locally
- Serve replicated manifest and segments at standard protocol paths
- Detect primary failure and promote to self-generation
- Continue media sequence numbering across promotion
- Detect primary recovery and demote back to replication

The mirror holds the same private key as the primary.  Only the active
metadata signer (one origin at a time) signs fresh metadata; other
origins re-serve the active signer's metadata verbatim.  The signing
role transfers only during mirror promotion (section 10.8).
"""

from __future__ import annotations

import logging
import re
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any

import httpx

import config
from protocol.relay import HLSCache, _parse_segment_names, _rewrite_manifest

logger = logging.getLogger(__name__)


class MirrorState:
    """Mirror operational states."""

    REPLICATING = "replicating"  # Pulling HLS from primary
    PROMOTING = "promoting"  # Transitioning to self-generation
    PROMOTED = "promoted"  # Generating own stream
    DEMOTING = "demoting"  # Transitioning back to replication


class MirrorManager:
    """Manages mirror replication for a single channel.

    The mirror pulls HLS from a primary origin and serves it at the
    same protocol paths.  On primary failure it promotes to self-generation
    (via the playout engine), continuing the media sequence.  On primary recovery
    it demotes back to replication.
    """

    def __init__(
        self,
        channel_id: str,
        primary_hint: str,
        *,
        require_tls: bool = True,
    ) -> None:
        self.channel_id = channel_id
        self.primary_hint = primary_hint
        self.require_tls = require_tls

        # State machine
        self.state = MirrorState.REPLICATING

        # HLS cache (shared format with relay)
        self.hls_cache = HLSCache(max_segments=config.RELAY_MAX_SEGMENTS)

        # Media sequence tracking for promotion continuity
        self.last_primary_media_sequence: int = 0
        self.last_primary_segment_count: int = 0
        self.sequence_offset: int = 0  # Added to local nginx sequence

        # Health tracking
        self.consecutive_failures: int = 0
        self.primary_recovered_at: float | None = None  # monotonic timestamp
        self.last_poll: str | None = None

        # Cached upstream metadata (section 10.8): mirrors re-serve the
        # primary's metadata verbatim while replicating.  Only the active
        # metadata signer produces fresh signed metadata.
        self.cached_upstream_metadata: dict | None = None

        # HTTP client (reused)
        self._client: httpx.AsyncClient | None = None

    @property
    def primary_url(self) -> str:
        """Primary origin base URL."""
        scheme = "https" if self.require_tls else "http"
        return f"{scheme}://{self.primary_hint}"

    async def get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=config.MIRROR_UPSTREAM_TIMEOUT,
            )
        return self._client

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ── HLS Polling ──

    async def poll_primary_hls(self) -> bool:
        """Fetch HLS manifest and segments from the primary origin.

        Returns True if manifest was updated, False on failure.
        Tracks media sequence for promotion continuity.
        """
        client = await self.get_client()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            resp = await client.get(
                f"{self.primary_url}/tltv/v1/channels/{self.channel_id}/stream.m3u8",
                follow_redirects=True,
            )
            if resp.status_code != 200:
                self._record_failure()
                return False

            manifest_text = resp.text

            # Track media sequence from primary
            media_seq = parse_media_sequence(manifest_text)
            segment_names = _parse_segment_names(manifest_text)

            if media_seq is not None:
                self.last_primary_media_sequence = media_seq
                self.last_primary_segment_count = len(segment_names)

            # Rewrite manifest to use our local protocol segment paths
            rewritten = _rewrite_manifest(
                manifest_text,
                self.channel_id,
                str(resp.url),
            )
            self.hls_cache.update_manifest(rewritten)

            # Fetch new segments
            base_url = str(resp.url).rsplit("/", 1)[0]
            for seg_name in segment_names:
                if self.hls_cache.get_segment(seg_name) is not None:
                    continue
                if seg_name.startswith("http"):
                    seg_url = seg_name
                else:
                    seg_url = f"{base_url}/{seg_name}"
                try:
                    seg_resp = await client.get(seg_url)
                    if seg_resp.status_code == 200:
                        self.hls_cache.add_segment(seg_name, seg_resp.content)
                except Exception as exc:
                    logger.debug("Mirror: segment fetch failed: %s", exc)

            self.last_poll = now
            self._record_success()
            return True

        except Exception as exc:
            logger.debug(
                "Mirror: HLS fetch failed for %s: %s",
                self.channel_id[:16],
                exc,
            )
            self._record_failure()
            return False

    async def poll_local_hls(self, nginx_hls_url: str) -> bool:
        """Fetch HLS from local nginx and rewrite sequence numbers.

        Used in promoted mode to serve self-generated content with
        sequence numbers continuing from the primary's last value.
        """
        client = await self.get_client()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            resp = await client.get(nginx_hls_url)
            if resp.status_code != 200:
                return False

            manifest_text = resp.text
            local_seq = parse_media_sequence(manifest_text)
            segment_names = _parse_segment_names(manifest_text)

            # On first local manifest after promotion, compute offset
            if local_seq is not None and self.state == MirrorState.PROMOTING:
                # Next sequence after the primary's last segment
                primary_end = (
                    self.last_primary_media_sequence + self.last_primary_segment_count
                )
                self.sequence_offset = primary_end - local_seq
                self.state = MirrorState.PROMOTED
                logger.info(
                    "Mirror: promoted for %s — sequence offset %d "
                    "(primary ended at %d, local starts at %d)",
                    self.channel_id[:16],
                    self.sequence_offset,
                    primary_end - 1,
                    local_seq,
                )

            # Rewrite media sequence in manifest
            if self.sequence_offset != 0:
                manifest_text = adjust_media_sequence(
                    manifest_text, self.sequence_offset
                )

            # Rewrite segment URLs to protocol paths
            rewritten = _rewrite_manifest(
                manifest_text,
                self.channel_id,
                nginx_hls_url,
            )
            self.hls_cache.update_manifest(rewritten)

            # Fetch segments from local nginx
            base_url = nginx_hls_url.rsplit("/", 1)[0]
            for seg_name in segment_names:
                if self.hls_cache.get_segment(seg_name) is not None:
                    continue
                seg_url = f"{base_url}/{seg_name}"
                try:
                    seg_resp = await client.get(seg_url)
                    if seg_resp.status_code == 200:
                        self.hls_cache.add_segment(seg_name, seg_resp.content)
                except Exception as exc:
                    logger.debug("Mirror: local segment fetch failed: %s", exc)

            self.last_poll = now
            return True

        except Exception as exc:
            logger.debug("Mirror: local HLS fetch failed: %s", exc)
            return False

    # ── Health tracking ──

    def _record_failure(self) -> None:
        """Record an HLS fetch failure."""
        self.consecutive_failures += 1
        self.primary_recovered_at = None

    def _record_success(self) -> None:
        """Record a successful HLS fetch from primary."""
        if self.consecutive_failures > 0:
            logger.info(
                "Mirror: primary recovered for %s after %d failures",
                self.channel_id[:16],
                self.consecutive_failures,
            )
        self.consecutive_failures = 0

    def should_promote(self) -> bool:
        """Check if mirror should promote to self-generation.

        Returns True when consecutive failures exceed the threshold
        and we're currently replicating.
        """
        return (
            self.state == MirrorState.REPLICATING
            and self.consecutive_failures >= config.MIRROR_FAILURE_THRESHOLD
        )

    def should_demote(self, monotonic_now: float) -> bool:
        """Check if mirror should demote back to replication.

        Returns True when the primary has been healthy for at least
        MIRROR_DEMOTION_DELAY seconds and we're currently promoted.
        """
        if self.state not in (MirrorState.PROMOTED, MirrorState.PROMOTING):
            return False
        if self.primary_recovered_at is None:
            return False
        elapsed = monotonic_now - self.primary_recovered_at
        return elapsed >= config.MIRROR_DEMOTION_DELAY

    async def check_primary_health(self) -> bool:
        """Probe the primary to see if it's reachable.

        Called during promoted mode to detect when primary recovers,
        and during replicating mode to cache upstream metadata for
        verbatim re-serving (section 10.8).

        A successful metadata fetch (not a full HLS poll) is sufficient.
        """
        client = await self.get_client()
        try:
            resp = await client.get(
                f"{self.primary_url}/tltv/v1/channels/{self.channel_id}",
            )
            if resp.status_code == 200:
                self.cached_upstream_metadata = resp.json()
                return True
            return False
        except Exception:
            return False

    def mark_primary_recovered(self, monotonic_now: float) -> None:
        """Mark that the primary is reachable again.

        Starts the demotion delay timer.
        """
        if self.primary_recovered_at is None:
            self.primary_recovered_at = monotonic_now
            logger.info(
                "Mirror: primary reachable for %s, starting demotion timer",
                self.channel_id[:16],
            )

    def mark_primary_unreachable(self) -> None:
        """Mark that the primary is unreachable again.

        Resets the demotion delay timer.
        """
        self.primary_recovered_at = None

    def begin_promotion(self) -> None:
        """Begin transition to self-generation.

        Sets state to PROMOTING.  The actual state transition to
        PROMOTED happens in poll_local_hls when the first local
        manifest is received and sequence offset is computed.
        """
        self.state = MirrorState.PROMOTING
        logger.info(
            "Mirror: beginning promotion for %s (primary seq was %d + %d segments)",
            self.channel_id[:16],
            self.last_primary_media_sequence,
            self.last_primary_segment_count,
        )

    def begin_demotion(self) -> None:
        """Begin transition back to replication."""
        self.state = MirrorState.DEMOTING
        self.sequence_offset = 0
        self.primary_recovered_at = None
        logger.info(
            "Mirror: beginning demotion for %s, resuming replication",
            self.channel_id[:16],
        )

    def complete_demotion(self) -> None:
        """Complete the transition back to replication."""
        self.state = MirrorState.REPLICATING
        self.consecutive_failures = 0
        logger.info(
            "Mirror: demotion complete for %s, now replicating",
            self.channel_id[:16],
        )

    def status_dict(self) -> dict:
        """Return status information for API/debugging."""
        return {
            "channel_id": self.channel_id,
            "primary_hint": self.primary_hint,
            "state": self.state,
            "consecutive_failures": self.consecutive_failures,
            "last_primary_media_sequence": self.last_primary_media_sequence,
            "last_primary_segment_count": self.last_primary_segment_count,
            "sequence_offset": self.sequence_offset,
            "cached_segments": self.hls_cache.segment_count,
            "last_poll": self.last_poll,
        }


# ── HLS manifest helpers ──


def parse_media_sequence(manifest: str) -> int | None:
    """Extract EXT-X-MEDIA-SEQUENCE value from an HLS manifest."""
    match = re.search(r"#EXT-X-MEDIA-SEQUENCE:(\d+)", manifest)
    if match:
        return int(match.group(1))
    return None


def adjust_media_sequence(manifest: str, offset: int) -> str:
    """Adjust EXT-X-MEDIA-SEQUENCE in a manifest by adding an offset.

    This is used during mirror promotion to continue the sequence
    numbering from where the primary left off.
    """

    def _replace_seq(match: re.Match) -> str:
        seq = int(match.group(1))
        return f"#EXT-X-MEDIA-SEQUENCE:{seq + offset}"

    return re.sub(
        r"#EXT-X-MEDIA-SEQUENCE:(\d+)",
        _replace_seq,
        manifest,
    )
