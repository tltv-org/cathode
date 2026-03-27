"""TLTV URI scheme — formatting and parsing.

Implements PROTOCOL.md section 3:
- tltv://<channel_id>
- tltv://<channel_id>@<host:port>
- tltv://<channel_id>?via=<host1>,<host2>
- tltv://<channel_id>?token=<access_token>&via=<host1>

Case sensitivity: channel IDs are case-sensitive (base58).
Implementations MUST NOT apply host normalization (lowercasing)
to the channel ID component (section 2.5).

Section 3.1 rules:
- Duplicate query params: first occurrence wins.
- IPv6 hints must be bracketed: [::1]:port.
- Malformed hints are silently skipped.
- Loopback/private-network hints are excluded unless explicitly
  configured (see filter_private_hints parameter).
"""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass, field
from urllib.parse import urlparse

# Regex for bracketed IPv6 hint: [addr]:port
_IPV6_HINT_RE = re.compile(r"^\[([^\]]+)\](?::(\d+))?$")

# Regex for host:port or bare host
_HOST_PORT_RE = re.compile(r"^([^:]+)(?::(\d+))?$")


# Explicit SSRF block ranges per section 3.1.  We check these manually
# rather than relying on Python's is_private (which changed across 3.11
# patch releases and may not cover 100.64.0.0/10).
_BLOCKED_IPV4 = [
    ipaddress.ip_network("127.0.0.0/8"),  # Loopback
    ipaddress.ip_network("10.0.0.0/8"),  # RFC 1918
    ipaddress.ip_network("172.16.0.0/12"),  # RFC 1918
    ipaddress.ip_network("192.168.0.0/16"),  # RFC 1918
    ipaddress.ip_network("169.254.0.0/16"),  # Link-local
    ipaddress.ip_network("100.64.0.0/10"),  # RFC 6598 CGN/shared
]
_BLOCKED_IPV6 = [
    ipaddress.ip_network("::1/128"),  # Loopback
    ipaddress.ip_network("fe80::/10"),  # Link-local
    ipaddress.ip_network("fc00::/7"),  # Unique local (ULA)
]


def _is_private_or_loopback(host: str) -> bool:
    """Check if a host is a blocked address per section 3.1.

    Covers loopback, link-local, RFC 1918 private, IPv6 ULA (fc00::/7),
    RFC 6598 CGN (100.64.0.0/10), and IPv4-mapped IPv6 normalization.
    """
    try:
        addr = ipaddress.ip_address(host)

        # Normalize IPv4-mapped IPv6 (::ffff:x.x.x.x) to IPv4 before
        # checking — this is a common SSRF bypass vector (section 3.1).
        if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped:
            addr = addr.ipv4_mapped

        if isinstance(addr, ipaddress.IPv4Address):
            return any(addr in net for net in _BLOCKED_IPV4)
        else:
            return any(addr in net for net in _BLOCKED_IPV6)
    except ValueError:
        # Not an IP address — could be a hostname like "localhost"
        return host.lower() in ("localhost", "localhost.")


def _validate_hint(hint: str) -> str | None:
    """Validate a peer hint, returning the cleaned hint or None.

    - IPv6 addresses must be bracketed: [::1]:port
    - Malformed hints return None (silently skipped).
    - Does NOT filter private/loopback here (caller decides).
    """
    hint = hint.strip()
    if not hint:
        return None

    # Bracketed IPv6
    m = _IPV6_HINT_RE.match(hint)
    if m:
        addr_str = m.group(1)
        try:
            ipaddress.ip_address(addr_str)
        except ValueError:
            return None  # Malformed IPv6
        return hint

    # Bare IPv6 without brackets — reject per section 3.1
    if ":" in hint and not _HOST_PORT_RE.match(hint):
        # Contains colons but doesn't match host:port — likely
        # unbracketed IPv6, which is malformed
        return None

    # Normal host:port or bare host
    m = _HOST_PORT_RE.match(hint)
    if not m:
        return None
    # Validate port range (section 3.1)
    port_str = m.group(2)
    if port_str is not None:
        port = int(port_str)
        if port < 1 or port > 65535:
            return None
    return hint


def _filter_hints(hints: list[str], allow_private: bool = False) -> list[str]:
    """Validate and filter a list of hints.

    Malformed hints are silently skipped.
    Private/loopback hints are excluded unless allow_private is True.
    """
    result = []
    for raw in hints:
        clean = _validate_hint(raw)
        if clean is None:
            continue
        if not allow_private:
            # Extract the host part for private check
            m = _IPV6_HINT_RE.match(clean)
            if m:
                host = m.group(1)
            else:
                host = clean.split(":")[0]
            if _is_private_or_loopback(host):
                continue
        result.append(clean)
    return result


def _parse_query_first_occurrence(query_string: str) -> dict[str, str]:
    """Parse query string, using first occurrence for duplicate params.

    Section 3.1: duplicate query parameters use first occurrence only.
    """
    params: dict[str, str] = {}
    if not query_string:
        return params
    for part in query_string.split("&"):
        if "=" not in part:
            continue
        key, _, value = part.partition("=")
        if key not in params:
            params[key] = value
    return params


@dataclass
class TltvUri:
    """Parsed tltv:// URI components."""

    channel_id: str
    hints: list[str] = field(default_factory=list)
    token: str | None = None


def format_tltv_uri(
    channel_id: str,
    hints: list[str] | None = None,
    token: str | None = None,
) -> str:
    """Format a tltv:// URI from components.

    Uses @hint syntax for the first hint (spec section 3.1), additional
    hints go in the via= query parameter.  Matches the CLI reference
    implementation.
    """
    first_hint = None
    extra_hints = []
    if hints:
        first_hint = hints[0]
        extra_hints = hints[1:]

    if first_hint and not token:
        uri = f"tltv://{channel_id}@{first_hint}"
    else:
        uri = f"tltv://{channel_id}"

    params: list[str] = []
    if token:
        params.append(f"token={token}")
    if token and first_hint:
        # When token is present, first hint goes in via= too
        extra_hints = ([first_hint] + extra_hints) if first_hint else extra_hints
    if extra_hints:
        params.append(f"via={','.join(extra_hints)}")

    if params:
        uri += "?" + "&".join(params)

    return uri


def parse_tltv_uri(
    uri: str,
    allow_private_hints: bool = False,
) -> TltvUri:
    """Parse a tltv:// URI into components.

    Uses urlparse().netloc to preserve case (section 2.5).
    Does NOT apply host normalization.

    Section 3.1 rules applied:
    - Duplicate query params: first occurrence wins.
    - IPv6 hints must be bracketed.
    - Malformed hints are silently skipped.
    - Loopback/private-network hints excluded unless
      allow_private_hints is True.

    Args:
        uri: A tltv:// URI string.
        allow_private_hints: If True, loopback/private-network hints
            are kept. Default False per section 3.1.

    Returns:
        TltvUri with channel_id, hints, and token.

    Raises:
        ValueError: If the URI scheme is not 'tltv' or is malformed.
    """
    parsed = urlparse(uri)

    if parsed.scheme != "tltv":
        raise ValueError(f"Expected tltv:// scheme, got '{parsed.scheme}://'")

    # Extract channel ID from netloc (preserves case — section 2.5)
    # netloc may contain @host:port hint
    netloc = parsed.netloc
    if not netloc:
        raise ValueError("Missing channel ID in tltv:// URI")

    # Check for @hint format: channel_id@host:port
    if "@" in netloc:
        channel_id, hint = netloc.split("@", 1)
        raw_hints = [hint] if hint else []
    else:
        channel_id = netloc
        raw_hints = []

    if not channel_id:
        raise ValueError("Empty channel ID in tltv:// URI")

    # Parse query parameters — first occurrence wins (section 3.1)
    query = _parse_query_first_occurrence(parsed.query)

    token = query.get("token") or None

    # via parameter: comma-separated hints
    via_str = query.get("via")
    if via_str:
        raw_hints.extend(h.strip() for h in via_str.split(",") if h.strip())

    # Validate and filter hints
    hints = _filter_hints(raw_hints, allow_private=allow_private_hints)

    return TltvUri(channel_id=channel_id, hints=hints, token=token)
