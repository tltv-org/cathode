"""Middleware for cathode — security headers, API key auth, charset, rate limiting.

Security headers:
  CSP, X-Content-Type-Options, X-Frame-Options, Referrer-Policy on all
  responses.  These protect the phosphor SPA and API endpoints when
  served behind traefik.

API key authentication:
  Protects /api/* management endpoints.  When API_KEY is set in config,
  requests must include X-API-Key header or ?api_key= query param.
  Protocol endpoints (/tltv/v1/*, /.well-known/tltv) are always public.

JSON charset:
  Adds charset=utf-8 to application/json Content-Type headers per
  PROTOCOL.md section 8.8.

Request body size limit:
  Caps JSON request bodies at MAX_BODY_SIZE (default 1 MB).  Media
  upload and backup restore have their own size limits and are excluded.
"""

from __future__ import annotations

import hmac
import logging
import time
from collections import defaultdict

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

import config

logger = logging.getLogger(__name__)

# Paths that never require API key auth.
# Protocol endpoints are public per spec.  OpenAPI docs are public so
# AI agents and integrations can discover the API surface — the API key
# is the security boundary, not schema visibility.
_PUBLIC_PREFIXES = (
    "/tltv/",
    "/.well-known/",
    "/docs",
    "/openapi.json",
)


class APIKeyMiddleware(BaseHTTPMiddleware):
    """Require API key for /api/* management endpoints.

    When config.API_KEY is empty, all requests are allowed (dev mode)
    with a startup warning logged.
    """

    def __init__(self, app, api_key: str = ""):
        super().__init__(app)
        self.api_key = api_key
        if not api_key:
            logger.warning(
                "API_KEY not set — management API is unprotected. "
                "Set API_KEY env var for production."
            )

    async def dispatch(self, request: Request, call_next) -> Response:
        # No key configured — allow everything (dev mode)
        if not self.api_key:
            return await call_next(request)

        path = request.url.path

        # Public endpoints — no auth required
        if any(path.startswith(p) for p in _PUBLIC_PREFIXES):
            return await call_next(request)

        # Only protect /api/* routes
        if not path.startswith("/api/"):
            return await call_next(request)

        # Check API key from header or query param
        key = request.headers.get("x-api-key") or request.query_params.get("api_key")
        if not key or not hmac.compare_digest(key, self.api_key):
            return JSONResponse(
                content={
                    "error": "access_denied",
                    "message": "Invalid or missing API key",
                },
                status_code=403,
                media_type="application/json; charset=utf-8",
            )

        return await call_next(request)


class CharsetMiddleware(BaseHTTPMiddleware):
    """Add charset=utf-8 to application/json Content-Type headers.

    PROTOCOL.md section 8.8: All JSON responses MUST use
    Content-Type: application/json; charset=utf-8.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        ct = response.headers.get("content-type", "")
        if ct == "application/json":
            response.headers["content-type"] = "application/json; charset=utf-8"
        return response


def _get_client_ip(request: Request) -> str:
    """Extract the real client IP, respecting reverse proxy headers.

    Reads X-Forwarded-For (set by Traefik/nginx/Caddy) first, falling
    back to the TCP connection address.  Only the leftmost (client) IP
    from X-Forwarded-For is used — intermediate proxies are ignored.
    """
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        # X-Forwarded-For: client, proxy1, proxy2
        return forwarded.split(",")[0].strip()
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    return request.client.host if request.client else "unknown"


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Simple IP-based rate limiting for /api/* endpoints.

    Allows RATE_LIMIT requests per RATE_WINDOW seconds per IP.
    Only applies to /api/* routes — protocol endpoints are unlimited.

    Behind a reverse proxy (Traefik, nginx, Caddy), uses X-Forwarded-For
    or X-Real-IP to identify the real client.
    """

    def __init__(self, app, rate_limit: int = 120, rate_window: int = 60):
        super().__init__(app)
        self.rate_limit = rate_limit
        self.rate_window = rate_window
        self._requests: dict[str, list[float]] = defaultdict(list)
        self._request_count = 0
        self._cleanup_interval = 100

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path
        if not path.startswith("/api/"):
            return await call_next(request)

        client_ip = _get_client_ip(request)
        now = time.monotonic()

        # Prune old entries for this IP
        entries = self._requests[client_ip]
        cutoff = now - self.rate_window
        self._requests[client_ip] = [t for t in entries if t > cutoff]

        if len(self._requests[client_ip]) >= self.rate_limit:
            logger.warning("Rate limit exceeded for %s on %s", client_ip, path)
            return JSONResponse(
                content={"error": "rate_limited", "message": "Too many requests"},
                status_code=429,
                media_type="application/json; charset=utf-8",
            )

        self._requests[client_ip].append(now)

        # Periodic cleanup of stale IPs
        self._request_count += 1
        if self._request_count >= self._cleanup_interval:
            self._cleanup_stale_ips()
            self._request_count = 0

        return await call_next(request)

    def _cleanup_stale_ips(self) -> None:
        """Remove IPs with only expired timestamps."""
        now = time.monotonic()
        stale = [
            ip
            for ip, times in self._requests.items()
            if all(t < now - self.rate_window for t in times)
        ]
        for ip in stale:
            del self._requests[ip]


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers to all responses.

    - Content-Security-Policy: restrictive default for the phosphor SPA
    - X-Content-Type-Options: nosniff — prevent MIME type sniffing
    - X-Frame-Options: DENY — prevent clickjacking
    - Referrer-Policy: strict-origin-when-cross-origin (overridden to
      no-referrer for private channels by protocol route handlers)
    """

    _HEADERS = {
        "Content-Security-Policy": (
            "default-src 'self'; "
            "script-src 'self'; "
            "style-src 'self' 'unsafe-inline'; "
            "media-src 'self' blob:; "
            "connect-src 'self'; "
            "img-src 'self' data:"
        ),
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "Referrer-Policy": "strict-origin-when-cross-origin",
    }

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        for header, value in self._HEADERS.items():
            # Don't overwrite headers already set by route handlers
            # (e.g. private channel Referrer-Policy: no-referrer)
            if header not in response.headers:
                response.headers[header] = value
        return response


# Paths excluded from body size limits (they have their own).
_LARGE_BODY_PATHS = (
    "/api/media/upload",
    "/api/restore",
)

# Default max body size for JSON API requests (1 MB).
MAX_BODY_SIZE = 1 * 1024 * 1024


class BodySizeLimitMiddleware(BaseHTTPMiddleware):
    """Reject requests with bodies larger than MAX_BODY_SIZE.

    Prevents OOM from unbounded JSON payloads.  Media upload and
    backup restore are excluded — they have their own limits.
    """

    def __init__(self, app, max_size: int = MAX_BODY_SIZE):
        super().__init__(app)
        self.max_size = max_size

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path
        if any(path.startswith(p) for p in _LARGE_BODY_PATHS):
            return await call_next(request)

        # Check Content-Length header first (fast path)
        cl = request.headers.get("content-length")
        if cl and int(cl) > self.max_size:
            return JSONResponse(
                content={
                    "error": "payload_too_large",
                    "message": f"Request body exceeds {self.max_size} bytes",
                },
                status_code=413,
                media_type="application/json; charset=utf-8",
            )

        return await call_next(request)
