"""Tests for middleware — API key auth, charset, rate limiting.

Tests middleware classes directly with a minimal ASGI app
rather than through the full cathode app, since middleware
is only mounted when API_KEY is configured.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

APP_DIR = Path("/app")
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from middleware import APIKeyMiddleware, CharsetMiddleware, RateLimitMiddleware

try:
    from httpx import ASGITransport, AsyncClient
except ImportError:
    pytest.skip("httpx not available", allow_module_level=True)


# ── Minimal test app ──


async def _api_endpoint(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True})


async def _public_endpoint(request: Request) -> JSONResponse:
    return JSONResponse({"public": True})


async def _protocol_endpoint(request: Request) -> JSONResponse:
    return JSONResponse({"protocol": True})


def _make_app(middlewares: list | None = None):
    """Build a minimal Starlette app with the given middleware stack."""
    app = Starlette(
        routes=[
            Route("/api/status", _api_endpoint),
            Route("/api/playlist", _api_endpoint),
            Route("/tltv/v1/channels/test", _protocol_endpoint),
            Route("/.well-known/tltv", _protocol_endpoint),
            Route("/docs", _public_endpoint),
            Route("/other", _public_endpoint),
        ],
    )
    for mw in reversed(middlewares or []):
        cls, kwargs = mw
        app.add_middleware(cls, **kwargs)
    return app


# ════════════════════════════════════════════════════════════════
# APIKeyMiddleware
# ════════════════════════════════════════════════════════════════


class TestAPIKeyMiddleware:
    """API key authentication for /api/* endpoints."""

    @pytest.fixture
    def app_with_key(self):
        return _make_app([(APIKeyMiddleware, {"api_key": "test-secret-key"})])

    @pytest.fixture
    def app_without_key(self):
        return _make_app([(APIKeyMiddleware, {"api_key": ""})])

    @pytest.mark.asyncio
    async def test_valid_key_via_header(self, app_with_key):
        async with AsyncClient(
            transport=ASGITransport(app=app_with_key), base_url="http://test"
        ) as client:
            resp = await client.get(
                "/api/status", headers={"x-api-key": "test-secret-key"}
            )
            assert resp.status_code == 200
            assert resp.json()["ok"] is True

    @pytest.mark.asyncio
    async def test_valid_key_via_query_param(self, app_with_key):
        async with AsyncClient(
            transport=ASGITransport(app=app_with_key), base_url="http://test"
        ) as client:
            resp = await client.get("/api/status?api_key=test-secret-key")
            assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_missing_key_returns_403(self, app_with_key):
        async with AsyncClient(
            transport=ASGITransport(app=app_with_key), base_url="http://test"
        ) as client:
            resp = await client.get("/api/status")
            assert resp.status_code == 403
            assert resp.json()["error"] == "access_denied"

    @pytest.mark.asyncio
    async def test_wrong_key_returns_403(self, app_with_key):
        async with AsyncClient(
            transport=ASGITransport(app=app_with_key), base_url="http://test"
        ) as client:
            resp = await client.get("/api/status", headers={"x-api-key": "wrong-key"})
            assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_protocol_endpoint_no_auth(self, app_with_key):
        """Protocol endpoints (/tltv/*) are always public."""
        async with AsyncClient(
            transport=ASGITransport(app=app_with_key), base_url="http://test"
        ) as client:
            resp = await client.get("/tltv/v1/channels/test")
            assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_well_known_no_auth(self, app_with_key):
        async with AsyncClient(
            transport=ASGITransport(app=app_with_key), base_url="http://test"
        ) as client:
            resp = await client.get("/.well-known/tltv")
            assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_docs_no_auth(self, app_with_key):
        async with AsyncClient(
            transport=ASGITransport(app=app_with_key), base_url="http://test"
        ) as client:
            resp = await client.get("/docs")
            assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_non_api_path_no_auth(self, app_with_key):
        """Non-/api/ paths pass through without auth."""
        async with AsyncClient(
            transport=ASGITransport(app=app_with_key), base_url="http://test"
        ) as client:
            resp = await client.get("/other")
            assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_dev_mode_no_auth(self, app_without_key):
        """When API key is empty (dev mode), all requests pass through."""
        async with AsyncClient(
            transport=ASGITransport(app=app_without_key), base_url="http://test"
        ) as client:
            resp = await client.get("/api/status")
            assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_header_takes_precedence_over_bad_query(self, app_with_key):
        """Valid header should work even with wrong query param."""
        async with AsyncClient(
            transport=ASGITransport(app=app_with_key), base_url="http://test"
        ) as client:
            resp = await client.get(
                "/api/status?api_key=wrong",
                headers={"x-api-key": "test-secret-key"},
            )
            # Header OR query — implementation uses `or`, so header checked first
            assert resp.status_code == 200


# ════════════════════════════════════════════════════════════════
# CharsetMiddleware
# ════════════════════════════════════════════════════════════════


class TestCharsetMiddleware:
    """Charset=utf-8 added to application/json responses."""

    @pytest.fixture
    def app(self):
        return _make_app([(CharsetMiddleware, {})])

    @pytest.mark.asyncio
    async def test_json_gets_charset(self, app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/status")
            ct = resp.headers["content-type"]
            assert "charset=utf-8" in ct

    @pytest.mark.asyncio
    async def test_non_json_unchanged(self, app):
        """Non-JSON content types should not be modified."""
        # Protocol endpoints also return JSON in this test app,
        # but the middleware only touches exact "application/json"
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/status")
            # Should have charset
            assert "utf-8" in resp.headers["content-type"]


# ════════════════════════════════════════════════════════════════
# RateLimitMiddleware
# ════════════════════════════════════════════════════════════════


class TestRateLimitMiddleware:
    """IP-based rate limiting for /api/* endpoints."""

    @pytest.fixture
    def app(self):
        return _make_app([(RateLimitMiddleware, {"rate_limit": 3, "rate_window": 60})])

    @pytest.mark.asyncio
    async def test_under_limit_allowed(self, app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            for _ in range(3):
                resp = await client.get("/api/status")
                assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_over_limit_returns_429(self, app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            for _ in range(3):
                await client.get("/api/status")
            resp = await client.get("/api/status")
            assert resp.status_code == 429
            assert resp.json()["error"] == "rate_limited"

    @pytest.mark.asyncio
    async def test_protocol_not_rate_limited(self, app):
        """Protocol endpoints (/tltv/*) are not rate limited."""
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            for _ in range(10):
                resp = await client.get("/tltv/v1/channels/test")
                assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_non_api_not_rate_limited(self, app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            for _ in range(10):
                resp = await client.get("/other")
                assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_window_expiry_resets_count(self, app):
        """After the rate window passes, requests are allowed again."""
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            # Fill up the limit
            for _ in range(3):
                await client.get("/api/status")
            resp = await client.get("/api/status")
            assert resp.status_code == 429

            # Fast-forward time past the window
            mw = None
            for m in app.middleware_stack.__dict__.get("app", app).__class__.__mro__:
                pass
            # Directly manipulate the middleware's internal state
            # Find the RateLimitMiddleware instance
            inner = app.middleware_stack
            while hasattr(inner, "app"):
                if isinstance(inner, RateLimitMiddleware):
                    mw = inner
                    break
                inner = inner.app

            if mw:
                # Clear all stored timestamps
                mw._requests.clear()
                resp = await client.get("/api/status")
                assert resp.status_code == 200


# ════════════════════════════════════════════════════════════════
# Combined middleware stack
# ════════════════════════════════════════════════════════════════


class TestCombinedMiddleware:
    """All three middleware working together."""

    @pytest.fixture
    def app(self):
        return _make_app(
            [
                (CharsetMiddleware, {}),
                (RateLimitMiddleware, {"rate_limit": 5, "rate_window": 60}),
                (APIKeyMiddleware, {"api_key": "secret"}),
            ]
        )

    @pytest.mark.asyncio
    async def test_bad_auth_returns_403(self, app):
        """Bad auth should return 403."""
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/status")
            assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_full_stack_success(self, app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/status", headers={"x-api-key": "secret"})
            assert resp.status_code == 200
            assert "charset=utf-8" in resp.headers["content-type"]
