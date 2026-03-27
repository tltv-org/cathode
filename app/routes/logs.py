"""Log endpoints — /api/logs.

Provides access to cathode's log buffer and a real-time SSE stream.
A ring-buffer logging handler captures recent log entries in memory.
The SSE stream pushes new entries to connected clients in real time.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timezone

from fastapi import APIRouter, Query
from starlette.responses import StreamingResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/logs", tags=["logs"])

# ── Source classification ──
# Map logger names to UI source categories that phosphor filters on.

_SOURCE_MAP = {
    "playout": "engine",
    "mixer": "engine",
    "input_layer": "engine",
    "output_layer": "engine",
    "scheduler": "scheduler",
    "protocol": "protocol",
    "peers": "protocol",
    "relay": "protocol",
    "mirror": "protocol",
    "signing": "protocol",
    "identity": "protocol",
    "tokens": "protocol",
    "plugins": "plugin",
    "routes": "api",
    "middleware": "api",
    "uvicorn": "api",
    "main": "cathode",
    "channel": "cathode",
    "config": "cathode",
}


def _classify_source(logger_name: str) -> str:
    """Map a logger name to a phosphor source category."""
    # Check exact match first, then prefix match
    if logger_name in _SOURCE_MAP:
        return _SOURCE_MAP[logger_name]
    for prefix, source in _SOURCE_MAP.items():
        if logger_name.startswith(prefix):
            return source
    return "cathode"


# ── Ring buffer log handler ──


class LogBuffer(logging.Handler):
    """In-memory ring buffer that captures log records.

    Stores the most recent ``maxlen`` entries as dicts.  Also notifies
    any connected SSE clients via an asyncio Event.
    """

    def __init__(self, maxlen: int = 1000):
        super().__init__()
        self._buffer: deque[dict] = deque(maxlen=maxlen)
        self._event: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def emit(self, record: logging.LogRecord) -> None:
        entry = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "source": _classify_source(record.name),
            "logger": record.name,
            "message": self.format(record),
            "module": record.module,
            "lineno": record.lineno,
        }
        self._buffer.append(entry)

        # Notify SSE listeners (thread-safe)
        if self._event and self._loop:
            try:
                self._loop.call_soon_threadsafe(self._event.set)
            except RuntimeError:
                pass  # loop closed

    def entries(
        self,
        limit: int = 100,
        level: str | None = None,
        source: str | None = None,
    ) -> list[dict]:
        """Return recent entries, newest first."""
        items = list(self._buffer)
        if level:
            level_upper = level.upper()
            items = [e for e in items if e["level"] == level_upper]
        if source:
            items = [e for e in items if e["source"] == source]
        items.reverse()
        return items[:limit]

    def attach_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Attach the asyncio event loop for SSE notifications."""
        self._loop = loop
        self._event = asyncio.Event()

    @property
    def event(self) -> asyncio.Event | None:
        return self._event


# Singleton — installed once during app startup
log_buffer = LogBuffer()
log_buffer.setFormatter(logging.Formatter("%(message)s"))


def install() -> None:
    """Install the log buffer handler on the root logger.

    Called once during app lifespan startup.
    """
    root = logging.getLogger()
    # Avoid duplicate installs
    if log_buffer not in root.handlers:
        root.addHandler(log_buffer)
    # Attach event loop for SSE
    try:
        loop = asyncio.get_running_loop()
        log_buffer.attach_event_loop(loop)
    except RuntimeError:
        pass
    logger.debug("Log buffer installed (%d max entries)", log_buffer._buffer.maxlen)


# ── Endpoints ──


@router.get("")
async def get_logs(
    limit: int = Query(100, ge=1, le=1000),
    level: str | None = Query(
        None, description="Filter by level (DEBUG, INFO, WARNING, ERROR)"
    ),
    source: str | None = Query(
        None,
        description="Filter by source (engine, scheduler, protocol, plugin, api, cathode)",
    ),
):
    """Return recent log entries from the in-memory buffer.

    Newest entries first.  Optional filters by level and source category.
    """
    entries = log_buffer.entries(limit=limit, level=level, source=source)
    return {"entries": entries, "count": len(entries)}


@router.get("/stream")
async def stream_logs(
    level: str | None = Query(None, description="Filter by level"),
    source: str | None = Query(None, description="Filter by source category"),
):
    """Server-Sent Events stream of log entries in real time.

    Sends new log entries as they occur.  Each SSE message is a JSON
    object with timestamp, level, source, logger, message, module, and
    lineno fields.
    """

    async def event_generator():
        """Yield SSE events as new log entries arrive."""
        event = log_buffer.event
        if event is None:
            return

        # Track position in the buffer
        seen = len(log_buffer._buffer)
        level_upper = level.upper() if level else None

        while True:
            event.clear()
            # Check for new entries since last seen
            current_len = len(log_buffer._buffer)
            if current_len > seen:
                new_entries = list(log_buffer._buffer)[seen:]
                for entry in new_entries:
                    if level_upper and entry["level"] != level_upper:
                        continue
                    if source and entry["source"] != source:
                        continue
                    data = json.dumps(entry)
                    yield f"data: {data}\n\n"
                seen = current_len
            elif current_len < seen:
                # Buffer wrapped
                seen = current_len

            # Wait for new entries (with timeout for keepalive)
            try:
                await asyncio.wait_for(event.wait(), timeout=30)
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
