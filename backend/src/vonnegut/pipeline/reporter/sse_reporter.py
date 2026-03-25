from __future__ import annotations
from collections.abc import Awaitable, Callable
from typing import Any

from vonnegut.pipeline.reporter.base import Reporter


class SSEReporter(Reporter):
    """Bridges the pipeline Reporter interface to an async callback for SSE streaming."""

    def __init__(self, callback: Callable[[dict], Awaitable[None]]) -> None:
        self._callback = callback

    async def emit(self, event_type: str, **data: Any) -> None:
        event = {"type": event_type, **data}
        await self._callback(event)
