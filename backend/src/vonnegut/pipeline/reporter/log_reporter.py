from __future__ import annotations

import logging
from typing import Any

from vonnegut.pipeline.reporter.base import Reporter

_DEFAULT_LOGGER = logging.getLogger("vonnegut.pipeline")


class LogReporter(Reporter):
    """Writes pipeline events to a Python logger."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or _DEFAULT_LOGGER

    async def emit(self, event_type: str, **data: Any) -> None:
        level = logging.ERROR if event_type in ("pipeline_failed", "node_error") else logging.INFO
        extra = " ".join(f"{k}={v}" for k, v in data.items()) if data else ""
        self._logger.log(level, "[pipeline] %s %s", event_type, extra)
