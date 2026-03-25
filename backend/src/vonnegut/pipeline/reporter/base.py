from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


class Reporter(ABC):
    @abstractmethod
    async def emit(self, event_type: str, **data: Any) -> None: ...


class NullReporter(Reporter):
    async def emit(self, event_type: str, **data: Any) -> None:
        pass


class CollectorReporter(Reporter):
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    async def emit(self, event_type: str, **data: Any) -> None:
        self.events.append({"type": event_type, **data})

    def events_of_type(self, event_type: str) -> list[dict[str, Any]]:
        return [e for e in self.events if e["type"] == event_type]
