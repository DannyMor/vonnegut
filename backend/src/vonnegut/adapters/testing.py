# backend/src/vonnegut/adapters/testing.py
from vonnegut.adapters.base import DatabaseAdapter
from vonnegut.adapters.memory import InMemoryAdapter


class InMemoryAdapterFactory:
    """Test adapter factory — returns a shared InMemoryAdapter for all connections.
    Inject this in tests to avoid needing real database connections."""

    def __init__(self, adapter: InMemoryAdapter):
        self._adapter = adapter

    async def create(self, connection: dict) -> DatabaseAdapter:
        return self._adapter


# Alias for backwards compatibility with task spec
TestAdapterFactory = InMemoryAdapterFactory
