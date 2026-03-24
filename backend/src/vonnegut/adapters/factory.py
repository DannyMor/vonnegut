import json as json_mod

from vonnegut.adapters.base import AdapterFactory, DatabaseAdapter
from vonnegut.adapters.postgres_direct import PostgresDirectAdapter
from vonnegut.adapters.postgres_exec import PostgresExecAdapter

_adapter_registry: dict[str, type] = {
    "postgres_direct": PostgresDirectAdapter,
    "postgres_pod": PostgresExecAdapter,
}


class DefaultAdapterFactory:
    """Production adapter factory — registry-based dispatch."""

    async def create(self, connection: dict) -> DatabaseAdapter:
        config = connection["config"] if isinstance(connection["config"], dict) else json_mod.loads(connection["config"])
        conn_type = config.get("type")
        adapter_cls = _adapter_registry.get(conn_type)
        if adapter_cls is None:
            raise ValueError(f"Unsupported connection type: {conn_type}")
        adapter = adapter_cls.from_config(config)
        await adapter.connect()
        return adapter
