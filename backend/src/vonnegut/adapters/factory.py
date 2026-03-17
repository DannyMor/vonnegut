# backend/src/vonnegut/adapters/factory.py
import json as json_mod

from vonnegut.adapters.base import AdapterFactory, DatabaseAdapter
from vonnegut.adapters.postgres_direct import PostgresDirectAdapter


class DefaultAdapterFactory:
    """Production adapter factory — creates real database adapters."""

    async def create(self, connection: dict) -> DatabaseAdapter:
        conn_type = connection["type"]
        config = connection["config"] if isinstance(connection["config"], dict) else json_mod.loads(connection["config"])

        if conn_type == "postgres_direct":
            adapter = PostgresDirectAdapter(
                host=config["host"],
                port=config["port"],
                database=config["database"],
                user=config["user"],
                password=config["password"],
            )
        else:
            raise ValueError(f"Unsupported connection type: {conn_type}")

        await adapter.connect()
        return adapter
