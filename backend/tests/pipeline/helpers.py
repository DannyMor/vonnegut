from __future__ import annotations
from typing import Any

import duckdb
import pyarrow as pa

from vonnegut.adapters.base import DatabaseAdapter, ColumnSchema


class InMemoryDatabaseAdapter(DatabaseAdapter):
    """A real DatabaseAdapter backed by an in-process DuckDB database.

    Supports registering tables from dicts and executing real SQL against them.
    Used as a test double that behaves like a real database.
    """

    def __init__(self) -> None:
        self._conn = duckdb.connect()
        self._tables: dict[str, list[dict[str, Any]]] = {}

    def seed_table(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        self._tables[table_name] = rows
        if rows:
            arrow_table = pa.Table.from_pylist(rows)
            self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self._conn.register(f"_seed_{table_name}", arrow_table)
            self._conn.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM _seed_{table_name}",
            )
            self._conn.unregister(f"_seed_{table_name}")

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        self._conn.close()

    async def execute(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        result = self._conn.execute(query).fetchall()
        columns = [desc[0] for desc in self._conn.description]
        return [dict(zip(columns, row)) for row in result]

    async def fetch_tables(self) -> list[str]:
        return list(self._tables.keys())

    async def fetch_schema(self, table: str) -> list[ColumnSchema]:
        result = self._conn.execute(f"DESCRIBE {table}").fetchall()
        return [
            ColumnSchema(
                name=row[0], type=row[1], category="", nullable=True,
                default=None, is_primary_key=False, foreign_key=None, is_unique=False,
            )
            for row in result
        ]

    async def fetch_sample(self, table: str, rows: int = 10) -> list[dict[str, Any]]:
        return await self.execute(f"SELECT * FROM {table} LIMIT {rows}")

    async def fetch_databases(self) -> list[str]:
        return ["memory"]
