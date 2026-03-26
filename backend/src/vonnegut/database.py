from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Protocol, runtime_checkable

import aiosqlite


@runtime_checkable
class AppDatabase(Protocol):
    """Abstract interface for the application metadata database.

    Implementations handle connection lifecycle, query execution,
    and transaction management. Repositories depend on this protocol,
    never on a concrete database.
    """

    async def initialize(self) -> None: ...
    async def close(self) -> None: ...
    async def execute(self, query: str, params: Sequence = ()) -> None: ...
    async def fetch_one(self, query: str, params: Sequence = ()) -> dict | None: ...
    async def fetch_all(self, query: str, params: Sequence = ()) -> list[dict]: ...
    def transaction(self) -> AsyncIterator[None]: ...


_SCHEMA = """
CREATE TABLE IF NOT EXISTS connections (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    config TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipelines (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    source_connection_id TEXT NOT NULL REFERENCES connections(id),
    target_connection_id TEXT NOT NULL REFERENCES connections(id),
    source_table TEXT NOT NULL,
    target_table TEXT NOT NULL,
    source_query TEXT NOT NULL DEFAULT '',
    source_schema TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'draft'
        CHECK(status IN ('draft', 'testing', 'running', 'completed', 'failed', 'cancelled')),
    truncate_target INTEGER NOT NULL DEFAULT 0,
    rows_processed INTEGER,
    total_rows INTEGER,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_steps (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT,
    position INTEGER NOT NULL,
    step_type TEXT NOT NULL CHECK(step_type IN ('sql', 'code', 'ai')),
    config TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transformations (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    "order" INTEGER NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('column_mapping', 'sql_expression', 'ai_generated')),
    config TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_metadata (
    pipeline_id TEXT PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,
    validation_status TEXT NOT NULL DEFAULT 'DRAFT'
        CHECK(validation_status IN ('DRAFT', 'VALIDATING', 'VALID', 'INVALID')),
    validated_hash TEXT,
    last_validated_at TEXT,
    node_schemas TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL
);
"""


class SqliteDatabase:
    """SQLite implementation of AppDatabase."""

    def __init__(self, url: str):
        self._path = url.split("///")[-1] if ":///" in url else url
        self._conn: aiosqlite.Connection | None = None

    async def initialize(self):
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(_SCHEMA)
        await self._conn.execute("PRAGMA foreign_keys = ON")
        await self._conn.execute("PRAGMA journal_mode = WAL")
        await self._conn.commit()

    async def close(self):
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def execute(self, query: str, params: Sequence = ()) -> None:
        await self._conn.execute(query, params)
        await self._conn.commit()

    async def fetch_one(self, query: str, params: Sequence = ()) -> dict | None:
        cursor = await self._conn.execute(query, params)
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def fetch_all(self, query: str, params: Sequence = ()) -> list[dict]:
        cursor = await self._conn.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    @asynccontextmanager
    async def transaction(self):
        await self._conn.execute("BEGIN")
        try:
            yield
            await self._conn.commit()
        except BaseException:
            await self._conn.rollback()
            raise


def _sqlite_to_pg_params(query: str) -> str:
    """Convert SQLite-style ? placeholders to psycopg %s placeholders."""
    return query.replace("?", "%s")


_PG_SCHEMA_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS connections (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        config TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS pipelines (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        source_connection_id TEXT NOT NULL REFERENCES connections(id),
        target_connection_id TEXT NOT NULL REFERENCES connections(id),
        source_table TEXT NOT NULL,
        target_table TEXT NOT NULL,
        source_query TEXT NOT NULL DEFAULT '',
        source_schema TEXT NOT NULL DEFAULT '[]',
        status TEXT NOT NULL DEFAULT 'draft'
            CHECK(status IN ('draft', 'testing', 'running', 'completed', 'failed', 'cancelled')),
        truncate_target INTEGER NOT NULL DEFAULT 0,
        rows_processed INTEGER,
        total_rows INTEGER,
        error_message TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS pipeline_steps (
        id TEXT PRIMARY KEY,
        pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        description TEXT,
        position INTEGER NOT NULL,
        step_type TEXT NOT NULL CHECK(step_type IN ('sql', 'code', 'ai')),
        config TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS transformations (
        id TEXT PRIMARY KEY,
        pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
        "order" INTEGER NOT NULL,
        type TEXT NOT NULL CHECK(type IN ('column_mapping', 'sql_expression', 'ai_generated')),
        config TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS pipeline_metadata (
        pipeline_id TEXT PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,
        validation_status TEXT NOT NULL DEFAULT 'DRAFT'
            CHECK(validation_status IN ('DRAFT', 'VALIDATING', 'VALID', 'INVALID')),
        validated_hash TEXT,
        last_validated_at TEXT,
        node_schemas TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL
    )""",
]


class PostgresDatabase:
    """PostgreSQL implementation of AppDatabase using psycopg3 (async)."""

    def __init__(self, url: str):
        self._url = url
        self._pool = None

    async def initialize(self):
        import psycopg_pool
        self._pool = psycopg_pool.AsyncConnectionPool(
            self._url, min_size=1, max_size=5, open=False,
        )
        await self._pool.open()
        async with self._pool.connection() as conn:
            for stmt in _PG_SCHEMA_STATEMENTS:
                await conn.execute(stmt)
            await conn.commit()

    async def close(self):
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def execute(self, query: str, params: Sequence = ()) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(_sqlite_to_pg_params(query), params)
            await conn.commit()

    async def fetch_one(self, query: str, params: Sequence = ()) -> dict | None:
        async with self._pool.connection() as conn:
            cursor = await conn.execute(_sqlite_to_pg_params(query), params)
            row = await cursor.fetchone()
            if row is None:
                return None
            cols = [desc[0] for desc in cursor.description]
            return dict(zip(cols, row))

    async def fetch_all(self, query: str, params: Sequence = ()) -> list[dict]:
        async with self._pool.connection() as conn:
            cursor = await conn.execute(_sqlite_to_pg_params(query), params)
            rows = await cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            return [dict(zip(cols, row)) for row in rows]

    @asynccontextmanager
    async def transaction(self):
        async with self._pool.connection() as conn:
            async with conn.transaction():
                self._tx_conn = conn
                yield
                del self._tx_conn
