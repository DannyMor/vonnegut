import aiosqlite

_SCHEMA = """
CREATE TABLE IF NOT EXISTS connections (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('postgres_direct', 'postgres_pod')),
    config TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS migrations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    source_connection_id TEXT NOT NULL REFERENCES connections(id),
    target_connection_id TEXT NOT NULL REFERENCES connections(id),
    source_table TEXT NOT NULL,
    target_table TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft'
        CHECK(status IN ('draft', 'testing', 'running', 'completed', 'failed', 'cancelled')),
    truncate_target INTEGER NOT NULL DEFAULT 0,
    rows_processed INTEGER,
    total_rows INTEGER,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transformations (
    id TEXT PRIMARY KEY,
    migration_id TEXT NOT NULL REFERENCES migrations(id) ON DELETE CASCADE,
    "order" INTEGER NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('column_mapping', 'sql_expression', 'ai_generated')),
    config TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


class Database:
    def __init__(self, url: str):
        # Extract file path from URL like "sqlite+aiosqlite:///path/to/db"
        self._path = url.split("///")[-1] if ":///" in url else url
        self._conn: aiosqlite.Connection | None = None

    async def initialize(self):
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(_SCHEMA)
        await self._conn.execute("PRAGMA foreign_keys = ON")
        await self._conn.commit()

    async def close(self):
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def execute(self, query: str, params: tuple = ()) -> None:
        await self._conn.execute(query, params)
        await self._conn.commit()

    async def fetch_one(self, query: str, params: tuple = ()) -> dict | None:
        cursor = await self._conn.execute(query, params)
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def fetch_all(self, query: str, params: tuple = ()) -> list[dict]:
        cursor = await self._conn.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
