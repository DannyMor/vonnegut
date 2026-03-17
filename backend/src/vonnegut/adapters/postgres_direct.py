from typing import Any

from psycopg import AsyncConnection, sql

from vonnegut.adapters.base import ColumnSchema, DatabaseAdapter


class PostgresDirectAdapter(DatabaseAdapter):
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self._conninfo = f"host={host} port={port} dbname={database} user={user} password={password}"
        self._conn: AsyncConnection | None = None

    async def connect(self) -> None:
        self._conn = await AsyncConnection.connect(self._conninfo)

    async def disconnect(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def execute(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        cursor = await self._conn.execute(query, params)
        if cursor.description is None:
            return []
        columns = [desc[0] for desc in cursor.description]
        rows = await cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    async def fetch_tables(self) -> list[str]:
        cursor = await self._conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        )
        rows = await cursor.fetchall()
        return [row[0] for row in rows]

    async def fetch_schema(self, table: str) -> list[ColumnSchema]:
        cursor = await self._conn.execute(
            """
            SELECT c.column_name, c.data_type, c.is_nullable,
                   CASE WHEN pk.column_name IS NOT NULL THEN 'PRI' END as key
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = %s
            ) pk ON c.column_name = pk.column_name
            WHERE c.table_name = %s ORDER BY c.ordinal_position
            """,
            (table, table),
        )
        rows = await cursor.fetchall()
        return [
            ColumnSchema(column=r[0], type=r[1], nullable=r[2] == "YES", is_primary_key=r[3] == "PRI")
            for r in rows
        ]

    async def fetch_sample(self, table: str, rows: int = 10) -> list[dict[str, Any]]:
        query = sql.SQL("SELECT * FROM {} LIMIT %s").format(sql.Identifier(table))
        cursor = await self._conn.execute(query, (rows,))
        columns = [desc[0] for desc in cursor.description]
        result = await cursor.fetchall()
        return [dict(zip(columns, row)) for row in result]

    async def fetch_databases(self) -> list[str]:
        cursor = await self._conn.execute(
            "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
        )
        rows = await cursor.fetchall()
        return [row[0] for row in rows]
