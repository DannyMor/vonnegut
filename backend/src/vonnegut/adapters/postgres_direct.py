from typing import Any

from psycopg import AsyncConnection, sql

from vonnegut.adapters.base import ColumnSchema, DatabaseAdapter
from vonnegut.adapters.pg_types import pg_type_category


class PostgresDirectAdapter(DatabaseAdapter):
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self._conn_kwargs = dict(host=host, port=port, dbname=database, user=user, password=password)
        self._conn: AsyncConnection | None = None

    @classmethod
    def from_config(cls, config: dict) -> "PostgresDirectAdapter":
        return cls(
            host=config["host"], port=config["port"], database=config["database"],
            user=config["user"], password=config["password"],
        )

    async def connect(self) -> None:
        self._conn = await AsyncConnection.connect(**self._conn_kwargs)

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
            SELECT
                c.column_name,
                c.udt_name,
                c.is_nullable,
                c.column_default,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_pk,
                fk.fk_ref,
                CASE WHEN uq.column_name IS NOT NULL THEN true ELSE false END as is_unique
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT DISTINCT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = %s
            ) pk ON c.column_name = pk.column_name
            LEFT JOIN (
                SELECT DISTINCT ON (ku.column_name) ku.column_name,
                       ccu.table_name || '.' || ccu.column_name AS fk_ref
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY' AND ku.table_name = %s
            ) fk ON c.column_name = fk.column_name
            LEFT JOIN (
                SELECT DISTINCT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                WHERE tc.constraint_type = 'UNIQUE' AND tc.table_name = %s
            ) uq ON c.column_name = uq.column_name
            WHERE c.table_schema = 'public' AND c.table_name = %s
            ORDER BY c.ordinal_position
            """,
            (table, table, table, table),
        )
        rows = await cursor.fetchall()
        return [
            ColumnSchema(
                name=r[0],
                type=r[1],
                category=pg_type_category(r[1]),
                nullable=r[2] == "YES",
                default=r[3],
                is_primary_key=bool(r[4]),
                foreign_key=r[5],
                is_unique=bool(r[6]),
            )
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
