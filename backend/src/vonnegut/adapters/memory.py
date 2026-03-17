import re
from typing import Any

from vonnegut.adapters.base import ColumnSchema, DatabaseAdapter


class InMemoryAdapter(DatabaseAdapter):
    """In-memory implementation of DatabaseAdapter for testing.
    Stores tables as dicts of schema + rows. Supports basic SQL-like operations."""

    def __init__(self, tables: dict[str, dict] | None = None):
        self._tables: dict[str, dict] = tables or {}
        self._connected = False

    def add_table(self, name: str, rows: list[dict]) -> None:
        """Helper for tests — add a table with rows (auto-generates schema)."""
        if rows:
            schema = [
                ColumnSchema(column=col, type="text", nullable=True, is_primary_key=False)
                for col in rows[0].keys()
            ]
        else:
            schema = []
        self._tables[name] = {"schema": schema, "rows": list(rows)}

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False

    async def execute(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        q = query.strip().upper()

        count_match = re.match(r"SELECT\s+COUNT\(\*\)\s+AS\s+\w+\s+FROM\s+(\w+)", q)
        if count_match:
            table = count_match.group(1).lower()
            table_key = self._find_table(table)
            return [{"count": len(self._tables[table_key]["rows"])}]

        select_match = re.match(r"SELECT\s+\*\s+FROM\s+(\w+)", q)
        if select_match:
            table = select_match.group(1).lower()
            table_key = self._find_table(table)
            return list(self._tables[table_key]["rows"])

        truncate_match = re.match(r"TRUNCATE\s+TABLE\s+(\w+)", q)
        if truncate_match:
            table = truncate_match.group(1).lower()
            table_key = self._find_table(table)
            self._tables[table_key]["rows"] = []
            return []

        insert_match = re.match(r"INSERT\s+INTO\s+(\w+)\s*(?:\(([^)]+)\))?", q)
        if insert_match and "INSERT" in q:
            table = insert_match.group(1).lower()
            table_key = self._find_table(table)
            col_str = insert_match.group(2)
            if col_str:
                columns = [c.strip().lower() for c in col_str.split(",")]
            else:
                schema = self._tables[table_key]["schema"]
                columns = [col.column for col in schema]
            row = dict(zip(columns, params))
            self._tables[table_key]["rows"].append(row)
            return []

        return []

    async def fetch_tables(self) -> list[str]:
        return sorted(self._tables.keys())

    async def fetch_schema(self, table: str) -> list[ColumnSchema]:
        table_key = self._find_table(table)
        return list(self._tables[table_key]["schema"])

    async def fetch_sample(self, table: str, rows: int = 10) -> list[dict[str, Any]]:
        table_key = self._find_table(table)
        return list(self._tables[table_key]["rows"][:rows])

    def _find_table(self, name: str) -> str:
        for key in self._tables:
            if key.lower() == name.lower():
                return key
        raise ValueError(f"Table not found: {name}")
