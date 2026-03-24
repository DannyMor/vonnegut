from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass
class ColumnSchema:
    name: str
    type: str
    category: str
    nullable: bool
    default: str | None
    is_primary_key: bool
    foreign_key: str | None   # "table.column" or None
    is_unique: bool


class DatabaseAdapter(ABC):
    """Interface for all database adapters. Implementations must handle
     the connection lifecycle and provide schema introspection and data access."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the database."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close the database connection and clean up resources."""

    @abstractmethod
    async def execute(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Execute a query and return rows as a list of dicts."""

    @abstractmethod
    async def fetch_tables(self) -> list[str]:
        """Return the list of table names in the database."""

    @abstractmethod
    async def fetch_schema(self, table: str) -> list[ColumnSchema]:
        """Return column schema for a table."""

    @abstractmethod
    async def fetch_sample(self, table: str, rows: int = 10) -> list[dict[str, Any]]:
        """Return sample rows from a table."""

    @abstractmethod
    async def fetch_databases(self) -> list[str]:
        """Return list of database names on this server."""


class AdapterFactory(Protocol):
    """Protocol for creating adapters from connection config.
    Implement this to swap how adapters are created (e.g., for testing)."""

    async def create(self, connection: dict) -> DatabaseAdapter: ...
