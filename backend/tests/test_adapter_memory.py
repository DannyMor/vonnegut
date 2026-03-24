import pytest

from vonnegut.adapters.memory import InMemoryAdapter
from vonnegut.adapters.base import DatabaseAdapter, ColumnSchema


@pytest.fixture
def adapter():
    tables = {
        "users": {
            "schema": [
                ColumnSchema(name="id", type="int4", category="number", nullable=False, default=None, is_primary_key=True, foreign_key=None, is_unique=False),
                ColumnSchema(name="name", type="text", category="text", nullable=True, default=None, is_primary_key=False, foreign_key=None, is_unique=False),
                ColumnSchema(name="email", type="text", category="text", nullable=False, default=None, is_primary_key=False, foreign_key=None, is_unique=True),
            ],
            "rows": [
                {"id": 1, "name": "Alice", "email": "alice@example.com"},
                {"id": 2, "name": "Bob", "email": "bob@example.com"},
                {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
            ],
        },
        "orders": {
            "schema": [
                ColumnSchema(name="id", type="int4", category="number", nullable=False, default=None, is_primary_key=True, foreign_key=None, is_unique=False),
                ColumnSchema(name="user_id", type="int4", category="number", nullable=False, default=None, is_primary_key=False, foreign_key="users.id", is_unique=False),
                ColumnSchema(name="amount", type="numeric", category="number", nullable=False, default=None, is_primary_key=False, foreign_key=None, is_unique=False),
            ],
            "rows": [
                {"id": 1, "user_id": 1, "amount": 99.99},
            ],
        },
    }
    return InMemoryAdapter(tables=tables)


def test_implements_database_adapter(adapter):
    assert isinstance(adapter, DatabaseAdapter)


@pytest.mark.asyncio
async def test_connect_disconnect(adapter):
    await adapter.connect()
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_fetch_tables(adapter):
    tables = await adapter.fetch_tables()
    assert set(tables) == {"users", "orders"}


@pytest.mark.asyncio
async def test_fetch_schema(adapter):
    schema = await adapter.fetch_schema("users")
    assert len(schema) == 3
    assert isinstance(schema[0], ColumnSchema)
    assert schema[0].name == "id"
    assert schema[0].type == "int4"
    assert schema[0].category == "number"
    assert schema[0].is_primary_key is True
    assert schema[1].nullable is True
    assert schema[2].is_unique is True


@pytest.mark.asyncio
async def test_fetch_schema_foreign_key(adapter):
    schema = await adapter.fetch_schema("orders")
    user_id_col = next(c for c in schema if c.name == "user_id")
    assert user_id_col.foreign_key == "users.id"


@pytest.mark.asyncio
async def test_fetch_sample(adapter):
    rows = await adapter.fetch_sample("users", rows=2)
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_fetch_sample_all_rows(adapter):
    rows = await adapter.fetch_sample("users", rows=100)
    assert len(rows) == 3


@pytest.mark.asyncio
async def test_execute_select(adapter):
    rows = await adapter.execute("SELECT * FROM users")
    assert len(rows) == 3


@pytest.mark.asyncio
async def test_execute_count(adapter):
    rows = await adapter.execute("SELECT COUNT(*) as count FROM users")
    assert rows[0]["count"] == 3


@pytest.mark.asyncio
async def test_execute_insert(adapter):
    await adapter.execute(
        "INSERT INTO users VALUES (%s, %s, %s)",
        (4, "Diana", "diana@example.com"),
    )
    rows = await adapter.fetch_sample("users", rows=100)
    assert len(rows) == 4


@pytest.mark.asyncio
async def test_execute_truncate(adapter):
    await adapter.execute("TRUNCATE TABLE users")
    rows = await adapter.fetch_sample("users", rows=100)
    assert len(rows) == 0


@pytest.mark.asyncio
async def test_fetch_databases():
    adapter = InMemoryAdapter()
    adapter.add_database("analytics")
    adapter.add_database("production")
    result = await adapter.fetch_databases()
    assert result == ["analytics", "production"]


@pytest.mark.asyncio
async def test_fetch_databases_empty():
    adapter = InMemoryAdapter()
    result = await adapter.fetch_databases()
    assert result == []


@pytest.mark.asyncio
async def test_add_table_auto_schema():
    adapter = InMemoryAdapter()
    adapter.add_table("items", [{"id": 1, "title": "Book"}])
    schema = await adapter.fetch_schema("items")
    assert len(schema) == 2
    assert schema[0].name == "id"
    assert schema[0].category == "text"  # auto-generated defaults to text
    assert schema[0].foreign_key is None
