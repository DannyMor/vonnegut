import pytest

from vonnegut.adapters.memory import InMemoryAdapter
from vonnegut.adapters.base import DatabaseAdapter, ColumnSchema


@pytest.fixture
def adapter():
    tables = {
        "users": {
            "schema": [
                ColumnSchema(column="id", type="integer", nullable=False, is_primary_key=True),
                ColumnSchema(column="name", type="text", nullable=True, is_primary_key=False),
                ColumnSchema(column="email", type="text", nullable=False, is_primary_key=False),
            ],
            "rows": [
                {"id": 1, "name": "Alice", "email": "alice@example.com"},
                {"id": 2, "name": "Bob", "email": "bob@example.com"},
                {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
            ],
        },
        "orders": {
            "schema": [
                ColumnSchema(column="id", type="integer", nullable=False, is_primary_key=True),
                ColumnSchema(column="user_id", type="integer", nullable=False, is_primary_key=False),
                ColumnSchema(column="amount", type="numeric", nullable=False, is_primary_key=False),
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
    assert schema[0].column == "id"
    assert schema[0].type == "integer"
    assert schema[0].is_primary_key is True
    assert schema[1].nullable is True


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
