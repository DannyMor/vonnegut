import pytest


@pytest.mark.asyncio
async def test_database_initializes_tables(db):
    tables = await db.fetch_all("SELECT name FROM sqlite_master WHERE type='table'")
    table_names = {row["name"] for row in tables}
    assert "connections" in table_names
    assert "migrations" in table_names
    assert "transformations" in table_names


@pytest.mark.asyncio
async def test_insert_and_fetch_connection(db):
    await db.execute(
        """INSERT INTO connections (id, name, type, config, created_at, updated_at)
           VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
        ("test-id", "my-conn", "postgres_direct", '{"host":"localhost"}'),
    )
    rows = await db.fetch_all("SELECT * FROM connections WHERE id = ?", ("test-id",))
    assert len(rows) == 1
    assert rows[0]["name"] == "my-conn"
    assert rows[0]["type"] == "postgres_direct"
