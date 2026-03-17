# backend/tests/test_api_explorer.py
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from vonnegut.main import create_app
from vonnegut.database import Database
from vonnegut.adapters.base import ColumnSchema
from vonnegut.adapters.memory import InMemoryAdapter
from vonnegut.adapters.testing import TestAdapterFactory


@pytest_asyncio.fixture
def test_adapter():
    return InMemoryAdapter(tables={
        "users": {
            "schema": [
                ColumnSchema(column="id", type="integer", nullable=False, is_primary_key=True),
                ColumnSchema(column="name", type="text", nullable=True, is_primary_key=False),
            ],
            "rows": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ],
        },
        "orders": {
            "schema": [
                ColumnSchema(column="id", type="integer", nullable=False, is_primary_key=True),
            ],
            "rows": [{"id": 1}],
        },
    })


@pytest_asyncio.fixture
async def app(tmp_path, encryption_key, test_adapter):
    db = Database(f"sqlite+aiosqlite:///{tmp_path}/test.db")
    await db.initialize()
    factory = TestAdapterFactory(test_adapter)
    application = create_app(db=db, encryption_key=encryption_key, adapter_factory=factory)
    yield application
    await db.close()


@pytest_asyncio.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _create_connection(client):
    resp = await client.post("/api/v1/connections", json={
        "name": "Test DB", "type": "postgres_direct",
        "config": {"host": "localhost", "port": 5432, "database": "db", "user": "u", "password": "p"},
    })
    return resp.json()["id"]


@pytest.mark.asyncio
async def test_list_tables(client):
    conn_id = await _create_connection(client)
    resp = await client.get(f"/api/v1/connections/{conn_id}/tables")
    assert resp.status_code == 200
    assert set(resp.json()) == {"users", "orders"}


@pytest.mark.asyncio
async def test_get_table_schema(client):
    conn_id = await _create_connection(client)
    resp = await client.get(f"/api/v1/connections/{conn_id}/tables/users/schema")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["column"] == "id"
    assert data[0]["is_primary_key"] is True


@pytest.mark.asyncio
async def test_get_table_sample(client):
    conn_id = await _create_connection(client)
    resp = await client.get(f"/api/v1/connections/{conn_id}/tables/users/sample?rows=2")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_tables_nonexistent_connection(client):
    resp = await client.get("/api/v1/connections/nonexistent/tables")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_databases(client, test_adapter):
    test_adapter.add_database("analytics")
    test_adapter.add_database("production")
    conn_id = await _create_connection(client)
    resp = await client.get(f"/api/v1/connections/{conn_id}/databases")
    assert resp.status_code == 200
    assert resp.json() == ["analytics", "production"]


@pytest.mark.asyncio
async def test_databases_nonexistent_connection(client):
    resp = await client.get("/api/v1/connections/nonexistent/databases")
    assert resp.status_code == 404
