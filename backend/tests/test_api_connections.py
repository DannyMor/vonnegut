# backend/tests/test_api_connections.py
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from vonnegut.main import create_app
from vonnegut.database import Database


@pytest_asyncio.fixture
async def app(tmp_path, encryption_key):
    db = Database(f"sqlite+aiosqlite:///{tmp_path}/test.db")
    await db.initialize()
    application = create_app(db=db, encryption_key=encryption_key)
    yield application
    await db.close()


@pytest_asyncio.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_create_connection(client):
    resp = await client.post("/api/v1/connections", json={
        "name": "Test DB",
        "type": "postgres_direct",
        "config": {"host": "localhost", "port": 5432, "database": "db", "user": "u", "password": "p"},
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Test DB"
    assert data["config"]["password"] == "********"


@pytest.mark.asyncio
async def test_list_connections(client):
    await client.post("/api/v1/connections", json={
        "name": "DB1", "type": "postgres_direct",
        "config": {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
    })
    resp = await client.get("/api/v1/connections")
    assert resp.status_code == 200
    assert len(resp.json()) >= 1


@pytest.mark.asyncio
async def test_get_connection(client):
    create_resp = await client.post("/api/v1/connections", json={
        "name": "Fetch Me", "type": "postgres_direct",
        "config": {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
    })
    conn_id = create_resp.json()["id"]
    resp = await client.get(f"/api/v1/connections/{conn_id}")
    assert resp.status_code == 200
    assert resp.json()["name"] == "Fetch Me"
    assert resp.json()["config"]["password"] == "********"


@pytest.mark.asyncio
async def test_get_nonexistent_connection(client):
    resp = await client.get("/api/v1/connections/nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_connection(client):
    create_resp = await client.post("/api/v1/connections", json={
        "name": "Old Name", "type": "postgres_direct",
        "config": {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
    })
    conn_id = create_resp.json()["id"]
    resp = await client.put(f"/api/v1/connections/{conn_id}", json={"name": "New Name"})
    assert resp.status_code == 200
    assert resp.json()["name"] == "New Name"


@pytest.mark.asyncio
async def test_delete_connection(client):
    create_resp = await client.post("/api/v1/connections", json={
        "name": "Delete Me", "type": "postgres_direct",
        "config": {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
    })
    conn_id = create_resp.json()["id"]
    resp = await client.delete(f"/api/v1/connections/{conn_id}")
    assert resp.status_code == 204
    resp = await client.get(f"/api/v1/connections/{conn_id}")
    assert resp.status_code == 404
