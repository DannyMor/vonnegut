# backend/tests/test_connection_test.py
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from vonnegut.main import create_app
from vonnegut.database import Database
from vonnegut.adapters.base import DatabaseAdapter
from vonnegut.adapters.memory import InMemoryAdapter
from vonnegut.adapters.testing import TestAdapterFactory


class FailingAdapterFactory:
    """Factory that raises on create -- simulates connection failure."""
    __test__ = False
    async def create(self, connection: dict) -> DatabaseAdapter:
        raise Exception("Connection refused")


@pytest_asyncio.fixture
async def in_memory_adapter():
    adapter = InMemoryAdapter()
    return adapter


@pytest_asyncio.fixture
async def test_adapter_factory(in_memory_adapter):
    return TestAdapterFactory(in_memory_adapter)


@pytest_asyncio.fixture
async def app(tmp_path, encryption_key, test_adapter_factory):
    db = Database(f"sqlite+aiosqlite:///{tmp_path}/test.db")
    await db.initialize()
    application = create_app(db=db, encryption_key=encryption_key, adapter_factory=test_adapter_factory)
    yield application
    await db.close()


@pytest_asyncio.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_connection_test_success(client):
    create_resp = await client.post("/api/v1/connections", json={
        "name": "Test",
        "config": {"type": "postgres_direct", "host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
    })
    conn_id = create_resp.json()["id"]

    resp = await client.post(f"/api/v1/connections/{conn_id}/test")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_connection_test_failure(tmp_path, encryption_key):
    """Use a FailingAdapterFactory to simulate connection failure."""
    db = Database(f"sqlite+aiosqlite:///{tmp_path}/test_fail.db")
    await db.initialize()
    app = create_app(db=db, encryption_key=encryption_key, adapter_factory=FailingAdapterFactory())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post("/api/v1/connections", json={
            "name": "Bad",
            "config": {"type": "postgres_direct", "host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
        })
        conn_id = create_resp.json()["id"]

        resp = await client.post(f"/api/v1/connections/{conn_id}/test")
        assert resp.status_code == 200
        assert resp.json()["status"] == "error"
        assert "Connection refused" in resp.json()["message"]
    await db.close()
