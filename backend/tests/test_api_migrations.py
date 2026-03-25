# backend/tests/test_api_migrations.py
import json

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from vonnegut.main import create_app
from vonnegut.database import SqliteDatabase as Database


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


async def _create_connections(client):
    src = await client.post("/api/v1/connections", json={
        "name": "Source",
        "config": {"type": "postgres_direct", "host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
    })
    tgt = await client.post("/api/v1/connections", json={
        "name": "Target",
        "config": {"type": "postgres_direct", "host": "h2", "port": 5432, "database": "d2", "user": "u", "password": "p"},
    })
    return src.json()["id"], tgt.json()["id"]


@pytest.mark.asyncio
async def test_create_migration(client):
    src_id, tgt_id = await _create_connections(client)
    resp = await client.post("/api/v1/migrations", json={
        "name": "Test Migration",
        "source_connection_id": src_id,
        "target_connection_id": tgt_id,
        "source_table": "users",
        "target_table": "users_copy",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Test Migration"
    assert data["status"] == "draft"


@pytest.mark.asyncio
async def test_list_migrations(client):
    src_id, tgt_id = await _create_connections(client)
    await client.post("/api/v1/migrations", json={
        "name": "Mig1", "source_connection_id": src_id, "target_connection_id": tgt_id,
        "source_table": "t1", "target_table": "t2",
    })
    resp = await client.get("/api/v1/migrations")
    assert resp.status_code == 200
    assert len(resp.json()) >= 1


@pytest.mark.asyncio
async def test_get_migration(client):
    src_id, tgt_id = await _create_connections(client)
    create_resp = await client.post("/api/v1/migrations", json={
        "name": "Fetch Me", "source_connection_id": src_id, "target_connection_id": tgt_id,
        "source_table": "t1", "target_table": "t2",
    })
    mig_id = create_resp.json()["id"]
    resp = await client.get(f"/api/v1/migrations/{mig_id}")
    assert resp.status_code == 200
    assert resp.json()["name"] == "Fetch Me"
    assert resp.json()["transformations"] == []


@pytest.mark.asyncio
async def test_update_migration(client):
    src_id, tgt_id = await _create_connections(client)
    create_resp = await client.post("/api/v1/migrations", json={
        "name": "Old", "source_connection_id": src_id, "target_connection_id": tgt_id,
        "source_table": "t1", "target_table": "t2",
    })
    mig_id = create_resp.json()["id"]
    resp = await client.put(f"/api/v1/migrations/{mig_id}", json={"name": "New"})
    assert resp.status_code == 200
    assert resp.json()["name"] == "New"


@pytest.mark.asyncio
async def test_delete_migration(client):
    src_id, tgt_id = await _create_connections(client)
    create_resp = await client.post("/api/v1/migrations", json={
        "name": "Del", "source_connection_id": src_id, "target_connection_id": tgt_id,
        "source_table": "t1", "target_table": "t2",
    })
    mig_id = create_resp.json()["id"]
    resp = await client.delete(f"/api/v1/migrations/{mig_id}")
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_run_stream_requires_validation(client):
    src_id, tgt_id = await _create_connections(client)
    create_resp = await client.post("/api/v1/migrations", json={
        "name": "Gated", "source_connection_id": src_id, "target_connection_id": tgt_id,
        "source_table": "t1", "target_table": "t2",
    })
    mig_id = create_resp.json()["id"]
    resp = await client.post(f"/api/v1/migrations/{mig_id}/run-stream")
    assert resp.status_code == 409
    assert "validated" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_run_requires_validation(client):
    src_id, tgt_id = await _create_connections(client)
    create_resp = await client.post("/api/v1/migrations", json={
        "name": "Gated", "source_connection_id": src_id, "target_connection_id": tgt_id,
        "source_table": "t1", "target_table": "t2",
    })
    mig_id = create_resp.json()["id"]
    resp = await client.post(f"/api/v1/migrations/{mig_id}/run")
    assert resp.status_code == 409
    assert "validated" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_run_stream_allowed_when_valid(app, client):
    """Run-stream should pass the validation gate when metadata is VALID."""
    src_id, tgt_id = await _create_connections(client)
    create_resp = await client.post("/api/v1/migrations", json={
        "name": "Valid Run", "source_connection_id": src_id, "target_connection_id": tgt_id,
        "source_table": "t1", "target_table": "t2",
    })
    mig_id = create_resp.json()["id"]

    # Manually set metadata to VALID
    metadata_repo = app.state.pipeline_metadata_repo
    await metadata_repo.get_or_create(mig_id)
    await metadata_repo.update_validation(mig_id, "VALID", validated_hash="abc123")

    # Should pass the validation gate (will fail later on connection, but not 409)
    resp = await client.post(f"/api/v1/migrations/{mig_id}/run-stream")
    assert resp.status_code != 409


@pytest.mark.asyncio
async def test_validation_endpoint(app, client):
    src_id, tgt_id = await _create_connections(client)
    create_resp = await client.post("/api/v1/migrations", json={
        "name": "Check Validation", "source_connection_id": src_id,
        "target_connection_id": tgt_id,
        "source_table": "t1", "target_table": "t2",
    })
    mig_id = create_resp.json()["id"]

    resp = await client.get(f"/api/v1/migrations/{mig_id}/validation")
    assert resp.status_code == 200
    data = resp.json()
    assert data["validation_status"] == "DRAFT"
    assert data["validated_hash"] is None
