# backend/tests/test_api_transformations.py
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


async def _create_pipeline(client):
    src = await client.post("/api/v1/connections", json={
        "name": "Src",
        "config": {"type": "postgres_direct", "host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
    })
    tgt = await client.post("/api/v1/connections", json={
        "name": "Tgt",
        "config": {"type": "postgres_direct", "host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
    })
    pipeline = await client.post("/api/v1/pipelines", json={
        "name": "Pipeline", "source_connection_id": src.json()["id"],
        "target_connection_id": tgt.json()["id"],
        "source_table": "t1", "target_table": "t2",
    })
    return pipeline.json()["id"]


@pytest.mark.asyncio
async def test_add_transformation(client):
    pipeline_id = await _create_pipeline(client)
    resp = await client.post(f"/api/v1/pipelines/{pipeline_id}/transformations", json={
        "type": "sql_expression",
        "config": {"expression": "UPPER(name)", "output_column": "name_upper"},
    })
    assert resp.status_code == 201
    assert resp.json()["type"] == "sql_expression"
    assert resp.json()["order"] == 0


@pytest.mark.asyncio
async def test_add_multiple_transformations_ordered(client):
    pipeline_id = await _create_pipeline(client)
    await client.post(f"/api/v1/pipelines/{pipeline_id}/transformations", json={
        "type": "column_mapping",
        "config": {"mappings": [{"source_col": "a", "target_col": "b", "drop": False}]},
    })
    resp = await client.post(f"/api/v1/pipelines/{pipeline_id}/transformations", json={
        "type": "sql_expression",
        "config": {"expression": "UPPER(b)", "output_column": "b_upper"},
    })
    assert resp.json()["order"] == 1


@pytest.mark.asyncio
async def test_update_transformation(client):
    pipeline_id = await _create_pipeline(client)
    create_resp = await client.post(f"/api/v1/pipelines/{pipeline_id}/transformations", json={
        "type": "sql_expression",
        "config": {"expression": "UPPER(name)", "output_column": "name_upper"},
    })
    t_id = create_resp.json()["id"]
    resp = await client.put(f"/api/v1/pipelines/{pipeline_id}/transformations/{t_id}", json={
        "config": {"expression": "LOWER(name)", "output_column": "name_lower"},
    })
    assert resp.status_code == 200
    assert resp.json()["config"]["expression"] == "LOWER(name)"


@pytest.mark.asyncio
async def test_delete_transformation(client):
    pipeline_id = await _create_pipeline(client)
    create_resp = await client.post(f"/api/v1/pipelines/{pipeline_id}/transformations", json={
        "type": "sql_expression",
        "config": {"expression": "UPPER(name)", "output_column": "x"},
    })
    t_id = create_resp.json()["id"]
    resp = await client.delete(f"/api/v1/pipelines/{pipeline_id}/transformations/{t_id}")
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_reorder_transformations(client):
    pipeline_id = await _create_pipeline(client)
    r1 = await client.post(f"/api/v1/pipelines/{pipeline_id}/transformations", json={
        "type": "sql_expression", "config": {"expression": "UPPER(a)", "output_column": "x"},
    })
    r2 = await client.post(f"/api/v1/pipelines/{pipeline_id}/transformations", json={
        "type": "sql_expression", "config": {"expression": "LOWER(b)", "output_column": "y"},
    })
    id1, id2 = r1.json()["id"], r2.json()["id"]
    resp = await client.put(f"/api/v1/pipelines/{pipeline_id}/transformations/reorder", json={
        "order": [id2, id1],
    })
    assert resp.status_code == 200
    pipeline = await client.get(f"/api/v1/pipelines/{pipeline_id}")
    transforms = pipeline.json()["transformations"]
    assert transforms[0]["id"] == id2
    assert transforms[1]["id"] == id1
