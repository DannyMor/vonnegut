import json
import pytest

from vonnegut.repositories import (
    ConnectionRepository,
    PipelineRepository,
    PipelineMetadataRepository,
    PipelineStepRepository,
    TransformationRepository,
)


@pytest.fixture
def conn_repo(db):
    return ConnectionRepository(db)


@pytest.fixture
def pipeline_repo(db):
    return PipelineRepository(db)


@pytest.fixture
def step_repo(db):
    return PipelineStepRepository(db)


@pytest.fixture
def metadata_repo(db):
    return PipelineMetadataRepository(db)


@pytest.fixture
def transform_repo(db):
    return TransformationRepository(db)


async def _create_connection(conn_repo: ConnectionRepository) -> dict:
    return await conn_repo.create("test-conn", json.dumps({"type": "postgres_direct"}))


async def _create_pipeline(conn_repo: ConnectionRepository, pipeline_repo: PipelineRepository) -> dict:
    conn = await _create_connection(conn_repo)
    return await pipeline_repo.create(
        name="Test Pipeline",
        source_connection_id=conn["id"],
        target_connection_id=conn["id"],
        source_table="src",
        target_table="tgt",
    )


class TestConnectionRepository:
    @pytest.mark.asyncio
    async def test_create_and_get(self, conn_repo):
        row = await conn_repo.create("my-conn", '{"type":"pg"}')
        assert row["name"] == "my-conn"
        fetched = await conn_repo.get(row["id"])
        assert fetched["name"] == "my-conn"

    @pytest.mark.asyncio
    async def test_list_all(self, conn_repo):
        await conn_repo.create("a", '{}')
        await conn_repo.create("b", '{}')
        rows = await conn_repo.list_all()
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_update(self, conn_repo):
        row = await conn_repo.create("old", '{}')
        updated = await conn_repo.update(row["id"], "new", '{"updated": true}')
        assert updated["name"] == "new"

    @pytest.mark.asyncio
    async def test_delete(self, conn_repo):
        row = await conn_repo.create("del", '{}')
        assert await conn_repo.delete(row["id"]) is True
        assert await conn_repo.get(row["id"]) is None
        assert await conn_repo.delete("nonexistent") is False


class TestPipelineRepository:
    @pytest.mark.asyncio
    async def test_create_and_get(self, conn_repo, pipeline_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        assert pipeline["name"] == "Test Pipeline"
        assert pipeline["status"] == "draft"
        fetched = await pipeline_repo.get(pipeline["id"])
        assert fetched["name"] == "Test Pipeline"

    @pytest.mark.asyncio
    async def test_list_all(self, conn_repo, pipeline_repo):
        await _create_pipeline(conn_repo, pipeline_repo)
        await _create_pipeline(conn_repo, pipeline_repo)
        rows = await pipeline_repo.list_all()
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_update(self, conn_repo, pipeline_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        updated = await pipeline_repo.update(pipeline["id"], name="Updated")
        assert updated["name"] == "Updated"

    @pytest.mark.asyncio
    async def test_delete(self, conn_repo, pipeline_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        assert await pipeline_repo.delete(pipeline["id"]) is True
        assert await pipeline_repo.get(pipeline["id"]) is None

    @pytest.mark.asyncio
    async def test_update_status(self, conn_repo, pipeline_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        await pipeline_repo.update_status(pipeline["id"], "running", rows_processed=50, total_rows=100)
        status = await pipeline_repo.get_status(pipeline["id"])
        assert status["status"] == "running"
        assert status["rows_processed"] == 50
        assert status["total_rows"] == 100

    @pytest.mark.asyncio
    async def test_update_status_with_error(self, conn_repo, pipeline_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        await pipeline_repo.update_status(pipeline["id"], "failed", error_message="boom")
        status = await pipeline_repo.get_status(pipeline["id"])
        assert status["status"] == "failed"
        assert status["error_message"] == "boom"


class TestPipelineStepRepository:
    @pytest.mark.asyncio
    async def test_create_and_list(self, conn_repo, pipeline_repo, step_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        await step_repo.create(pipeline["id"], "Step 1", "sql", {"expression": "SELECT 1"})
        await step_repo.create(pipeline["id"], "Step 2", "code", {"function_code": "def transform(df): return df"})
        steps = await step_repo.list_by_pipeline(pipeline["id"])
        assert len(steps) == 2
        assert steps[0]["position"] == 0
        assert steps[1]["position"] == 1

    @pytest.mark.asyncio
    async def test_insert_after(self, conn_repo, pipeline_repo, step_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        s1 = await step_repo.create(pipeline["id"], "First", "sql", {"expression": "SELECT 1"})
        await step_repo.create(pipeline["id"], "Third", "sql", {"expression": "SELECT 3"})
        await step_repo.create(pipeline["id"], "Second", "sql", {"expression": "SELECT 2"}, insert_after=s1["id"])
        steps = await step_repo.list_by_pipeline(pipeline["id"])
        names = [s["name"] for s in steps]
        assert names == ["First", "Second", "Third"]

    @pytest.mark.asyncio
    async def test_update(self, conn_repo, pipeline_repo, step_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        s = await step_repo.create(pipeline["id"], "Old", "sql", {"expression": "SELECT 1"})
        updated = await step_repo.update(s["id"], pipeline["id"], name="New")
        assert updated["name"] == "New"

    @pytest.mark.asyncio
    async def test_delete_reorders(self, conn_repo, pipeline_repo, step_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        s1 = await step_repo.create(pipeline["id"], "A", "sql", {"expression": "SELECT 1"})
        await step_repo.create(pipeline["id"], "B", "sql", {"expression": "SELECT 2"})
        await step_repo.create(pipeline["id"], "C", "sql", {"expression": "SELECT 3"})
        await step_repo.delete(s1["id"], pipeline["id"])
        steps = await step_repo.list_by_pipeline(pipeline["id"])
        assert len(steps) == 2
        assert steps[0]["position"] == 0
        assert steps[1]["position"] == 1


class TestTransformationRepository:
    @pytest.mark.asyncio
    async def test_create_and_list(self, conn_repo, pipeline_repo, transform_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        await transform_repo.create(pipeline["id"], "column_mapping", {"mappings": []})
        await transform_repo.create(pipeline["id"], "sql_expression", {"expr": "UPPER(name)"})
        rows = await transform_repo.list_by_pipeline(pipeline["id"])
        assert len(rows) == 2
        assert rows[0]["order"] == 0
        assert rows[1]["order"] == 1

    @pytest.mark.asyncio
    async def test_update(self, conn_repo, pipeline_repo, transform_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        t = await transform_repo.create(pipeline["id"], "column_mapping", {"old": True})
        updated = await transform_repo.update(t["id"], pipeline["id"], {"new": True})
        assert json.loads(updated["config"]) == {"new": True}

    @pytest.mark.asyncio
    async def test_delete(self, conn_repo, pipeline_repo, transform_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        t = await transform_repo.create(pipeline["id"], "column_mapping", {})
        assert await transform_repo.delete(t["id"], pipeline["id"]) is True
        assert await transform_repo.delete(t["id"], pipeline["id"]) is False

    @pytest.mark.asyncio
    async def test_reorder(self, conn_repo, pipeline_repo, transform_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        t1 = await transform_repo.create(pipeline["id"], "column_mapping", {})
        t2 = await transform_repo.create(pipeline["id"], "sql_expression", {})
        # Reverse order: t2 first (order=0), t1 second (order=1)
        await transform_repo.reorder(pipeline["id"], [t2["id"], t1["id"]])
        rows = await transform_repo.list_by_pipeline(pipeline["id"])
        # list_by_pipeline orders by "order" column
        assert rows[0]["id"] == t2["id"]
        assert rows[0]["order"] == 0
        assert rows[1]["id"] == t1["id"]
        assert rows[1]["order"] == 1


class TestPipelineMetadataRepository:
    @pytest.mark.asyncio
    async def test_get_or_create(self, conn_repo, pipeline_repo, metadata_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        row = await metadata_repo.get_or_create(pipeline["id"])
        assert row["validation_status"] == "DRAFT"
        assert row["validated_hash"] is None
        # Second call returns same row
        row2 = await metadata_repo.get_or_create(pipeline["id"])
        assert row2["pipeline_id"] == row["pipeline_id"]

    @pytest.mark.asyncio
    async def test_update_validation_valid(self, conn_repo, pipeline_repo, metadata_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        schemas = {"source": [{"name": "id", "type": "int64"}]}
        row = await metadata_repo.update_validation(
            pipeline["id"], "VALID", validated_hash="abc123", node_schemas=schemas,
        )
        assert row["validation_status"] == "VALID"
        assert row["validated_hash"] == "abc123"
        assert row["last_validated_at"] is not None
        assert json.loads(row["node_schemas"]) == schemas

    @pytest.mark.asyncio
    async def test_update_validation_invalid(self, conn_repo, pipeline_repo, metadata_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        row = await metadata_repo.update_validation(pipeline["id"], "INVALID")
        assert row["validation_status"] == "INVALID"
        assert row["validated_hash"] is None

    @pytest.mark.asyncio
    async def test_reset_to_draft(self, conn_repo, pipeline_repo, metadata_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        await metadata_repo.update_validation(pipeline["id"], "VALID", validated_hash="abc")
        await metadata_repo.reset_to_draft(pipeline["id"])
        row = await metadata_repo.get(pipeline["id"])
        assert row["validation_status"] == "DRAFT"

    @pytest.mark.asyncio
    async def test_cascade_delete(self, conn_repo, pipeline_repo, metadata_repo):
        pipeline = await _create_pipeline(conn_repo, pipeline_repo)
        await metadata_repo.get_or_create(pipeline["id"])
        await pipeline_repo.delete(pipeline["id"])
        row = await metadata_repo.get(pipeline["id"])
        assert row is None
