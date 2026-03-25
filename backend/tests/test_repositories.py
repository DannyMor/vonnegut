import json
import pytest

from vonnegut.repositories import (
    ConnectionRepository,
    MigrationRepository,
    PipelineStepRepository,
    TransformationRepository,
)


@pytest.fixture
def conn_repo(db):
    return ConnectionRepository(db)


@pytest.fixture
def mig_repo(db):
    return MigrationRepository(db)


@pytest.fixture
def step_repo(db):
    return PipelineStepRepository(db)


@pytest.fixture
def transform_repo(db):
    return TransformationRepository(db)


async def _create_connection(conn_repo: ConnectionRepository) -> dict:
    return await conn_repo.create("test-conn", json.dumps({"type": "postgres_direct"}))


async def _create_migration(conn_repo: ConnectionRepository, mig_repo: MigrationRepository) -> dict:
    conn = await _create_connection(conn_repo)
    return await mig_repo.create(
        name="Test Migration",
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


class TestMigrationRepository:
    @pytest.mark.asyncio
    async def test_create_and_get(self, conn_repo, mig_repo):
        mig = await _create_migration(conn_repo, mig_repo)
        assert mig["name"] == "Test Migration"
        assert mig["status"] == "draft"
        fetched = await mig_repo.get(mig["id"])
        assert fetched["name"] == "Test Migration"

    @pytest.mark.asyncio
    async def test_list_all(self, conn_repo, mig_repo):
        await _create_migration(conn_repo, mig_repo)
        await _create_migration(conn_repo, mig_repo)
        rows = await mig_repo.list_all()
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_update(self, conn_repo, mig_repo):
        mig = await _create_migration(conn_repo, mig_repo)
        updated = await mig_repo.update(mig["id"], name="Updated")
        assert updated["name"] == "Updated"

    @pytest.mark.asyncio
    async def test_delete(self, conn_repo, mig_repo):
        mig = await _create_migration(conn_repo, mig_repo)
        assert await mig_repo.delete(mig["id"]) is True
        assert await mig_repo.get(mig["id"]) is None

    @pytest.mark.asyncio
    async def test_update_status(self, conn_repo, mig_repo):
        mig = await _create_migration(conn_repo, mig_repo)
        await mig_repo.update_status(mig["id"], "running", rows_processed=50, total_rows=100)
        status = await mig_repo.get_status(mig["id"])
        assert status["status"] == "running"
        assert status["rows_processed"] == 50
        assert status["total_rows"] == 100

    @pytest.mark.asyncio
    async def test_update_status_with_error(self, conn_repo, mig_repo):
        mig = await _create_migration(conn_repo, mig_repo)
        await mig_repo.update_status(mig["id"], "failed", error_message="boom")
        status = await mig_repo.get_status(mig["id"])
        assert status["status"] == "failed"
        assert status["error_message"] == "boom"


class TestPipelineStepRepository:
    @pytest.mark.asyncio
    async def test_create_and_list(self, conn_repo, mig_repo, step_repo):
        mig = await _create_migration(conn_repo, mig_repo)
        await step_repo.create(mig["id"], "Step 1", "sql", {"expression": "SELECT 1"})
        await step_repo.create(mig["id"], "Step 2", "code", {"function_code": "def transform(df): return df"})
        steps = await step_repo.list_by_migration(mig["id"])
        assert len(steps) == 2
        assert steps[0]["position"] == 0
        assert steps[1]["position"] == 1

    @pytest.mark.asyncio
    async def test_insert_after(self, conn_repo, mig_repo, step_repo):
        mig = await _create_migration(conn_repo, mig_repo)
        s1 = await step_repo.create(mig["id"], "First", "sql", {"expression": "SELECT 1"})
        await step_repo.create(mig["id"], "Third", "sql", {"expression": "SELECT 3"})
        await step_repo.create(mig["id"], "Second", "sql", {"expression": "SELECT 2"}, insert_after=s1["id"])
        steps = await step_repo.list_by_migration(mig["id"])
        names = [s["name"] for s in steps]
        assert names == ["First", "Second", "Third"]

    @pytest.mark.asyncio
    async def test_update(self, conn_repo, mig_repo, step_repo):
        mig = await _create_migration(conn_repo, mig_repo)
        s = await step_repo.create(mig["id"], "Old", "sql", {"expression": "SELECT 1"})
        updated = await step_repo.update(s["id"], mig["id"], name="New")
        assert updated["name"] == "New"

    @pytest.mark.asyncio
    async def test_delete_reorders(self, conn_repo, mig_repo, step_repo):
        mig = await _create_migration(conn_repo, mig_repo)
        s1 = await step_repo.create(mig["id"], "A", "sql", {"expression": "SELECT 1"})
        await step_repo.create(mig["id"], "B", "sql", {"expression": "SELECT 2"})
        await step_repo.create(mig["id"], "C", "sql", {"expression": "SELECT 3"})
        await step_repo.delete(s1["id"], mig["id"])
        steps = await step_repo.list_by_migration(mig["id"])
        assert len(steps) == 2
        assert steps[0]["position"] == 0
        assert steps[1]["position"] == 1


class TestTransformationRepository:
    @pytest.mark.asyncio
    async def test_create_and_list(self, conn_repo, mig_repo, transform_repo):
        mig = await _create_migration(conn_repo, mig_repo)
        await transform_repo.create(mig["id"], "column_mapping", {"mappings": []})
        await transform_repo.create(mig["id"], "sql_expression", {"expr": "UPPER(name)"})
        rows = await transform_repo.list_by_migration(mig["id"])
        assert len(rows) == 2
        assert rows[0]["order"] == 0
        assert rows[1]["order"] == 1

    @pytest.mark.asyncio
    async def test_update(self, conn_repo, mig_repo, transform_repo):
        mig = await _create_migration(conn_repo, mig_repo)
        t = await transform_repo.create(mig["id"], "column_mapping", {"old": True})
        updated = await transform_repo.update(t["id"], mig["id"], {"new": True})
        assert json.loads(updated["config"]) == {"new": True}

    @pytest.mark.asyncio
    async def test_delete(self, conn_repo, mig_repo, transform_repo):
        mig = await _create_migration(conn_repo, mig_repo)
        t = await transform_repo.create(mig["id"], "column_mapping", {})
        assert await transform_repo.delete(t["id"], mig["id"]) is True
        assert await transform_repo.delete(t["id"], mig["id"]) is False

    @pytest.mark.asyncio
    async def test_reorder(self, conn_repo, mig_repo, transform_repo):
        mig = await _create_migration(conn_repo, mig_repo)
        t1 = await transform_repo.create(mig["id"], "column_mapping", {})
        t2 = await transform_repo.create(mig["id"], "sql_expression", {})
        # Reverse order: t2 first (order=0), t1 second (order=1)
        await transform_repo.reorder(mig["id"], [t2["id"], t1["id"]])
        rows = await transform_repo.list_by_migration(mig["id"])
        # list_by_migration orders by "order" column
        assert rows[0]["id"] == t2["id"]
        assert rows[0]["order"] == 0
        assert rows[1]["id"] == t1["id"]
        assert rows[1]["order"] == 1
