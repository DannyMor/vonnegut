# backend/tests/test_migration_runner.py
import threading

import pytest

from vonnegut.adapters.memory import InMemoryAdapter
from vonnegut.services.migration_runner import MigrationRunner
from vonnegut.services.transformation_engine import TransformationEngine


@pytest.fixture
def engine():
    return TransformationEngine()


@pytest.fixture
def runner(engine):
    return MigrationRunner(engine=engine)


@pytest.fixture
def source_adapter():
    adapter = InMemoryAdapter()
    adapter.add_table("users", [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ])
    return adapter


@pytest.fixture
def target_adapter():
    adapter = InMemoryAdapter()
    adapter.add_table("users_copy", [])
    return adapter


@pytest.mark.asyncio
async def test_run_test_migration(runner, source_adapter):
    await source_adapter.connect()

    transformations = [
        {"type": "sql_expression", "config": {"expression": "UPPER(name)", "output_column": "name_upper"}},
    ]

    result = await runner.run_test(source_adapter, "users", transformations, rows=10)
    assert len(result["before"]) == 2
    assert len(result["after"]) == 2
    assert result["after"][0]["name_upper"] == "ALICE"

    await source_adapter.disconnect()


@pytest.mark.asyncio
async def test_run_migration_basic(runner, source_adapter, target_adapter):
    await source_adapter.connect()
    await target_adapter.connect()

    progress = {}

    async def on_progress(rows_done, total):
        progress["rows_done"] = rows_done
        progress["total"] = total

    cancel_flag = threading.Event()

    result = await runner.run(
        source_adapter=source_adapter,
        target_adapter=target_adapter,
        source_table="users",
        target_table="users_copy",
        transformations=[],
        truncate_target=False,
        row_limit=100_000,
        batch_size=1000,
        on_progress=on_progress,
        cancel_flag=cancel_flag,
    )
    assert result["status"] == "completed"
    assert result["rows_processed"] == 2

    # Verify data was actually written to target
    rows = await target_adapter.execute("SELECT * FROM users_copy")
    assert len(rows) == 2

    await source_adapter.disconnect()
    await target_adapter.disconnect()


@pytest.mark.asyncio
async def test_run_migration_exceeds_row_limit(runner, target_adapter):
    """Source with 200k rows should fail pre-flight check."""
    source_adapter = InMemoryAdapter()
    rows = [{"id": i, "val": f"row_{i}"} for i in range(200_000)]
    source_adapter.add_table("big_table", rows)
    await source_adapter.connect()
    await target_adapter.connect()

    cancel_flag = threading.Event()

    async def noop_progress(rows_done, total):
        pass

    with pytest.raises(ValueError, match="exceeds.*limit"):
        await runner.run(
            source_adapter=source_adapter,
            target_adapter=target_adapter,
            source_table="big_table",
            target_table="users_copy",
            transformations=[],
            truncate_target=False,
            row_limit=100_000,
            batch_size=1000,
            on_progress=noop_progress,
            cancel_flag=cancel_flag,
        )

    await source_adapter.disconnect()
    await target_adapter.disconnect()
