import pytest
from vonnegut.pipeline.results import CheckStatus
from vonnegut.pipeline.engine.validator.connection_checks import (
    check_source_connection,
    check_target_connection,
    check_source_query,
    check_target_table,
)
from tests.pipeline.helpers import InMemoryDatabaseAdapter


class _TestAdapterFactory:
    """Factory that creates InMemoryDatabaseAdapters for testing."""

    def __init__(self, adapter: InMemoryDatabaseAdapter | None = None):
        self._adapter = adapter

    async def create(self, connection: dict):
        if self._adapter is None:
            raise ConnectionError("Connection failed")
        return self._adapter


class _FailingAdapterFactory:
    """Factory that always fails to create an adapter."""

    async def create(self, connection: dict):
        raise ConnectionError("Cannot connect to database")


@pytest.mark.asyncio
async def test_source_connection_success():
    adapter = InMemoryDatabaseAdapter()
    adapter.seed_table("users", [{"id": 1}])
    factory = _TestAdapterFactory(adapter)
    result = await check_source_connection(factory, {})
    assert result.status == CheckStatus.PASSED


@pytest.mark.asyncio
async def test_source_connection_failure():
    factory = _FailingAdapterFactory()
    result = await check_source_connection(factory, {})
    assert result.status == CheckStatus.FAILED
    assert "cannot connect" in result.message.lower()


@pytest.mark.asyncio
async def test_target_connection_success():
    adapter = InMemoryDatabaseAdapter()
    factory = _TestAdapterFactory(adapter)
    result = await check_target_connection(factory, {})
    assert result.status == CheckStatus.PASSED


@pytest.mark.asyncio
async def test_target_connection_failure():
    factory = _FailingAdapterFactory()
    result = await check_target_connection(factory, {})
    assert result.status == CheckStatus.FAILED


@pytest.mark.asyncio
async def test_source_query_success():
    adapter = InMemoryDatabaseAdapter()
    adapter.seed_table("users", [{"id": 1, "name": "Alice"}])
    factory = _TestAdapterFactory(adapter)
    result = await check_source_query(factory, {}, "SELECT * FROM users")
    assert result.status == CheckStatus.PASSED


@pytest.mark.asyncio
async def test_source_query_failure():
    adapter = InMemoryDatabaseAdapter()
    factory = _TestAdapterFactory(adapter)
    result = await check_source_query(factory, {}, "SELECT * FROM nonexistent")
    assert result.status == CheckStatus.FAILED
    assert "source query failed" in result.message.lower()


@pytest.mark.asyncio
async def test_target_table_exists():
    adapter = InMemoryDatabaseAdapter()
    adapter.seed_table("orders", [{"id": 1, "total": 100}])
    factory = _TestAdapterFactory(adapter)
    result, schema = await check_target_table(factory, {}, "orders")
    assert result.status == CheckStatus.PASSED
    assert len(schema) == 2


@pytest.mark.asyncio
async def test_target_table_not_found():
    adapter = InMemoryDatabaseAdapter()
    factory = _TestAdapterFactory(adapter)
    result, schema = await check_target_table(factory, {}, "nonexistent")
    assert result.status == CheckStatus.FAILED
    assert len(schema) == 0


@pytest.mark.asyncio
async def test_target_table_connection_failure():
    factory = _FailingAdapterFactory()
    result, schema = await check_target_table(factory, {}, "any_table")
    assert result.status == CheckStatus.FAILED
    assert "cannot connect" in result.message.lower()
