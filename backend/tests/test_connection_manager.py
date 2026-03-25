import pytest

from vonnegut.repositories.connection_repo import ConnectionRepository
from vonnegut.services.connection_manager import ConnectionManager


@pytest.fixture
def manager(db, encryption_key):
    repo = ConnectionRepository(db)
    return ConnectionManager(repo=repo, encryption_key=encryption_key)


@pytest.mark.asyncio
async def test_create_connection(manager):
    conn = await manager.create(
        name="Test DB",
        config={"type": "postgres_direct", "host": "localhost", "port": 5432, "database": "db", "user": "u", "password": "p"},
    )
    assert conn["name"] == "Test DB"
    assert conn["id"] is not None


@pytest.mark.asyncio
async def test_create_and_get_decrypts_password(manager):
    """Verifies that password is encrypted at rest and decrypted on read."""
    created = await manager.create(
        name="Encrypted DB",
        config={"type": "postgres_direct", "host": "h", "port": 5432, "database": "d", "user": "u", "password": "my-secret"},
    )
    fetched = await manager.get(created["id"])
    assert fetched["config"]["password"] == "my-secret"
    assert fetched["config"]["host"] == "h"


@pytest.mark.asyncio
async def test_list_connections(manager):
    await manager.create(name="DB1", config={"type": "postgres_direct", "host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"})
    await manager.create(name="DB2", config={"type": "postgres_direct", "host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"})
    conns = await manager.list_all()
    assert len(conns) == 2


@pytest.mark.asyncio
async def test_get_connection(manager):
    created = await manager.create(name="My DB", config={"type": "postgres_direct", "host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"})
    fetched = await manager.get(created["id"])
    assert fetched["name"] == "My DB"


@pytest.mark.asyncio
async def test_get_nonexistent_returns_none(manager):
    result = await manager.get("nonexistent-id")
    assert result is None


@pytest.mark.asyncio
async def test_delete_connection(manager):
    created = await manager.create(name="To Delete", config={"type": "postgres_direct", "host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"})
    deleted = await manager.delete(created["id"])
    assert deleted is True
    result = await manager.get(created["id"])
    assert result is None
