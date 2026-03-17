import pytest
import pytest_asyncio
from cryptography.fernet import Fernet

from vonnegut.database import Database


@pytest.fixture
def encryption_key():
    return Fernet.generate_key().decode()


@pytest_asyncio.fixture
async def db(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path}/test.db")
    await database.initialize()
    yield database
    await database.close()
