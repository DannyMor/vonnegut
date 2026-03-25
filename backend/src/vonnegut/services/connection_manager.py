import json
import logging

from vonnegut.models.connection import encrypt_config, decrypt_config
from vonnegut.repositories.connection_repo import ConnectionRepository

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Service layer for connections. Handles encryption/decryption on top of the repository."""

    def __init__(self, repo: ConnectionRepository, encryption_key: str):
        self._repo = repo
        self._key = encryption_key

    async def create(self, name: str, config: dict) -> dict:
        encrypted = encrypt_config(config, self._key)
        row = await self._repo.create(name, json.dumps(encrypted))
        # Return with decrypted config
        return {**row, "config": config}

    async def list_all(self) -> list[dict]:
        return await self._repo.list_all()

    async def get(self, conn_id: str) -> dict | None:
        row = await self._repo.get(conn_id)
        if row is None:
            return None
        config = json.loads(row["config"]) if isinstance(row["config"], str) else row["config"]
        config = decrypt_config(config, self._key)
        return {**row, "config": config}

    async def update(self, conn_id: str, name: str | None = None, config: dict | None = None) -> dict | None:
        existing = await self.get(conn_id)
        if existing is None:
            return None
        new_name = name if name is not None else existing["name"]
        new_config = config if config is not None else existing["config"]
        # Preserve existing password if the update sends an empty one
        if config is not None and "password" in new_config and not new_config["password"] and "password" in existing["config"]:
            new_config = {**new_config, "password": existing["config"]["password"]}
        encrypted = encrypt_config(new_config, self._key)
        await self._repo.update(conn_id, new_name, json.dumps(encrypted))
        return await self.get(conn_id)

    async def delete(self, conn_id: str) -> bool:
        return await self._repo.delete(conn_id)
