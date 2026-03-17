import json
import uuid
from datetime import datetime, timezone

from vonnegut.database import Database
from vonnegut.models.connection import encrypt_config, decrypt_config


class ConnectionManager:
    def __init__(self, db: Database, encryption_key: str):
        self._db = db
        self._key = encryption_key

    async def create(self, name: str, type: str, config: dict) -> dict:
        conn_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        encrypted = encrypt_config(config, self._key)
        await self._db.execute(
            """INSERT INTO connections (id, name, type, config, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (conn_id, name, type, json.dumps(encrypted), now, now),
        )
        return {"id": conn_id, "name": name, "type": type, "config": config, "created_at": now, "updated_at": now}

    async def list_all(self) -> list[dict]:
        rows = await self._db.fetch_all("SELECT * FROM connections ORDER BY created_at DESC")
        return rows

    async def get(self, conn_id: str) -> dict | None:
        row = await self._db.fetch_one("SELECT * FROM connections WHERE id = ?", (conn_id,))
        if row is None:
            return None
        row["config"] = json.loads(row["config"])
        row["config"] = decrypt_config(row["config"], self._key)
        return row

    async def update(self, conn_id: str, name: str | None = None, config: dict | None = None) -> dict | None:
        existing = await self.get(conn_id)
        if existing is None:
            return None
        new_name = name if name is not None else existing["name"]
        new_config = config if config is not None else existing["config"]
        now = datetime.now(timezone.utc).isoformat()
        encrypted = encrypt_config(new_config, self._key)
        await self._db.execute(
            "UPDATE connections SET name = ?, config = ?, updated_at = ? WHERE id = ?",
            (new_name, json.dumps(encrypted), now, conn_id),
        )
        return await self.get(conn_id)

    async def delete(self, conn_id: str) -> bool:
        existing = await self._db.fetch_one("SELECT id FROM connections WHERE id = ?", (conn_id,))
        if existing is None:
            return False
        await self._db.execute("DELETE FROM connections WHERE id = ?", (conn_id,))
        return True
