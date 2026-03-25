from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from vonnegut.database import AppDatabase


class ConnectionRepository:
    def __init__(self, db: AppDatabase):
        self._db = db

    async def create(self, name: str, config_json: str) -> dict:
        conn_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """INSERT INTO connections (id, name, config, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (conn_id, name, config_json, now, now),
        )
        return {"id": conn_id, "name": name, "config": config_json, "created_at": now, "updated_at": now}

    async def get(self, conn_id: str) -> dict | None:
        return await self._db.fetch_one("SELECT * FROM connections WHERE id = ?", (conn_id,))

    async def list_all(self) -> list[dict]:
        return await self._db.fetch_all("SELECT * FROM connections ORDER BY created_at DESC")

    async def update(self, conn_id: str, name: str, config_json: str) -> dict | None:
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "UPDATE connections SET name = ?, config = ?, updated_at = ? WHERE id = ?",
            (name, config_json, now, conn_id),
        )
        return await self.get(conn_id)

    async def delete(self, conn_id: str) -> bool:
        existing = await self._db.fetch_one("SELECT id FROM connections WHERE id = ?", (conn_id,))
        if existing is None:
            return False
        await self._db.execute("DELETE FROM connections WHERE id = ?", (conn_id,))
        return True
