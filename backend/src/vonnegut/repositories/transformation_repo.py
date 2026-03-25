from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from vonnegut.database import AppDatabase


class TransformationRepository:
    def __init__(self, db: AppDatabase):
        self._db = db

    async def list_by_migration(self, migration_id: str) -> list[dict]:
        return await self._db.fetch_all(
            'SELECT * FROM transformations WHERE migration_id = ? ORDER BY "order"',
            (migration_id,),
        )

    async def get(self, t_id: str) -> dict | None:
        return await self._db.fetch_one("SELECT * FROM transformations WHERE id = ?", (t_id,))

    async def get_for_migration(self, t_id: str, migration_id: str) -> dict | None:
        return await self._db.fetch_one(
            "SELECT * FROM transformations WHERE id = ? AND migration_id = ?",
            (t_id, migration_id),
        )

    async def create(self, migration_id: str, type: str, config: dict) -> dict:
        result = await self._db.fetch_one(
            'SELECT COALESCE(MAX("order"), -1) as max_order FROM transformations WHERE migration_id = ?',
            (migration_id,),
        )
        next_order = result["max_order"] + 1

        t_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """INSERT INTO transformations (id, migration_id, "order", type, config, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (t_id, migration_id, next_order, type, json.dumps(config), now, now),
        )
        return {
            "id": t_id, "migration_id": migration_id, "order": next_order,
            "type": type, "config": json.dumps(config),
            "created_at": now, "updated_at": now,
        }

    async def update(self, t_id: str, migration_id: str, config: dict) -> dict | None:
        existing = await self.get_for_migration(t_id, migration_id)
        if existing is None:
            return None

        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "UPDATE transformations SET config = ?, updated_at = ? WHERE id = ?",
            (json.dumps(config), now, t_id),
        )
        return await self.get(t_id)

    async def delete(self, t_id: str, migration_id: str) -> bool:
        existing = await self.get_for_migration(t_id, migration_id)
        if existing is None:
            return False
        await self._db.execute("DELETE FROM transformations WHERE id = ?", (t_id,))
        return True

    async def reorder(self, migration_id: str, ordered_ids: list[str]) -> None:
        for idx, t_id in enumerate(ordered_ids):
            await self._db.execute(
                'UPDATE transformations SET "order" = ? WHERE id = ? AND migration_id = ?',
                (idx, t_id, migration_id),
            )
