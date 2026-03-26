from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from vonnegut.database import AppDatabase


class TransformationRepository:
    def __init__(self, db: AppDatabase):
        self._db = db

    async def list_by_pipeline(self, pipeline_id: str) -> list[dict]:
        return await self._db.fetch_all(
            'SELECT * FROM transformations WHERE pipeline_id = ? ORDER BY "order"',
            (pipeline_id,),
        )

    async def get(self, t_id: str) -> dict | None:
        return await self._db.fetch_one("SELECT * FROM transformations WHERE id = ?", (t_id,))

    async def get_for_pipeline(self, t_id: str, pipeline_id: str) -> dict | None:
        return await self._db.fetch_one(
            "SELECT * FROM transformations WHERE id = ? AND pipeline_id = ?",
            (t_id, pipeline_id),
        )

    async def create(self, pipeline_id: str, type: str, config: dict) -> dict:
        result = await self._db.fetch_one(
            'SELECT COALESCE(MAX("order"), -1) as max_order FROM transformations WHERE pipeline_id = ?',
            (pipeline_id,),
        )
        next_order = result["max_order"] + 1

        t_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """INSERT INTO transformations (id, pipeline_id, "order", type, config, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (t_id, pipeline_id, next_order, type, json.dumps(config), now, now),
        )
        return {
            "id": t_id, "pipeline_id": pipeline_id, "order": next_order,
            "type": type, "config": json.dumps(config),
            "created_at": now, "updated_at": now,
        }

    async def update(self, t_id: str, pipeline_id: str, config: dict) -> dict | None:
        existing = await self.get_for_pipeline(t_id, pipeline_id)
        if existing is None:
            return None

        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "UPDATE transformations SET config = ?, updated_at = ? WHERE id = ?",
            (json.dumps(config), now, t_id),
        )
        return await self.get(t_id)

    async def delete(self, t_id: str, pipeline_id: str) -> bool:
        existing = await self.get_for_pipeline(t_id, pipeline_id)
        if existing is None:
            return False
        await self._db.execute("DELETE FROM transformations WHERE id = ?", (t_id,))
        return True

    async def reorder(self, pipeline_id: str, ordered_ids: list[str]) -> None:
        for idx, t_id in enumerate(ordered_ids):
            await self._db.execute(
                'UPDATE transformations SET "order" = ? WHERE id = ? AND pipeline_id = ?',
                (idx, t_id, pipeline_id),
            )
