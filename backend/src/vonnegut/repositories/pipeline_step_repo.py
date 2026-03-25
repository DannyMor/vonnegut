from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from vonnegut.database import AppDatabase


class PipelineStepRepository:
    def __init__(self, db: AppDatabase):
        self._db = db

    async def list_by_migration(self, migration_id: str) -> list[dict]:
        return await self._db.fetch_all(
            "SELECT * FROM pipeline_steps WHERE migration_id = ? ORDER BY position",
            (migration_id,),
        )

    async def get(self, step_id: str) -> dict | None:
        return await self._db.fetch_one("SELECT * FROM pipeline_steps WHERE id = ?", (step_id,))

    async def get_for_migration(self, step_id: str, migration_id: str) -> dict | None:
        return await self._db.fetch_one(
            "SELECT * FROM pipeline_steps WHERE id = ? AND migration_id = ?",
            (step_id, migration_id),
        )

    async def create(
        self,
        migration_id: str,
        name: str,
        step_type: str,
        config: dict,
        description: str | None = None,
        insert_after: str | None = None,
    ) -> dict:
        if insert_after:
            after_step = await self.get_for_migration(insert_after, migration_id)
            if after_step is None:
                raise ValueError("insert_after step not found")
            new_position = after_step["position"] + 1
            await self._db.execute(
                "UPDATE pipeline_steps SET position = position + 1 WHERE migration_id = ? AND position >= ?",
                (migration_id, new_position),
            )
        else:
            result = await self._db.fetch_one(
                "SELECT COALESCE(MAX(position), -1) as max_pos FROM pipeline_steps WHERE migration_id = ?",
                (migration_id,),
            )
            new_position = result["max_pos"] + 1

        step_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """INSERT INTO pipeline_steps (id, migration_id, name, description, position, step_type, config, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (step_id, migration_id, name, description, new_position,
             step_type, json.dumps(config), now, now),
        )
        return {
            "id": step_id, "migration_id": migration_id, "name": name,
            "description": description, "position": new_position,
            "step_type": step_type, "config": json.dumps(config),
            "created_at": now, "updated_at": now,
        }

    async def update(self, step_id: str, migration_id: str, **fields) -> dict | None:
        existing = await self.get_for_migration(step_id, migration_id)
        if existing is None:
            return None

        now = datetime.now(timezone.utc).isoformat()
        name = fields.get("name", existing["name"])
        description = fields.get("description", existing["description"])
        step_type = fields.get("step_type", existing["step_type"])
        config = json.dumps(fields["config"]) if "config" in fields and fields["config"] is not None else existing["config"]

        await self._db.execute(
            "UPDATE pipeline_steps SET name=?, description=?, step_type=?, config=?, updated_at=? WHERE id=?",
            (name, description, step_type, config, now, step_id),
        )
        return await self.get(step_id)

    async def delete(self, step_id: str, migration_id: str) -> bool:
        existing = await self._db.fetch_one(
            "SELECT position FROM pipeline_steps WHERE id = ? AND migration_id = ?",
            (step_id, migration_id),
        )
        if existing is None:
            return False
        await self._db.execute("DELETE FROM pipeline_steps WHERE id = ?", (step_id,))
        await self._db.execute(
            "UPDATE pipeline_steps SET position = position - 1 WHERE migration_id = ? AND position > ?",
            (migration_id, existing["position"]),
        )
        return True
