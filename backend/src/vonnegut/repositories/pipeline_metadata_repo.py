from __future__ import annotations

import json
from datetime import datetime, timezone

from vonnegut.database import AppDatabase


class PipelineMetadataRepository:
    def __init__(self, db: AppDatabase):
        self._db = db

    async def get(self, migration_id: str) -> dict | None:
        return await self._db.fetch_one(
            "SELECT * FROM pipeline_metadata WHERE migration_id = ?",
            (migration_id,),
        )

    async def get_or_create(self, migration_id: str) -> dict:
        row = await self.get(migration_id)
        if row is not None:
            return row
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """INSERT INTO pipeline_metadata (migration_id, validation_status, node_schemas, updated_at)
               VALUES (?, 'DRAFT', '{}', ?)""",
            (migration_id, now),
        )
        return await self.get(migration_id)

    async def update_validation(
        self,
        migration_id: str,
        validation_status: str,
        validated_hash: str | None = None,
        node_schemas: dict | None = None,
    ) -> dict:
        # Ensure row exists
        await self.get_or_create(migration_id)
        now = datetime.now(timezone.utc).isoformat()
        schemas_json = json.dumps(node_schemas) if node_schemas is not None else None

        if schemas_json is not None:
            await self._db.execute(
                """UPDATE pipeline_metadata
                   SET validation_status=?, validated_hash=?, last_validated_at=?,
                       node_schemas=?, updated_at=?
                   WHERE migration_id=?""",
                (validation_status, validated_hash, now, schemas_json, now, migration_id),
            )
        else:
            await self._db.execute(
                """UPDATE pipeline_metadata
                   SET validation_status=?, validated_hash=?, last_validated_at=?, updated_at=?
                   WHERE migration_id=?""",
                (validation_status, validated_hash, now, now, migration_id),
            )
        return await self.get(migration_id)

    async def reset_to_draft(self, migration_id: str) -> None:
        """Reset validation status to DRAFT (e.g., when pipeline definition changes)."""
        row = await self.get(migration_id)
        if row is None:
            return
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "UPDATE pipeline_metadata SET validation_status='DRAFT', updated_at=? WHERE migration_id=?",
            (now, migration_id),
        )

    async def delete(self, migration_id: str) -> None:
        await self._db.execute(
            "DELETE FROM pipeline_metadata WHERE migration_id = ?",
            (migration_id,),
        )
