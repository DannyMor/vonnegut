from __future__ import annotations

import json
from datetime import datetime, timezone

from vonnegut.database import AppDatabase


class PipelineMetadataRepository:
    def __init__(self, db: AppDatabase):
        self._db = db

    async def get(self, pipeline_id: str) -> dict | None:
        return await self._db.fetch_one(
            "SELECT * FROM pipeline_metadata WHERE pipeline_id = ?",
            (pipeline_id,),
        )

    async def get_or_create(self, pipeline_id: str) -> dict:
        row = await self.get(pipeline_id)
        if row is not None:
            return row
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """INSERT INTO pipeline_metadata (pipeline_id, validation_status, node_schemas, updated_at)
               VALUES (?, 'DRAFT', '{}', ?)""",
            (pipeline_id, now),
        )
        return await self.get(pipeline_id)

    async def update_validation(
        self,
        pipeline_id: str,
        validation_status: str,
        validated_hash: str | None = None,
        node_schemas: dict | None = None,
    ) -> dict:
        # Ensure row exists
        await self.get_or_create(pipeline_id)
        now = datetime.now(timezone.utc).isoformat()
        schemas_json = json.dumps(node_schemas) if node_schemas is not None else None

        if schemas_json is not None:
            await self._db.execute(
                """UPDATE pipeline_metadata
                   SET validation_status=?, validated_hash=?, last_validated_at=?,
                       node_schemas=?, updated_at=?
                   WHERE pipeline_id=?""",
                (validation_status, validated_hash, now, schemas_json, now, pipeline_id),
            )
        else:
            await self._db.execute(
                """UPDATE pipeline_metadata
                   SET validation_status=?, validated_hash=?, last_validated_at=?, updated_at=?
                   WHERE pipeline_id=?""",
                (validation_status, validated_hash, now, now, pipeline_id),
            )
        return await self.get(pipeline_id)

    async def reset_to_draft(self, pipeline_id: str) -> None:
        """Reset validation status to DRAFT (e.g., when pipeline definition changes)."""
        row = await self.get(pipeline_id)
        if row is None:
            return
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "UPDATE pipeline_metadata SET validation_status='DRAFT', updated_at=? WHERE pipeline_id=?",
            (now, pipeline_id),
        )

    async def delete(self, pipeline_id: str) -> None:
        await self._db.execute(
            "DELETE FROM pipeline_metadata WHERE pipeline_id = ?",
            (pipeline_id,),
        )
