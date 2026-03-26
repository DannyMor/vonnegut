from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from vonnegut.database import AppDatabase


class PipelineRepository:
    def __init__(self, db: AppDatabase):
        self._db = db

    async def create(
        self,
        name: str,
        source_connection_id: str,
        target_connection_id: str,
        source_table: str,
        target_table: str,
        source_query: str = "",
        source_schema: list[dict] | None = None,
        truncate_target: bool = False,
    ) -> dict:
        pipeline_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """INSERT INTO pipelines
               (id, name, source_connection_id, target_connection_id, source_table, target_table,
                source_query, source_schema, status, truncate_target, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?)""",
            (pipeline_id, name, source_connection_id, target_connection_id,
             source_table, target_table, source_query,
             json.dumps(source_schema or []), int(truncate_target), now, now),
        )
        return await self.get(pipeline_id)

    async def get(self, pipeline_id: str) -> dict | None:
        return await self._db.fetch_one("SELECT * FROM pipelines WHERE id = ?", (pipeline_id,))

    async def list_all(self) -> list[dict]:
        return await self._db.fetch_all("SELECT * FROM pipelines ORDER BY created_at DESC")

    async def update(self, pipeline_id: str, **fields) -> dict | None:
        existing = await self.get(pipeline_id)
        if existing is None:
            return None

        name = fields.get("name", existing["name"])
        source_table = fields.get("source_table", existing["source_table"])
        target_table = fields.get("target_table", existing["target_table"])
        source_query = fields.get("source_query", existing["source_query"])
        truncate_target = fields.get("truncate_target", existing["truncate_target"])
        if isinstance(truncate_target, bool):
            truncate_target = int(truncate_target)

        source_schema = existing["source_schema"]
        if "source_schema" in fields and fields["source_schema"] is not None:
            source_schema = json.dumps(fields["source_schema"])

        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """UPDATE pipelines
               SET name=?, source_table=?, target_table=?, source_query=?,
                   source_schema=?, truncate_target=?, updated_at=?
               WHERE id=?""",
            (name, source_table, target_table, source_query,
             source_schema, truncate_target, now, pipeline_id),
        )
        return await self.get(pipeline_id)

    async def delete(self, pipeline_id: str) -> bool:
        existing = await self._db.fetch_one("SELECT id FROM pipelines WHERE id = ?", (pipeline_id,))
        if existing is None:
            return False
        await self._db.execute("DELETE FROM pipelines WHERE id = ?", (pipeline_id,))
        return True

    async def update_status(
        self,
        pipeline_id: str,
        status: str,
        rows_processed: int | None = None,
        total_rows: int | None = None,
        error_message: str | None = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        if rows_processed is not None and total_rows is not None:
            await self._db.execute(
                "UPDATE pipelines SET status=?, rows_processed=?, total_rows=?, error_message=?, updated_at=? WHERE id=?",
                (status, rows_processed, total_rows, error_message, now, pipeline_id),
            )
        elif rows_processed is not None:
            await self._db.execute(
                "UPDATE pipelines SET status=?, rows_processed=?, error_message=?, updated_at=? WHERE id=?",
                (status, rows_processed, error_message, now, pipeline_id),
            )
        elif error_message is not None:
            await self._db.execute(
                "UPDATE pipelines SET status=?, error_message=?, updated_at=? WHERE id=?",
                (status, error_message, now, pipeline_id),
            )
        else:
            await self._db.execute(
                "UPDATE pipelines SET status=?, updated_at=? WHERE id=?",
                (status, now, pipeline_id),
            )

    async def get_status(self, pipeline_id: str) -> dict | None:
        return await self._db.fetch_one(
            "SELECT status, rows_processed, total_rows, error_message FROM pipelines WHERE id = ?",
            (pipeline_id,),
        )
