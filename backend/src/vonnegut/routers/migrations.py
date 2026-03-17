# backend/src/vonnegut/routers/migrations.py
import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, status

from vonnegut.models.migration import MigrationCreate, MigrationResponse, MigrationUpdate
from vonnegut.models.transformation import TransformationResponse

router = APIRouter(tags=["migrations"])


def _get_db(request: Request):
    return request.app.state.db


async def _get_transformations(db, migration_id: str) -> list[TransformationResponse]:
    rows = await db.fetch_all(
        'SELECT * FROM transformations WHERE migration_id = ? ORDER BY "order"',
        (migration_id,),
    )
    return [
        TransformationResponse(
            id=r["id"], migration_id=r["migration_id"], order=r["order"],
            type=r["type"], config=json.loads(r["config"]),
            created_at=r["created_at"], updated_at=r["updated_at"],
        )
        for r in rows
    ]


async def _migration_response(db, row: dict) -> MigrationResponse:
    transforms = await _get_transformations(db, row["id"])
    return MigrationResponse(
        id=row["id"], name=row["name"],
        source_connection_id=row["source_connection_id"],
        target_connection_id=row["target_connection_id"],
        source_table=row["source_table"], target_table=row["target_table"],
        status=row["status"], truncate_target=bool(row["truncate_target"]),
        rows_processed=row["rows_processed"], total_rows=row["total_rows"],
        error_message=row["error_message"],
        created_at=row["created_at"], updated_at=row["updated_at"],
        transformations=transforms,
    )


@router.post("/migrations", response_model=MigrationResponse, status_code=status.HTTP_201_CREATED)
async def create_migration(body: MigrationCreate, request: Request):
    db = _get_db(request)
    mig_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        """INSERT INTO migrations
           (id, name, source_connection_id, target_connection_id, source_table, target_table,
            status, truncate_target, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?)""",
        (mig_id, body.name, body.source_connection_id, body.target_connection_id,
         body.source_table, body.target_table, int(body.truncate_target), now, now),
    )
    row = await db.fetch_one("SELECT * FROM migrations WHERE id = ?", (mig_id,))
    return await _migration_response(db, row)


@router.get("/migrations", response_model=list[MigrationResponse])
async def list_migrations(request: Request):
    db = _get_db(request)
    rows = await db.fetch_all("SELECT * FROM migrations ORDER BY created_at DESC")
    return [await _migration_response(db, r) for r in rows]


@router.get("/migrations/{mig_id}", response_model=MigrationResponse)
async def get_migration(mig_id: str, request: Request):
    db = _get_db(request)
    row = await db.fetch_one("SELECT * FROM migrations WHERE id = ?", (mig_id,))
    if row is None:
        raise HTTPException(status_code=404, detail="Migration not found")
    return await _migration_response(db, row)


@router.put("/migrations/{mig_id}", response_model=MigrationResponse)
async def update_migration(mig_id: str, body: MigrationUpdate, request: Request):
    db = _get_db(request)
    existing = await db.fetch_one("SELECT * FROM migrations WHERE id = ?", (mig_id,))
    if existing is None:
        raise HTTPException(status_code=404, detail="Migration not found")
    now = datetime.now(timezone.utc).isoformat()
    new_name = body.name if body.name is not None else existing["name"]
    new_source = body.source_table if body.source_table is not None else existing["source_table"]
    new_target = body.target_table if body.target_table is not None else existing["target_table"]
    new_truncate = int(body.truncate_target) if body.truncate_target is not None else existing["truncate_target"]
    await db.execute(
        "UPDATE migrations SET name=?, source_table=?, target_table=?, truncate_target=?, updated_at=? WHERE id=?",
        (new_name, new_source, new_target, new_truncate, now, mig_id),
    )
    row = await db.fetch_one("SELECT * FROM migrations WHERE id = ?", (mig_id,))
    return await _migration_response(db, row)


@router.delete("/migrations/{mig_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_migration(mig_id: str, request: Request):
    db = _get_db(request)
    existing = await db.fetch_one("SELECT id FROM migrations WHERE id = ?", (mig_id,))
    if existing is None:
        raise HTTPException(status_code=404, detail="Migration not found")
    await db.execute("DELETE FROM migrations WHERE id = ?", (mig_id,))
