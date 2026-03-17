# backend/src/vonnegut/routers/migrations.py
import asyncio
import json
import threading
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, status

from vonnegut.models.migration import MigrationCreate, MigrationResponse, MigrationUpdate
from vonnegut.models.transformation import TransformationResponse
from vonnegut.services.transformation_engine import TransformationEngine
from vonnegut.services.migration_runner import MigrationRunner

router = APIRouter(tags=["migrations"])

# In-memory state for running migrations
_running_migrations: dict[str, threading.Event] = {}


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


def _get_adapter_factory(request: Request):
    return request.app.state.adapter_factory


@router.post("/migrations/{mig_id}/test")
async def test_migration(mig_id: str, request: Request):
    db = _get_db(request)
    row = await db.fetch_one("SELECT * FROM migrations WHERE id = ?", (mig_id,))
    if row is None:
        raise HTTPException(status_code=404, detail="Migration not found")

    transforms = await _get_transformations(db, mig_id)
    transform_dicts = [{"type": t.type, "config": t.config} for t in transforms]

    manager = request.app.state.connection_manager
    source_conn = await manager.get(row["source_connection_id"])
    adapter_factory = _get_adapter_factory(request)
    source_adapter = await adapter_factory.create(source_conn)

    try:
        engine = TransformationEngine()
        runner = MigrationRunner(engine=engine)
        result = await runner.run_test(source_adapter, row["source_table"], transform_dicts, rows=10)
        return result
    finally:
        await source_adapter.disconnect()


@router.post("/migrations/{mig_id}/run")
async def run_migration(mig_id: str, request: Request):
    db = _get_db(request)
    row = await db.fetch_one("SELECT * FROM migrations WHERE id = ?", (mig_id,))
    if row is None:
        raise HTTPException(status_code=404, detail="Migration not found")
    if row["status"] == "running":
        raise HTTPException(status_code=409, detail="Migration is already running")

    settings = request.app.state.settings
    await db.execute(
        "UPDATE migrations SET status = 'running', rows_processed = 0, error_message = NULL, updated_at = ? WHERE id = ?",
        (datetime.now(timezone.utc).isoformat(), mig_id),
    )

    cancel_flag = threading.Event()
    _running_migrations[mig_id] = cancel_flag

    adapter_factory = _get_adapter_factory(request)

    async def _run():
        manager = request.app.state.connection_manager
        source_conn = await manager.get(row["source_connection_id"])
        target_conn = await manager.get(row["target_connection_id"])
        source_adapter = await adapter_factory.create(source_conn)
        target_adapter = await adapter_factory.create(target_conn)

        transforms = await _get_transformations(db, mig_id)
        transform_dicts = [{"type": t.type, "config": t.config} for t in transforms]

        engine = TransformationEngine()
        runner = MigrationRunner(engine=engine)

        async def on_progress(rows_done, total):
            await db.execute(
                "UPDATE migrations SET rows_processed = ?, total_rows = ?, updated_at = ? WHERE id = ?",
                (rows_done, total, datetime.now(timezone.utc).isoformat(), mig_id),
            )

        try:
            result = await runner.run(
                source_adapter=source_adapter,
                target_adapter=target_adapter,
                source_table=row["source_table"],
                target_table=row["target_table"],
                transformations=transform_dicts,
                truncate_target=bool(row["truncate_target"]),
                row_limit=settings.migration_row_limit,
                batch_size=settings.migration_batch_size,
                on_progress=on_progress,
                cancel_flag=cancel_flag,
            )
            await db.execute(
                "UPDATE migrations SET status = ?, rows_processed = ?, updated_at = ? WHERE id = ?",
                (result["status"], result["rows_processed"], datetime.now(timezone.utc).isoformat(), mig_id),
            )
        except Exception as e:
            await db.execute(
                "UPDATE migrations SET status = 'failed', error_message = ?, updated_at = ? WHERE id = ?",
                (str(e), datetime.now(timezone.utc).isoformat(), mig_id),
            )
        finally:
            await source_adapter.disconnect()
            await target_adapter.disconnect()
            _running_migrations.pop(mig_id, None)

    asyncio.create_task(_run())
    return {"status": "started", "migration_id": mig_id}


@router.post("/migrations/{mig_id}/cancel")
async def cancel_migration(mig_id: str, request: Request):
    cancel_flag = _running_migrations.get(mig_id)
    if cancel_flag is None:
        raise HTTPException(status_code=404, detail="No running migration found")
    cancel_flag.set()
    return {"status": "cancelling", "migration_id": mig_id}


@router.get("/migrations/{mig_id}/status")
async def get_migration_status(mig_id: str, request: Request):
    db = _get_db(request)
    row = await db.fetch_one(
        "SELECT status, rows_processed, total_rows, error_message FROM migrations WHERE id = ?",
        (mig_id,),
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Migration not found")
    return dict(row)
