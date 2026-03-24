# backend/src/vonnegut/routers/migrations.py
import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from psycopg import sql as psql

from vonnegut.models.migration import MigrationCreate, MigrationResponse, MigrationUpdate
from vonnegut.models.pipeline import PipelineStepResponse
from vonnegut.models.transformation import TransformationResponse
from vonnegut.services.transformation_engine import TransformationEngine
from vonnegut.services.migration_runner import MigrationRunner

logger = logging.getLogger(__name__)
router = APIRouter(tags=["migrations"])

# In-memory state for running migrations
_running_migrations: dict[str, asyncio.Event] = {}


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


async def _get_pipeline_steps(db, migration_id: str) -> list[PipelineStepResponse]:
    rows = await db.fetch_all(
        "SELECT * FROM pipeline_steps WHERE migration_id = ? ORDER BY position",
        (migration_id,),
    )
    return [
        PipelineStepResponse(
            id=r["id"], migration_id=r["migration_id"], name=r["name"],
            description=r["description"], position=r["position"],
            step_type=r["step_type"], config=json.loads(r["config"]),
            created_at=r["created_at"], updated_at=r["updated_at"],
        )
        for r in rows
    ]


async def _migration_response(db, row: dict) -> MigrationResponse:
    transforms = await _get_transformations(db, row["id"])
    pipeline_steps = await _get_pipeline_steps(db, row["id"])
    return MigrationResponse(
        id=row["id"], name=row["name"],
        source_connection_id=row["source_connection_id"],
        target_connection_id=row["target_connection_id"],
        source_table=row["source_table"], target_table=row["target_table"],
        source_query=row["source_query"],
        source_schema=json.loads(row["source_schema"]),
        status=row["status"], truncate_target=bool(row["truncate_target"]),
        rows_processed=row["rows_processed"], total_rows=row["total_rows"],
        error_message=row["error_message"],
        created_at=row["created_at"], updated_at=row["updated_at"],
        transformations=transforms,
        pipeline_steps=pipeline_steps,
    )


@router.post("/migrations", response_model=MigrationResponse, status_code=status.HTTP_201_CREATED)
async def create_migration(body: MigrationCreate, request: Request):
    db = _get_db(request)
    mig_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        """INSERT INTO migrations
           (id, name, source_connection_id, target_connection_id, source_table, target_table,
            source_query, source_schema, status, truncate_target, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?)""",
        (mig_id, body.name, body.source_connection_id, body.target_connection_id,
         body.source_table, body.target_table, body.source_query,
         json.dumps(body.source_schema), int(body.truncate_target), now, now),
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
    new_query = body.source_query if body.source_query is not None else existing["source_query"]
    new_schema = json.dumps(body.source_schema) if body.source_schema is not None else existing["source_schema"]
    await db.execute(
        "UPDATE migrations SET name=?, source_table=?, target_table=?, source_query=?, source_schema=?, truncate_target=?, updated_at=? WHERE id=?",
        (new_name, new_source, new_target, new_query, new_schema, new_truncate, now, mig_id),
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

    # Load pipeline steps
    step_rows = await db.fetch_all(
        "SELECT * FROM pipeline_steps WHERE migration_id = ? ORDER BY position",
        (mig_id,),
    )
    steps = [
        {"id": s["id"], "name": s["name"], "position": s["position"],
         "step_type": s["step_type"], "config": json.loads(s["config"])}
        for s in step_rows
    ]

    manager = request.app.state.connection_manager
    adapter_factory = _get_adapter_factory(request)
    source_conn = await manager.get(row["source_connection_id"])
    source_adapter = await adapter_factory.create(source_conn)

    # Get target schema if configured
    target_schema = None
    if row["target_connection_id"] and row["target_table"]:
        target_conn = await manager.get(row["target_connection_id"])
        target_adapter = await adapter_factory.create(target_conn)
        try:
            target_schema = await target_adapter.fetch_schema(row["target_table"])
        finally:
            await target_adapter.disconnect()

    try:
        from vonnegut.services.pipeline_engine import PipelineEngine
        engine = PipelineEngine()
        source_query = row["source_query"] or psql.SQL("SELECT * FROM {}").format(psql.Identifier(row["source_table"])).as_string(None)
        result = await engine.run_test(
            source_adapter=source_adapter,
            source_query=source_query,
            steps=steps,
            limit=10,
            target_schema=target_schema,
        )
        return result
    finally:
        await source_adapter.disconnect()


@router.post("/migrations/{mig_id}/test-stream")
async def test_migration_stream(mig_id: str, request: Request):
    """SSE endpoint that streams step-by-step test progress."""
    db = _get_db(request)
    row = await db.fetch_one("SELECT * FROM migrations WHERE id = ?", (mig_id,))
    if row is None:
        raise HTTPException(status_code=404, detail="Migration not found")

    step_rows = await db.fetch_all(
        "SELECT * FROM pipeline_steps WHERE migration_id = ? ORDER BY position",
        (mig_id,),
    )
    steps = [
        {"id": s["id"], "name": s["name"], "position": s["position"],
         "step_type": s["step_type"], "config": json.loads(s["config"])}
        for s in step_rows
    ]

    manager = request.app.state.connection_manager
    adapter_factory = _get_adapter_factory(request)

    async def event_generator():
        queue: asyncio.Queue[dict | None] = asyncio.Queue()

        async def on_progress(event: dict):
            event["timestamp"] = datetime.now(timezone.utc).isoformat()
            await queue.put(event)

        async def run_pipeline():
            source_adapter = None
            try:
                # Connect to source
                await queue.put({"type": "step_start", "node_id": "_connect",
                                 "name": "Connecting to source",
                                 "timestamp": datetime.now(timezone.utc).isoformat()})
                t0 = time.monotonic()
                source_conn = await manager.get(row["source_connection_id"])
                if source_conn is None:
                    raise ValueError("Source connection not found")
                source_adapter = await adapter_factory.create(source_conn)
                dur = round((time.monotonic() - t0) * 1000)
                await queue.put({"type": "step_complete", "node_id": "_connect",
                                 "name": "Connecting to source", "status": "ok",
                                 "duration_ms": dur,
                                 "timestamp": datetime.now(timezone.utc).isoformat()})

                # Get target schema if configured
                target_schema = None
                if row["target_connection_id"] and row["target_table"]:
                    target_conn = await manager.get(row["target_connection_id"])
                    if target_conn:
                        target_adapter = await adapter_factory.create(target_conn)
                        try:
                            target_schema = await target_adapter.fetch_schema(row["target_table"])
                        finally:
                            await target_adapter.disconnect()

                from vonnegut.services.pipeline_engine import PipelineEngine
                engine = PipelineEngine()
                source_query = row["source_query"] or psql.SQL("SELECT * FROM {}").format(psql.Identifier(row["source_table"])).as_string(None)
                result = await engine.run_test(
                    source_adapter=source_adapter,
                    source_query=source_query,
                    steps=steps,
                    limit=10,
                    target_schema=target_schema,
                    on_progress=on_progress,
                )
                await queue.put({"type": "result", "data": result,
                                 "timestamp": datetime.now(timezone.utc).isoformat()})
            except Exception as e:
                await queue.put({"type": "error", "error": str(e),
                                 "timestamp": datetime.now(timezone.utc).isoformat()})
            finally:
                if source_adapter:
                    await source_adapter.disconnect()
                await queue.put(None)  # sentinel

        task = asyncio.create_task(run_pipeline())

        try:
            while True:
                event = await queue.get()
                if event is None:
                    yield f"data: {json.dumps({'type': 'done'})}\n\n"
                    break
                yield f"data: {json.dumps(event)}\n\n"
        except asyncio.CancelledError:
            task.cancel()

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.post("/migrations/{mig_id}/run-stream")
async def run_migration_stream(mig_id: str, request: Request):
    """SSE endpoint that streams step-by-step run progress, then writes to target."""
    db = _get_db(request)
    row = await db.fetch_one("SELECT * FROM migrations WHERE id = ?", (mig_id,))
    if row is None:
        raise HTTPException(status_code=404, detail="Migration not found")
    if row["status"] == "running":
        raise HTTPException(status_code=409, detail="Migration is already running")

    step_rows = await db.fetch_all(
        "SELECT * FROM pipeline_steps WHERE migration_id = ? ORDER BY position",
        (mig_id,),
    )
    steps = [
        {"id": s["id"], "name": s["name"], "position": s["position"],
         "step_type": s["step_type"], "config": json.loads(s["config"])}
        for s in step_rows
    ]

    manager = request.app.state.connection_manager
    adapter_factory = _get_adapter_factory(request)
    settings = request.app.state.settings

    await db.execute(
        "UPDATE migrations SET status = 'running', rows_processed = 0, error_message = NULL, updated_at = ? WHERE id = ?",
        (datetime.now(timezone.utc).isoformat(), mig_id),
    )

    async def event_generator():
        queue: asyncio.Queue[dict | None] = asyncio.Queue()

        async def on_progress(event: dict):
            event["timestamp"] = datetime.now(timezone.utc).isoformat()
            await queue.put(event)

        async def run_pipeline():
            source_adapter = None
            target_adapter = None
            try:
                # Connect to source
                await queue.put({"type": "step_start", "node_id": "_connect_source",
                                 "name": "Connecting to source",
                                 "timestamp": datetime.now(timezone.utc).isoformat()})
                t0 = time.monotonic()
                source_conn = await manager.get(row["source_connection_id"])
                if source_conn is None:
                    raise ValueError("Source connection not found")
                source_adapter = await adapter_factory.create(source_conn)
                dur = round((time.monotonic() - t0) * 1000)
                await queue.put({"type": "step_complete", "node_id": "_connect_source",
                                 "name": "Connecting to source", "status": "ok",
                                 "duration_ms": dur,
                                 "timestamp": datetime.now(timezone.utc).isoformat()})

                # Connect to target
                await queue.put({"type": "step_start", "node_id": "_connect_target",
                                 "name": "Connecting to target",
                                 "timestamp": datetime.now(timezone.utc).isoformat()})
                t0 = time.monotonic()
                target_conn = await manager.get(row["target_connection_id"])
                if target_conn is None:
                    raise ValueError("Target connection not found")
                target_adapter = await adapter_factory.create(target_conn)
                dur = round((time.monotonic() - t0) * 1000)
                await queue.put({"type": "step_complete", "node_id": "_connect_target",
                                 "name": "Connecting to target", "status": "ok",
                                 "duration_ms": dur,
                                 "timestamp": datetime.now(timezone.utc).isoformat()})

                # Get target schema
                target_schema = None
                if row["target_table"]:
                    try:
                        target_schema = await target_adapter.fetch_schema(row["target_table"])
                    except Exception:
                        pass

                # Run pipeline (no row limit for actual run)
                from vonnegut.services.pipeline_engine import PipelineEngine
                engine = PipelineEngine()
                source_query = row["source_query"] or psql.SQL("SELECT * FROM {}").format(psql.Identifier(row["source_table"])).as_string(None)
                result = await engine.run_test(
                    source_adapter=source_adapter,
                    source_query=source_query,
                    steps=steps,
                    limit=settings.migration_row_limit,
                    target_schema=target_schema,
                    on_progress=on_progress,
                )

                # Check if pipeline had errors
                pipeline_errors = [s for s in result.get("steps", []) if s["status"] == "error"]
                if pipeline_errors:
                    error_msgs = []
                    for s in pipeline_errors:
                        for e in s.get("validation", {}).get("errors", []):
                            error_msgs.append(e.get("message", "Unknown error"))
                    msg = "; ".join(error_msgs) or "Pipeline failed"
                    await db.execute(
                        "UPDATE migrations SET status = 'failed', error_message = ?, updated_at = ? WHERE id = ?",
                        (msg, datetime.now(timezone.utc).isoformat(), mig_id),
                    )
                    return

                # Get final transformed rows from the last non-target step
                all_steps = result.get("steps", [])
                data_steps = [s for s in all_steps if s["node_id"] != "target"]
                final_step = data_steps[-1] if data_steps else None
                transformed = final_step["sample_data"] if final_step else []

                if not transformed:
                    await db.execute(
                        "UPDATE migrations SET status = 'completed', rows_processed = 0, total_rows = 0, updated_at = ? WHERE id = ?",
                        (datetime.now(timezone.utc).isoformat(), mig_id),
                    )
                    await queue.put({"type": "info", "message": "No rows to write",
                                     "timestamp": datetime.now(timezone.utc).isoformat()})
                    return

                # Write to target
                await queue.put({"type": "step_start", "node_id": "_write",
                                 "name": f"Writing {len(transformed)} rows to target",
                                 "timestamp": datetime.now(timezone.utc).isoformat()})
                t0 = time.monotonic()

                if bool(row["truncate_target"]):
                    truncate_q = psql.SQL("TRUNCATE TABLE {}").format(psql.Identifier(row["target_table"]))
                    await target_adapter.execute(truncate_q.as_string(None))

                columns = list(transformed[0].keys())
                col_ids = psql.SQL(", ").join(psql.Identifier(c) for c in columns)
                placeholders = psql.SQL(", ").join(psql.Placeholder() * len(columns))
                insert_query = psql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    psql.Identifier(row["target_table"]), col_ids, placeholders,
                )
                insert_str = insert_query.as_string(None)

                rows_written = 0
                for r in transformed:
                    values = tuple(r[c] for c in columns)
                    await target_adapter.execute(insert_str, values)
                    rows_written += 1

                dur = round((time.monotonic() - t0) * 1000)
                await queue.put({"type": "step_complete", "node_id": "_write",
                                 "name": f"Writing {len(transformed)} rows to target",
                                 "status": "ok", "duration_ms": dur,
                                 "row_count": rows_written,
                                 "timestamp": datetime.now(timezone.utc).isoformat()})

                await db.execute(
                    "UPDATE migrations SET status = 'completed', rows_processed = ?, total_rows = ?, updated_at = ? WHERE id = ?",
                    (rows_written, rows_written, datetime.now(timezone.utc).isoformat(), mig_id),
                )

            except Exception as e:
                await queue.put({"type": "error", "error": str(e),
                                 "timestamp": datetime.now(timezone.utc).isoformat()})
                await db.execute(
                    "UPDATE migrations SET status = 'failed', error_message = ?, updated_at = ? WHERE id = ?",
                    (str(e), datetime.now(timezone.utc).isoformat(), mig_id),
                )
            finally:
                if source_adapter:
                    await source_adapter.disconnect()
                if target_adapter:
                    await target_adapter.disconnect()
                await queue.put(None)  # sentinel

        task = asyncio.create_task(run_pipeline())

        try:
            while True:
                event = await queue.get()
                if event is None:
                    yield f"data: {json.dumps({'type': 'done'})}\n\n"
                    break
                yield f"data: {json.dumps(event)}\n\n"
        except asyncio.CancelledError:
            task.cancel()

    return StreamingResponse(event_generator(), media_type="text/event-stream")


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

    cancel_flag = asyncio.Event()
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

    task = asyncio.create_task(_run())
    task.add_done_callback(lambda t: logger.error("Migration task %s failed: %s", mig_id, t.exception()) if t.exception() else None)
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
