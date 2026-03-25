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
from vonnegut.pipeline.pipeline_runner import PipelineRunner
from vonnegut.services.transformation_engine import TransformationEngine
from vonnegut.services.migration_runner import MigrationRunner

logger = logging.getLogger(__name__)
router = APIRouter(tags=["migrations"])

# In-memory state for running migrations
_running_migrations: dict[str, asyncio.Event] = {}


def _get_repos(request: Request):
    return (
        request.app.state.migration_repo,
        request.app.state.pipeline_step_repo,
        request.app.state.transformation_repo,
    )


def _transform_row_to_response(row: dict) -> TransformationResponse:
    return TransformationResponse(
        id=row["id"], migration_id=row["migration_id"], order=row["order"],
        type=row["type"], config=json.loads(row["config"]),
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


def _step_row_to_response(row: dict) -> PipelineStepResponse:
    return PipelineStepResponse(
        id=row["id"], migration_id=row["migration_id"], name=row["name"],
        description=row["description"], position=row["position"],
        step_type=row["step_type"], config=json.loads(row["config"]),
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


async def _migration_response(request: Request, row: dict) -> MigrationResponse:
    _, step_repo, transform_repo = _get_repos(request)
    transform_rows = await transform_repo.list_by_migration(row["id"])
    step_rows = await step_repo.list_by_migration(row["id"])
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
        transformations=[_transform_row_to_response(r) for r in transform_rows],
        pipeline_steps=[_step_row_to_response(r) for r in step_rows],
    )


def _load_steps_for_pipeline(step_rows: list[dict]) -> list[dict]:
    """Convert DB rows to the dict format expected by PipelineRunner."""
    return [
        {"id": s["id"], "name": s["name"], "position": s["position"],
         "step_type": s["step_type"], "config": json.loads(s["config"])}
        for s in step_rows
    ]


@router.post("/migrations", response_model=MigrationResponse, status_code=status.HTTP_201_CREATED)
async def create_migration(body: MigrationCreate, request: Request):
    mig_repo, _, _ = _get_repos(request)
    row = await mig_repo.create(
        name=body.name,
        source_connection_id=body.source_connection_id,
        target_connection_id=body.target_connection_id,
        source_table=body.source_table,
        target_table=body.target_table,
        source_query=body.source_query,
        source_schema=body.source_schema,
        truncate_target=body.truncate_target,
    )
    return await _migration_response(request, row)


@router.get("/migrations", response_model=list[MigrationResponse])
async def list_migrations(request: Request):
    mig_repo, _, _ = _get_repos(request)
    rows = await mig_repo.list_all()
    return [await _migration_response(request, r) for r in rows]


@router.get("/migrations/{mig_id}", response_model=MigrationResponse)
async def get_migration(mig_id: str, request: Request):
    mig_repo, _, _ = _get_repos(request)
    row = await mig_repo.get(mig_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Migration not found")
    return await _migration_response(request, row)


@router.put("/migrations/{mig_id}", response_model=MigrationResponse)
async def update_migration(mig_id: str, body: MigrationUpdate, request: Request):
    mig_repo, _, _ = _get_repos(request)
    fields = body.model_dump(exclude_none=True)
    row = await mig_repo.update(mig_id, **fields)
    if row is None:
        raise HTTPException(status_code=404, detail="Migration not found")
    return await _migration_response(request, row)


@router.delete("/migrations/{mig_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_migration(mig_id: str, request: Request):
    mig_repo, _, _ = _get_repos(request)
    deleted = await mig_repo.delete(mig_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Migration not found")


def _get_adapter_factory(request: Request):
    return request.app.state.adapter_factory


@router.post("/migrations/{mig_id}/test")
async def test_migration(mig_id: str, request: Request):
    mig_repo, step_repo, _ = _get_repos(request)
    row = await mig_repo.get(mig_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Migration not found")

    step_rows = await step_repo.list_by_migration(mig_id)
    steps = _load_steps_for_pipeline(step_rows)

    manager = request.app.state.connection_manager
    adapter_factory = _get_adapter_factory(request)
    source_conn = await manager.get(row["source_connection_id"])
    source_adapter = await adapter_factory.create(source_conn)

    target_schema = None
    if row["target_connection_id"] and row["target_table"]:
        target_conn = await manager.get(row["target_connection_id"])
        target_adapter = await adapter_factory.create(target_conn)
        try:
            target_schema = await target_adapter.fetch_schema(row["target_table"])
        finally:
            await target_adapter.disconnect()

    try:
        runner = PipelineRunner()
        source_query = row["source_query"] or psql.SQL("SELECT * FROM {}").format(psql.Identifier(row["source_table"])).as_string(None)
        result = await runner.run_test(
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
    mig_repo, step_repo, _ = _get_repos(request)
    row = await mig_repo.get(mig_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Migration not found")

    step_rows = await step_repo.list_by_migration(mig_id)
    steps = _load_steps_for_pipeline(step_rows)

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

                target_schema = None
                if row["target_connection_id"] and row["target_table"]:
                    target_conn = await manager.get(row["target_connection_id"])
                    if target_conn:
                        target_adapter = await adapter_factory.create(target_conn)
                        try:
                            target_schema = await target_adapter.fetch_schema(row["target_table"])
                        finally:
                            await target_adapter.disconnect()

                runner = PipelineRunner()
                source_query = row["source_query"] or psql.SQL("SELECT * FROM {}").format(psql.Identifier(row["source_table"])).as_string(None)
                result = await runner.run_test(
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
    mig_repo, step_repo, _ = _get_repos(request)
    row = await mig_repo.get(mig_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Migration not found")
    if row["status"] == "running":
        raise HTTPException(status_code=409, detail="Migration is already running")

    step_rows = await step_repo.list_by_migration(mig_id)
    steps = _load_steps_for_pipeline(step_rows)

    manager = request.app.state.connection_manager
    adapter_factory = _get_adapter_factory(request)
    settings = request.app.state.settings

    await mig_repo.update_status(mig_id, "running", rows_processed=0)

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
                runner = PipelineRunner()
                source_query = row["source_query"] or psql.SQL("SELECT * FROM {}").format(psql.Identifier(row["source_table"])).as_string(None)
                result = await runner.run_test(
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
                    await mig_repo.update_status(mig_id, "failed", error_message=msg)
                    return

                # Get final transformed rows from the last non-target step
                all_steps = result.get("steps", [])
                data_steps = [s for s in all_steps if s["node_id"] != "target"]
                final_step = data_steps[-1] if data_steps else None
                transformed = final_step["sample_data"] if final_step else []

                if not transformed:
                    await mig_repo.update_status(mig_id, "completed", rows_processed=0, total_rows=0)
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

                await mig_repo.update_status(mig_id, "completed", rows_processed=rows_written, total_rows=rows_written)

            except Exception as e:
                await queue.put({"type": "error", "error": str(e),
                                 "timestamp": datetime.now(timezone.utc).isoformat()})
                await mig_repo.update_status(mig_id, "failed", error_message=str(e))
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
    mig_repo, _, transform_repo = _get_repos(request)
    row = await mig_repo.get(mig_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Migration not found")
    if row["status"] == "running":
        raise HTTPException(status_code=409, detail="Migration is already running")

    settings = request.app.state.settings
    await mig_repo.update_status(mig_id, "running", rows_processed=0)

    cancel_flag = asyncio.Event()
    _running_migrations[mig_id] = cancel_flag

    adapter_factory = _get_adapter_factory(request)

    async def _run():
        manager = request.app.state.connection_manager
        source_conn = await manager.get(row["source_connection_id"])
        target_conn = await manager.get(row["target_connection_id"])
        source_adapter = await adapter_factory.create(source_conn)
        target_adapter = await adapter_factory.create(target_conn)

        transform_rows = await transform_repo.list_by_migration(mig_id)
        transform_dicts = [{"type": r["type"], "config": json.loads(r["config"])} for r in transform_rows]

        engine = TransformationEngine()
        runner = MigrationRunner(engine=engine)

        async def on_progress(rows_done, total):
            await mig_repo.update_status(mig_id, "running", rows_processed=rows_done, total_rows=total)

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
            await mig_repo.update_status(mig_id, result["status"], rows_processed=result["rows_processed"])
        except Exception as e:
            await mig_repo.update_status(mig_id, "failed", error_message=str(e))
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
    mig_repo, _, _ = _get_repos(request)
    row = await mig_repo.get_status(mig_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Migration not found")
    return dict(row)
