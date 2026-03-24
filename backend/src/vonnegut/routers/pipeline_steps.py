import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, status

from vonnegut.models.pipeline import PipelineStepCreate, PipelineStepResponse, PipelineStepUpdate

router = APIRouter(tags=["pipeline-steps"])


def _get_db(request: Request):
    return request.app.state.db


@router.post(
    "/migrations/{mig_id}/steps",
    response_model=PipelineStepResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_step(mig_id: str, body: PipelineStepCreate, request: Request):
    db = _get_db(request)
    mig = await db.fetch_one("SELECT id FROM migrations WHERE id = ?", (mig_id,))
    if mig is None:
        raise HTTPException(status_code=404, detail="Migration not found")

    if body.insert_after:
        after_step = await db.fetch_one(
            "SELECT position FROM pipeline_steps WHERE id = ? AND migration_id = ?",
            (body.insert_after, mig_id),
        )
        if after_step is None:
            raise HTTPException(status_code=404, detail="insert_after step not found")
        new_position = after_step["position"] + 1
        await db.execute(
            "UPDATE pipeline_steps SET position = position + 1 WHERE migration_id = ? AND position >= ?",
            (mig_id, new_position),
        )
    else:
        result = await db.fetch_one(
            "SELECT COALESCE(MAX(position), -1) as max_pos FROM pipeline_steps WHERE migration_id = ?",
            (mig_id,),
        )
        new_position = result["max_pos"] + 1

    step_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        """INSERT INTO pipeline_steps (id, migration_id, name, description, position, step_type, config, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (step_id, mig_id, body.name, body.description, new_position,
         body.step_type, json.dumps(body.config), now, now),
    )
    return PipelineStepResponse(
        id=step_id, migration_id=mig_id, name=body.name,
        description=body.description, position=new_position,
        step_type=body.step_type, config=body.config,
        created_at=now, updated_at=now,
    )


@router.put(
    "/migrations/{mig_id}/steps/{step_id}",
    response_model=PipelineStepResponse,
)
async def update_step(mig_id: str, step_id: str, body: PipelineStepUpdate, request: Request):
    db = _get_db(request)
    existing = await db.fetch_one(
        "SELECT * FROM pipeline_steps WHERE id = ? AND migration_id = ?",
        (step_id, mig_id),
    )
    if existing is None:
        raise HTTPException(status_code=404, detail="Pipeline step not found")

    now = datetime.now(timezone.utc).isoformat()
    new_name = body.name if body.name is not None else existing["name"]
    new_desc = body.description if body.description is not None else existing["description"]
    new_type = body.step_type if body.step_type is not None else existing["step_type"]
    new_config = json.dumps(body.config) if body.config is not None else existing["config"]

    await db.execute(
        "UPDATE pipeline_steps SET name=?, description=?, step_type=?, config=?, updated_at=? WHERE id=?",
        (new_name, new_desc, new_type, new_config, now, step_id),
    )
    row = await db.fetch_one("SELECT * FROM pipeline_steps WHERE id = ?", (step_id,))
    return PipelineStepResponse(
        id=row["id"], migration_id=row["migration_id"], name=row["name"],
        description=row["description"], position=row["position"],
        step_type=row["step_type"], config=json.loads(row["config"]),
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


@router.delete(
    "/migrations/{mig_id}/steps/{step_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_step(mig_id: str, step_id: str, request: Request):
    db = _get_db(request)
    existing = await db.fetch_one(
        "SELECT position FROM pipeline_steps WHERE id = ? AND migration_id = ?",
        (step_id, mig_id),
    )
    if existing is None:
        raise HTTPException(status_code=404, detail="Pipeline step not found")

    await db.execute("DELETE FROM pipeline_steps WHERE id = ?", (step_id,))
    await db.execute(
        "UPDATE pipeline_steps SET position = position - 1 WHERE migration_id = ? AND position > ?",
        (mig_id, existing["position"]),
    )
