# backend/src/vonnegut/routers/transformations.py
import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, status

from vonnegut.models.transformation import (
    TransformationCreate,
    TransformationResponse,
    TransformationUpdate,
    ReorderRequest,
)

router = APIRouter(tags=["transformations"])


def _get_db(request: Request):
    return request.app.state.db


@router.post(
    "/migrations/{mig_id}/transformations",
    response_model=TransformationResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_transformation(mig_id: str, body: TransformationCreate, request: Request):
    db = _get_db(request)
    mig = await db.fetch_one("SELECT id FROM migrations WHERE id = ?", (mig_id,))
    if mig is None:
        raise HTTPException(status_code=404, detail="Migration not found")

    result = await db.fetch_one(
        'SELECT COALESCE(MAX("order"), -1) as max_order FROM transformations WHERE migration_id = ?',
        (mig_id,),
    )
    next_order = result["max_order"] + 1

    t_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        """INSERT INTO transformations (id, migration_id, "order", type, config, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (t_id, mig_id, next_order, body.type, json.dumps(body.config), now, now),
    )
    return TransformationResponse(
        id=t_id, migration_id=mig_id, order=next_order,
        type=body.type, config=body.config, created_at=now, updated_at=now,
    )


@router.put("/migrations/{mig_id}/transformations/reorder")
async def reorder_transformations(mig_id: str, body: ReorderRequest, request: Request):
    db = _get_db(request)
    for idx, t_id in enumerate(body.order):
        await db.execute(
            'UPDATE transformations SET "order" = ? WHERE id = ? AND migration_id = ?',
            (idx, t_id, mig_id),
        )
    return {"status": "ok"}


@router.put(
    "/migrations/{mig_id}/transformations/{t_id}",
    response_model=TransformationResponse,
)
async def update_transformation(mig_id: str, t_id: str, body: TransformationUpdate, request: Request):
    db = _get_db(request)
    existing = await db.fetch_one(
        "SELECT * FROM transformations WHERE id = ? AND migration_id = ?", (t_id, mig_id),
    )
    if existing is None:
        raise HTTPException(status_code=404, detail="Transformation not found")
    now = datetime.now(timezone.utc).isoformat()
    new_config = json.dumps(body.config) if body.config is not None else existing["config"]
    await db.execute(
        "UPDATE transformations SET config = ?, updated_at = ? WHERE id = ?",
        (new_config, now, t_id),
    )
    row = await db.fetch_one("SELECT * FROM transformations WHERE id = ?", (t_id,))
    return TransformationResponse(
        id=row["id"], migration_id=row["migration_id"], order=row["order"],
        type=row["type"], config=json.loads(row["config"]),
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


@router.delete(
    "/migrations/{mig_id}/transformations/{t_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_transformation(mig_id: str, t_id: str, request: Request):
    db = _get_db(request)
    existing = await db.fetch_one(
        "SELECT id FROM transformations WHERE id = ? AND migration_id = ?", (t_id, mig_id),
    )
    if existing is None:
        raise HTTPException(status_code=404, detail="Transformation not found")
    await db.execute("DELETE FROM transformations WHERE id = ?", (t_id,))
