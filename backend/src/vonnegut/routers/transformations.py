# backend/src/vonnegut/routers/transformations.py
import json

from fastapi import APIRouter, HTTPException, Request, status

from vonnegut.models.transformation import (
    TransformationCreate,
    TransformationResponse,
    TransformationUpdate,
    ReorderRequest,
)

router = APIRouter(tags=["transformations"])


def _get_repos(request: Request):
    return request.app.state.migration_repo, request.app.state.transformation_repo


def _row_to_response(row: dict) -> TransformationResponse:
    return TransformationResponse(
        id=row["id"], migration_id=row["migration_id"], order=row["order"],
        type=row["type"], config=json.loads(row["config"]),
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


@router.post(
    "/migrations/{mig_id}/transformations",
    response_model=TransformationResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_transformation(mig_id: str, body: TransformationCreate, request: Request):
    mig_repo, transform_repo = _get_repos(request)
    mig = await mig_repo.get(mig_id)
    if mig is None:
        raise HTTPException(status_code=404, detail="Migration not found")

    row = await transform_repo.create(migration_id=mig_id, type=body.type, config=body.config)
    return _row_to_response(row)


@router.put("/migrations/{mig_id}/transformations/reorder")
async def reorder_transformations(mig_id: str, body: ReorderRequest, request: Request):
    _, transform_repo = _get_repos(request)
    await transform_repo.reorder(mig_id, body.order)
    return {"status": "ok"}


@router.put(
    "/migrations/{mig_id}/transformations/{t_id}",
    response_model=TransformationResponse,
)
async def update_transformation(mig_id: str, t_id: str, body: TransformationUpdate, request: Request):
    _, transform_repo = _get_repos(request)
    if body.config is None:
        raise HTTPException(status_code=400, detail="config is required")
    row = await transform_repo.update(t_id, mig_id, body.config)
    if row is None:
        raise HTTPException(status_code=404, detail="Transformation not found")
    return _row_to_response(row)


@router.delete(
    "/migrations/{mig_id}/transformations/{t_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_transformation(mig_id: str, t_id: str, request: Request):
    _, transform_repo = _get_repos(request)
    deleted = await transform_repo.delete(t_id, mig_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Transformation not found")
