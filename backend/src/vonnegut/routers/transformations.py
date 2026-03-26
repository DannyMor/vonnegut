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
    return request.app.state.pipeline_repo, request.app.state.transformation_repo


def _row_to_response(row: dict) -> TransformationResponse:
    return TransformationResponse(
        id=row["id"], pipeline_id=row["pipeline_id"], order=row["order"],
        type=row["type"], config=json.loads(row["config"]),
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


@router.post(
    "/pipelines/{pipeline_id}/transformations",
    response_model=TransformationResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_transformation(pipeline_id: str, body: TransformationCreate, request: Request):
    pipeline_repo, transform_repo = _get_repos(request)
    pipeline = await pipeline_repo.get(pipeline_id)
    if pipeline is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    row = await transform_repo.create(pipeline_id=pipeline_id, type=body.type, config=body.config)
    return _row_to_response(row)


@router.put("/pipelines/{pipeline_id}/transformations/reorder")
async def reorder_transformations(pipeline_id: str, body: ReorderRequest, request: Request):
    _, transform_repo = _get_repos(request)
    await transform_repo.reorder(pipeline_id, body.order)
    return {"status": "ok"}


@router.put(
    "/pipelines/{pipeline_id}/transformations/{t_id}",
    response_model=TransformationResponse,
)
async def update_transformation(pipeline_id: str, t_id: str, body: TransformationUpdate, request: Request):
    _, transform_repo = _get_repos(request)
    if body.config is None:
        raise HTTPException(status_code=400, detail="config is required")
    row = await transform_repo.update(t_id, pipeline_id, body.config)
    if row is None:
        raise HTTPException(status_code=404, detail="Transformation not found")
    return _row_to_response(row)


@router.delete(
    "/pipelines/{pipeline_id}/transformations/{t_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_transformation(pipeline_id: str, t_id: str, request: Request):
    _, transform_repo = _get_repos(request)
    deleted = await transform_repo.delete(t_id, pipeline_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Transformation not found")
