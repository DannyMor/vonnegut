import json

from fastapi import APIRouter, HTTPException, Request, status

from vonnegut.models.pipeline import PipelineStepCreate, PipelineStepResponse, PipelineStepUpdate

router = APIRouter(tags=["pipeline-steps"])


def _get_repos(request: Request):
    return request.app.state.pipeline_repo, request.app.state.pipeline_step_repo


async def _invalidate_metadata(request: Request, pipeline_id: str) -> None:
    """Reset validation to DRAFT when pipeline steps change."""
    metadata_repo = request.app.state.pipeline_metadata_repo
    if metadata_repo is not None:
        await metadata_repo.reset_to_draft(pipeline_id)


@router.post(
    "/pipelines/{pipeline_id}/steps",
    response_model=PipelineStepResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_step(pipeline_id: str, body: PipelineStepCreate, request: Request):
    pipeline_repo, step_repo = _get_repos(request)
    pipeline = await pipeline_repo.get(pipeline_id)
    if pipeline is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    try:
        row = await step_repo.create(
            pipeline_id=pipeline_id,
            name=body.name,
            step_type=body.step_type,
            config=body.config,
            description=body.description,
            insert_after=body.insert_after,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    await _invalidate_metadata(request, pipeline_id)

    return PipelineStepResponse(
        id=row["id"], pipeline_id=row["pipeline_id"], name=row["name"],
        description=row["description"], position=row["position"],
        step_type=row["step_type"], config=json.loads(row["config"]),
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


@router.put(
    "/pipelines/{pipeline_id}/steps/{step_id}",
    response_model=PipelineStepResponse,
)
async def update_step(pipeline_id: str, step_id: str, body: PipelineStepUpdate, request: Request):
    _, step_repo = _get_repos(request)
    fields = body.model_dump(exclude_none=True)
    row = await step_repo.update(step_id, pipeline_id, **fields)
    if row is None:
        raise HTTPException(status_code=404, detail="Pipeline step not found")

    await _invalidate_metadata(request, pipeline_id)

    return PipelineStepResponse(
        id=row["id"], pipeline_id=row["pipeline_id"], name=row["name"],
        description=row["description"], position=row["position"],
        step_type=row["step_type"], config=json.loads(row["config"]),
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


@router.delete(
    "/pipelines/{pipeline_id}/steps/{step_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_step(pipeline_id: str, step_id: str, request: Request):
    _, step_repo = _get_repos(request)
    deleted = await step_repo.delete(step_id, pipeline_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Pipeline step not found")

    await _invalidate_metadata(request, pipeline_id)
