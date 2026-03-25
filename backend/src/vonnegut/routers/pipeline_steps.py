import json

from fastapi import APIRouter, HTTPException, Request, status

from vonnegut.models.pipeline import PipelineStepCreate, PipelineStepResponse, PipelineStepUpdate

router = APIRouter(tags=["pipeline-steps"])


def _get_repos(request: Request):
    return request.app.state.migration_repo, request.app.state.pipeline_step_repo


@router.post(
    "/migrations/{mig_id}/steps",
    response_model=PipelineStepResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_step(mig_id: str, body: PipelineStepCreate, request: Request):
    mig_repo, step_repo = _get_repos(request)
    mig = await mig_repo.get(mig_id)
    if mig is None:
        raise HTTPException(status_code=404, detail="Migration not found")

    try:
        row = await step_repo.create(
            migration_id=mig_id,
            name=body.name,
            step_type=body.step_type,
            config=body.config,
            description=body.description,
            insert_after=body.insert_after,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return PipelineStepResponse(
        id=row["id"], migration_id=row["migration_id"], name=row["name"],
        description=row["description"], position=row["position"],
        step_type=row["step_type"], config=json.loads(row["config"]),
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


@router.put(
    "/migrations/{mig_id}/steps/{step_id}",
    response_model=PipelineStepResponse,
)
async def update_step(mig_id: str, step_id: str, body: PipelineStepUpdate, request: Request):
    _, step_repo = _get_repos(request)
    fields = body.model_dump(exclude_none=True)
    row = await step_repo.update(step_id, mig_id, **fields)
    if row is None:
        raise HTTPException(status_code=404, detail="Pipeline step not found")

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
    _, step_repo = _get_repos(request)
    deleted = await step_repo.delete(step_id, mig_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Pipeline step not found")
