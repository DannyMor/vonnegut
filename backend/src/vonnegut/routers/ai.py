# backend/src/vonnegut/routers/ai.py
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from vonnegut.services.ai_assistant import AIAssistant

router = APIRouter(tags=["ai"])


class SuggestTransformationRequest(BaseModel):
    prompt: str
    source_schema: list[dict]
    sample_data: list[dict]
    target_schema: list[dict] | None = None


class SuggestTransformationResponse(BaseModel):
    expression: str
    output_column: str
    explanation: str


@router.post("/ai/suggest-transformation", response_model=SuggestTransformationResponse)
async def suggest_transformation(body: SuggestTransformationRequest, request: Request):
    api_key = request.app.state.settings.anthropic_api_key
    assistant = AIAssistant(api_key=api_key)
    try:
        result = assistant.suggest_transformation(
            prompt=body.prompt,
            source_schema=body.source_schema,
            sample_data=body.sample_data,
            target_schema=body.target_schema,
        )
        return SuggestTransformationResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI suggestion failed: {str(e)}")
