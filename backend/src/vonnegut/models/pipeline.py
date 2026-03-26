from typing import Literal

from pydantic import BaseModel, field_validator


StepType = Literal["sql", "code", "ai"]


class ColumnDef(BaseModel):
    name: str
    type: str


class SQLConfig(BaseModel):
    expression: str


class CodeConfig(BaseModel):
    function_code: str


class AIConfig(BaseModel):
    prompt: str
    generated_type: Literal["sql", "code"] | None = None
    generated_code: str | None = None
    approved: bool = False


class PipelineStepCreate(BaseModel):
    step_type: StepType
    name: str
    description: str | None = None
    config: dict
    insert_after: str | None = None

    @field_validator("name", mode="before")
    @classmethod
    def trim_strings(cls, v: str) -> str:
        return v.strip() if isinstance(v, str) else v


class PipelineStepUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    step_type: StepType | None = None
    config: dict | None = None

    @field_validator("name", "description", mode="before")
    @classmethod
    def trim_strings(cls, v: str | None) -> str | None:
        return v.strip() if isinstance(v, str) else v


class PipelineStepResponse(BaseModel):
    id: str
    pipeline_id: str
    name: str
    description: str | None
    position: int
    step_type: StepType
    config: dict
    created_at: str
    updated_at: str
