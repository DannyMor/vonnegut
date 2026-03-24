from typing import Literal

from pydantic import BaseModel, field_validator

from vonnegut.models.pipeline import PipelineStepResponse
from vonnegut.models.transformation import TransformationResponse

MigrationStatusType = Literal[
    "draft", "testing", "running", "completed", "failed", "cancelled"
]


class MigrationCreate(BaseModel):
    name: str
    source_connection_id: str
    target_connection_id: str
    source_table: str
    target_table: str
    source_query: str = ""
    source_schema: list[dict] = []
    truncate_target: bool = False

    @field_validator("name", "source_table", "target_table", mode="before")
    @classmethod
    def trim_strings(cls, v: str) -> str:
        return v.strip() if isinstance(v, str) else v


class MigrationUpdate(BaseModel):
    name: str | None = None
    source_table: str | None = None
    target_table: str | None = None
    source_query: str | None = None
    source_schema: list[dict] | None = None
    truncate_target: bool | None = None

    @field_validator("name", "source_table", "target_table", mode="before")
    @classmethod
    def trim_strings(cls, v: str | None) -> str | None:
        return v.strip() if isinstance(v, str) else v


class MigrationResponse(BaseModel):
    id: str
    name: str
    source_connection_id: str
    target_connection_id: str
    source_table: str
    target_table: str
    source_query: str = ""
    source_schema: list[dict] = []
    status: MigrationStatusType
    truncate_target: bool
    rows_processed: int | None
    total_rows: int | None
    error_message: str | None
    created_at: str
    updated_at: str
    transformations: list[TransformationResponse] = []
    pipeline_steps: list[PipelineStepResponse] = []
