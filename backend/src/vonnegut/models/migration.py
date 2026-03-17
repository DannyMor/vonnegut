from typing import Literal

from pydantic import BaseModel

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
    truncate_target: bool = False


class MigrationUpdate(BaseModel):
    name: str | None = None
    source_table: str | None = None
    target_table: str | None = None
    truncate_target: bool | None = None


class MigrationResponse(BaseModel):
    id: str
    name: str
    source_connection_id: str
    target_connection_id: str
    source_table: str
    target_table: str
    status: MigrationStatusType
    truncate_target: bool
    rows_processed: int | None
    total_rows: int | None
    error_message: str | None
    created_at: str
    updated_at: str
    transformations: list[TransformationResponse] = []
