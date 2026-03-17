from typing import Literal

from pydantic import BaseModel


class TransformationCreate(BaseModel):
    type: Literal["column_mapping", "sql_expression", "ai_generated"]
    config: dict


class TransformationUpdate(BaseModel):
    config: dict | None = None


class TransformationResponse(BaseModel):
    id: str
    migration_id: str
    order: int
    type: Literal["column_mapping", "sql_expression", "ai_generated"]
    config: dict
    created_at: str
    updated_at: str


class ReorderRequest(BaseModel):
    order: list[str]
