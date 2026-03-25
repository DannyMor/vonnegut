from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from vonnegut.pipeline.schema.types import Schema


class ValidationStatus(str, Enum):
    DRAFT = "DRAFT"
    VALIDATING = "VALIDATING"
    VALID = "VALID"
    INVALID = "INVALID"


@dataclass
class NodeMetadata:
    node_id: str
    input_schemas: dict[str, Schema] = field(default_factory=dict)
    output_schema: Schema | None = None
    validation_status: str = "pending"
    last_validated_at: datetime | None = None


@dataclass
class PipelineMetadata:
    pipeline_id: str
    node_metadata: dict[str, NodeMetadata] = field(default_factory=dict)
    validated_hash: str | None = None
    validation_status: ValidationStatus = ValidationStatus.DRAFT
    last_validated_at: datetime | None = None
