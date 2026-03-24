from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa
    from vonnegut.pipeline.schema.types import Schema


class CheckStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"


@dataclass(frozen=True)
class CheckResult:
    rule_name: str
    status: CheckStatus
    message: str
    details: dict | None = None


@dataclass
class ValidationSuccess:
    output_schema: Schema
    output_data: pa.Table | None
    checks: list[CheckResult] = field(default_factory=list)


@dataclass
class ValidationFailure:
    errors: list[CheckResult]
    output_schema: Schema | None = None
    output_data: pa.Table | None = None


NodeValidationResult = ValidationSuccess | ValidationFailure


@dataclass
class ExecutionSuccess:
    pass


@dataclass
class ExecutionFailure:
    node_id: str
    error: str


ExecutionResult = ExecutionSuccess | ExecutionFailure
