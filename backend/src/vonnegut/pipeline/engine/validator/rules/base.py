from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from vonnegut.pipeline.results import CheckResult

if TYPE_CHECKING:
    import pyarrow as pa
    from vonnegut.pipeline.dag.node import Node
    from vonnegut.pipeline.dag.plan import ExecutionContext
    from vonnegut.pipeline.schema.types import Schema


class ValidationRule(ABC):
    name: str
    critical: bool = True

    @abstractmethod
    def check(
        self,
        node: Node,
        context: ExecutionContext,
        input_data: dict[str, pa.Table],
        output_data: pa.Table | None,
        input_schemas: dict[str, Schema],
        output_schema: Schema | None,
    ) -> CheckResult: ...
