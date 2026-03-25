from __future__ import annotations
from typing import TYPE_CHECKING

from vonnegut.pipeline.results import CheckResult, CheckStatus
from vonnegut.pipeline.engine.validator.rules.base import ValidationRule
from vonnegut.pipeline.sql_utils import parse_sql_strict

if TYPE_CHECKING:
    import pyarrow as pa
    from vonnegut.pipeline.dag.node import Node
    from vonnegut.pipeline.dag.plan import ExecutionContext
    from vonnegut.pipeline.schema.types import Schema


class SqlParseRule(ValidationRule):
    name = "sql_parse"
    critical = True

    def check(
        self,
        node: Node,
        context: ExecutionContext,
        input_data: dict[str, pa.Table],
        output_data: pa.Table | None,
        input_schemas: dict[str, Schema],
        output_schema: Schema | None,
    ) -> CheckResult:
        from vonnegut.pipeline.dag.node import SqlNodeConfig

        config = node.config
        assert isinstance(config, SqlNodeConfig)

        _, error = parse_sql_strict(config.expression)
        if error is not None:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.FAILED,
                message=error,
            )

        return CheckResult(
            rule_name=self.name,
            status=CheckStatus.PASSED,
            message="SQL is a valid single SELECT statement",
        )
