from __future__ import annotations
from typing import TYPE_CHECKING

import sqlglot
from sqlglot import exp

from vonnegut.pipeline.results import CheckResult, CheckStatus
from vonnegut.pipeline.engine.validator.rules.base import ValidationRule

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

        expression = config.expression.strip()
        parse_expr = expression.replace("{prev}", "__prev__")

        try:
            statements = sqlglot.parse(parse_expr, error_level=sqlglot.ErrorLevel.WARN)
        except sqlglot.errors.ParseError as e:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.FAILED,
                message=f"SQL parse error: {e}",
            )

        if len(statements) > 1:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.FAILED,
                message="SQL must be a single statement (no semicolons)",
            )

        if not statements or statements[0] is None:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.FAILED,
                message="SQL expression is empty or could not be parsed",
            )

        if not isinstance(statements[0], exp.Select):
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.FAILED,
                message=f"SQL must be a SELECT statement, got: {type(statements[0]).__name__}",
            )

        return CheckResult(
            rule_name=self.name,
            status=CheckStatus.PASSED,
            message="SQL is a valid single SELECT statement",
        )
