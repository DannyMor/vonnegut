from __future__ import annotations
import ast
from typing import TYPE_CHECKING

from vonnegut.pipeline.results import CheckResult, CheckStatus
from vonnegut.pipeline.engine.validator.rules.base import ValidationRule

if TYPE_CHECKING:
    import pyarrow as pa
    from vonnegut.pipeline.dag.node import Node
    from vonnegut.pipeline.dag.plan import ExecutionContext
    from vonnegut.pipeline.schema.types import Schema


class SyntaxCheckRule(ValidationRule):
    name = "syntax_check"
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
        from vonnegut.pipeline.dag.node import CodeNodeConfig

        config = node.config
        assert isinstance(config, CodeNodeConfig)
        code = config.function_code

        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.FAILED,
                message=f"Syntax error: {e}",
            )

        func_names = [
            n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
        ]
        if "transform" not in func_names:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.FAILED,
                message="Code must define a 'transform(df)' function",
            )

        return CheckResult(
            rule_name=self.name,
            status=CheckStatus.PASSED,
            message="Syntax valid, transform() function found",
        )


class ColumnNameRule(ValidationRule):
    name = "column_name_check"
    critical = False

    def check(
        self,
        node: Node,
        context: ExecutionContext,
        input_data: dict[str, pa.Table],
        output_data: pa.Table | None,
        input_schemas: dict[str, Schema],
        output_schema: Schema | None,
    ) -> CheckResult:
        if output_schema is None:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.PASSED,
                message="No output schema to check",
            )

        names = output_schema.column_names

        empty = [i for i, n in enumerate(names) if not n.strip()]
        if empty:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.FAILED,
                message=f"Empty column name(s) at positions: {empty}",
            )

        seen = set()
        dupes = []
        for n in names:
            if n in seen:
                dupes.append(n)
            seen.add(n)
        if dupes:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.FAILED,
                message=f"Duplicate column names: {dupes}",
            )

        return CheckResult(
            rule_name=self.name,
            status=CheckStatus.PASSED,
            message="All column names valid and unique",
        )
