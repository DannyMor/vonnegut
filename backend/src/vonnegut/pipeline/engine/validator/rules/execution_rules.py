from __future__ import annotations
from typing import TYPE_CHECKING

from vonnegut.pipeline.results import CheckResult, CheckStatus
from vonnegut.pipeline.engine.validator.rules.base import ValidationRule

if TYPE_CHECKING:
    import pyarrow as pa
    from vonnegut.pipeline.dag.node import Node
    from vonnegut.pipeline.dag.plan import ExecutionContext
    from vonnegut.pipeline.schema.types import Schema


class SqlExecutionRule(ValidationRule):
    """Post-execution rule: validates SQL output is a non-empty table with columns."""
    name = "sql_execution_check"
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
        if output_data is None:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.PASSED,
                message="No output data (pre-execution phase)",
            )

        if output_data.num_columns == 0:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.FAILED,
                message="SQL produced a result with no columns",
            )

        if output_data.num_rows == 0:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.WARNING,
                message="SQL produced an empty result (0 rows)",
            )

        return CheckResult(
            rule_name=self.name,
            status=CheckStatus.PASSED,
            message=f"SQL produced {output_data.num_rows} rows, {output_data.num_columns} columns",
        )


class CodeExecutionRule(ValidationRule):
    """Post-execution rule: validates code transform output is a valid table."""
    name = "code_execution_check"
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
        if output_data is None:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.PASSED,
                message="No output data (pre-execution phase)",
            )

        if output_data.num_columns == 0:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.FAILED,
                message="Code transform returned a table with no columns",
            )

        if output_data.num_rows == 0:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.WARNING,
                message="Code transform returned an empty table (0 rows)",
            )

        return CheckResult(
            rule_name=self.name,
            status=CheckStatus.PASSED,
            message=f"Code transform produced {output_data.num_rows} rows, {output_data.num_columns} columns",
        )


class SchemaStabilityRule(ValidationRule):
    """Post-execution rule: re-runs code transform on a subset of input to check schema stability.

    If the output schema changes depending on input data, the transform is unreliable.
    Only applies to code nodes with available input data.
    """
    name = "schema_stability"
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

        if output_data is None or output_schema is None:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.PASSED,
                message="No output to check stability against",
            )

        config = node.config
        if not isinstance(config, CodeNodeConfig):
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.PASSED,
                message="Not a code node, skipping stability check",
            )

        default_input = input_data.get("default")
        if default_input is None or default_input.num_rows < 2:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.PASSED,
                message="Not enough input rows to test schema stability",
            )

        # Run the code on a subset (first half vs second half)
        mid = default_input.num_rows // 2
        subset = default_input.slice(0, mid)

        try:
            import polars as pl
            pl_subset = pl.from_arrow(subset)
            namespace: dict = {}
            exec(config.function_code, namespace)
            transform_fn = namespace.get("transform")
            if transform_fn is None:
                return CheckResult(
                    rule_name=self.name,
                    status=CheckStatus.FAILED,
                    message="Could not find transform() function",
                )
            subset_result = transform_fn(pl_subset)
            if isinstance(subset_result, pl.DataFrame):
                subset_columns = set(subset_result.columns)
            else:
                return CheckResult(
                    rule_name=self.name,
                    status=CheckStatus.FAILED,
                    message="transform() did not return a DataFrame",
                )
        except Exception as e:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.WARNING,
                message=f"Could not verify schema stability: {e}",
            )

        full_columns = set(output_data.column_names)
        if subset_columns != full_columns:
            missing = full_columns - subset_columns
            extra = subset_columns - full_columns
            parts = []
            if missing:
                parts.append(f"missing from subset: {missing}")
            if extra:
                parts.append(f"extra in subset: {extra}")
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.FAILED,
                message=f"Schema unstable across data subsets: {'; '.join(parts)}",
            )

        return CheckResult(
            rule_name=self.name,
            status=CheckStatus.PASSED,
            message="Schema is stable across data subsets",
        )
