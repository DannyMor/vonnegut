from __future__ import annotations
import pyarrow as pa

from vonnegut.pipeline.dag.node import Node
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.engine.executor.base import NodeExecutor
from vonnegut.pipeline.engine.validator.rules.base import ValidationRule
from vonnegut.pipeline.results import (
    CheckResult,
    CheckStatus,
    ValidationSuccess,
    ValidationFailure,
    NodeValidationResult,
)
from vonnegut.pipeline.schema.adapters import ArrowSchemaAdapter


class NodeValidator:
    def __init__(self, executor: NodeExecutor, rules: list[ValidationRule]) -> None:
        self.executor = executor
        self.rules = rules

    async def validate(
        self,
        node: Node,
        context: ExecutionContext,
        inputs: dict[str, pa.Table],
    ) -> NodeValidationResult:
        # 1. Run pre-execution rules (no output available yet)
        pre_checks: list[CheckResult] = []
        for rule in self.rules:
            result = rule.check(
                node, context, inputs, None, context.input_schemas, None,
            )
            pre_checks.append(result)
            if result.status == CheckStatus.FAILED and rule.critical:
                return ValidationFailure(
                    errors=[r for r in pre_checks if r.status == CheckStatus.FAILED],
                )

        # 2. Execute the node
        try:
            output_data = await self.executor.execute(context, inputs)
            output_schema = ArrowSchemaAdapter.from_arrow(output_data.schema)
        except Exception as exec_error:
            return ValidationFailure(
                errors=[
                    CheckResult(
                        rule_name="execution",
                        status=CheckStatus.FAILED,
                        message=str(exec_error),
                    ),
                ],
            )

        # 3. Run post-execution rules (output available)
        post_checks: list[CheckResult] = []
        for rule in self.rules:
            result = rule.check(
                node, context, inputs, output_data, context.input_schemas, output_schema,
            )
            post_checks.append(result)
            if result.status == CheckStatus.FAILED and rule.critical:
                failed = [
                    r
                    for r in pre_checks + post_checks
                    if r.status == CheckStatus.FAILED
                ]
                return ValidationFailure(
                    errors=failed,
                    output_schema=output_schema,
                    output_data=output_data,
                )

        all_checks = pre_checks + post_checks
        failed = [r for r in all_checks if r.status == CheckStatus.FAILED]
        if failed:
            return ValidationFailure(
                errors=failed,
                output_schema=output_schema,
                output_data=output_data,
            )

        return ValidationSuccess(
            output_schema=output_schema,
            output_data=output_data,
            checks=all_checks,
        )
