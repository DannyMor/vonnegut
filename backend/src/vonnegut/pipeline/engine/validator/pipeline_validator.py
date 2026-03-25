from __future__ import annotations
from abc import ABC, abstractmethod

from vonnegut.pipeline.results import CheckResult, CheckStatus
from vonnegut.pipeline.schema.types import Schema
from vonnegut.pipeline.dag.plan import PlanNode, PlanEdge


class PipelineValidationRule(ABC):
    name: str

    @abstractmethod
    def check(
        self,
        edge: PlanEdge,
        from_node: PlanNode,
        to_node: PlanNode,
        from_schema: Schema,
        to_input_name: str | None,
    ) -> CheckResult: ...


class SchemaCompatibilityRule(PipelineValidationRule):
    name = "schema_compatibility"

    def check(
        self,
        edge: PlanEdge,
        from_node: PlanNode,
        to_node: PlanNode,
        from_schema: Schema,
        to_input_name: str | None,
    ) -> CheckResult:
        if not from_schema.columns:
            return CheckResult(
                rule_name=self.name,
                status=CheckStatus.WARNING,
                message=f"Empty schema from node '{from_node.id}' — cannot verify compatibility",
            )
        return CheckResult(
            rule_name=self.name,
            status=CheckStatus.PASSED,
            message=f"Schema from '{from_node.id}' has {len(from_schema.columns)} columns",
        )


class PipelineValidator:
    def __init__(
        self, rules: list[PipelineValidationRule] | None = None,
    ) -> None:
        self.rules = rules or [SchemaCompatibilityRule()]

    def validate_edge(
        self,
        edge: PlanEdge,
        from_node: PlanNode,
        to_node: PlanNode,
        from_schema: Schema,
        to_input_name: str | None,
    ) -> list[CheckResult]:
        results = []
        for rule in self.rules:
            results.append(
                rule.check(edge, from_node, to_node, from_schema, to_input_name),
            )
        return results
