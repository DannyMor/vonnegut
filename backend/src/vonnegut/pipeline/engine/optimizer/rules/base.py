from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from vonnegut.pipeline.dag.plan import LogicalPlan
from vonnegut.pipeline.schema.types import Schema


@dataclass
class OptimizationContext:
    """Context available to optimizer rules.

    node_schemas: maps node_id → validated output Schema (from test run).
    statistics: reserved for future cost-based optimization.
    """
    node_schemas: dict[str, Schema] = field(default_factory=dict)
    statistics: dict | None = None


class OptimizationRule(ABC):
    @abstractmethod
    def apply(
        self, plan: LogicalPlan, context: OptimizationContext,
    ) -> LogicalPlan: ...
