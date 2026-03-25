import pytest
from vonnegut.pipeline.engine.optimizer.optimizer import Optimizer
from vonnegut.pipeline.engine.optimizer.rules.base import OptimizationContext
from vonnegut.pipeline.dag.plan import LogicalPlan, PlanNode, PlanEdge, ExecutionPlan
from vonnegut.pipeline.dag.node import (
    NodeType,
    SourceNodeConfig,
    SqlNodeConfig,
    TargetNodeConfig,
)


def _make_plan() -> LogicalPlan:
    return LogicalPlan(
        nodes={
            "src": PlanNode(
                id="src",
                type=NodeType.SOURCE,
                config=SourceNodeConfig(connection_id="c1", table="t1"),
            ),
            "sql": PlanNode(
                id="sql",
                type=NodeType.SQL,
                config=SqlNodeConfig(expression="SELECT * FROM {prev}"),
            ),
            "tgt": PlanNode(
                id="tgt",
                type=NodeType.TARGET,
                config=TargetNodeConfig(
                    connection_id="c2", table="t2", truncate=False,
                ),
            ),
        },
        edges=[
            PlanEdge(from_node_id="src", to_node_id="sql"),
            PlanEdge(from_node_id="sql", to_node_id="tgt"),
        ],
    )


class TestOptimizer:
    def test_pass_through_preserves_all_nodes(self):
        optimizer = Optimizer(rules=[])
        ctx = OptimizationContext()
        exec_plan = optimizer.optimize(_make_plan(), ctx)
        assert isinstance(exec_plan, ExecutionPlan)
        assert len(exec_plan.contexts) == 3
        assert exec_plan.contexts[0].node_id == "src"
        assert exec_plan.contexts[1].node_id == "sql"
        assert exec_plan.contexts[2].node_id == "tgt"

    def test_edges_preserved(self):
        optimizer = Optimizer(rules=[])
        ctx = OptimizationContext()
        exec_plan = optimizer.optimize(_make_plan(), ctx)
        assert len(exec_plan.edges) == 2
