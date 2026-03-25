"""Remove no-op SQL nodes that just pass data through unchanged.

A no-op SQL node is one whose expression is effectively `SELECT * FROM {prev}`.
Uses sqlglot AST analysis instead of regex.
"""
from __future__ import annotations

from vonnegut.pipeline.dag.node import NodeType, SqlNodeConfig
from vonnegut.pipeline.dag.plan import LogicalPlan, PlanEdge
from vonnegut.pipeline.engine.optimizer.rules.base import OptimizationRule, OptimizationContext
from vonnegut.pipeline.sql_utils import is_select_star_from_single_table


def _is_noop_sql(config) -> bool:
    if not isinstance(config, SqlNodeConfig):
        return False
    return is_select_star_from_single_table(config.expression)


class NoOpRemovalRule(OptimizationRule):
    """Remove SQL nodes that are just `SELECT * FROM {prev}`."""

    def apply(self, plan: LogicalPlan, context: OptimizationContext) -> LogicalPlan:
        noop_ids = {
            nid for nid, pn in plan.nodes.items()
            if pn.type == NodeType.SQL and _is_noop_sql(pn.config)
        }

        if not noop_ids:
            return plan

        new_nodes = {nid: pn for nid, pn in plan.nodes.items() if nid not in noop_ids}
        new_edges: list[PlanEdge] = []

        for noop_id in noop_ids:
            upstream = [e for e in plan.edges if e.to_node_id == noop_id]
            downstream = [e for e in plan.edges if e.from_node_id == noop_id]

            if len(upstream) == 1 and downstream:
                for d_edge in downstream:
                    new_edges.append(PlanEdge(
                        from_node_id=upstream[0].from_node_id,
                        to_node_id=d_edge.to_node_id,
                        input_name=d_edge.input_name,
                    ))

        for edge in plan.edges:
            if edge.from_node_id not in noop_ids and edge.to_node_id not in noop_ids:
                new_edges.append(edge)

        return LogicalPlan(nodes=new_nodes, edges=new_edges)
