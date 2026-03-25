"""Prune unused columns from SQL node projections.

For each SQL node, determines which columns are actually consumed by downstream
nodes and removes unused projections. This reduces data scanned and transferred.

Uses validated schemas from OptimizationContext when available for precise
column knowledge. Falls back to SQL AST analysis when schemas aren't present.

Only prunes when:
- The SQL node is in the SAFE tier (no aggregation, window, DISTINCT, etc.)
- All downstream consumers have known column requirements
- The projection list is explicit (not SELECT *)
"""
from __future__ import annotations

from vonnegut.pipeline.dag.node import NodeType, SqlNodeConfig
from vonnegut.pipeline.dag.plan import LogicalPlan, PlanNode, PlanEdge
from vonnegut.pipeline.engine.optimizer.rules.base import OptimizationRule, OptimizationContext
from vonnegut.pipeline.sql_utils import (
    SafetyTier,
    classify_safety,
    get_consumed_columns,
    get_produced_columns,
    prune_columns,
)


def _get_downstream_edges(node_id: str, edges: list[PlanEdge]) -> list[PlanEdge]:
    return [e for e in edges if e.from_node_id == node_id]


def _get_produced_from_schema(node_id: str, context: OptimizationContext) -> set[str] | None:
    """Get produced column names from validated schema if available."""
    schema = context.node_schemas.get(node_id)
    if schema is None:
        return None
    return set(schema.column_names)


class ColumnPruningRule(OptimizationRule):
    """Remove unused columns from SQL node projections based on downstream usage."""

    def apply(self, plan: LogicalPlan, context: OptimizationContext) -> LogicalPlan:
        changed = False
        new_nodes = dict(plan.nodes)

        for nid, pn in plan.nodes.items():
            if pn.type != NodeType.SQL:
                continue
            assert isinstance(pn.config, SqlNodeConfig)

            # Only prune SAFE nodes
            if classify_safety(pn.config.expression) != SafetyTier.SAFE:
                continue

            # Determine produced columns: prefer schema, fall back to SQL analysis
            produced = _get_produced_from_schema(nid, context)
            if produced is None:
                produced = get_produced_columns(pn.config.expression)
            if "*" in produced:
                continue  # Can't prune SELECT *

            # Collect columns needed by all downstream consumers
            downstream_edges = _get_downstream_edges(nid, plan.edges)
            if not downstream_edges:
                continue

            needed: set[str] = set()
            can_prune = True

            for edge in downstream_edges:
                downstream_node = plan.nodes.get(edge.to_node_id)
                if downstream_node is None:
                    can_prune = False
                    break

                if downstream_node.type == NodeType.SQL:
                    assert isinstance(downstream_node.config, SqlNodeConfig)
                    consumed = get_consumed_columns(downstream_node.config.expression)
                    needed.update(consumed)
                elif downstream_node.type == NodeType.TARGET:
                    # Target needs all columns — check schema for exact set
                    target_schema = context.node_schemas.get(edge.to_node_id)
                    if target_schema is not None:
                        needed.update(target_schema.column_names)
                    else:
                        # Without schema, assume target needs everything
                        can_prune = False
                        break
                else:
                    # Non-SQL, non-target downstream — can't determine column usage
                    can_prune = False
                    break

            if not can_prune or not needed:
                continue

            # Only prune if we can actually remove columns
            prunable = produced - needed
            if not prunable:
                continue

            pruned_sql = prune_columns(pn.config.expression, needed)
            if pruned_sql != pn.config.expression:
                new_nodes[nid] = PlanNode(
                    id=nid,
                    type=NodeType.SQL,
                    config=SqlNodeConfig(expression=pruned_sql),
                )
                changed = True

        if not changed:
            return plan

        return LogicalPlan(nodes=new_nodes, edges=list(plan.edges))
