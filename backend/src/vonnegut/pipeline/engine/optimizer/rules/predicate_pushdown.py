"""Push WHERE predicates closer to the data source.

When a SQL node has a WHERE clause and its upstream is also a SQL node,
the predicate can be pushed into the upstream node to filter data earlier.
This reduces the amount of data flowing through the pipeline.

Only pushes when:
- The downstream SQL node has a WHERE clause with column references
- The upstream SQL node is in the SAFE tier
- All predicate columns exist in the upstream node's output (validated via schema or SQL analysis)
- The predicate doesn't reference columns created by the downstream node itself
"""
from __future__ import annotations

from vonnegut.pipeline.dag.node import NodeType, SqlNodeConfig
from vonnegut.pipeline.dag.plan import LogicalPlan, PlanNode, PlanEdge
from vonnegut.pipeline.engine.optimizer.rules.base import OptimizationRule, OptimizationContext
from vonnegut.pipeline.sql_utils import (
    SafetyTier,
    classify_safety,
    extract_where_predicate,
    get_predicate_columns,
    get_produced_columns,
    add_where_to_sql,
    remove_where,
)


def _get_upstream_edge(node_id: str, edges: list[PlanEdge]) -> PlanEdge | None:
    upstreams = [e for e in edges if e.to_node_id == node_id]
    return upstreams[0] if len(upstreams) == 1 else None


class PredicatePushdownRule(OptimizationRule):
    """Push WHERE predicates from SQL nodes into their upstream SQL nodes."""

    def apply(self, plan: LogicalPlan, context: OptimizationContext) -> LogicalPlan:
        changed = False
        new_nodes = dict(plan.nodes)

        for nid, pn in plan.nodes.items():
            if pn.type != NodeType.SQL:
                continue
            assert isinstance(pn.config, SqlNodeConfig)

            # Only push from SAFE nodes (no aggregation/window/etc)
            if classify_safety(pn.config.expression) != SafetyTier.SAFE:
                continue

            # Extract the WHERE predicate
            predicate = extract_where_predicate(pn.config.expression)
            if predicate is None:
                continue

            predicate_cols = get_predicate_columns(predicate)
            if not predicate_cols:
                continue

            # Find the single upstream node
            upstream_edge = _get_upstream_edge(nid, plan.edges)
            if upstream_edge is None:
                continue

            upstream_node = new_nodes.get(upstream_edge.from_node_id)
            if upstream_node is None or upstream_node.type != NodeType.SQL:
                continue
            assert isinstance(upstream_node.config, SqlNodeConfig)

            # Upstream must be SAFE
            if classify_safety(upstream_node.config.expression) != SafetyTier.SAFE:
                continue

            # Check that all predicate columns exist in upstream's output
            upstream_id = upstream_edge.from_node_id
            upstream_schema = context.node_schemas.get(upstream_id)
            if upstream_schema is not None:
                upstream_cols = set(upstream_schema.column_names)
            else:
                upstream_cols = get_produced_columns(upstream_node.config.expression)

            if "*" in upstream_cols:
                # SELECT * — columns unknown, safe to push since they pass through
                pass
            elif not predicate_cols.issubset(upstream_cols):
                continue  # Predicate references columns not in upstream output

            # Push the predicate into upstream
            new_upstream_sql = add_where_to_sql(
                upstream_node.config.expression, predicate,
            )
            new_nodes[upstream_id] = PlanNode(
                id=upstream_id,
                type=NodeType.SQL,
                config=SqlNodeConfig(expression=new_upstream_sql),
            )

            # Remove WHERE from the downstream node
            new_downstream_sql = remove_where(pn.config.expression)
            new_nodes[nid] = PlanNode(
                id=nid,
                type=NodeType.SQL,
                config=SqlNodeConfig(expression=new_downstream_sql),
            )
            changed = True

        if not changed:
            return plan

        return LogicalPlan(nodes=new_nodes, edges=list(plan.edges))
