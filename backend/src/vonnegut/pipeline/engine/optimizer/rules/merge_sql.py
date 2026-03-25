"""Merge consecutive SQL nodes into a single CTE chain.

When two SQL nodes are adjacent (sql_a → sql_b), they can be merged into
a single SQL node using CTEs:

    WITH _step_0 AS (sql_a_expression),
         _step_1 AS (SELECT ... FROM _step_0)
    SELECT * FROM _step_1

This reduces the number of DuckDB round-trips during execution.
"""
from __future__ import annotations

import sqlglot

from vonnegut.pipeline.dag.node import NodeType, SqlNodeConfig
from vonnegut.pipeline.dag.plan import LogicalPlan, PlanNode, PlanEdge
from vonnegut.pipeline.engine.optimizer.rules.base import OptimizationRule, OptimizationContext


def _get_upstream(node_id: str, edges: list[PlanEdge]) -> str | None:
    """Get the single upstream node id, or None if not exactly one."""
    upstreams = [e.from_node_id for e in edges if e.to_node_id == node_id]
    return upstreams[0] if len(upstreams) == 1 else None


def _get_downstream(node_id: str, edges: list[PlanEdge]) -> list[str]:
    """Get downstream node ids."""
    return [e.to_node_id for e in edges if e.from_node_id == node_id]


def _find_sql_chains(plan: LogicalPlan) -> list[list[str]]:
    """Find maximal chains of consecutive SQL nodes.

    A chain is a sequence [a, b, c] where each pair is directly connected
    and all nodes are SQL nodes. We only chain when a node has exactly one
    downstream consumer (no fan-out).
    """
    sql_nodes = {
        nid for nid, pn in plan.nodes.items() if pn.type == NodeType.SQL
    }

    # Track which nodes have already been claimed by a chain
    used: set[str] = set()
    chains: list[list[str]] = []

    # Find chain starts: SQL nodes whose upstream is NOT a SQL node (or has no upstream)
    for nid in sql_nodes:
        if nid in used:
            continue
        upstream = _get_upstream(nid, plan.edges)
        if upstream in sql_nodes and upstream not in used:
            continue  # This node will be chained from its upstream

        # Start a chain from this node
        chain = [nid]
        current = nid
        while True:
            downstreams = _get_downstream(current, plan.edges)
            if len(downstreams) != 1:
                break
            next_id = downstreams[0]
            if next_id not in sql_nodes or next_id in used:
                break
            # Check that next_id has only one upstream (this node)
            if _get_upstream(next_id, plan.edges) != current:
                break
            chain.append(next_id)
            current = next_id

        if len(chain) >= 2:
            used.update(chain)
            chains.append(chain)

    return chains


def _merge_chain(chain: list[str], plan: LogicalPlan) -> tuple[str, PlanNode]:
    """Merge a chain of SQL nodes into a single CTE-based node.

    Returns (merged_node_id, merged_plan_node).
    """
    merged_id = chain[0]  # Keep the first node's id

    cte_parts = []
    for i, nid in enumerate(chain):
        node = plan.nodes[nid]
        assert isinstance(node.config, SqlNodeConfig)
        expr = node.config.expression.strip()

        if i == 0:
            # First node: replace {prev} with the upstream reference (kept as {prev})
            cte_parts.append(f"_step_{i} AS ({expr})")
        else:
            # Subsequent nodes: replace {prev} with the previous CTE name
            resolved = expr.replace("{prev}", f"_step_{i - 1}")
            cte_parts.append(f"_step_{i} AS ({resolved})")

    last_step = f"_step_{len(chain) - 1}"
    merged_sql = f"WITH {', '.join(cte_parts)} SELECT * FROM {last_step}"

    return merged_id, PlanNode(
        id=merged_id,
        type=NodeType.SQL,
        config=SqlNodeConfig(expression=merged_sql),
    )


class MergeSqlNodesRule(OptimizationRule):
    """Merge consecutive SQL nodes into CTE chains."""

    def apply(self, plan: LogicalPlan, context: OptimizationContext) -> LogicalPlan:
        chains = _find_sql_chains(plan)
        if not chains:
            return plan

        new_nodes = dict(plan.nodes)
        removed: set[str] = set()
        # Map from removed node id → replacement node id
        rewire: dict[str, str] = {}

        for chain in chains:
            merged_id, merged_node = _merge_chain(chain, plan)
            new_nodes[merged_id] = merged_node

            # Remove all nodes in the chain except the merged one
            for nid in chain[1:]:
                del new_nodes[nid]
                removed.add(nid)
                rewire[nid] = merged_id

        # Rebuild edges, skipping internal chain edges and rewiring external ones
        new_edges: list[PlanEdge] = []
        for edge in plan.edges:
            if edge.from_node_id in removed and edge.to_node_id in removed:
                continue  # Internal chain edge
            if edge.from_node_id in removed:
                continue  # This was an internal chain edge to a non-chain node
            # Rewire edges pointing to removed nodes
            to_id = edge.to_node_id
            if to_id in removed:
                # This shouldn't happen if chains are maximal, but handle it
                to_id = rewire[to_id]

            new_from = rewire.get(edge.from_node_id, edge.from_node_id)
            new_edges.append(PlanEdge(
                from_node_id=new_from,
                to_node_id=to_id,
                input_name=edge.input_name,
            ))

        return LogicalPlan(nodes=new_nodes, edges=new_edges)
