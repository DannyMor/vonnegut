"""Merge consecutive SQL nodes into a single CTE chain.

When two SQL nodes are adjacent (sql_a → sql_b), they can be merged into
a single SQL node using CTEs, reducing the number of DuckDB round-trips.

Safety guards: nodes containing aggregations, window functions, DISTINCT,
subqueries, or non-deterministic functions are NOT merged, as inlining
could change semantics.
"""
from __future__ import annotations

from vonnegut.pipeline.dag.node import NodeType, SqlNodeConfig
from vonnegut.pipeline.dag.plan import LogicalPlan, PlanNode, PlanEdge
from vonnegut.pipeline.engine.optimizer.rules.base import OptimizationRule, OptimizationContext
from vonnegut.pipeline.sql_utils import is_safe_to_merge, build_cte_chain


def _get_upstream(node_id: str, edges: list[PlanEdge]) -> str | None:
    upstreams = [e.from_node_id for e in edges if e.to_node_id == node_id]
    return upstreams[0] if len(upstreams) == 1 else None


def _get_downstream(node_id: str, edges: list[PlanEdge]) -> list[str]:
    return [e.to_node_id for e in edges if e.from_node_id == node_id]


def _find_sql_chains(plan: LogicalPlan) -> list[list[str]]:
    """Find maximal chains of consecutive SQL nodes that are safe to merge.

    A chain is a sequence [a, b, c] where each pair is directly connected,
    all nodes are SQL nodes, each has exactly one downstream consumer (no fan-out),
    and all nodes pass the safety check (no aggregations, window functions, etc.).
    """
    sql_nodes = {
        nid for nid, pn in plan.nodes.items()
        if pn.type == NodeType.SQL
    }

    # Filter to only nodes that are safe to merge
    safe_nodes = set()
    for nid in sql_nodes:
        pn = plan.nodes[nid]
        assert isinstance(pn.config, SqlNodeConfig)
        if is_safe_to_merge(pn.config.expression):
            safe_nodes.add(nid)

    used: set[str] = set()
    chains: list[list[str]] = []

    for nid in safe_nodes:
        if nid in used:
            continue
        upstream = _get_upstream(nid, plan.edges)
        if upstream in safe_nodes and upstream not in used:
            continue

        chain = [nid]
        current = nid
        while True:
            downstreams = _get_downstream(current, plan.edges)
            if len(downstreams) != 1:
                break
            next_id = downstreams[0]
            if next_id not in safe_nodes or next_id in used:
                break
            if _get_upstream(next_id, plan.edges) != current:
                break
            chain.append(next_id)
            current = next_id

        if len(chain) >= 2:
            used.update(chain)
            chains.append(chain)

    return chains


def _merge_chain(chain: list[str], plan: LogicalPlan) -> tuple[str, PlanNode]:
    """Merge a chain of SQL nodes into a single CTE-based node using sqlglot."""
    merged_id = chain[0]

    steps = []
    for i, nid in enumerate(chain):
        node = plan.nodes[nid]
        assert isinstance(node.config, SqlNodeConfig)
        steps.append((f"_step_{i}", node.config.expression))

    merged_sql = build_cte_chain(steps)

    return merged_id, PlanNode(
        id=merged_id,
        type=NodeType.SQL,
        config=SqlNodeConfig(expression=merged_sql),
    )


class MergeSqlNodesRule(OptimizationRule):
    """Merge consecutive SQL nodes into CTE chains.

    Only merges nodes that pass safety checks (no aggregations, window functions,
    DISTINCT, subqueries, or non-deterministic functions).
    """

    def apply(self, plan: LogicalPlan, context: OptimizationContext) -> LogicalPlan:
        chains = _find_sql_chains(plan)
        if not chains:
            return plan

        new_nodes = dict(plan.nodes)
        removed: set[str] = set()
        rewire: dict[str, str] = {}

        for chain in chains:
            merged_id, merged_node = _merge_chain(chain, plan)
            new_nodes[merged_id] = merged_node

            for nid in chain[1:]:
                del new_nodes[nid]
                removed.add(nid)
                rewire[nid] = merged_id

        new_edges: list[PlanEdge] = []
        for edge in plan.edges:
            if edge.from_node_id in removed and edge.to_node_id in removed:
                continue
            if edge.from_node_id in removed:
                continue
            to_id = edge.to_node_id
            if to_id in removed:
                to_id = rewire[to_id]

            new_from = rewire.get(edge.from_node_id, edge.from_node_id)
            new_edges.append(PlanEdge(
                from_node_id=new_from,
                to_node_id=to_id,
                input_name=edge.input_name,
            ))

        return LogicalPlan(nodes=new_nodes, edges=new_edges)
