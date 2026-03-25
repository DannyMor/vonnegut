from __future__ import annotations

from vonnegut.pipeline.dag.plan import (
    LogicalPlan,
    ExecutionPlan,
    ExecutionContext,
)
from vonnegut.pipeline.dag.graph import topological_sort
from vonnegut.pipeline.dag.node import Node
from vonnegut.pipeline.dag.edge import Edge
from vonnegut.pipeline.engine.optimizer.rules.base import (
    OptimizationRule,
    OptimizationContext,
)


class Optimizer:
    def __init__(self, rules: list[OptimizationRule] | None = None) -> None:
        self.rules = rules or []

    def optimize(
        self, plan: LogicalPlan, context: OptimizationContext,
    ) -> ExecutionPlan:
        current = plan
        for rule in self.rules:
            current = rule.apply(current, context)

        nodes_for_sort = {
            nid: Node(id=nid, type=pn.type, config=pn.config)
            for nid, pn in current.nodes.items()
        }
        edges_for_sort = [
            Edge(
                id=f"e_{i}",
                from_node_id=pe.from_node_id,
                to_node_id=pe.to_node_id,
                input_name=pe.input_name,
            )
            for i, pe in enumerate(current.edges)
        ]
        order = topological_sort(nodes_for_sort, edges_for_sort)

        contexts = []
        for node_id in order:
            pn = current.nodes[node_id]
            # Build input_schemas from upstream node schemas
            input_schemas = {}
            for edge in current.edges:
                if edge.to_node_id == node_id:
                    schema = context.node_schemas.get(edge.from_node_id)
                    if schema is not None:
                        key = edge.input_name or "default"
                        input_schemas[key] = schema
            contexts.append(
                ExecutionContext(
                    node_id=node_id,
                    node_type=pn.type,
                    config=pn.config,
                    input_schemas=input_schemas,
                ),
            )

        return ExecutionPlan(contexts=contexts, edges=list(current.edges))
