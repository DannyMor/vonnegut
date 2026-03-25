from __future__ import annotations
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import TypeVar

from vonnegut.pipeline.dag.node import Node, NodeType
from vonnegut.pipeline.dag.edge import Edge

T = TypeVar("T")


class CycleError(Exception):
    pass


def topological_sort(nodes: dict[str, Node], edges: list[Edge]) -> list[str]:
    in_degree: dict[str, int] = {nid: 0 for nid in nodes}
    adjacency: dict[str, list[str]] = defaultdict(list)

    for edge in edges:
        adjacency[edge.from_node_id].append(edge.to_node_id)
        in_degree[edge.to_node_id] = in_degree.get(edge.to_node_id, 0) + 1

    queue = deque(nid for nid, deg in in_degree.items() if deg == 0)
    result: list[str] = []

    while queue:
        nid = queue.popleft()
        result.append(nid)
        for neighbor in adjacency[nid]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(result) != len(nodes):
        raise CycleError("Pipeline contains a cycle")

    return result


def collect_inputs(
    node_id: str,
    edges: list[Edge],
    outputs: dict[str, T],
) -> dict[str, T]:
    inputs: dict[str, T] = {}
    for edge in edges:
        if edge.to_node_id == node_id and edge.from_node_id in outputs:
            key = edge.input_name or "default"
            inputs[key] = outputs[edge.from_node_id]
    return inputs


def get_incoming_edges(node_id: str, edges: list[Edge]) -> list[Edge]:
    return [e for e in edges if e.to_node_id == node_id]


@dataclass
class PipelineGraph:
    nodes: dict[str, Node]
    edges: list[Edge] = field(default_factory=list)

    def validate(self) -> None:
        topological_sort(self.nodes, self.edges)

        if len(self.nodes) > 1:
            connected = set()
            for edge in self.edges:
                connected.add(edge.from_node_id)
                connected.add(edge.to_node_id)
            orphans = set(self.nodes.keys()) - connected
            if orphans:
                raise ValueError(f"Orphaned nodes not connected by any edge: {orphans}")

    def execution_order(self) -> list[str]:
        return topological_sort(self.nodes, self.edges)
