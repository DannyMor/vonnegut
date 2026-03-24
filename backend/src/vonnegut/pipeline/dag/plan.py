from __future__ import annotations
from dataclasses import dataclass, field

from vonnegut.pipeline.dag.node import NodeType, NodeConfig
from vonnegut.pipeline.schema.types import Schema


@dataclass(frozen=True)
class PlanNode:
    id: str
    type: NodeType
    config: NodeConfig


@dataclass(frozen=True)
class PlanEdge:
    from_node_id: str
    to_node_id: str
    input_name: str | None = None


@dataclass
class LogicalPlan:
    nodes: dict[str, PlanNode]
    edges: list[PlanEdge] = field(default_factory=list)


@dataclass(frozen=True)
class ExecutionContext:
    node_id: str
    node_type: NodeType
    config: NodeConfig
    input_schemas: dict[str, Schema] = field(default_factory=dict)
    connection_info: dict | None = None


@dataclass
class ExecutionPlan:
    contexts: list[ExecutionContext] = field(default_factory=list)
    edges: list[PlanEdge] = field(default_factory=list)
