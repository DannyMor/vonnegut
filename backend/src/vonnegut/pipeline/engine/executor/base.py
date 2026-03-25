from __future__ import annotations
from abc import ABC, abstractmethod
import pyarrow as pa

from vonnegut.pipeline.dag.node import NodeType
from vonnegut.pipeline.dag.plan import ExecutionContext


class NodeExecutor(ABC):
    @abstractmethod
    async def execute(
        self, context: ExecutionContext, inputs: dict[str, pa.Table]
    ) -> pa.Table: ...


class ExecutorRegistry:
    def __init__(self) -> None:
        self._executors: dict[NodeType, NodeExecutor] = {}

    def register(self, node_type: NodeType, executor: NodeExecutor) -> None:
        self._executors[node_type] = executor

    def get(self, node_type: NodeType) -> NodeExecutor:
        executor = self._executors.get(node_type)
        if executor is None:
            raise KeyError(f"No executor registered for node type: {node_type}")
        return executor
