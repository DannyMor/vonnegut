from __future__ import annotations
import pyarrow as pa

from vonnegut.pipeline.dag.node import SourceNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.engine.executor.base import NodeExecutor
from vonnegut.adapters.base import DatabaseAdapter


class SourceExecutor(NodeExecutor):
    def __init__(self, adapter_factory: object) -> None:
        self._adapter_factory = adapter_factory

    async def execute(
        self, context: ExecutionContext, inputs: dict[str, pa.Table]
    ) -> pa.Table:
        config = context.config
        assert isinstance(config, SourceNodeConfig)

        adapter: DatabaseAdapter = context.connection_info["adapter"]
        query = config.query or f"SELECT * FROM {config.table}"

        limit = context.connection_info.get("limit")
        if limit:
            query = f"SELECT * FROM ({query}) AS _src LIMIT {limit}"

        rows = await adapter.execute(query)
        if not rows:
            return pa.table({})
        return pa.Table.from_pylist(rows)
