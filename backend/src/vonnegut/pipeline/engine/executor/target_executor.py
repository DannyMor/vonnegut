from __future__ import annotations
import pyarrow as pa

from vonnegut.pipeline.dag.node import TargetNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.engine.executor.base import NodeExecutor
from vonnegut.adapters.base import DatabaseAdapter


class TargetExecutor(NodeExecutor):
    async def execute(
        self, context: ExecutionContext, inputs: dict[str, pa.Table]
    ) -> pa.Table:
        config = context.config
        assert isinstance(config, TargetNodeConfig)

        input_table = inputs.get("default")
        if input_table is None:
            return pa.table({})

        allow_writes = context.connection_info.get("allow_writes", False) if context.connection_info else False

        if not allow_writes:
            return input_table

        adapter: DatabaseAdapter = context.connection_info["adapter"]
        rows = input_table.to_pylist()

        if config.truncate:
            await adapter.execute(f"TRUNCATE TABLE {config.table}")

        if rows:
            columns = list(rows[0].keys())
            placeholders = ", ".join(["%s"] * len(columns))
            col_names = ", ".join(columns)
            insert_sql = f"INSERT INTO {config.table} ({col_names}) VALUES ({placeholders})"
            for row in rows:
                await adapter.execute(insert_sql, tuple(row[c] for c in columns))

        return input_table
