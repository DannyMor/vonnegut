from __future__ import annotations
import duckdb
import pyarrow as pa

from vonnegut.pipeline.dag.node import SqlNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.engine.executor.base import NodeExecutor


class SqlExecutor(NodeExecutor):
    async def execute(
        self, context: ExecutionContext, inputs: dict[str, pa.Table]
    ) -> pa.Table:
        config = context.config
        assert isinstance(config, SqlNodeConfig)

        expression = config.expression
        input_table = inputs.get("default")

        conn = duckdb.connect()
        try:
            if input_table is not None:
                conn.register("prev", input_table)
                sql = expression.replace("{prev}", "prev")
            else:
                sql = expression

            result = conn.execute(sql).to_arrow_table()
            return result
        finally:
            conn.close()
