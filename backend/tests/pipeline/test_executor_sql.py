import pyarrow as pa
import pytest
from vonnegut.pipeline.engine.executor.sql_executor import SqlExecutor
from vonnegut.pipeline.dag.node import NodeType, SqlNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext


def _make_context(expression: str) -> ExecutionContext:
    return ExecutionContext(
        node_id="sql1",
        node_type=NodeType.SQL,
        config=SqlNodeConfig(expression=expression),
    )


def _make_input_table() -> pa.Table:
    return pa.table({"id": [1, 2, 3], "name": ["alice", "bob", "charlie"], "age": [30, 25, 35]})


class TestSqlExecutor:
    @pytest.mark.asyncio
    async def test_select_all(self):
        executor = SqlExecutor()
        result = await executor.execute(
            _make_context("SELECT * FROM {prev}"),
            {"default": _make_input_table()},
        )
        assert isinstance(result, pa.Table)
        assert result.num_rows == 3

    @pytest.mark.asyncio
    async def test_filter(self):
        executor = SqlExecutor()
        result = await executor.execute(
            _make_context("SELECT * FROM {prev} WHERE age > 28"),
            {"default": _make_input_table()},
        )
        assert result.num_rows == 2

    @pytest.mark.asyncio
    async def test_add_column(self):
        executor = SqlExecutor()
        result = await executor.execute(
            _make_context("SELECT *, age * 2 AS double_age FROM {prev}"),
            {"default": _make_input_table()},
        )
        assert "double_age" in result.column_names

    @pytest.mark.asyncio
    async def test_invalid_sql_raises(self):
        executor = SqlExecutor()
        with pytest.raises(Exception):
            await executor.execute(
                _make_context("NOT VALID SQL"),
                {"default": _make_input_table()},
            )
