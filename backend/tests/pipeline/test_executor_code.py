import pyarrow as pa
import pytest
from vonnegut.pipeline.engine.executor.code_executor import CodeExecutor
from vonnegut.pipeline.dag.node import NodeType, CodeNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext


def _make_context(code: str) -> ExecutionContext:
    return ExecutionContext(
        node_id="code1",
        node_type=NodeType.CODE,
        config=CodeNodeConfig(function_code=code),
    )


def _make_input_table() -> pa.Table:
    return pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})


class TestCodeExecutor:
    @pytest.mark.asyncio
    async def test_identity_transform(self):
        code = "def transform(df):\n    return df\n"
        executor = CodeExecutor()
        result = await executor.execute(_make_context(code), {"default": _make_input_table()})
        assert isinstance(result, pa.Table)
        assert result.num_rows == 3

    @pytest.mark.asyncio
    async def test_adds_column(self):
        code = "def transform(df):\n    return df.with_columns(pl.col('id') * 2)\n"
        executor = CodeExecutor()
        result = await executor.execute(_make_context(code), {"default": _make_input_table()})
        assert result.column("id").to_pylist() == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_missing_transform_function_raises(self):
        code = "x = 42\n"
        executor = CodeExecutor()
        with pytest.raises(ValueError, match="transform"):
            await executor.execute(_make_context(code), {"default": _make_input_table()})

    @pytest.mark.asyncio
    async def test_import_blocked(self):
        code = "import os\ndef transform(df):\n    return df\n"
        executor = CodeExecutor()
        with pytest.raises(ValueError, match="Import"):
            await executor.execute(_make_context(code), {"default": _make_input_table()})

    @pytest.mark.asyncio
    async def test_polars_available(self):
        code = "def transform(df):\n    return df.select(pl.col('id'))\n"
        executor = CodeExecutor()
        result = await executor.execute(_make_context(code), {"default": _make_input_table()})
        assert result.column_names == ["id"]
