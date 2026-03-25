import pyarrow as pa
import pytest
from vonnegut.pipeline.engine.executor.code_executor import (
    CodeExecutor,
    check_code_safety,
)
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
    async def test_from_import_blocked(self):
        code = "from os import path\ndef transform(df):\n    return df\n"
        executor = CodeExecutor()
        with pytest.raises(ValueError, match="Import"):
            await executor.execute(_make_context(code), {"default": _make_input_table()})

    @pytest.mark.asyncio
    async def test_polars_available(self):
        code = "def transform(df):\n    return df.select(pl.col('id'))\n"
        executor = CodeExecutor()
        result = await executor.execute(_make_context(code), {"default": _make_input_table()})
        assert result.column_names == ["id"]

    @pytest.mark.asyncio
    async def test_allowed_modules_available(self):
        code = (
            "def transform(df):\n"
            "    import math  # this triggers AST check\n"
            "    return df\n"
        )
        executor = CodeExecutor()
        # import inside function body is still blocked at AST level
        with pytest.raises(ValueError, match="Import"):
            await executor.execute(_make_context(code), {"default": _make_input_table()})

    @pytest.mark.asyncio
    async def test_math_module_in_globals(self):
        code = "def transform(df):\n    x = math.sqrt(4)\n    return df\n"
        executor = CodeExecutor()
        result = await executor.execute(_make_context(code), {"default": _make_input_table()})
        assert result.num_rows == 3

    @pytest.mark.asyncio
    async def test_timeout_kills_long_running_code(self):
        code = (
            "def transform(df):\n"
            "    while True:\n"
            "        pass\n"
        )
        executor = CodeExecutor(timeout_seconds=0.5)
        with pytest.raises(ValueError, match="timed out"):
            await executor.execute(_make_context(code), {"default": _make_input_table()})


class TestCheckCodeSafety:
    def test_clean_code_passes(self):
        code = "def transform(df):\n    return df.select(pl.col('id'))\n"
        assert check_code_safety(code) == []

    def test_import_detected(self):
        violations = check_code_safety("import os\ndef transform(df): return df")
        assert any("Import" in v for v in violations)

    def test_from_import_detected(self):
        violations = check_code_safety("from pathlib import Path\ndef transform(df): return df")
        assert any("Import" in v for v in violations)

    def test_dunder_globals_blocked(self):
        violations = check_code_safety("def transform(df):\n    x = df.__globals__\n    return df")
        assert any("__globals__" in v for v in violations)

    def test_dunder_subclasses_blocked(self):
        violations = check_code_safety("def transform(df):\n    x = ''.__class__.__subclasses__\n    return df")
        assert any("__subclasses__" in v for v in violations)

    def test_dunder_import_blocked(self):
        violations = check_code_safety("def transform(df):\n    x = __import__('os')\n    return df")
        assert any("__import__" in v for v in violations)

    def test_dunder_code_blocked(self):
        violations = check_code_safety("def transform(df):\n    x = transform.__code__\n    return df")
        assert any("__code__" in v for v in violations)

    def test_dunder_builtins_blocked(self):
        violations = check_code_safety("def transform(df):\n    x = __builtins__\n    return df")
        assert any("__builtins__" in v for v in violations)

    def test_eval_blocked(self):
        violations = check_code_safety("def transform(df):\n    eval('1+1')\n    return df")
        assert any("eval" in v for v in violations)

    def test_exec_blocked(self):
        violations = check_code_safety("def transform(df):\n    exec('x=1')\n    return df")
        assert any("exec" in v for v in violations)

    def test_open_blocked(self):
        violations = check_code_safety("def transform(df):\n    f = open('/etc/passwd')\n    return df")
        assert any("open" in v for v in violations)

    def test_compile_blocked(self):
        violations = check_code_safety("def transform(df):\n    compile('x', '', 'exec')\n    return df")
        assert any("compile" in v for v in violations)

    def test_getattr_blocked(self):
        violations = check_code_safety("def transform(df):\n    getattr(df, 'x')\n    return df")
        assert any("getattr" in v for v in violations)

    def test_string_dunder_access_blocked(self):
        """Block dict-style access like x['__globals__']."""
        violations = check_code_safety("def transform(df):\n    x['__globals__']\n    return df")
        assert any("__globals__" in v for v in violations)

    def test_dunder_class_blocked(self):
        violations = check_code_safety("def transform(df):\n    x = ''.__class__\n    return df")
        assert any("__class__" in v for v in violations)

    def test_breakpoint_blocked(self):
        violations = check_code_safety("def transform(df):\n    breakpoint()\n    return df")
        assert any("breakpoint" in v for v in violations)

    def test_syntax_error_returns_empty(self):
        """Syntax errors are caught at compile time, not here."""
        violations = check_code_safety("def transform(df:\n")
        assert violations == []

    def test_multiple_violations_reported(self):
        code = "import os\ndef transform(df):\n    eval('1')\n    return df"
        violations = check_code_safety(code)
        assert len(violations) >= 2
