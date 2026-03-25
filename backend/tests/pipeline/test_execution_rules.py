import pyarrow as pa
import pytest
from vonnegut.pipeline.results import CheckStatus
from vonnegut.pipeline.dag.node import Node, NodeType, CodeNodeConfig, SqlNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.engine.validator.rules.execution_rules import (
    SqlExecutionRule,
    CodeExecutionRule,
    SchemaStabilityRule,
)


def _sql_node(expr: str = "SELECT 1") -> Node:
    return Node(id="s1", type=NodeType.SQL, config=SqlNodeConfig(expression=expr))


def _sql_ctx(expr: str = "SELECT 1") -> ExecutionContext:
    return ExecutionContext(
        node_id="s1", node_type=NodeType.SQL,
        config=SqlNodeConfig(expression=expr),
    )


def _code_node(code: str) -> Node:
    return Node(id="c1", type=NodeType.CODE, config=CodeNodeConfig(function_code=code))


def _code_ctx(code: str) -> ExecutionContext:
    return ExecutionContext(
        node_id="c1", node_type=NodeType.CODE,
        config=CodeNodeConfig(function_code=code),
    )


class TestSqlExecutionRule:
    def test_passes_with_valid_output(self):
        rule = SqlExecutionRule()
        output = pa.table({"id": [1, 2], "name": ["a", "b"]})
        result = rule.check(_sql_node(), _sql_ctx(), {}, output, {}, None)
        assert result.status == CheckStatus.PASSED
        assert "2 rows" in result.message

    def test_warning_on_empty_result(self):
        rule = SqlExecutionRule()
        output = pa.table({"id": pa.array([], type=pa.int64())})
        result = rule.check(_sql_node(), _sql_ctx(), {}, output, {}, None)
        assert result.status == CheckStatus.WARNING
        assert "empty" in result.message.lower()

    def test_fails_on_no_columns(self):
        rule = SqlExecutionRule()
        output = pa.table({})
        result = rule.check(_sql_node(), _sql_ctx(), {}, output, {}, None)
        assert result.status == CheckStatus.FAILED
        assert "no columns" in result.message.lower()

    def test_passes_pre_execution(self):
        rule = SqlExecutionRule()
        result = rule.check(_sql_node(), _sql_ctx(), {}, None, {}, None)
        assert result.status == CheckStatus.PASSED


class TestCodeExecutionRule:
    def test_passes_with_valid_output(self):
        rule = CodeExecutionRule()
        output = pa.table({"x": [1, 2, 3]})
        result = rule.check(
            _code_node(""), _code_ctx(""), {}, output, {}, None,
        )
        assert result.status == CheckStatus.PASSED

    def test_warning_on_empty_result(self):
        rule = CodeExecutionRule()
        output = pa.table({"x": pa.array([], type=pa.int64())})
        result = rule.check(
            _code_node(""), _code_ctx(""), {}, output, {}, None,
        )
        assert result.status == CheckStatus.WARNING

    def test_fails_on_no_columns(self):
        rule = CodeExecutionRule()
        output = pa.table({})
        result = rule.check(
            _code_node(""), _code_ctx(""), {}, output, {}, None,
        )
        assert result.status == CheckStatus.FAILED


class TestSchemaStabilityRule:
    def test_stable_schema_passes(self):
        rule = SchemaStabilityRule()
        code = "def transform(df):\n    return df.with_columns(df['x'] * 2)"
        input_table = pa.table({"x": [1, 2, 3, 4]})
        output_table = pa.table({"x": [2, 4, 6, 8]})

        from vonnegut.pipeline.schema.types import Schema, Column, DataType
        output_schema = Schema(columns=[Column("x", DataType.INT64)])

        result = rule.check(
            _code_node(code), _code_ctx(code),
            {"default": input_table}, output_table, {}, output_schema,
        )
        assert result.status == CheckStatus.PASSED

    def test_unstable_schema_fails(self):
        rule = SchemaStabilityRule()
        # This code produces different columns depending on whether 'special' value exists
        code = (
            "def transform(df):\n"
            "    if 'special' in df['name'].to_list():\n"
            "        return df.with_columns(extra=df['x'] * 2)\n"
            "    return df\n"
        )
        # Full dataset has 'special' in second half → output has 'extra' column
        # But first-half subset (rows 0-1) won't have 'special' → no 'extra' column
        input_table = pa.table({"x": [1, 2, 3, 4], "name": ["a", "b", "c", "special"]})
        output_table = pa.table({"x": [1, 2, 3, 4], "name": ["a", "b", "c", "special"], "extra": [2, 4, 6, 8]})

        from vonnegut.pipeline.schema.types import Schema, Column, DataType
        output_schema = Schema(columns=[
            Column("x", DataType.INT64),
            Column("name", DataType.UTF8),
            Column("extra", DataType.INT64),
        ])

        result = rule.check(
            _code_node(code), _code_ctx(code),
            {"default": input_table}, output_table, {}, output_schema,
        )
        assert result.status == CheckStatus.FAILED
        assert "unstable" in result.message.lower()

    def test_skips_non_code_node(self):
        rule = SchemaStabilityRule()
        output = pa.table({"x": [1]})

        from vonnegut.pipeline.schema.types import Schema, Column, DataType
        schema = Schema(columns=[Column("x", DataType.INT64)])

        result = rule.check(
            _sql_node(), _sql_ctx(), {}, output, {}, schema,
        )
        assert result.status == CheckStatus.PASSED

    def test_skips_insufficient_input(self):
        rule = SchemaStabilityRule()
        code = "def transform(df):\n    return df"
        input_table = pa.table({"x": [1]})  # Only 1 row
        output_table = pa.table({"x": [1]})

        from vonnegut.pipeline.schema.types import Schema, Column, DataType
        schema = Schema(columns=[Column("x", DataType.INT64)])

        result = rule.check(
            _code_node(code), _code_ctx(code),
            {"default": input_table}, output_table, {}, schema,
        )
        assert result.status == CheckStatus.PASSED
        assert "not enough" in result.message.lower()
