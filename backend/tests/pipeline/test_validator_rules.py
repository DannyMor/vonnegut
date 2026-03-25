import pytest
from vonnegut.pipeline.results import CheckStatus
from vonnegut.pipeline.dag.node import Node, NodeType, CodeNodeConfig, SqlNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.schema.types import Schema, Column, DataType
from vonnegut.pipeline.engine.validator.rules.code_rules import (
    SyntaxCheckRule,
    ColumnNameRule,
)
from vonnegut.pipeline.engine.validator.rules.sql_rules import SqlParseRule


def _node(code: str) -> Node:
    return Node(id="c1", type=NodeType.CODE, config=CodeNodeConfig(function_code=code))


def _ctx(code: str) -> ExecutionContext:
    return ExecutionContext(
        node_id="c1",
        node_type=NodeType.CODE,
        config=CodeNodeConfig(function_code=code),
    )


class TestSyntaxCheckRule:
    def test_valid_code_passes(self):
        rule = SyntaxCheckRule()
        result = rule.check(
            _node("def transform(df):\n    return df\n"),
            _ctx("def transform(df):\n    return df\n"),
            {},
            None,
            {},
            None,
        )
        assert result.status == CheckStatus.PASSED

    def test_syntax_error_fails(self):
        rule = SyntaxCheckRule()
        result = rule.check(
            _node("def transform(df)\n    return df\n"),
            _ctx("def transform(df)\n    return df\n"),
            {},
            None,
            {},
            None,
        )
        assert result.status == CheckStatus.FAILED

    def test_no_transform_function_fails(self):
        rule = SyntaxCheckRule()
        result = rule.check(
            _node("x = 42\n"),
            _ctx("x = 42\n"),
            {},
            None,
            {},
            None,
        )
        assert result.status == CheckStatus.FAILED


class TestColumnNameRule:
    def test_unique_columns_pass(self):
        rule = ColumnNameRule()
        schema = Schema(
            columns=[Column("a", DataType.INT64), Column("b", DataType.UTF8)],
        )
        result = rule.check(_node(""), _ctx(""), {}, None, {}, schema)
        assert result.status == CheckStatus.PASSED

    def test_duplicate_columns_fail(self):
        rule = ColumnNameRule()
        schema = Schema(
            columns=[Column("a", DataType.INT64), Column("a", DataType.UTF8)],
        )
        result = rule.check(_node(""), _ctx(""), {}, None, {}, schema)
        assert result.status == CheckStatus.FAILED

    def test_empty_column_name_fails(self):
        rule = ColumnNameRule()
        schema = Schema(columns=[Column("", DataType.INT64)])
        result = rule.check(_node(""), _ctx(""), {}, None, {}, schema)
        assert result.status == CheckStatus.FAILED


def _sql_node(expr: str) -> Node:
    return Node(id="s1", type=NodeType.SQL, config=SqlNodeConfig(expression=expr))


def _sql_ctx(expr: str) -> ExecutionContext:
    return ExecutionContext(
        node_id="s1",
        node_type=NodeType.SQL,
        config=SqlNodeConfig(expression=expr),
    )


class TestSqlParseRule:
    def test_valid_select_passes(self):
        rule = SqlParseRule()
        result = rule.check(
            _sql_node("SELECT * FROM {prev}"),
            _sql_ctx("SELECT * FROM {prev}"),
            {},
            None,
            {},
            None,
        )
        assert result.status == CheckStatus.PASSED

    def test_invalid_sql_fails(self):
        rule = SqlParseRule()
        result = rule.check(
            _sql_node("NOT VALID SQL AT ALL"),
            _sql_ctx("NOT VALID SQL AT ALL"),
            {},
            None,
            {},
            None,
        )
        assert result.status == CheckStatus.FAILED

    def test_multiple_statements_fails(self):
        rule = SqlParseRule()
        result = rule.check(
            _sql_node("SELECT 1; SELECT 2"),
            _sql_ctx("SELECT 1; SELECT 2"),
            {},
            None,
            {},
            None,
        )
        assert result.status == CheckStatus.FAILED

    def test_non_select_fails(self):
        rule = SqlParseRule()
        result = rule.check(
            _sql_node("DROP TABLE users"),
            _sql_ctx("DROP TABLE users"),
            {},
            None,
            {},
            None,
        )
        assert result.status == CheckStatus.FAILED
