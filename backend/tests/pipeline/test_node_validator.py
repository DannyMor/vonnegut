import pyarrow as pa
import pytest
from vonnegut.pipeline.results import ValidationSuccess, ValidationFailure
from vonnegut.pipeline.dag.node import Node, NodeType, CodeNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.engine.executor.code_executor import CodeExecutor
from vonnegut.pipeline.engine.validator.node_validator import NodeValidator
from vonnegut.pipeline.engine.validator.rules.code_rules import (
    SyntaxCheckRule,
    ColumnNameRule,
)


def _make_validator() -> NodeValidator:
    return NodeValidator(
        executor=CodeExecutor(),
        rules=[SyntaxCheckRule(), ColumnNameRule()],
    )


class TestNodeValidator:
    @pytest.mark.asyncio
    async def test_valid_code_returns_success(self):
        validator = _make_validator()
        node = Node(
            id="c1",
            type=NodeType.CODE,
            config=CodeNodeConfig(function_code="def transform(df):\n    return df\n"),
        )
        ctx = ExecutionContext(
            node_id="c1", node_type=NodeType.CODE, config=node.config,
        )
        input_table = pa.table({"id": [1, 2], "name": ["a", "b"]})

        result = await validator.validate(node, ctx, {"default": input_table})

        match result:
            case ValidationSuccess(output_schema=schema):
                assert schema is not None
                assert len(schema.columns) == 2
            case ValidationFailure():
                pytest.fail("Expected ValidationSuccess")

    @pytest.mark.asyncio
    async def test_syntax_error_returns_failure(self):
        validator = _make_validator()
        node = Node(
            id="c1",
            type=NodeType.CODE,
            config=CodeNodeConfig(
                function_code="def transform(df)\n    return df\n",
            ),
        )
        ctx = ExecutionContext(
            node_id="c1", node_type=NodeType.CODE, config=node.config,
        )

        result = await validator.validate(node, ctx, {"default": pa.table({"x": [1]})})

        match result:
            case ValidationFailure(errors=errors):
                assert len(errors) > 0
                assert errors[0].rule_name == "syntax_check"
            case ValidationSuccess():
                pytest.fail("Expected ValidationFailure")

    @pytest.mark.asyncio
    async def test_execution_error_returns_failure(self):
        validator = _make_validator()
        code = "def transform(df):\n    raise RuntimeError('boom')\n"
        node = Node(
            id="c1",
            type=NodeType.CODE,
            config=CodeNodeConfig(function_code=code),
        )
        ctx = ExecutionContext(
            node_id="c1", node_type=NodeType.CODE, config=node.config,
        )

        result = await validator.validate(node, ctx, {"default": pa.table({"x": [1]})})

        match result:
            case ValidationFailure(errors=errors):
                assert any("boom" in e.message for e in errors)
            case ValidationSuccess():
                pytest.fail("Expected ValidationFailure")
