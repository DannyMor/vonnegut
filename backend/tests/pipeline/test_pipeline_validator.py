import pytest
from vonnegut.pipeline.results import CheckStatus
from vonnegut.pipeline.schema.types import Schema, Column, DataType
from vonnegut.pipeline.dag.plan import PlanNode, PlanEdge
from vonnegut.pipeline.dag.node import NodeType, SqlNodeConfig
from vonnegut.pipeline.engine.validator.pipeline_validator import (
    PipelineValidator,
    SchemaCompatibilityRule,
)


class TestSchemaCompatibilityRule:
    def test_compatible_schemas_pass(self):
        rule = SchemaCompatibilityRule()
        from_schema = Schema(
            columns=[Column("id", DataType.INT64), Column("name", DataType.UTF8)],
        )
        edge = PlanEdge(from_node_id="a", to_node_id="b")
        from_node = PlanNode(
            id="a", type=NodeType.SQL, config=SqlNodeConfig(expression=""),
        )
        to_node = PlanNode(
            id="b", type=NodeType.SQL, config=SqlNodeConfig(expression=""),
        )
        result = rule.check(edge, from_node, to_node, from_schema, None)
        assert result.status == CheckStatus.PASSED

    def test_empty_schema_warns(self):
        rule = SchemaCompatibilityRule()
        from_schema = Schema(columns=[])
        edge = PlanEdge(from_node_id="a", to_node_id="b")
        from_node = PlanNode(
            id="a", type=NodeType.SQL, config=SqlNodeConfig(expression=""),
        )
        to_node = PlanNode(
            id="b", type=NodeType.SQL, config=SqlNodeConfig(expression=""),
        )
        result = rule.check(edge, from_node, to_node, from_schema, None)
        assert result.status == CheckStatus.WARNING


class TestPipelineValidator:
    def test_validate_edge_returns_results(self):
        validator = PipelineValidator()
        from_schema = Schema(
            columns=[Column("id", DataType.INT64)],
        )
        edge = PlanEdge(from_node_id="a", to_node_id="b")
        from_node = PlanNode(
            id="a", type=NodeType.SQL, config=SqlNodeConfig(expression=""),
        )
        to_node = PlanNode(
            id="b", type=NodeType.SQL, config=SqlNodeConfig(expression=""),
        )
        results = validator.validate_edge(edge, from_node, to_node, from_schema, None)
        assert len(results) == 1
        assert results[0].status == CheckStatus.PASSED
