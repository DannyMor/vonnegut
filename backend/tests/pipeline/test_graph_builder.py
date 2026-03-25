import json
import pytest
from vonnegut.pipeline.graph_builder import build_graph_from_migration
from vonnegut.pipeline.dag.node import NodeType, SourceNodeConfig, SqlNodeConfig, CodeNodeConfig, TargetNodeConfig


def _make_migration() -> dict:
    return {
        "source_connection_id": "conn1",
        "source_table": "users",
        "source_query": None,
        "target_connection_id": "conn2",
        "target_table": "users_target",
        "truncate_target": True,
    }


class TestBuildGraphFromMigration:
    def test_source_only_pipeline(self):
        graph = build_graph_from_migration(_make_migration(), [])
        assert "source" in graph.nodes
        assert "target" in graph.nodes
        assert len(graph.nodes) == 2
        assert len(graph.edges) == 1
        assert graph.nodes["source"].type == NodeType.SOURCE
        assert graph.nodes["target"].type == NodeType.TARGET

    def test_with_sql_step(self):
        steps = [
            {
                "id": "step1",
                "step_type": "sql",
                "config": json.dumps({"expression": "SELECT * FROM {prev} WHERE age > 18"}),
            },
        ]
        graph = build_graph_from_migration(_make_migration(), steps)
        assert len(graph.nodes) == 3
        assert graph.nodes["step1"].type == NodeType.SQL
        assert isinstance(graph.nodes["step1"].config, SqlNodeConfig)
        # source -> step1 -> target
        assert len(graph.edges) == 2

    def test_with_code_step(self):
        steps = [
            {
                "id": "step1",
                "step_type": "code",
                "config": {"function_code": "def transform(df):\n    return df\n"},
            },
        ]
        graph = build_graph_from_migration(_make_migration(), steps)
        assert graph.nodes["step1"].type == NodeType.CODE
        assert isinstance(graph.nodes["step1"].config, CodeNodeConfig)

    def test_with_multiple_steps(self):
        steps = [
            {"id": "s1", "step_type": "sql", "config": json.dumps({"expression": "SELECT * FROM {prev}"})},
            {"id": "s2", "step_type": "code", "config": json.dumps({"function_code": "def transform(df):\n    return df\n"})},
        ]
        graph = build_graph_from_migration(_make_migration(), steps)
        assert len(graph.nodes) == 4
        assert len(graph.edges) == 3
        # Verify chain: source -> s1 -> s2 -> target
        edge_pairs = [(e.from_node_id, e.to_node_id) for e in graph.edges]
        assert ("source", "s1") in edge_pairs
        assert ("s1", "s2") in edge_pairs
        assert ("s2", "target") in edge_pairs

    def test_approved_ai_step_becomes_code(self):
        steps = [
            {
                "id": "ai1",
                "step_type": "ai",
                "config": json.dumps({"approved": True, "generated_code": "def transform(df):\n    return df\n"}),
            },
        ]
        graph = build_graph_from_migration(_make_migration(), steps)
        assert graph.nodes["ai1"].type == NodeType.CODE

    def test_unapproved_ai_step_skipped(self):
        steps = [
            {
                "id": "ai1",
                "step_type": "ai",
                "config": json.dumps({"approved": False, "generated_code": "def transform(df):\n    return df\n"}),
            },
        ]
        graph = build_graph_from_migration(_make_migration(), steps)
        assert "ai1" not in graph.nodes
        assert len(graph.nodes) == 2  # source + target only

    def test_source_uses_custom_query(self):
        mig = _make_migration()
        mig["source_query"] = "SELECT id, name FROM users WHERE active = true"
        graph = build_graph_from_migration(mig, [])
        config = graph.nodes["source"].config
        assert isinstance(config, SourceNodeConfig)
        assert config.query == "SELECT id, name FROM users WHERE active = true"

    def test_target_truncate_flag(self):
        graph = build_graph_from_migration(_make_migration(), [])
        config = graph.nodes["target"].config
        assert isinstance(config, TargetNodeConfig)
        assert config.truncate is True

    def test_graph_validates(self):
        steps = [
            {"id": "s1", "step_type": "sql", "config": json.dumps({"expression": "SELECT * FROM {prev}"})},
        ]
        graph = build_graph_from_migration(_make_migration(), steps)
        graph.validate()  # should not raise
