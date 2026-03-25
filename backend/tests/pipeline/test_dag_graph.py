import pytest
from vonnegut.pipeline.dag.node import Node, NodeType, SourceNodeConfig, SqlNodeConfig, CodeNodeConfig, TargetNodeConfig
from vonnegut.pipeline.dag.edge import Edge
from vonnegut.pipeline.dag.graph import PipelineGraph, topological_sort, collect_inputs, CycleError


class TestTopologicalSort:
    def test_linear_chain(self):
        nodes = {
            "src": Node(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1")),
            "sql": Node(id="sql", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT * FROM {prev}")),
            "tgt": Node(id="tgt", type=NodeType.TARGET, config=TargetNodeConfig(connection_id="c2", table="t2", truncate=False)),
        }
        edges = [
            Edge(id="e1", from_node_id="src", to_node_id="sql"),
            Edge(id="e2", from_node_id="sql", to_node_id="tgt"),
        ]
        order = topological_sort(nodes, edges)
        assert order == ["src", "sql", "tgt"]

    def test_cycle_raises(self):
        nodes = {
            "a": Node(id="a", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT 1")),
            "b": Node(id="b", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT 1")),
        }
        edges = [
            Edge(id="e1", from_node_id="a", to_node_id="b"),
            Edge(id="e2", from_node_id="b", to_node_id="a"),
        ]
        with pytest.raises(CycleError):
            topological_sort(nodes, edges)

    def test_single_node(self):
        nodes = {"src": Node(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1"))}
        order = topological_sort(nodes, [])
        assert order == ["src"]


class TestCollectInputs:
    def test_collects_default_input(self):
        edges = [Edge(id="e1", from_node_id="src", to_node_id="sql")]
        outputs = {"src": "table_data"}
        inputs = collect_inputs("sql", edges, outputs)
        assert inputs == {"default": "table_data"}

    def test_collects_named_inputs(self):
        edges = [
            Edge(id="e1", from_node_id="a", to_node_id="join", input_name="left"),
            Edge(id="e2", from_node_id="b", to_node_id="join", input_name="right"),
        ]
        outputs = {"a": "table_a", "b": "table_b"}
        inputs = collect_inputs("join", edges, outputs)
        assert inputs == {"left": "table_a", "right": "table_b"}

    def test_source_has_no_inputs(self):
        edges = [Edge(id="e1", from_node_id="src", to_node_id="sql")]
        inputs = collect_inputs("src", edges, {})
        assert inputs == {}


class TestPipelineGraph:
    def test_validate_linear_chain(self):
        graph = PipelineGraph(
            nodes={
                "src": Node(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1")),
                "tgt": Node(id="tgt", type=NodeType.TARGET, config=TargetNodeConfig(connection_id="c2", table="t2", truncate=False)),
            },
            edges=[Edge(id="e1", from_node_id="src", to_node_id="tgt")],
        )
        graph.validate()

    def test_validate_rejects_cycle(self):
        graph = PipelineGraph(
            nodes={
                "a": Node(id="a", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT 1")),
                "b": Node(id="b", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT 1")),
            },
            edges=[
                Edge(id="e1", from_node_id="a", to_node_id="b"),
                Edge(id="e2", from_node_id="b", to_node_id="a"),
            ],
        )
        with pytest.raises(CycleError):
            graph.validate()

    def test_validate_rejects_orphaned_node(self):
        graph = PipelineGraph(
            nodes={
                "src": Node(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1")),
                "orphan": Node(id="orphan", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT 1")),
            },
            edges=[],
        )
        with pytest.raises(ValueError, match="orphan"):
            graph.validate()
