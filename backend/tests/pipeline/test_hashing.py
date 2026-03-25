from vonnegut.pipeline.control_plane.hashing import compute_pipeline_hash
from vonnegut.pipeline.dag.node import (
    Node,
    NodeType,
    SourceNodeConfig,
    SqlNodeConfig,
)
from vonnegut.pipeline.dag.edge import Edge


def test_same_pipeline_same_hash():
    nodes = {
        "src": Node(
            id="src",
            type=NodeType.SOURCE,
            config=SourceNodeConfig(connection_id="c1", table="t1"),
        ),
    }
    edges = [Edge(id="e1", from_node_id="src", to_node_id="tgt")]
    h1 = compute_pipeline_hash(nodes, edges)
    h2 = compute_pipeline_hash(nodes, edges)
    assert h1 == h2


def test_different_config_different_hash():
    n1 = {
        "src": Node(
            id="src",
            type=NodeType.SOURCE,
            config=SourceNodeConfig(connection_id="c1", table="t1"),
        ),
    }
    n2 = {
        "src": Node(
            id="src",
            type=NodeType.SOURCE,
            config=SourceNodeConfig(connection_id="c1", table="t2"),
        ),
    }
    h1 = compute_pipeline_hash(n1, [])
    h2 = compute_pipeline_hash(n2, [])
    assert h1 != h2


def test_different_edges_different_hash():
    nodes = {
        "a": Node(
            id="a",
            type=NodeType.SQL,
            config=SqlNodeConfig(expression="SELECT 1"),
        ),
        "b": Node(
            id="b",
            type=NodeType.SQL,
            config=SqlNodeConfig(expression="SELECT 2"),
        ),
    }
    e1 = [Edge(id="e1", from_node_id="a", to_node_id="b")]
    e2 = [Edge(id="e1", from_node_id="b", to_node_id="a")]
    h1 = compute_pipeline_hash(nodes, e1)
    h2 = compute_pipeline_hash(nodes, e2)
    assert h1 != h2


def test_hash_is_deterministic_string():
    nodes = {
        "src": Node(
            id="src",
            type=NodeType.SOURCE,
            config=SourceNodeConfig(connection_id="c1", table="t1"),
        ),
    }
    h = compute_pipeline_hash(nodes, [])
    assert isinstance(h, str)
    assert len(h) == 64
