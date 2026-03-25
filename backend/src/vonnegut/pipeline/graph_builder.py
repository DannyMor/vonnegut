"""Convert existing migration + pipeline_steps data into PipelineGraph."""
from __future__ import annotations
import json

from vonnegut.pipeline.dag.node import (
    Node,
    NodeType,
    SourceNodeConfig,
    SqlNodeConfig,
    CodeNodeConfig,
    TargetNodeConfig,
)
from vonnegut.pipeline.dag.edge import Edge
from vonnegut.pipeline.dag.graph import PipelineGraph


def build_graph_from_migration(
    mig: dict,
    steps: list[dict],
) -> PipelineGraph:
    """Build a PipelineGraph from migration row and pipeline_steps rows.

    Args:
        mig: Migration row dict with source_connection_id, source_table,
             source_query, target_connection_id, target_table, truncate_target.
        steps: List of pipeline step dicts, each with id, step_type, config.
    """
    nodes: dict[str, Node] = {}
    edges: list[Edge] = []

    # Source node
    source_query = mig.get("source_query") or f"SELECT * FROM {mig['source_table']}"
    nodes["source"] = Node(
        id="source",
        type=NodeType.SOURCE,
        config=SourceNodeConfig(
            connection_id=mig["source_connection_id"],
            table=mig["source_table"],
            query=source_query,
        ),
    )

    # Transform nodes
    prev_id = "source"
    for step in steps:
        node_id = step["id"]
        step_type = step["step_type"]
        config_data = (
            json.loads(step["config"])
            if isinstance(step["config"], str)
            else step["config"]
        )

        if step_type == "sql":
            config = SqlNodeConfig(expression=config_data.get("expression", ""))
        elif step_type == "code":
            config = CodeNodeConfig(
                function_code=config_data.get("function_code", ""),
            )
        elif step_type == "ai" and config_data.get("approved"):
            code = config_data.get("generated_code", "")
            config = CodeNodeConfig(function_code=code)
        else:
            continue  # Skip unapproved AI steps

        node_type = NodeType.CODE if step_type == "ai" else NodeType(step_type)
        nodes[node_id] = Node(id=node_id, type=node_type, config=config)
        edges.append(
            Edge(
                id=f"e_{prev_id}_{node_id}",
                from_node_id=prev_id,
                to_node_id=node_id,
            ),
        )
        prev_id = node_id

    # Target node
    nodes["target"] = Node(
        id="target",
        type=NodeType.TARGET,
        config=TargetNodeConfig(
            connection_id=mig["target_connection_id"],
            table=mig["target_table"],
            truncate=bool(mig.get("truncate_target")),
        ),
    )
    edges.append(
        Edge(
            id=f"e_{prev_id}_target",
            from_node_id=prev_id,
            to_node_id="target",
        ),
    )

    return PipelineGraph(nodes=nodes, edges=edges)
