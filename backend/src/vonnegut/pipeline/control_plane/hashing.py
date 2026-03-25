from __future__ import annotations
import hashlib
import json
from dataclasses import asdict

from vonnegut.pipeline.dag.node import Node
from vonnegut.pipeline.dag.edge import Edge


def compute_pipeline_hash(nodes: dict[str, Node], edges: list[Edge]) -> str:
    node_data = {}
    for nid in sorted(nodes.keys()):
        node = nodes[nid]
        node_data[nid] = {
            "type": node.type.value,
            "config": asdict(node.config),
        }

    edge_data = [
        {
            "from": e.from_node_id,
            "to": e.to_node_id,
            "input_name": e.input_name,
        }
        for e in sorted(
            edges,
            key=lambda e: (e.from_node_id, e.to_node_id, e.input_name or ""),
        )
    ]

    payload = json.dumps({"nodes": node_data, "edges": edge_data}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()
