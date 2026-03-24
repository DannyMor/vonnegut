from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class Edge:
    id: str
    from_node_id: str
    to_node_id: str
    input_name: str | None = None
