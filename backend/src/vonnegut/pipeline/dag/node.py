from __future__ import annotations
from dataclasses import dataclass
from enum import Enum


class NodeType(str, Enum):
    SOURCE = "source"
    SQL = "sql"
    CODE = "code"
    TARGET = "target"


@dataclass(frozen=True)
class SourceNodeConfig:
    connection_id: str
    table: str
    query: str | None = None


@dataclass(frozen=True)
class SqlNodeConfig:
    expression: str


@dataclass(frozen=True)
class CodeNodeConfig:
    function_code: str


@dataclass(frozen=True)
class TargetNodeConfig:
    connection_id: str
    table: str
    truncate: bool


NodeConfig = SourceNodeConfig | SqlNodeConfig | CodeNodeConfig | TargetNodeConfig


@dataclass(frozen=True)
class Node:
    id: str
    type: NodeType
    config: NodeConfig
