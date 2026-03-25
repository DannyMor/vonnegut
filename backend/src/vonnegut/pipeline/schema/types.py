from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum


class DataType(str, Enum):
    INT64 = "int64"
    INT32 = "int32"
    INT16 = "int16"
    INT8 = "int8"
    UINT64 = "uint64"
    UINT32 = "uint32"
    FLOAT64 = "float64"
    FLOAT32 = "float32"
    UTF8 = "utf8"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    TIME = "time"
    BINARY = "binary"
    NULL = "null"


@dataclass(frozen=True)
class Column:
    name: str
    dtype: DataType
    nullable: bool = True


@dataclass(frozen=True)
class Schema:
    columns: tuple[Column, ...] | list[Column] = field(default_factory=list)

    def __post_init__(self):
        if isinstance(self.columns, list):
            object.__setattr__(self, "columns", tuple(self.columns))

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def get_column(self, name: str) -> Column | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None
