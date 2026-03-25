from __future__ import annotations
import pyarrow as pa
import polars as pl
from vonnegut.pipeline.schema.types import DataType, Column, Schema

# Arrow type mappings
_ARROW_TO_CANONICAL: dict[pa.DataType, DataType] = {
    pa.int8(): DataType.INT8,
    pa.int16(): DataType.INT16,
    pa.int32(): DataType.INT32,
    pa.int64(): DataType.INT64,
    pa.uint32(): DataType.UINT32,
    pa.uint64(): DataType.UINT64,
    pa.float32(): DataType.FLOAT32,
    pa.float64(): DataType.FLOAT64,
    pa.utf8(): DataType.UTF8,
    pa.large_utf8(): DataType.UTF8,
    pa.bool_(): DataType.BOOLEAN,
    pa.date32(): DataType.DATE,
    pa.binary(): DataType.BINARY,
}

_CANONICAL_TO_ARROW: dict[DataType, pa.DataType] = {
    DataType.INT8: pa.int8(),
    DataType.INT16: pa.int16(),
    DataType.INT32: pa.int32(),
    DataType.INT64: pa.int64(),
    DataType.UINT32: pa.uint32(),
    DataType.UINT64: pa.uint64(),
    DataType.FLOAT32: pa.float32(),
    DataType.FLOAT64: pa.float64(),
    DataType.UTF8: pa.utf8(),
    DataType.BOOLEAN: pa.bool_(),
    DataType.TIMESTAMP: pa.timestamp("us"),
    DataType.DATE: pa.date32(),
    DataType.TIME: pa.time64("us"),
    DataType.BINARY: pa.binary(),
    DataType.NULL: pa.null(),
}

# Polars type mappings
_POLARS_TO_CANONICAL: dict[type, DataType] = {
    pl.Int8: DataType.INT8,
    pl.Int16: DataType.INT16,
    pl.Int32: DataType.INT32,
    pl.Int64: DataType.INT64,
    pl.UInt32: DataType.UINT32,
    pl.UInt64: DataType.UINT64,
    pl.Float32: DataType.FLOAT32,
    pl.Float64: DataType.FLOAT64,
    pl.Utf8: DataType.UTF8,
    pl.String: DataType.UTF8,
    pl.Boolean: DataType.BOOLEAN,
    pl.Date: DataType.DATE,
    pl.Binary: DataType.BINARY,
}

# Postgres type string mappings
_PG_TYPE_TO_CANONICAL: dict[str, DataType] = {
    "integer": DataType.INT64,
    "int": DataType.INT64,
    "int4": DataType.INT64,
    "bigint": DataType.INT64,
    "int8": DataType.INT64,
    "smallint": DataType.INT32,
    "int2": DataType.INT32,
    "real": DataType.FLOAT32,
    "float4": DataType.FLOAT32,
    "double precision": DataType.FLOAT64,
    "float8": DataType.FLOAT64,
    "numeric": DataType.FLOAT64,
    "decimal": DataType.FLOAT64,
    "text": DataType.UTF8,
    "varchar": DataType.UTF8,
    "character varying": DataType.UTF8,
    "char": DataType.UTF8,
    "boolean": DataType.BOOLEAN,
    "bool": DataType.BOOLEAN,
    "timestamp": DataType.TIMESTAMP,
    "timestamp without time zone": DataType.TIMESTAMP,
    "timestamp with time zone": DataType.TIMESTAMP,
    "timestamptz": DataType.TIMESTAMP,
    "date": DataType.DATE,
    "time": DataType.TIME,
    "bytea": DataType.BINARY,
    "uuid": DataType.UTF8,
    "json": DataType.UTF8,
    "jsonb": DataType.UTF8,
}


class ArrowSchemaAdapter:
    @staticmethod
    def from_arrow(arrow_schema: pa.Schema) -> Schema:
        columns = []
        for field in arrow_schema:
            dtype = _ARROW_TO_CANONICAL.get(field.type)
            if dtype is None and pa.types.is_timestamp(field.type):
                dtype = DataType.TIMESTAMP
            if dtype is None and pa.types.is_time(field.type):
                dtype = DataType.TIME
            columns.append(Column(
                name=field.name,
                dtype=dtype or DataType.UTF8,
                nullable=field.nullable,
            ))
        return Schema(columns=columns)

    @staticmethod
    def to_arrow(schema: Schema) -> pa.Schema:
        fields = []
        for col in schema.columns:
            arrow_type = _CANONICAL_TO_ARROW.get(col.dtype, pa.utf8())
            fields.append(pa.field(col.name, arrow_type, nullable=col.nullable))
        return pa.schema(fields)


class PolarsSchemaAdapter:
    @staticmethod
    def from_dataframe(df: pl.DataFrame) -> Schema:
        columns = []
        for name, dtype in zip(df.columns, df.dtypes):
            canonical = _POLARS_TO_CANONICAL.get(type(dtype), DataType.UTF8)
            columns.append(Column(name=name, dtype=canonical, nullable=True))
        return Schema(columns=columns)

    @staticmethod
    def from_polars_schema(polars_schema: dict) -> Schema:
        columns = []
        for name, dtype in polars_schema.items():
            canonical = _POLARS_TO_CANONICAL.get(dtype, DataType.UTF8)
            columns.append(Column(name=name, dtype=canonical, nullable=True))
        return Schema(columns=columns)


class PostgresSchemaAdapter:
    @staticmethod
    def from_column_metadata(metadata: list[dict]) -> Schema:
        columns = []
        for col in metadata:
            pg_type = col["type"].lower().strip()
            dtype = _PG_TYPE_TO_CANONICAL.get(pg_type, DataType.UTF8)
            columns.append(Column(
                name=col["name"],
                dtype=dtype,
                nullable=col.get("nullable", True),
            ))
        return Schema(columns=columns)
