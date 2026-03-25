import pyarrow as pa
import polars as pl
import pytest
from vonnegut.pipeline.schema.types import DataType, Column, Schema
from vonnegut.pipeline.schema.adapters import (
    ArrowSchemaAdapter,
    DuckDBSchemaAdapter,
    PolarsSchemaAdapter,
    PostgresSchemaAdapter,
)


class TestArrowSchemaAdapter:
    def test_from_arrow(self):
        arrow_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.utf8()),
        ])
        schema = ArrowSchemaAdapter.from_arrow(arrow_schema)
        assert len(schema.columns) == 2
        assert schema.columns[0] == Column("id", DataType.INT64, nullable=False)
        assert schema.columns[1] == Column("name", DataType.UTF8, nullable=True)

    def test_to_arrow(self):
        schema = Schema(columns=[
            Column("id", DataType.INT64, nullable=False),
            Column("name", DataType.UTF8),
        ])
        arrow_schema = ArrowSchemaAdapter.to_arrow(schema)
        assert arrow_schema.field("id").type == pa.int64()
        assert arrow_schema.field("name").type == pa.utf8()

    def test_roundtrip(self):
        original = Schema(columns=[
            Column("id", DataType.INT64, nullable=False),
            Column("score", DataType.FLOAT64),
            Column("active", DataType.BOOLEAN),
        ])
        roundtripped = ArrowSchemaAdapter.from_arrow(ArrowSchemaAdapter.to_arrow(original))
        assert roundtripped == original


class TestPolarsSchemaAdapter:
    def test_from_dataframe(self):
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        schema = PolarsSchemaAdapter.from_dataframe(df)
        assert schema.get_column("id").dtype == DataType.INT64
        assert schema.get_column("name").dtype == DataType.UTF8

    def test_from_polars_schema(self):
        polars_schema = {"id": pl.Int64, "name": pl.Utf8}
        schema = PolarsSchemaAdapter.from_polars_schema(polars_schema)
        assert len(schema.columns) == 2


class TestPostgresSchemaAdapter:
    def test_from_column_metadata(self):
        metadata = [
            {"name": "id", "type": "integer", "nullable": False},
            {"name": "email", "type": "varchar", "nullable": True},
            {"name": "created_at", "type": "timestamp", "nullable": True},
        ]
        schema = PostgresSchemaAdapter.from_column_metadata(metadata)
        assert schema.get_column("id").dtype == DataType.INT64
        assert schema.get_column("email").dtype == DataType.UTF8
        assert schema.get_column("created_at").dtype == DataType.TIMESTAMP


class TestDuckDBSchemaAdapter:
    def test_from_description(self):
        # Simulates DuckDB cursor.description format
        description = [
            ("id", "INTEGER", None, None, None, None, False),
            ("name", "VARCHAR", None, None, None, None, True),
            ("score", "DOUBLE", None, None, None, None, True),
            ("active", "BOOLEAN", None, None, None, None, True),
        ]
        schema = DuckDBSchemaAdapter.from_description(description)
        assert len(schema.columns) == 4
        assert schema.get_column("id").dtype == DataType.INT32
        assert schema.get_column("id").nullable is False
        assert schema.get_column("name").dtype == DataType.UTF8
        assert schema.get_column("score").dtype == DataType.FLOAT64
        assert schema.get_column("active").dtype == DataType.BOOLEAN

    def test_from_description_duckdb_specific_types(self):
        description = [
            ("a", "TINYINT", None, None, None, None, None),
            ("b", "HUGEINT", None, None, None, None, None),
            ("c", "BLOB", None, None, None, None, None),
        ]
        schema = DuckDBSchemaAdapter.from_description(description)
        assert schema.get_column("a").dtype == DataType.INT8
        assert schema.get_column("b").dtype == DataType.INT64
        assert schema.get_column("c").dtype == DataType.BINARY

    def test_from_description_unknown_type_defaults_to_utf8(self):
        description = [("x", "STRUCT(a INTEGER)", None, None, None, None, True)]
        schema = DuckDBSchemaAdapter.from_description(description)
        assert schema.get_column("x").dtype == DataType.UTF8

    def test_from_column_types(self):
        column_types = {"id": "BIGINT", "name": "VARCHAR", "ts": "TIMESTAMP"}
        schema = DuckDBSchemaAdapter.from_column_types(column_types)
        assert schema.get_column("id").dtype == DataType.INT64
        assert schema.get_column("name").dtype == DataType.UTF8
        assert schema.get_column("ts").dtype == DataType.TIMESTAMP

    def test_from_column_types_case_insensitive(self):
        column_types = {"a": "Boolean", "b": "FLOAT"}
        schema = DuckDBSchemaAdapter.from_column_types(column_types)
        assert schema.get_column("a").dtype == DataType.BOOLEAN
        assert schema.get_column("b").dtype == DataType.FLOAT32
