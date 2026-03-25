import pytest
from vonnegut.pipeline.schema.types import DataType, Column, Schema


class TestDataType:
    def test_all_canonical_types_exist(self):
        assert DataType.INT64
        assert DataType.FLOAT64
        assert DataType.UTF8
        assert DataType.BOOLEAN
        assert DataType.TIMESTAMP
        assert DataType.DATE
        assert DataType.BINARY
        assert DataType.NULL

    def test_enum_values_are_strings(self):
        assert DataType.INT64.value == "int64"
        assert DataType.UTF8.value == "utf8"


class TestColumn:
    def test_create_column(self):
        col = Column(name="age", dtype=DataType.INT64, nullable=False)
        assert col.name == "age"
        assert col.dtype == DataType.INT64
        assert col.nullable is False

    def test_column_defaults_nullable(self):
        col = Column(name="name", dtype=DataType.UTF8)
        assert col.nullable is True


class TestSchema:
    def test_create_schema(self):
        schema = Schema(columns=[
            Column(name="id", dtype=DataType.INT64, nullable=False),
            Column(name="name", dtype=DataType.UTF8),
        ])
        assert len(schema.columns) == 2

    def test_column_names(self):
        schema = Schema(columns=[
            Column(name="id", dtype=DataType.INT64),
            Column(name="name", dtype=DataType.UTF8),
        ])
        assert schema.column_names == ["id", "name"]

    def test_get_column(self):
        col = Column(name="id", dtype=DataType.INT64)
        schema = Schema(columns=[col])
        assert schema.get_column("id") == col
        assert schema.get_column("missing") is None

    def test_empty_schema(self):
        schema = Schema(columns=[])
        assert len(schema.columns) == 0
        assert schema.column_names == []

    def test_schema_equality(self):
        s1 = Schema(columns=[Column(name="id", dtype=DataType.INT64)])
        s2 = Schema(columns=[Column(name="id", dtype=DataType.INT64)])
        assert s1 == s2
