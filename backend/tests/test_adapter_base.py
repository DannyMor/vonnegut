import pytest
from inspect import signature

from vonnegut.adapters.base import DatabaseAdapter, ColumnSchema, AdapterFactory


def test_database_adapter_is_abstract():
    with pytest.raises(TypeError):
        DatabaseAdapter()


def test_database_adapter_defines_interface():
    methods = ["connect", "disconnect", "execute", "fetch_tables", "fetch_schema", "fetch_sample"]
    for method in methods:
        assert hasattr(DatabaseAdapter, method), f"Missing method: {method}"


def test_column_schema_dataclass():
    col = ColumnSchema(column="id", type="integer", nullable=False, is_primary_key=True)
    assert col.column == "id"
    assert col.is_primary_key is True


def test_fetch_schema_returns_column_schema():
    sig = signature(DatabaseAdapter.fetch_schema)
    assert "table" in sig.parameters
