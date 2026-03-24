import pytest
from inspect import signature

from vonnegut.adapters.base import DatabaseAdapter, ColumnSchema, AdapterFactory


def test_database_adapter_is_abstract():
    with pytest.raises(TypeError):
        DatabaseAdapter()


def test_database_adapter_defines_interface():
    methods = ["connect", "disconnect", "execute", "fetch_tables", "fetch_schema", "fetch_sample", "fetch_databases"]
    for method in methods:
        assert hasattr(DatabaseAdapter, method), f"Missing method: {method}"


def test_column_schema_dataclass():
    col = ColumnSchema(
        name="id", type="int4", category="number",
        nullable=False, default=None,
        is_primary_key=True, foreign_key=None, is_unique=False,
    )
    assert col.name == "id"
    assert col.category == "number"
    assert col.is_primary_key is True
    assert col.foreign_key is None


def test_column_schema_with_foreign_key():
    col = ColumnSchema(
        name="user_id", type="int4", category="number",
        nullable=False, default=None,
        is_primary_key=False, foreign_key="users.id", is_unique=False,
    )
    assert col.foreign_key == "users.id"


def test_column_schema_with_default():
    col = ColumnSchema(
        name="created_at", type="timestamptz", category="datetime",
        nullable=False, default="now()",
        is_primary_key=False, foreign_key=None, is_unique=False,
    )
    assert col.default == "now()"


def test_fetch_schema_returns_column_schema():
    sig = signature(DatabaseAdapter.fetch_schema)
    assert "table" in sig.parameters
