# backend/tests/test_transformation_engine.py
import pytest

from vonnegut.services.transformation_engine import TransformationEngine


@pytest.fixture
def engine():
    return TransformationEngine()


def test_apply_column_mapping_rename(engine):
    rows = [{"name": "Alice", "age": 30, "temp": "x"}]
    config = {
        "mappings": [
            {"source_col": "name", "target_col": "full_name", "drop": False},
            {"source_col": "age", "target_col": "age", "drop": False},
            {"source_col": "temp", "target_col": None, "drop": True},
        ]
    }
    result = engine.apply_column_mapping(rows, config)
    assert result == [{"full_name": "Alice", "age": 30}]


def test_apply_column_mapping_drop_only(engine):
    rows = [{"a": 1, "b": 2, "c": 3}]
    config = {
        "mappings": [
            {"source_col": "a", "target_col": "a", "drop": False},
            {"source_col": "b", "target_col": None, "drop": True},
            {"source_col": "c", "target_col": "c", "drop": False},
        ]
    }
    result = engine.apply_column_mapping(rows, config)
    assert result == [{"a": 1, "c": 3}]


def test_apply_sql_expression_upper(engine):
    rows = [{"name": "alice", "id": 1}, {"name": "bob", "id": 2}]
    config = {"expression": "UPPER(name)", "output_column": "name_upper"}
    result = engine.apply_sql_expression(rows, config)
    assert result[0]["name_upper"] == "ALICE"
    assert result[1]["name_upper"] == "BOB"


def test_apply_sql_expression_concat(engine):
    rows = [{"first": "Alice", "last": "Smith"}]
    config = {"expression": "CONCAT(first, ' ', last)", "output_column": "full_name"}
    result = engine.apply_sql_expression(rows, config)
    assert result[0]["full_name"] == "Alice Smith"


def test_apply_sql_expression_coalesce(engine):
    rows = [{"val": None}, {"val": "hello"}]
    config = {"expression": "COALESCE(val, 'default')", "output_column": "val_safe"}
    result = engine.apply_sql_expression(rows, config)
    assert result[0]["val_safe"] == "default"
    assert result[1]["val_safe"] == "hello"


def test_apply_pipeline(engine):
    rows = [{"name": "alice", "temp": "x", "age": 30}]
    transformations = [
        {
            "type": "column_mapping",
            "config": {
                "mappings": [
                    {"source_col": "name", "target_col": "name", "drop": False},
                    {"source_col": "age", "target_col": "age", "drop": False},
                    {"source_col": "temp", "target_col": None, "drop": True},
                ]
            },
        },
        {
            "type": "sql_expression",
            "config": {"expression": "UPPER(name)", "output_column": "name_upper"},
        },
    ]
    result = engine.apply_pipeline(rows, transformations)
    assert result == [{"name": "alice", "age": 30, "name_upper": "ALICE"}]
