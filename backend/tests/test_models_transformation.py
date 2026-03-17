import pytest
from pydantic import ValidationError

from vonnegut.models.transformation import (
    TransformationCreate,
    TransformationResponse,
    ReorderRequest,
)


def test_column_mapping_transformation():
    t = TransformationCreate(
        type="column_mapping",
        config={
            "mappings": [
                {"source_col": "name", "target_col": "full_name", "drop": False},
                {"source_col": "temp_col", "target_col": None, "drop": True},
            ]
        },
    )
    assert t.type == "column_mapping"
    assert len(t.config["mappings"]) == 2


def test_sql_expression_transformation():
    t = TransformationCreate(
        type="sql_expression",
        config={"expression": "UPPER(name)", "output_column": "name_upper"},
    )
    assert t.type == "sql_expression"


def test_ai_generated_transformation():
    t = TransformationCreate(
        type="ai_generated",
        config={
            "prompt": "lowercase the email",
            "generated_expression": "LOWER(email)",
            "approved": True,
        },
    )
    assert t.type == "ai_generated"


def test_invalid_transformation_type():
    with pytest.raises(ValidationError):
        TransformationCreate(type="python_func", config={})


def test_reorder_request():
    r = ReorderRequest(order=["id-1", "id-2", "id-3"])
    assert len(r.order) == 3


def test_transformation_response():
    t = TransformationResponse(
        id="t-1",
        migration_id="mig-1",
        order=0,
        type="sql_expression",
        config={"expression": "UPPER(name)", "output_column": "name_upper"},
        created_at="2026-01-01T00:00:00",
        updated_at="2026-01-01T00:00:00",
    )
    assert t.order == 0
