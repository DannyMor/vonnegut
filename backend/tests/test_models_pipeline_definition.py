import pytest
from pydantic import ValidationError

from vonnegut.models.pipeline_definition import PipelineCreate, PipelineResponse, PipelineStatusType


def test_pipeline_status_type_accepts_valid():
    p = PipelineResponse(
        id="x", name="x", source_connection_id="x", target_connection_id="x",
        source_table="x", target_table="x", status="cancelled", truncate_target=False,
        rows_processed=None, total_rows=None, error_message=None,
        created_at="2026-01-01T00:00:00", updated_at="2026-01-01T00:00:00",
    )
    assert p.status == "cancelled"


def test_pipeline_create():
    p = PipelineCreate(
        name="Test Pipeline",
        source_connection_id="conn-1",
        target_connection_id="conn-2",
        source_table="users",
        target_table="users_copy",
    )
    assert p.name == "Test Pipeline"
    assert p.truncate_target is False


def test_pipeline_create_with_truncate():
    p = PipelineCreate(
        name="Truncate Pipeline",
        source_connection_id="conn-1",
        target_connection_id="conn-2",
        source_table="users",
        target_table="users_copy",
        truncate_target=True,
    )
    assert p.truncate_target is True


def test_pipeline_response():
    p = PipelineResponse(
        id="pipeline-1",
        name="Test",
        source_connection_id="conn-1",
        target_connection_id="conn-2",
        source_table="users",
        target_table="users_copy",
        status="draft",
        truncate_target=False,
        rows_processed=None,
        total_rows=None,
        error_message=None,
        created_at="2026-01-01T00:00:00",
        updated_at="2026-01-01T00:00:00",
        transformations=[],
    )
    assert p.status == "draft"
    assert p.transformations == []
