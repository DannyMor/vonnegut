import pytest
from pydantic import ValidationError

from vonnegut.models.migration import MigrationCreate, MigrationResponse, MigrationStatusType


def test_migration_status_type_accepts_valid():
    m = MigrationResponse(
        id="x", name="x", source_connection_id="x", target_connection_id="x",
        source_table="x", target_table="x", status="cancelled", truncate_target=False,
        rows_processed=None, total_rows=None, error_message=None,
        created_at="2026-01-01T00:00:00", updated_at="2026-01-01T00:00:00",
    )
    assert m.status == "cancelled"


def test_migration_create():
    m = MigrationCreate(
        name="Test Migration",
        source_connection_id="conn-1",
        target_connection_id="conn-2",
        source_table="users",
        target_table="users_copy",
    )
    assert m.name == "Test Migration"
    assert m.truncate_target is False


def test_migration_create_with_truncate():
    m = MigrationCreate(
        name="Truncate Migration",
        source_connection_id="conn-1",
        target_connection_id="conn-2",
        source_table="users",
        target_table="users_copy",
        truncate_target=True,
    )
    assert m.truncate_target is True


def test_migration_response():
    m = MigrationResponse(
        id="mig-1",
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
    assert m.status == "draft"
    assert m.transformations == []
