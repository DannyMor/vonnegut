import pytest
from pydantic import ValidationError

from vonnegut.models.connection import (
    ConnectionCreate,
    ConnectionResponse,
    PostgresDirectConfig,
    PostgresPodConfig,
)


def test_postgres_direct_config():
    config = PostgresDirectConfig(
        type="postgres_direct",
        host="localhost", port=5432, database="mydb", user="admin", password="secret",
    )
    assert config.host == "localhost"
    assert config.port == 5432
    assert config.type == "postgres_direct"


def test_postgres_pod_config():
    config = PostgresPodConfig(
        type="postgres_pod",
        namespace="default", pod_selector="app=postgres",
        host="postgres.default.svc", user="admin", password="secret",
    )
    assert config.pod_selector == "app=postgres"
    assert config.pick_strategy == "first_ready"
    assert config.pick_filter is None
    assert config.container is None
    assert config.port == 5432
    assert config.host == "postgres.default.svc"


def test_postgres_pod_config_with_name_contains_strategy():
    config = PostgresPodConfig(
        type="postgres_pod",
        namespace="staging", pod_selector="app=postgres,release=v2",
        pick_strategy="name_contains", pick_filter="primary", container="pg",
        host="postgres-primary.staging.svc", user="admin", password="secret",
    )
    assert config.pick_strategy == "name_contains"
    assert config.pick_filter == "primary"
    assert config.container == "pg"


def test_postgres_pod_config_rejects_invalid_strategy():
    with pytest.raises(ValidationError):
        PostgresPodConfig(
            type="postgres_pod",
            namespace="default", pod_selector="app=postgres",
            pick_strategy="invalid", host="h", user="admin", password="secret",
        )


def test_connection_create_direct():
    conn = ConnectionCreate(
        name="My DB",
        config={
            "type": "postgres_direct",
            "host": "localhost", "port": 5432,
            "database": "db", "user": "u", "password": "p",
        },
    )
    assert isinstance(conn.config, PostgresDirectConfig)
    assert conn.config.type == "postgres_direct"


def test_connection_create_pod():
    conn = ConnectionCreate(
        name="Pod DB",
        config={
            "type": "postgres_pod",
            "namespace": "production", "pod_selector": "app=postgres",
            "host": "postgres.production.svc", "user": "admin", "password": "secret",
        },
    )
    assert isinstance(conn.config, PostgresPodConfig)
    assert conn.config.pod_selector == "app=postgres"


def test_connection_create_invalid_type():
    with pytest.raises(ValidationError):
        ConnectionCreate(
            name="Bad",
            config={"type": "mysql", "host": "localhost"},
        )


def test_connection_create_missing_required_field():
    with pytest.raises(ValidationError):
        ConnectionCreate(
            name="Bad",
            config={"type": "postgres_direct", "host": "localhost"},
        )


def test_connection_response_masks_password():
    resp = ConnectionResponse(
        id="abc", name="My DB",
        config={
            "type": "postgres_direct",
            "host": "localhost", "port": 5432,
            "database": "db", "user": "u", "password": "secret",
        },
        created_at="2026-01-01T00:00:00",
        updated_at="2026-01-01T00:00:00",
    )
    assert resp.config["password"] == "********"
