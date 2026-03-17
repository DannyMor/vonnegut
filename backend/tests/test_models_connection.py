import pytest
from pydantic import ValidationError

from vonnegut.models.connection import (
    Connection,
    ConnectionCreate,
    ConnectionResponse,
    PostgresDirectConfig,
    PostgresPodConfig,
)


def test_postgres_direct_config():
    config = PostgresDirectConfig(
        host="localhost", port=5432, database="mydb", user="admin", password="secret"
    )
    assert config.host == "localhost"
    assert config.port == 5432


def test_postgres_pod_config_optional_local_port():
    config = PostgresPodConfig(
        namespace="default", pod_selector="app=postgres", user="admin", password="secret",
    )
    assert config.local_port is None
    assert config.container is None


def test_postgres_pod_config_with_local_port():
    config = PostgresPodConfig(
        namespace="default", pod_selector="app=postgres", local_port=15432,
        user="admin", password="secret",
    )
    assert config.local_port == 15432


def test_connection_create_direct():
    conn = ConnectionCreate(
        name="My DB",
        type="postgres_direct",
        config={"host": "localhost", "port": 5432, "database": "db", "user": "u", "password": "p"},
    )
    assert conn.type == "postgres_direct"
    assert isinstance(conn.parsed_config, PostgresDirectConfig)


def test_connection_create_pod():
    conn = ConnectionCreate(
        name="Pod DB", type="postgres_pod",
        config={"namespace": "production", "pod_selector": "app=postgres", "user": "admin", "password": "secret"},
    )
    assert conn.type == "postgres_pod"
    assert isinstance(conn.parsed_config, PostgresPodConfig)
    assert conn.parsed_config.pod_selector == "app=postgres"


def test_connection_create_invalid_type():
    with pytest.raises(ValidationError):
        ConnectionCreate(
            name="Bad",
            type="mysql",
            config={"host": "localhost"},
        )


def test_connection_response_masks_password():
    resp = ConnectionResponse(
        id="abc",
        name="My DB",
        type="postgres_direct",
        config={"host": "localhost", "port": 5432, "database": "db", "user": "u", "password": "secret"},
        created_at="2026-01-01T00:00:00",
        updated_at="2026-01-01T00:00:00",
    )
    assert resp.config["password"] == "********"


def test_postgres_pod_config_with_selector():
    config = PostgresPodConfig(
        namespace="production", pod_selector="app=postgres", user="admin", password="secret",
    )
    assert config.pod_selector == "app=postgres"
    assert config.pick_strategy == "first_ready"
    assert config.pick_filter is None
    assert config.container is None
    assert config.local_port is None


def test_postgres_pod_config_with_name_contains_strategy():
    config = PostgresPodConfig(
        namespace="staging", pod_selector="app=postgres,release=v2",
        pick_strategy="name_contains", pick_filter="primary", container="pg",
        user="admin", password="secret",
    )
    assert config.pick_strategy == "name_contains"
    assert config.pick_filter == "primary"
    assert config.container == "pg"


def test_postgres_pod_config_rejects_invalid_strategy():
    with pytest.raises(ValidationError):
        PostgresPodConfig(
            namespace="default", pod_selector="app=postgres",
            pick_strategy="invalid", user="admin", password="secret",
        )
