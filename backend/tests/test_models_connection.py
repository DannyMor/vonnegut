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
        namespace="default",
        pod_name="pg-pod-0",
        container="postgres",
        database="mydb",
        user="admin",
        password="secret",
    )
    assert config.local_port is None


def test_postgres_pod_config_with_local_port():
    config = PostgresPodConfig(
        namespace="default",
        pod_name="pg-pod-0",
        container="postgres",
        database="mydb",
        user="admin",
        password="secret",
        local_port=15432,
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
        name="Pod DB",
        type="postgres_pod",
        config={
            "namespace": "default",
            "pod_name": "pg-0",
            "container": "postgres",
            "database": "db",
            "user": "u",
            "password": "p",
        },
    )
    assert conn.type == "postgres_pod"
    assert isinstance(conn.parsed_config, PostgresPodConfig)


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
