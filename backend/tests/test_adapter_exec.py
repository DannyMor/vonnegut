import json
import pytest
from unittest.mock import AsyncMock, patch

from vonnegut.adapters.postgres_exec import PostgresExecAdapter
from vonnegut.adapters.base import DatabaseAdapter


def _make_pod(name: str, phase: str = "Running", ready: bool = True) -> dict:
    return {
        "metadata": {"name": name},
        "status": {
            "phase": phase,
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
        },
    }


def _kubectl_pods_output(*pods) -> str:
    return json.dumps({"items": list(pods)})


@pytest.fixture
def adapter():
    return PostgresExecAdapter(
        namespace="default",
        pod_selector="app=myservice",
        pick_strategy="first_ready",
        pick_filter=None,
        container=None,
        host="postgres.default.svc",
        port=5432,
        database="mydb",
        user="admin",
        password="secret",
    )


def test_implements_database_adapter(adapter):
    assert isinstance(adapter, DatabaseAdapter)


@pytest.mark.asyncio
async def test_connect_resolves_pod(adapter):
    pods_json = _kubectl_pods_output(
        _make_pod("myservice-abc123", "Running", True),
        _make_pod("myservice-def456", "Running", True),
    )
    mock_process = AsyncMock()
    mock_process.communicate.return_value = (pods_json.encode(), b"")
    mock_process.returncode = 0

    with patch("asyncio.create_subprocess_exec", return_value=mock_process):
        await adapter.connect()

    assert adapter._resolved_pod == "myservice-abc123"


@pytest.mark.asyncio
async def test_connect_name_contains_strategy():
    adapter = PostgresExecAdapter(
        namespace="staging",
        pod_selector="app=postgres",
        pick_strategy="name_contains",
        pick_filter="primary",
        container=None,
        host="postgres.staging.svc",
        port=5432,
        database="mydb",
        user="admin",
        password="secret",
    )
    pods_json = _kubectl_pods_output(
        _make_pod("postgres-replica-001", "Running", True),
        _make_pod("postgres-primary-001", "Running", True),
    )
    mock_process = AsyncMock()
    mock_process.communicate.return_value = (pods_json.encode(), b"")
    mock_process.returncode = 0

    with patch("asyncio.create_subprocess_exec", return_value=mock_process):
        await adapter.connect()

    assert adapter._resolved_pod == "postgres-primary-001"


@pytest.mark.asyncio
async def test_connect_no_ready_pods_raises(adapter):
    pods_json = _kubectl_pods_output(
        _make_pod("myservice-abc123", "Pending", False),
    )
    mock_process = AsyncMock()
    mock_process.communicate.return_value = (pods_json.encode(), b"")
    mock_process.returncode = 0

    with patch("asyncio.create_subprocess_exec", return_value=mock_process):
        with pytest.raises(ConnectionError, match="No ready pods"):
            await adapter.connect()


@pytest.mark.asyncio
async def test_connect_no_pods_at_all_raises(adapter):
    pods_json = _kubectl_pods_output()
    mock_process = AsyncMock()
    mock_process.communicate.return_value = (pods_json.encode(), b"")
    mock_process.returncode = 0

    with patch("asyncio.create_subprocess_exec", return_value=mock_process):
        with pytest.raises(ConnectionError, match="No ready pods"):
            await adapter.connect()


@pytest.mark.asyncio
async def test_connect_kubectl_not_found(adapter):
    with patch("asyncio.create_subprocess_exec", side_effect=FileNotFoundError):
        with pytest.raises(ConnectionError, match="kubectl"):
            await adapter.connect()


@pytest.mark.asyncio
async def test_fetch_tables(adapter):
    pods_json = _kubectl_pods_output(_make_pod("myservice-abc123"))
    mock_connect = AsyncMock()
    mock_connect.communicate.return_value = (pods_json.encode(), b"")
    mock_connect.returncode = 0

    psql_output = "users\norders\n"
    mock_psql = AsyncMock()
    mock_psql.communicate.return_value = (psql_output.encode(), b"")
    mock_psql.returncode = 0

    with patch("asyncio.create_subprocess_exec", side_effect=[mock_connect, mock_psql]):
        await adapter.connect()
        tables = await adapter.fetch_tables()

    assert tables == ["users", "orders"]


@pytest.mark.asyncio
async def test_fetch_databases(adapter):
    pods_json = _kubectl_pods_output(_make_pod("myservice-abc123"))
    mock_connect = AsyncMock()
    mock_connect.communicate.return_value = (pods_json.encode(), b"")
    mock_connect.returncode = 0

    psql_output = "analytics\nmydb\npostgres\n"
    mock_psql = AsyncMock()
    mock_psql.communicate.return_value = (psql_output.encode(), b"")
    mock_psql.returncode = 0

    with patch("asyncio.create_subprocess_exec", side_effect=[mock_connect, mock_psql]):
        await adapter.connect()
        databases = await adapter.fetch_databases()

    assert databases == ["analytics", "mydb", "postgres"]


@pytest.mark.asyncio
async def test_run_psql_error(adapter):
    pods_json = _kubectl_pods_output(_make_pod("myservice-abc123"))
    mock_connect = AsyncMock()
    mock_connect.communicate.return_value = (pods_json.encode(), b"")
    mock_connect.returncode = 0

    mock_psql = AsyncMock()
    mock_psql.communicate.return_value = (b"", b"ERROR: relation does not exist")
    mock_psql.returncode = 1

    with patch("asyncio.create_subprocess_exec", side_effect=[mock_connect, mock_psql]):
        await adapter.connect()
        with pytest.raises(RuntimeError, match="relation does not exist"):
            await adapter.fetch_tables()


@pytest.mark.asyncio
async def test_disconnect(adapter):
    pods_json = _kubectl_pods_output(_make_pod("myservice-abc123"))
    mock_connect = AsyncMock()
    mock_connect.communicate.return_value = (pods_json.encode(), b"")
    mock_connect.returncode = 0

    with patch("asyncio.create_subprocess_exec", return_value=mock_connect):
        await adapter.connect()
        assert adapter._resolved_pod is not None
        await adapter.disconnect()
        assert adapter._resolved_pod is None
