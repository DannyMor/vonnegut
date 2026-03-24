# Kubectl Exec Adapter & Schema Display Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a kubectl exec-based pod adapter, expand schema metadata with type categories, and upgrade the frontend schema display with type icons and constraint indicators.

**Architecture:** Replace untyped connection config dicts with Pydantic discriminated unions keyed on `type`. Add a new `PostgresExecAdapter` that runs psql via `kubectl exec` into a jump-box pod. Expand `ColumnSchema` to include category, default, foreign key, and unique. Frontend gets type icons mapped from backend-provided categories.

**Tech Stack:** Python 3.14, FastAPI, Pydantic v2 (discriminated unions), psycopg3, asyncio.subprocess, React 19, TypeScript, Lucide React, shadcn/ui

---

## File Structure

### New files
| File | Responsibility |
|---|---|
| `backend/src/vonnegut/adapters/pg_types.py` | Postgres type-to-category mapping, shared by both Postgres adapters |
| `backend/src/vonnegut/adapters/postgres_exec.py` | `PostgresExecAdapter` — kubectl exec + psql implementation of `DatabaseAdapter` |
| `backend/tests/test_pg_types.py` | Tests for type category mapping |
| `backend/tests/test_adapter_exec.py` | Tests for exec adapter (mocking subprocess only) |

### Modified files
| File | Changes |
|---|---|
| `backend/src/vonnegut/adapters/base.py` | Expand `ColumnSchema` (8 fields, rename `column` → `name`) |
| `backend/src/vonnegut/adapters/memory.py` | Update for new `ColumnSchema` fields |
| `backend/src/vonnegut/adapters/postgres_direct.py` | Add `from_config()`, update `fetch_schema()` for new fields, use `pg_types` |
| `backend/src/vonnegut/adapters/factory.py` | Registry-based dispatch, use `from_config()` |
| `backend/src/vonnegut/adapters/testing.py` | No changes needed (passes through to InMemoryAdapter) |
| `backend/src/vonnegut/models/connection.py` | Discriminated union, `type` inside config, remove manual validators, remove `local_port` |
| `backend/src/vonnegut/services/connection_manager.py` | Adapt to new config shape (type inside config) |
| `backend/src/vonnegut/routers/connections.py` | Use typed Pydantic models, remove top-level `type` |
| `backend/src/vonnegut/routers/explorer.py` | No structural changes (schema shape changes are in adapter) |
| `backend/src/vonnegut/database.py` | Remove `type` column CHECK constraint (type is now in config JSON) |
| `backend/tests/test_models_connection.py` | Rewrite for discriminated union |
| `backend/tests/test_adapter_base.py` | Update for new `ColumnSchema` fields |
| `backend/tests/test_adapter_memory.py` | Update for new `ColumnSchema` fields |
| `backend/tests/test_api_connections.py` | Update payloads (type inside config) |
| `backend/tests/test_api_explorer.py` | Update `ColumnSchema` constructors, schema response assertions |
| `backend/tests/test_connection_manager.py` | Update payloads |
| `backend/tests/test_connection_encryption.py` | Update payloads if needed |
| `backend/tests/test_connection_test.py` | Update payloads |
| `backend/tests/test_api_migrations.py` | Update connection payloads |
| `backend/tests/test_migration_runner.py` | Update `ColumnSchema` if referenced |
| `frontend/src/types/connection.ts` | Discriminated union types |
| `frontend/src/lib/api.ts` | Update schema type, connection create type |
| `frontend/src/config/iconRegistry.ts` | Add type category + constraint icons |
| `frontend/src/components/connections/ConnectionForm.tsx` | Two-section layout, type inside config |
| `frontend/src/components/connections/ConnectionList.tsx` | Read type from `config.type` |
| `frontend/src/pages/ExplorerPage.tsx` | Type icons, constraint indicators, richer schema display |

---

## Chunk 1: Backend Models & Adapter Foundation

### Task 1: Expand ColumnSchema

**Files:**
- Modify: `backend/src/vonnegut/adapters/base.py`
- Modify: `backend/tests/test_adapter_base.py`

- [ ] **Step 1: Update ColumnSchema dataclass**

In `backend/src/vonnegut/adapters/base.py`, replace the existing `ColumnSchema`:

```python
@dataclass
class ColumnSchema:
    name: str
    type: str
    category: str
    nullable: bool
    default: str | None
    is_primary_key: bool
    foreign_key: str | None   # "table.column" or None
    is_unique: bool
```

- [ ] **Step 2: Update base test**

Replace the contents of `backend/tests/test_adapter_base.py`:

```python
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
```

- [ ] **Step 3: Run tests to verify**

Run: `cd /Users/dannymor/mydev/vonnegut/backend && uv run pytest tests/test_adapter_base.py -v`
Expected: PASS (base tests pass, but other tests using old ColumnSchema fields will fail — that's expected)

---

### Task 2: Update InMemoryAdapter for new ColumnSchema

**Files:**
- Modify: `backend/src/vonnegut/adapters/memory.py`
- Modify: `backend/tests/test_adapter_memory.py`

- [ ] **Step 1: Update InMemoryAdapter**

In `backend/src/vonnegut/adapters/memory.py`, update the `add_table` helper to generate schemas with new fields:

```python
def add_table(self, name: str, rows: list[dict]) -> None:
    """Helper for tests — add a table with rows (auto-generates schema)."""
    if rows:
        schema = [
            ColumnSchema(
                name=col, type="text", category="text",
                nullable=True, default=None,
                is_primary_key=False, foreign_key=None, is_unique=False,
            )
            for col in rows[0].keys()
        ]
    else:
        schema = []
    self._tables[name] = {"schema": schema, "rows": list(rows)}
```

Also update `fetch_schema` to return the schema as-is (no changes needed — it already returns `list[ColumnSchema]`).

- [ ] **Step 2: Update memory adapter tests**

Replace the contents of `backend/tests/test_adapter_memory.py`:

```python
import pytest

from vonnegut.adapters.memory import InMemoryAdapter
from vonnegut.adapters.base import DatabaseAdapter, ColumnSchema


@pytest.fixture
def adapter():
    tables = {
        "users": {
            "schema": [
                ColumnSchema(name="id", type="int4", category="number", nullable=False, default=None, is_primary_key=True, foreign_key=None, is_unique=False),
                ColumnSchema(name="name", type="text", category="text", nullable=True, default=None, is_primary_key=False, foreign_key=None, is_unique=False),
                ColumnSchema(name="email", type="text", category="text", nullable=False, default=None, is_primary_key=False, foreign_key=None, is_unique=True),
            ],
            "rows": [
                {"id": 1, "name": "Alice", "email": "alice@example.com"},
                {"id": 2, "name": "Bob", "email": "bob@example.com"},
                {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
            ],
        },
        "orders": {
            "schema": [
                ColumnSchema(name="id", type="int4", category="number", nullable=False, default=None, is_primary_key=True, foreign_key=None, is_unique=False),
                ColumnSchema(name="user_id", type="int4", category="number", nullable=False, default=None, is_primary_key=False, foreign_key="users.id", is_unique=False),
                ColumnSchema(name="amount", type="numeric", category="number", nullable=False, default=None, is_primary_key=False, foreign_key=None, is_unique=False),
            ],
            "rows": [
                {"id": 1, "user_id": 1, "amount": 99.99},
            ],
        },
    }
    return InMemoryAdapter(tables=tables)


def test_implements_database_adapter(adapter):
    assert isinstance(adapter, DatabaseAdapter)


@pytest.mark.asyncio
async def test_connect_disconnect(adapter):
    await adapter.connect()
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_fetch_tables(adapter):
    tables = await adapter.fetch_tables()
    assert set(tables) == {"users", "orders"}


@pytest.mark.asyncio
async def test_fetch_schema(adapter):
    schema = await adapter.fetch_schema("users")
    assert len(schema) == 3
    assert isinstance(schema[0], ColumnSchema)
    assert schema[0].name == "id"
    assert schema[0].type == "int4"
    assert schema[0].category == "number"
    assert schema[0].is_primary_key is True
    assert schema[1].nullable is True
    assert schema[2].is_unique is True


@pytest.mark.asyncio
async def test_fetch_schema_foreign_key(adapter):
    schema = await adapter.fetch_schema("orders")
    user_id_col = next(c for c in schema if c.name == "user_id")
    assert user_id_col.foreign_key == "users.id"


@pytest.mark.asyncio
async def test_fetch_sample(adapter):
    rows = await adapter.fetch_sample("users", rows=2)
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_fetch_sample_all_rows(adapter):
    rows = await adapter.fetch_sample("users", rows=100)
    assert len(rows) == 3


@pytest.mark.asyncio
async def test_execute_select(adapter):
    rows = await adapter.execute("SELECT * FROM users")
    assert len(rows) == 3


@pytest.mark.asyncio
async def test_execute_count(adapter):
    rows = await adapter.execute("SELECT COUNT(*) as count FROM users")
    assert rows[0]["count"] == 3


@pytest.mark.asyncio
async def test_execute_insert(adapter):
    await adapter.execute(
        "INSERT INTO users VALUES (%s, %s, %s)",
        (4, "Diana", "diana@example.com"),
    )
    rows = await adapter.fetch_sample("users", rows=100)
    assert len(rows) == 4


@pytest.mark.asyncio
async def test_execute_truncate(adapter):
    await adapter.execute("TRUNCATE TABLE users")
    rows = await adapter.fetch_sample("users", rows=100)
    assert len(rows) == 0


@pytest.mark.asyncio
async def test_fetch_databases():
    adapter = InMemoryAdapter()
    adapter.add_database("analytics")
    adapter.add_database("production")
    result = await adapter.fetch_databases()
    assert result == ["analytics", "production"]


@pytest.mark.asyncio
async def test_fetch_databases_empty():
    adapter = InMemoryAdapter()
    result = await adapter.fetch_databases()
    assert result == []


@pytest.mark.asyncio
async def test_add_table_auto_schema():
    adapter = InMemoryAdapter()
    adapter.add_table("items", [{"id": 1, "title": "Book"}])
    schema = await adapter.fetch_schema("items")
    assert len(schema) == 2
    assert schema[0].name == "id"
    assert schema[0].category == "text"  # auto-generated defaults to text
    assert schema[0].foreign_key is None
```

- [ ] **Step 3: Run tests**

Run: `cd /Users/dannymor/mydev/vonnegut/backend && uv run pytest tests/test_adapter_base.py tests/test_adapter_memory.py -v`
Expected: ALL PASS

---

### Task 3: Postgres type-to-category mapping

**Files:**
- Create: `backend/src/vonnegut/adapters/pg_types.py`
- Create: `backend/tests/test_pg_types.py`

- [ ] **Step 1: Write the tests**

Create `backend/tests/test_pg_types.py`:

```python
import pytest

from vonnegut.adapters.pg_types import pg_type_category


@pytest.mark.parametrize("pg_type,expected", [
    ("int2", "number"),
    ("int4", "number"),
    ("int8", "number"),
    ("float4", "number"),
    ("float8", "number"),
    ("numeric", "number"),
    ("varchar", "text"),
    ("text", "text"),
    ("char", "text"),
    ("bpchar", "text"),
    ("timestamp", "datetime"),
    ("timestamptz", "datetime"),
    ("date", "datetime"),
    ("time", "datetime"),
    ("timetz", "datetime"),
    ("bool", "boolean"),
    ("json", "json"),
    ("jsonb", "json"),
    ("uuid", "uuid"),
    ("_int4", "array"),
    ("_text", "array"),
    ("_uuid", "array"),
    ("bytea", "binary"),
    ("somecustomtype", "unknown"),
])
def test_pg_type_category(pg_type, expected):
    assert pg_type_category(pg_type) == expected
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dannymor/mydev/vonnegut/backend && uv run pytest tests/test_pg_types.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement the mapping**

Create `backend/src/vonnegut/adapters/pg_types.py`:

```python
_NUMBER_TYPES = {"int2", "int4", "int8", "float4", "float8", "numeric", "money", "serial", "bigserial"}
_TEXT_TYPES = {"varchar", "text", "char", "bpchar", "name", "citext"}
_DATETIME_TYPES = {"timestamp", "timestamptz", "date", "time", "timetz", "interval"}
_BOOLEAN_TYPES = {"bool"}
_JSON_TYPES = {"json", "jsonb"}
_UUID_TYPES = {"uuid"}
_BINARY_TYPES = {"bytea"}


def pg_type_category(pg_type: str) -> str:
    """Map a Postgres udt_name to a standard type category."""
    if pg_type.startswith("_"):
        return "array"
    if pg_type in _NUMBER_TYPES:
        return "number"
    if pg_type in _TEXT_TYPES:
        return "text"
    if pg_type in _DATETIME_TYPES:
        return "datetime"
    if pg_type in _BOOLEAN_TYPES:
        return "boolean"
    if pg_type in _JSON_TYPES:
        return "json"
    if pg_type in _UUID_TYPES:
        return "uuid"
    if pg_type in _BINARY_TYPES:
        return "binary"
    return "unknown"
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/dannymor/mydev/vonnegut/backend && uv run pytest tests/test_pg_types.py -v`
Expected: ALL PASS

---

### Task 4: Discriminated union connection models

**Files:**
- Modify: `backend/src/vonnegut/models/connection.py`
- Modify: `backend/tests/test_models_connection.py`

- [ ] **Step 1: Rewrite connection models**

Replace the contents of `backend/src/vonnegut/models/connection.py`:

```python
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Discriminator, Tag, model_validator

from vonnegut.encryption import encrypt, decrypt

_SENSITIVE_FIELDS = {"password"}


def encrypt_config(config: dict, key: str) -> dict:
    result = dict(config)
    for field in _SENSITIVE_FIELDS:
        if field in result:
            result[field] = encrypt(result[field], key)
    return result


def decrypt_config(config: dict, key: str) -> dict:
    result = dict(config)
    for field in _SENSITIVE_FIELDS:
        if field in result:
            result[field] = decrypt(result[field], key)
    return result


class PostgresDirectConfig(BaseModel):
    type: Literal["postgres_direct"]
    host: str
    port: int = 5432
    database: str
    user: str
    password: str


class PostgresPodConfig(BaseModel):
    type: Literal["postgres_pod"]
    namespace: str
    pod_selector: str
    pick_strategy: Literal["first_ready", "name_contains"] = "first_ready"
    pick_filter: str | None = None
    container: str | None = None
    host: str
    port: int = 5432
    database: str = ""
    user: str
    password: str


ConnectionConfig = Annotated[
    Union[
        Annotated[PostgresDirectConfig, Tag("postgres_direct")],
        Annotated[PostgresPodConfig, Tag("postgres_pod")],
    ],
    Discriminator("type"),
]


class ConnectionCreate(BaseModel):
    name: str
    config: ConnectionConfig


class ConnectionUpdate(BaseModel):
    name: str | None = None
    config: dict | None = None


class ConnectionResponse(BaseModel):
    id: str
    name: str
    config: dict
    created_at: str
    updated_at: str

    @model_validator(mode="after")
    def mask_password(self):
        if "password" in self.config:
            self.config = {**self.config, "password": "********"}
        return self
```

Note: `Connection` model is removed (was unused outside of the response). `ConnectionResponse.config` stays as `dict` because it's the masked output — we don't need typed validation on the response side. The `type` field is removed from `ConnectionResponse` top-level since it lives inside `config`.

- [ ] **Step 2: Rewrite connection model tests**

Replace the contents of `backend/tests/test_models_connection.py`:

```python
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
```

- [ ] **Step 3: Run tests**

Run: `cd /Users/dannymor/mydev/vonnegut/backend && uv run pytest tests/test_models_connection.py -v`
Expected: ALL PASS

---

### Task 5: Update ConnectionManager and database schema

**Files:**
- Modify: `backend/src/vonnegut/services/connection_manager.py`
- Modify: `backend/src/vonnegut/database.py`
- Modify: `backend/tests/test_connection_manager.py`

- [ ] **Step 1: Update database schema**

In `backend/src/vonnegut/database.py`, remove the `type` column from the connections table. The type now lives inside the config JSON. Update the `_SCHEMA`:

```sql
CREATE TABLE IF NOT EXISTS connections (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    config TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
```

Remove the `type TEXT NOT NULL CHECK(type IN ('postgres_direct', 'postgres_pod')),` line.

- [ ] **Step 2: Update ConnectionManager**

In `backend/src/vonnegut/services/connection_manager.py`, update `create()` to stop storing `type` as a separate column. The `type` is inside the config dict:

```python
import json
import uuid
from datetime import datetime, timezone

from vonnegut.database import Database
from vonnegut.models.connection import encrypt_config, decrypt_config


class ConnectionManager:
    def __init__(self, db: Database, encryption_key: str):
        self._db = db
        self._key = encryption_key

    async def create(self, name: str, config: dict) -> dict:
        conn_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        encrypted = encrypt_config(config, self._key)
        await self._db.execute(
            """INSERT INTO connections (id, name, config, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (conn_id, name, json.dumps(encrypted), now, now),
        )
        return {"id": conn_id, "name": name, "config": config, "created_at": now, "updated_at": now}

    async def list_all(self) -> list[dict]:
        rows = await self._db.fetch_all("SELECT * FROM connections ORDER BY created_at DESC")
        return rows

    async def get(self, conn_id: str) -> dict | None:
        row = await self._db.fetch_one("SELECT * FROM connections WHERE id = ?", (conn_id,))
        if row is None:
            return None
        row["config"] = json.loads(row["config"])
        row["config"] = decrypt_config(row["config"], self._key)
        return row

    async def update(self, conn_id: str, name: str | None = None, config: dict | None = None) -> dict | None:
        existing = await self.get(conn_id)
        if existing is None:
            return None
        new_name = name if name is not None else existing["name"]
        new_config = config if config is not None else existing["config"]
        now = datetime.now(timezone.utc).isoformat()
        encrypted = encrypt_config(new_config, self._key)
        await self._db.execute(
            "UPDATE connections SET name = ?, config = ?, updated_at = ? WHERE id = ?",
            (new_name, json.dumps(encrypted), now, conn_id),
        )
        return await self.get(conn_id)

    async def delete(self, conn_id: str) -> bool:
        existing = await self._db.fetch_one("SELECT id FROM connections WHERE id = ?", (conn_id,))
        if existing is None:
            return False
        await self._db.execute("DELETE FROM connections WHERE id = ?", (conn_id,))
        return True
```

- [ ] **Step 3: Update connection manager tests**

Read `backend/tests/test_connection_manager.py` and update all calls that pass `type=` as a separate argument. The `type` is now inside the `config` dict. Change calls like:

```python
# Before:
await manager.create(name="Test", type="postgres_direct", config={...})

# After:
await manager.create(name="Test", config={"type": "postgres_direct", ...})
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/dannymor/mydev/vonnegut/backend && uv run pytest tests/test_connection_manager.py -v`
Expected: ALL PASS

---

### Task 6: Update connections router

**Files:**
- Modify: `backend/src/vonnegut/routers/connections.py`
- Modify: `backend/tests/test_api_connections.py`

- [ ] **Step 1: Update the router**

In `backend/src/vonnegut/routers/connections.py`, update `create_connection` to pass config as a dict (no separate `type`):

```python
@router.post("/connections", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(body: ConnectionCreate, request: Request):
    manager = _get_manager(request)
    conn = await manager.create(name=body.name, config=body.config.model_dump())
    return ConnectionResponse(**conn)
```

Update `list_connections` — remove references to `row["type"]`:

```python
@router.get("/connections", response_model=list[ConnectionResponse])
async def list_connections(request: Request):
    manager = _get_manager(request)
    rows = await manager.list_all()
    result = []
    for row in rows:
        config = row["config"] if isinstance(row["config"], dict) else json.loads(row["config"])
        result.append(ConnectionResponse(
            id=row["id"], name=row["name"],
            config=config, created_at=row["created_at"], updated_at=row["updated_at"],
        ))
    return result
```

Update `get_connection` and `update_connection` similarly — remove `type` from the dict unpacking where needed.

- [ ] **Step 2: Update API connection tests**

In `backend/tests/test_api_connections.py`, update all JSON payloads to put `type` inside `config`:

```python
# Before:
{"name": "Test DB", "type": "postgres_direct", "config": {"host": "localhost", ...}}

# After:
{"name": "Test DB", "config": {"type": "postgres_direct", "host": "localhost", ...}}
```

Remove any assertions on `data["type"]` — type is now in `data["config"]["type"]`.

- [ ] **Step 3: Run tests**

Run: `cd /Users/dannymor/mydev/vonnegut/backend && uv run pytest tests/test_api_connections.py -v`
Expected: ALL PASS

---

### Task 7: Update remaining backend tests

**Files:**
- Modify: `backend/tests/test_api_explorer.py`
- Modify: `backend/tests/test_connection_encryption.py`
- Modify: `backend/tests/test_connection_test.py`
- Modify: `backend/tests/test_api_migrations.py`

- [ ] **Step 1: Update explorer API tests**

In `backend/tests/test_api_explorer.py`:
1. Update all `ColumnSchema(column=...,` to `ColumnSchema(name=..., category=..., default=None, foreign_key=None, is_unique=False,`
2. Update `_create_connection` payload to put `type` inside `config`
3. Update schema response assertions from `data[0]["column"]` to `data[0]["name"]`

- [ ] **Step 2: Update remaining test files**

Read and update `test_connection_encryption.py`, `test_connection_test.py`, and `test_api_migrations.py` — in each file, update connection payloads to put `type` inside `config`, and remove any references to a top-level `type` field.

- [ ] **Step 3: Run full test suite**

Run: `cd /Users/dannymor/mydev/vonnegut/backend && uv run pytest tests/ -v`
Expected: ALL PASS (all 89+ tests green)

---

### Task 8: Update PostgresDirectAdapter

**Files:**
- Modify: `backend/src/vonnegut/adapters/postgres_direct.py`
- Modify: `backend/src/vonnegut/adapters/factory.py`

- [ ] **Step 1: Add from_config and update fetch_schema**

In `backend/src/vonnegut/adapters/postgres_direct.py`:

```python
from typing import Any

from psycopg import AsyncConnection, sql

from vonnegut.adapters.base import ColumnSchema, DatabaseAdapter
from vonnegut.adapters.pg_types import pg_type_category


class PostgresDirectAdapter(DatabaseAdapter):
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self._conninfo = f"host={host} port={port} dbname={database} user={user} password={password}"
        self._conn: AsyncConnection | None = None

    @classmethod
    def from_config(cls, config: dict) -> "PostgresDirectAdapter":
        return cls(
            host=config["host"], port=config["port"], database=config["database"],
            user=config["user"], password=config["password"],
        )

    async def connect(self) -> None:
        self._conn = await AsyncConnection.connect(self._conninfo)

    async def disconnect(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def execute(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        cursor = await self._conn.execute(query, params)
        if cursor.description is None:
            return []
        columns = [desc[0] for desc in cursor.description]
        rows = await cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    async def fetch_tables(self) -> list[str]:
        cursor = await self._conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        )
        rows = await cursor.fetchall()
        return [row[0] for row in rows]

    async def fetch_schema(self, table: str) -> list[ColumnSchema]:
        cursor = await self._conn.execute(
            """
            SELECT
                c.column_name,
                c.udt_name,
                c.is_nullable,
                c.column_default,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_pk,
                fk.fk_ref,
                CASE WHEN uq.column_name IS NOT NULL THEN true ELSE false END as is_unique
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = %s
            ) pk ON c.column_name = pk.column_name
            LEFT JOIN (
                SELECT ku.column_name,
                       ccu.table_name || '.' || ccu.column_name AS fk_ref
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY' AND ku.table_name = %s
            ) fk ON c.column_name = fk.column_name
            LEFT JOIN (
                SELECT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                WHERE tc.constraint_type = 'UNIQUE' AND tc.table_name = %s
            ) uq ON c.column_name = uq.column_name
            WHERE c.table_schema = 'public' AND c.table_name = %s
            ORDER BY c.ordinal_position
            """,
            (table, table, table, table),
        )
        rows = await cursor.fetchall()
        return [
            ColumnSchema(
                name=r[0],
                type=r[1],
                category=pg_type_category(r[1]),
                nullable=r[2] == "YES",
                default=r[3],
                is_primary_key=bool(r[4]),
                foreign_key=r[5],
                is_unique=bool(r[6]),
            )
            for r in rows
        ]

    async def fetch_sample(self, table: str, rows: int = 10) -> list[dict[str, Any]]:
        query = sql.SQL("SELECT * FROM {} LIMIT %s").format(sql.Identifier(table))
        cursor = await self._conn.execute(query, (rows,))
        columns = [desc[0] for desc in cursor.description]
        result = await cursor.fetchall()
        return [dict(zip(columns, row)) for row in result]

    async def fetch_databases(self) -> list[str]:
        cursor = await self._conn.execute(
            "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
        )
        rows = await cursor.fetchall()
        return [row[0] for row in rows]
```

- [ ] **Step 2: Update factory to registry-based dispatch**

Replace the contents of `backend/src/vonnegut/adapters/factory.py`:

```python
import json as json_mod

from vonnegut.adapters.base import AdapterFactory, DatabaseAdapter
from vonnegut.adapters.postgres_direct import PostgresDirectAdapter

_adapter_registry: dict[str, type] = {
    "postgres_direct": PostgresDirectAdapter,
}


class DefaultAdapterFactory:
    """Production adapter factory — registry-based dispatch."""

    async def create(self, connection: dict) -> DatabaseAdapter:
        config = connection["config"] if isinstance(connection["config"], dict) else json_mod.loads(connection["config"])
        conn_type = config.get("type")
        adapter_cls = _adapter_registry.get(conn_type)
        if adapter_cls is None:
            raise ValueError(f"Unsupported connection type: {conn_type}")
        adapter = adapter_cls.from_config(config)
        await adapter.connect()
        return adapter
```

Note: `from_config()` on each adapter accepts a `dict` (not a Pydantic model) since configs come from the DB as dicts. The `PostgresDirectAdapter.from_config` in Step 1 already uses dict access (`config["host"]`, etc.).

- [ ] **Step 3: Run full test suite**

Run: `cd /Users/dannymor/mydev/vonnegut/backend && uv run pytest tests/ -v`
Expected: ALL PASS

---

## Chunk 2: Kubectl Exec Adapter

### Task 9: PostgresExecAdapter — pod resolution

**Files:**
- Create: `backend/src/vonnegut/adapters/postgres_exec.py`
- Create: `backend/tests/test_adapter_exec.py`

- [ ] **Step 1: Write pod resolution tests**

Create `backend/tests/test_adapter_exec.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dannymor/mydev/vonnegut/backend && uv run pytest tests/test_adapter_exec.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement PostgresExecAdapter — pod resolution**

Create `backend/src/vonnegut/adapters/postgres_exec.py`:

```python
import asyncio
import csv
import io
import json
from typing import Any
from urllib.parse import quote

from vonnegut.adapters.base import ColumnSchema, DatabaseAdapter
from vonnegut.adapters.pg_types import pg_type_category

_DEFAULT_TIMEOUT = 30


class PostgresExecAdapter(DatabaseAdapter):
    def __init__(
        self,
        namespace: str,
        pod_selector: str,
        pick_strategy: str,
        pick_filter: str | None,
        container: str | None,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        timeout: int = _DEFAULT_TIMEOUT,
    ):
        self._namespace = namespace
        self._pod_selector = pod_selector
        self._pick_strategy = pick_strategy
        self._pick_filter = pick_filter
        self._container = container
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._timeout = timeout
        self._resolved_pod: str | None = None

    @classmethod
    def from_config(cls, config: dict) -> "PostgresExecAdapter":
        return cls(
            namespace=config["namespace"],
            pod_selector=config["pod_selector"],
            pick_strategy=config.get("pick_strategy", "first_ready"),
            pick_filter=config.get("pick_filter"),
            container=config.get("container"),
            host=config["host"],
            port=config.get("port", 5432),
            database=config.get("database", ""),
            user=config["user"],
            password=config["password"],
        )

    async def connect(self) -> None:
        """Resolve a pod using label selectors."""
        try:
            process = await asyncio.create_subprocess_exec(
                "kubectl", "get", "pods",
                "-n", self._namespace,
                "-l", self._pod_selector,
                "-o", "json",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            raise ConnectionError("kubectl not found on PATH")

        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise ConnectionError(f"kubectl failed: {stderr.decode().strip()}")

        data = json.loads(stdout.decode())
        pods = data.get("items", [])

        # Filter to Running + Ready
        ready_pods = []
        for pod in pods:
            phase = pod.get("status", {}).get("phase")
            conditions = pod.get("status", {}).get("conditions", [])
            is_ready = any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in conditions
            )
            if phase == "Running" and is_ready:
                ready_pods.append(pod)

        # Apply pick strategy
        if self._pick_strategy == "name_contains" and self._pick_filter:
            ready_pods = [
                p for p in ready_pods
                if self._pick_filter in p["metadata"]["name"]
            ]

        if not ready_pods:
            raise ConnectionError(
                f"No ready pods matching selector '{self._pod_selector}' "
                f"in namespace '{self._namespace}'"
            )

        self._resolved_pod = ready_pods[0]["metadata"]["name"]

    async def disconnect(self) -> None:
        self._resolved_pod = None

    def _psql_uri(self, database: str | None = None) -> str:
        db = database or self._database
        pw = quote(self._password, safe="")
        return f"postgresql://{self._user}:{pw}@{self._host}:{self._port}/{db}"

    @staticmethod
    def _validate_identifier(name: str) -> str:
        """Validate a SQL identifier (table name) to prevent injection."""
        if not name.isidentifier() and not all(c.isalnum() or c == "_" for c in name):
            raise ValueError(f"Invalid identifier: {name}")
        return name

    async def _run_psql(
        self, query: str, database: str | None = None, include_headers: bool = False,
    ) -> str:
        """Execute a psql command via kubectl exec and return stdout.

        Args:
            query: SQL to execute
            database: Override database (e.g., "postgres" for fetch_databases)
            include_headers: If True, use --csv (with header row). If False, use --csv -t (no headers).
        """
        if self._resolved_pod is None:
            raise RuntimeError("Not connected — call connect() first")

        cmd = [
            "kubectl", "exec",
            "-n", self._namespace,
            self._resolved_pod,
        ]
        if self._container:
            cmd.extend(["-c", self._container])

        psql_flags = ["--csv", "-c", query]
        if not include_headers:
            psql_flags.insert(1, "-t")

        cmd.extend(["--", "psql", self._psql_uri(database)] + psql_flags)

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=self._timeout,
            )
        except asyncio.TimeoutError:
            process.kill()
            raise RuntimeError(f"psql timed out after {self._timeout}s")

        if process.returncode != 0:
            raise RuntimeError(f"psql error: {stderr.decode().strip()}")

        return stdout.decode()

    def _parse_csv_rows(self, output: str) -> list[dict[str, Any]]:
        """Parse psql --csv output (with header row) into list of dicts."""
        text = output.strip()
        if not text:
            return []
        reader = csv.DictReader(io.StringIO(text))
        return [dict(row) for row in reader]

    async def execute(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        if params:
            raise NotImplementedError(
                "PostgresExecAdapter does not support parameterized queries. "
                "Pre-format the query string instead."
            )
        output = await self._run_psql(query, include_headers=True)
        return self._parse_csv_rows(output)

    async def fetch_tables(self) -> list[str]:
        output = await self._run_psql(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        )
        return [line.strip() for line in output.strip().splitlines() if line.strip()]

    async def fetch_schema(self, table: str) -> list[ColumnSchema]:
        safe_table = self._validate_identifier(table)
        query = f"""
            SELECT
                c.column_name,
                c.udt_name,
                c.is_nullable,
                c.column_default,
                CASE WHEN pk.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END as is_pk,
                fk.fk_ref,
                CASE WHEN uq.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END as is_unique
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = '{safe_table}'
            ) pk ON c.column_name = pk.column_name
            LEFT JOIN (
                SELECT ku.column_name,
                       ccu.table_name || '.' || ccu.column_name AS fk_ref
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY' AND ku.table_name = '{safe_table}'
            ) fk ON c.column_name = fk.column_name
            LEFT JOIN (
                SELECT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                WHERE tc.constraint_type = 'UNIQUE' AND tc.table_name = '{safe_table}'
            ) uq ON c.column_name = uq.column_name
            WHERE c.table_schema = 'public' AND c.table_name = '{safe_table}'
            ORDER BY c.ordinal_position
        """
        output = await self._run_psql(query, include_headers=True)
        rows = self._parse_csv_rows(output)
        return [
            ColumnSchema(
                name=r["column_name"],
                type=r["udt_name"],
                category=pg_type_category(r["udt_name"]),
                nullable=r["is_nullable"] == "YES",
                default=r["column_default"] if r["column_default"] else None,
                is_primary_key=r["is_pk"] == "YES",
                foreign_key=r["fk_ref"] if r.get("fk_ref") else None,
                is_unique=r["is_unique"] == "YES",
            )
            for r in rows
        ]

    async def fetch_sample(self, table: str, rows: int = 10) -> list[dict[str, Any]]:
        safe_table = self._validate_identifier(table)
        output = await self._run_psql(
            f'SELECT * FROM "{safe_table}" LIMIT {rows}', include_headers=True,
        )
        return self._parse_csv_rows(output)

    async def fetch_databases(self) -> list[str]:
        output = await self._run_psql(
            "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname",
            database="postgres",
        )
        return [line.strip() for line in output.strip().splitlines() if line.strip()]
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/dannymor/mydev/vonnegut/backend && uv run pytest tests/test_adapter_exec.py -v`
Expected: ALL PASS

---

### Task 10: PostgresExecAdapter — query execution tests

**Files:**
- Modify: `backend/tests/test_adapter_exec.py`

- [ ] **Step 1: Add query execution tests**

Append to `backend/tests/test_adapter_exec.py`:

```python
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
```

- [ ] **Step 2: Run tests**

Run: `cd /Users/dannymor/mydev/vonnegut/backend && uv run pytest tests/test_adapter_exec.py -v`
Expected: ALL PASS

---

### Task 11: Register exec adapter in factory

**Files:**
- Modify: `backend/src/vonnegut/adapters/factory.py`

- [ ] **Step 1: Add postgres_pod to registry**

In `backend/src/vonnegut/adapters/factory.py`, add the import and registry entry:

```python
import json as json_mod

from vonnegut.adapters.base import AdapterFactory, DatabaseAdapter
from vonnegut.adapters.postgres_direct import PostgresDirectAdapter
from vonnegut.adapters.postgres_exec import PostgresExecAdapter

_adapter_registry: dict[str, type] = {
    "postgres_direct": PostgresDirectAdapter,
    "postgres_pod": PostgresExecAdapter,
}


class DefaultAdapterFactory:
    """Production adapter factory — registry-based dispatch."""

    async def create(self, connection: dict) -> DatabaseAdapter:
        config = connection["config"] if isinstance(connection["config"], dict) else json_mod.loads(connection["config"])
        conn_type = config.get("type")
        adapter_cls = _adapter_registry.get(conn_type)
        if adapter_cls is None:
            raise ValueError(f"Unsupported connection type: {conn_type}")
        adapter = adapter_cls.from_config(config)
        await adapter.connect()
        return adapter
```

- [ ] **Step 2: Run full backend test suite**

Run: `cd /Users/dannymor/mydev/vonnegut/backend && uv run pytest tests/ -v`
Expected: ALL PASS

---

## Chunk 3: Frontend Changes

### Task 12: Frontend connection types

**Files:**
- Modify: `frontend/src/types/connection.ts`

- [ ] **Step 1: Update types to discriminated union**

Replace the contents of `frontend/src/types/connection.ts`:

```typescript
export interface PostgresDirectConfig {
  type: "postgres_direct";
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

export interface PostgresPodConfig {
  type: "postgres_pod";
  namespace: string;
  pod_selector: string;
  pick_strategy: "first_ready" | "name_contains";
  pick_filter?: string;
  container?: string;
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

export type ConnectionConfig = PostgresDirectConfig | PostgresPodConfig;

export interface Connection {
  id: string;
  name: string;
  config: ConnectionConfig;
  created_at: string;
  updated_at: string;
}

export interface ConnectionCreate {
  name: string;
  config: ConnectionConfig;
}

export interface ConnectionTestResult {
  status: "ok" | "error";
  message: string;
}

export interface ColumnSchema {
  name: string;
  type: string;
  category: string;
  nullable: boolean;
  default: string | null;
  is_primary_key: boolean;
  foreign_key: string | null;
  is_unique: boolean;
}
```

---

### Task 13: Update API client schema type

**Files:**
- Modify: `frontend/src/lib/api.ts`

- [ ] **Step 1: Update schema return type**

In `frontend/src/lib/api.ts`, update the import and the schema method:

```typescript
import type { Connection, ConnectionCreate, ConnectionTestResult, ColumnSchema } from "@/types/connection";
```

Update the `schema` method:

```typescript
schema: (id: string, table: string) =>
    request<ColumnSchema[]>(`/connections/${id}/tables/${table}/schema`),
```

---

### Task 14: Add type icons to icon registry

**Files:**
- Modify: `frontend/src/config/iconRegistry.ts`

- [ ] **Step 1: Add type category and constraint icons**

Replace the contents of `frontend/src/config/iconRegistry.ts`:

```typescript
import {
  Database,
  DatabaseZap,
  ArrowRightLeft,
  Code,
  Sparkles,
  CircleCheck,
  CircleX,
  Plug,
  Search,
  Workflow,
  Hash,
  Type,
  Calendar,
  ToggleLeft,
  Braces,
  Fingerprint,
  List,
  FileDigit,
  CircleHelp,
  Key,
  Link,
  Snowflake,
  type LucideIcon,
} from "lucide-react";

export const icons: Record<string, LucideIcon> = {
  // Existing
  source: Database,
  target: DatabaseZap,
  column_mapping: ArrowRightLeft,
  sql_expression: Code,
  ai_generated: Sparkles,
  connection_ok: CircleCheck,
  connection_error: CircleX,
  nav_connections: Plug,
  nav_explorer: Search,
  nav_migrations: Workflow,
  // Type categories
  type_number: Hash,
  type_text: Type,
  type_datetime: Calendar,
  type_boolean: ToggleLeft,
  type_json: Braces,
  type_uuid: Fingerprint,
  type_array: List,
  type_binary: FileDigit,
  type_unknown: CircleHelp,
  // Constraints
  constraint_pk: Key,
  constraint_fk: Link,
  constraint_unique: Snowflake,
};
```

---

### Task 15: Update ConnectionForm with two-section layout

**Files:**
- Modify: `frontend/src/components/connections/ConnectionForm.tsx`

- [ ] **Step 1: Rewrite ConnectionForm**

Replace the contents of `frontend/src/components/connections/ConnectionForm.tsx` with a form that:

1. Has a type selector at the top (`postgres_direct` / `postgres_pod`)
2. For `postgres_pod`, shows two bordered sections:
   - **"Pod Access"** section: Namespace, Pod Selector, Pick Strategy, Pick Filter (conditional), Container (optional)
   - **"Database"** section: Host, Port, User, Password, Database (with discover)
3. For `postgres_direct`, shows only the **"Database"** section: Host, Port, User, Password, Database (with discover)
4. The `type` field is placed inside the config when submitting
5. Uses `fieldset` with `legend` or a bordered `div` with a label for section grouping

Key implementation details:

```tsx
// State
const [connType, setConnType] = useState<"postgres_direct" | "postgres_pod">(
    initial?.config.type ?? "postgres_direct"
);

// Submit handler builds config with type inside:
const handleSubmit = () => {
    const baseConfig = { type: connType, host, port: Number(port), database, user, password };
    const config = connType === "postgres_direct"
        ? baseConfig
        : {
            ...baseConfig,
            namespace,
            pod_selector: podSelector,
            pick_strategy: pickStrategy,
            ...(pickStrategy === "name_contains" && pickFilter ? { pick_filter: pickFilter } : {}),
            ...(container ? { container } : {}),
          };
    onSave({ name, config });
    onClose();
};
```

Section styling:

```tsx
<div className="rounded-lg border p-4">
    <h3 className="text-sm font-medium mb-3 text-muted-foreground">Pod Access</h3>
    {/* pod fields */}
</div>
<div className="rounded-lg border p-4">
    <h3 className="text-sm font-medium mb-3 text-muted-foreground">Database</h3>
    {/* database fields */}
</div>
```

For database discovery, keep the existing discover button logic but update the initial state reading to use `initial?.config.type` instead of a separate `initial?.type`.

- [ ] **Step 2: Verify build**

Run: `cd /Users/dannymor/mydev/vonnegut/frontend && npm run build`
Expected: Build succeeds

---

### Task 16: Update ConnectionList

**Files:**
- Modify: `frontend/src/components/connections/ConnectionList.tsx`

- [ ] **Step 1: Update type access**

In `frontend/src/components/connections/ConnectionList.tsx`, update the subtitle and badge to read from `config.type`:

```tsx
<div className="text-sm text-muted-foreground">
    {conn.config.type === "postgres_direct"
        ? `${conn.config.host}:${conn.config.port}/${conn.config.database}`
        : `${conn.config.namespace} | ${conn.config.pod_selector} → ${conn.config.host}`}
</div>
```

Update the badge:

```tsx
<Badge variant="outline">{conn.config.type.replace("postgres_", "")}</Badge>
```

---

### Task 17: Update ExplorerPage with type icons and rich schema

**Files:**
- Modify: `frontend/src/pages/ExplorerPage.tsx`

- [ ] **Step 1: Update schema display**

In `frontend/src/pages/ExplorerPage.tsx`:

1. Import `icons` and `ColumnSchema` type
2. Update the schema state type to use `ColumnSchema`
3. Replace the schema table with a richer display:

```tsx
import type { ColumnSchema } from "@/types/connection";
import { icons } from "@/config/iconRegistry";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
```

Update state:

```tsx
const [schema, setSchema] = useState<ColumnSchema[]>([]);
```

Replace the schema table body:

```tsx
<TableBody>
    {schema.map((col) => {
        const TypeIcon = icons[`type_${col.category}`] || icons.type_unknown;
        return (
            <TableRow key={col.name}>
                <TableCell>
                    <div className="flex items-center gap-2">
                        <TypeIcon className="h-4 w-4 text-muted-foreground" />
                        <span className={col.nullable ? "text-muted-foreground" : "font-medium"}>
                            {col.name}
                        </span>
                    </div>
                </TableCell>
                <TableCell><Badge variant="outline">{col.type}</Badge></TableCell>
                <TableCell>
                    {col.default && (
                        <span className="text-xs text-muted-foreground font-mono">{col.default}</span>
                    )}
                </TableCell>
                <TableCell>
                    <div className="flex items-center gap-1">
                        {col.is_primary_key && (
                            <icons.constraint_pk className="h-3.5 w-3.5 text-amber-500" />
                        )}
                        {col.foreign_key && (
                            <TooltipProvider>
                                <Tooltip>
                                    <TooltipTrigger>
                                        <icons.constraint_fk className="h-3.5 w-3.5 text-blue-500" />
                                    </TooltipTrigger>
                                    <TooltipContent>→ {col.foreign_key}</TooltipContent>
                                </Tooltip>
                            </TooltipProvider>
                        )}
                        {col.is_unique && (
                            <icons.constraint_unique className="h-3.5 w-3.5 text-purple-500" />
                        )}
                    </div>
                </TableCell>
            </TableRow>
        );
    })}
</TableBody>
```

Update table headers to match:

```tsx
<TableHeader>
    <TableRow>
        <TableHead>Column</TableHead>
        <TableHead>Type</TableHead>
        <TableHead>Default</TableHead>
        <TableHead>Constraints</TableHead>
    </TableRow>
</TableHeader>
```

- [ ] **Step 2: Verify build**

Run: `cd /Users/dannymor/mydev/vonnegut/frontend && npm run build`
Expected: Build succeeds

---

### Task 18: Final verification

- [ ] **Step 1: Run full backend test suite**

Run: `cd /Users/dannymor/mydev/vonnegut/backend && uv run pytest tests/ -v`
Expected: ALL PASS

- [ ] **Step 2: Run frontend build**

Run: `cd /Users/dannymor/mydev/vonnegut/frontend && npm run build`
Expected: Build succeeds with no type errors

- [ ] **Step 3: Verify frontend type check**

Run: `cd /Users/dannymor/mydev/vonnegut/frontend && npx tsc --noEmit`
Expected: No errors
