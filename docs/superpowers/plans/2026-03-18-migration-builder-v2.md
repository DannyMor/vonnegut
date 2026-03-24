# Migration Builder v2 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign the migration builder into a visual pipeline editor with React Flow graph, slide-up editor panel, and a backend pipeline engine that composes SQL via CTEs with DuckDB bridging for code transforms.

**Architecture:** Single page: React Flow graph (top) with horizontal L→R pipeline, slide-up three-column editor panel (bottom). Backend replaces flat transformations with ordered pipeline steps. Pipeline engine composes consecutive SQL nodes into CTEs, bridges to DuckDB when code nodes appear.

**Tech Stack:** FastAPI, SQLite, DuckDB, polars, React, TypeScript, React Flow (@xyflow/react), Tailwind CSS, shadcn/ui

**Spec:** `docs/superpowers/specs/2026-03-18-migration-builder-v2-design.md`

---

## File Structure

### Backend — Create

| File | Responsibility |
|------|---------------|
| `backend/src/vonnegut/models/pipeline.py` | StepType literal, ColumnDef, PipelineStep models (Create/Update/Response), SQLConfig/CodeConfig/AIConfig |
| `backend/src/vonnegut/routers/pipeline_steps.py` | CRUD endpoints for pipeline steps on a migration |
| `backend/src/vonnegut/services/cte_compiler.py` | CTE name normalization, SQL chain composition |
| `backend/src/vonnegut/services/pipeline_engine.py` | Pipeline execution: CTE chains, DuckDB bridge, schema validation |
| `backend/tests/test_models_pipeline.py` | Pipeline model validation tests |
| `backend/tests/test_api_pipeline_steps.py` | Pipeline step API tests |
| `backend/tests/test_cte_compiler.py` | CTE compiler unit tests |
| `backend/tests/test_pipeline_engine.py` | Pipeline engine integration tests |

### Backend — Modify

| File | Changes |
|------|---------|
| `backend/src/vonnegut/models/migration.py` | Add `source_query`, `source_schema` fields to models |
| `backend/src/vonnegut/database.py` | Add `pipeline_steps` table, add columns to `migrations` |
| `backend/src/vonnegut/routers/migrations.py` | Update test/run endpoints to use pipeline engine |
| `backend/src/vonnegut/routers/ai.py` | Update AI endpoint with pipeline context |
| `backend/src/vonnegut/main.py` | Register pipeline_steps router |

### Frontend — Create

| File | Responsibility |
|------|---------------|
| `frontend/src/types/pipeline.ts` | StepType, PipelineStep, ColumnDef, TestResult types |
| `frontend/src/components/migration-builder/edges/AddStepEdge.tsx` | Custom React Flow edge with "+" dropdown |
| `frontend/src/components/migration-builder/nodes/PipelineNode.tsx` | Unified node component for all step types (sql, code, ai) |
| `frontend/src/components/migration-builder/EditorPanel.tsx` | Slide-up three-column editor panel |
| `frontend/src/components/migration-builder/SchemaPanel.tsx` | Collapsible input/output schema sidebar |
| `frontend/src/components/migration-builder/editors/SourceEditor.tsx` | Connection + table + query editor |
| `frontend/src/components/migration-builder/editors/TargetEditor.tsx` | Connection + table + validation |
| `frontend/src/components/migration-builder/editors/SqlEditor.tsx` | SQL expression editor |
| `frontend/src/components/migration-builder/editors/CodeEditor.tsx` | Python code editor |
| `frontend/src/components/migration-builder/editors/AiEditor.tsx` | AI prompt + generate + approve |

### Frontend — Modify

| File | Changes |
|------|---------|
| `frontend/src/types/migration.ts` | Add source_query, source_schema, pipeline_steps |
| `frontend/src/lib/api.ts` | Add pipeline step CRUD + updated test API |
| `frontend/src/config/nodeTheme.ts` | Replace old transform types with sql/code/ai |
| `frontend/src/config/iconRegistry.ts` | Add icons for new node types |
| `frontend/src/components/migration-builder/Canvas.tsx` | Full rewrite: new nodes, edge buttons, layout |
| `frontend/src/components/migration-builder/nodes/SourceNode.tsx` | Simplified for new data model |
| `frontend/src/components/migration-builder/nodes/TargetNode.tsx` | Simplified for new data model |
| `frontend/src/pages/MigrationBuilderPage.tsx` | Full rewrite: orchestrate graph + editor |

### Frontend — Delete

| File | Reason |
|------|--------|
| `frontend/src/components/migration-builder/nodes/TransformNode.tsx` | Replaced by PipelineNode |
| `frontend/src/types/transformation.ts` | Replaced by pipeline.ts |

---

## Chunk 1: Backend Data Model & CRUD

### Task 1: Pipeline Step Models

**Files:**
- Create: `backend/src/vonnegut/models/pipeline.py`
- Test: `backend/tests/test_models_pipeline.py`

- [ ] **Step 1: Write tests for pipeline models**

```python
# backend/tests/test_models_pipeline.py
import pytest
from pydantic import ValidationError
from vonnegut.models.pipeline import (
    StepType, ColumnDef, SQLConfig, CodeConfig, AIConfig,
    PipelineStepCreate, PipelineStepUpdate, PipelineStepResponse,
)


def test_step_type_valid():
    step = PipelineStepCreate(step_type="sql", name="test", config={"expression": "SELECT 1"})
    assert step.step_type == "sql"


def test_step_type_invalid():
    with pytest.raises(ValidationError):
        PipelineStepCreate(step_type="invalid", name="test", config={})


def test_column_def():
    col = ColumnDef(name="id", type="integer")
    assert col.name == "id"
    assert col.type == "integer"


def test_sql_config():
    cfg = SQLConfig(expression="SELECT a, lower(b) as b FROM prev")
    assert cfg.expression == "SELECT a, lower(b) as b FROM prev"


def test_code_config():
    cfg = CodeConfig(function_code="def transform(df): return df")
    assert cfg.function_code == "def transform(df): return df"


def test_ai_config_unapproved():
    cfg = AIConfig(prompt="hash the email")
    assert cfg.approved is False
    assert cfg.generated_type is None
    assert cfg.generated_code is None


def test_ai_config_approved():
    cfg = AIConfig(
        prompt="hash the email",
        generated_type="sql",
        generated_code="SELECT md5(email) FROM prev",
        approved=True,
    )
    assert cfg.approved is True


def test_pipeline_step_response():
    resp = PipelineStepResponse(
        id="abc", migration_id="mig1", name="Lower Emails",
        description="Lowercase all emails", position=0,
        step_type="sql", config={"expression": "SELECT lower(email) FROM prev"},
        created_at="2026-01-01T00:00:00Z", updated_at="2026-01-01T00:00:00Z",
    )
    assert resp.step_type == "sql"
    assert resp.description == "Lowercase all emails"


def test_pipeline_step_update_partial():
    upd = PipelineStepUpdate(name="Renamed")
    assert upd.name == "Renamed"
    assert upd.config is None
    assert upd.description is None
```

- [ ] **Step 2: Run tests — expect FAIL (module not found)**

Run: `cd backend && uv run pytest tests/test_models_pipeline.py -v`

- [ ] **Step 3: Implement pipeline models**

```python
# backend/src/vonnegut/models/pipeline.py
from typing import Literal

from pydantic import BaseModel


StepType = Literal["sql", "code", "ai"]


class ColumnDef(BaseModel):
    name: str
    type: str


class SQLConfig(BaseModel):
    expression: str


class CodeConfig(BaseModel):
    function_code: str


class AIConfig(BaseModel):
    prompt: str
    generated_type: Literal["sql", "code"] | None = None
    generated_code: str | None = None
    approved: bool = False


class PipelineStepCreate(BaseModel):
    step_type: StepType
    name: str
    description: str | None = None
    config: dict
    insert_after: str | None = None  # node_id to insert after; None = append


class PipelineStepUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    step_type: StepType | None = None
    config: dict | None = None


class PipelineStepResponse(BaseModel):
    id: str
    migration_id: str
    name: str
    description: str | None
    position: int
    step_type: StepType
    config: dict
    created_at: str
    updated_at: str
```

- [ ] **Step 4: Run tests — expect PASS**

Run: `cd backend && uv run pytest tests/test_models_pipeline.py -v`

- [ ] **Step 5: Commit**

```bash
git add backend/src/vonnegut/models/pipeline.py backend/tests/test_models_pipeline.py
git commit -m "feat: add pipeline step models with StepType literal and configs"
```

---

### Task 2: Update Migration Model & Database Schema

**Files:**
- Modify: `backend/src/vonnegut/models/migration.py`
- Modify: `backend/src/vonnegut/database.py`

- [ ] **Step 1: Update migration models**

Add to `backend/src/vonnegut/models/migration.py`:

```python
# Add import at top
from vonnegut.models.pipeline import ColumnDef, PipelineStepResponse

# Add fields to MigrationCreate:
    source_query: str = ""
    source_schema: list[dict] = []  # [{ "name": "col", "type": "int" }]

# Add fields to MigrationUpdate:
    source_query: str | None = None
    source_schema: list[dict] | None = None

# Add fields to MigrationResponse:
    source_query: str
    source_schema: list[dict]
    pipeline_steps: list[PipelineStepResponse] = []
    # Keep existing transformations field for backwards compat during migration
```

- [ ] **Step 2: Update database schema**

In `backend/src/vonnegut/database.py`, update `_SCHEMA`:

```sql
-- Add to migrations table (after target_table):
    source_query TEXT NOT NULL DEFAULT '',
    source_schema TEXT NOT NULL DEFAULT '[]',

-- Add new table:
CREATE TABLE IF NOT EXISTS pipeline_steps (
    id TEXT PRIMARY KEY,
    migration_id TEXT NOT NULL REFERENCES migrations(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT,
    position INTEGER NOT NULL,
    step_type TEXT NOT NULL CHECK(step_type IN ('sql', 'code', 'ai')),
    config TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
```

- [ ] **Step 3: Update migrations router helpers**

In `backend/src/vonnegut/routers/migrations.py`, update `_migration_response` to include new fields and load pipeline_steps:

```python
async def _get_pipeline_steps(db, migration_id: str) -> list[PipelineStepResponse]:
    rows = await db.fetch_all(
        "SELECT * FROM pipeline_steps WHERE migration_id = ? ORDER BY position",
        (migration_id,),
    )
    return [
        PipelineStepResponse(
            id=r["id"], migration_id=r["migration_id"], name=r["name"],
            description=r["description"], position=r["position"],
            step_type=r["step_type"], config=json.loads(r["config"]),
            created_at=r["created_at"], updated_at=r["updated_at"],
        )
        for r in rows
    ]
```

Update `_migration_response` to include `source_query`, `source_schema` (json.loads), and `pipeline_steps`.

Update `create_migration` and `update_migration` to handle the new fields.

- [ ] **Step 4: Run all existing tests to verify no regressions**

Run: `cd backend && uv run pytest -v`

- [ ] **Step 5: Commit**

```bash
git add backend/src/vonnegut/models/migration.py backend/src/vonnegut/database.py backend/src/vonnegut/routers/migrations.py
git commit -m "feat: add source_query, source_schema to migration and pipeline_steps table"
```

---

### Task 3: Pipeline Steps CRUD Router

**Files:**
- Create: `backend/src/vonnegut/routers/pipeline_steps.py`
- Modify: `backend/src/vonnegut/main.py`
- Test: `backend/tests/test_api_pipeline_steps.py`

- [ ] **Step 1: Write API tests**

```python
# backend/tests/test_api_pipeline_steps.py
import pytest
import pytest_asyncio
from cryptography.fernet import Fernet
from httpx import ASGITransport, AsyncClient

from vonnegut.database import Database
from vonnegut.main import create_app


@pytest_asyncio.fixture
async def app(tmp_path):
    db = Database(f"sqlite+aiosqlite:///{tmp_path}/test.db")
    await db.initialize()
    key = Fernet.generate_key().decode()
    application = create_app(db=db, encryption_key=key)
    yield application
    await db.close()


@pytest_asyncio.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _create_migration(client) -> str:
    """Helper: create a migration and return its id."""
    # First create two connections
    conn_data = {"name": "test-conn", "config": {
        "type": "postgres_direct", "host": "localhost", "port": 5432,
        "database": "testdb", "user": "user", "password": "pass",
    }}
    r1 = await client.post("/api/v1/connections", json=conn_data)
    r2 = await client.post("/api/v1/connections", json={**conn_data, "name": "test-conn-2"})
    conn1_id = r1.json()["id"]
    conn2_id = r2.json()["id"]

    mig = await client.post("/api/v1/migrations", json={
        "name": "Test Migration",
        "source_connection_id": conn1_id,
        "target_connection_id": conn2_id,
        "source_table": "users",
        "target_table": "users_copy",
    })
    return mig.json()["id"]


async def test_add_pipeline_step(client):
    mig_id = await _create_migration(client)
    resp = await client.post(f"/api/v1/migrations/{mig_id}/steps", json={
        "step_type": "sql",
        "name": "Lower Emails",
        "config": {"expression": "SELECT lower(email) FROM prev"},
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["step_type"] == "sql"
    assert data["name"] == "Lower Emails"
    assert data["position"] == 0


async def test_add_step_with_description(client):
    mig_id = await _create_migration(client)
    resp = await client.post(f"/api/v1/migrations/{mig_id}/steps", json={
        "step_type": "code",
        "name": "Hash PII",
        "description": "Hashes personally identifiable information",
        "config": {"function_code": "def transform(df): return df"},
    })
    assert resp.status_code == 201
    assert resp.json()["description"] == "Hashes personally identifiable information"


async def test_add_multiple_steps_ordering(client):
    mig_id = await _create_migration(client)
    r1 = await client.post(f"/api/v1/migrations/{mig_id}/steps", json={
        "step_type": "sql", "name": "Step A", "config": {"expression": "SELECT 1"},
    })
    r2 = await client.post(f"/api/v1/migrations/{mig_id}/steps", json={
        "step_type": "sql", "name": "Step B", "config": {"expression": "SELECT 2"},
    })
    assert r1.json()["position"] == 0
    assert r2.json()["position"] == 1


async def test_insert_step_after(client):
    mig_id = await _create_migration(client)
    r1 = await client.post(f"/api/v1/migrations/{mig_id}/steps", json={
        "step_type": "sql", "name": "Step A", "config": {"expression": "SELECT 1"},
    })
    await client.post(f"/api/v1/migrations/{mig_id}/steps", json={
        "step_type": "sql", "name": "Step C", "config": {"expression": "SELECT 3"},
    })
    # Insert Step B after Step A
    step_a_id = r1.json()["id"]
    r3 = await client.post(f"/api/v1/migrations/{mig_id}/steps", json={
        "step_type": "sql", "name": "Step B",
        "config": {"expression": "SELECT 2"},
        "insert_after": step_a_id,
    })
    assert r3.json()["position"] == 1
    # Verify Step C moved to position 2
    mig = await client.get(f"/api/v1/migrations/{mig_id}")
    steps = mig.json()["pipeline_steps"]
    assert [s["name"] for s in steps] == ["Step A", "Step B", "Step C"]


async def test_update_pipeline_step(client):
    mig_id = await _create_migration(client)
    r = await client.post(f"/api/v1/migrations/{mig_id}/steps", json={
        "step_type": "sql", "name": "Old Name", "config": {"expression": "SELECT 1"},
    })
    step_id = r.json()["id"]
    resp = await client.put(f"/api/v1/migrations/{mig_id}/steps/{step_id}", json={
        "name": "New Name",
    })
    assert resp.status_code == 200
    assert resp.json()["name"] == "New Name"


async def test_delete_pipeline_step(client):
    mig_id = await _create_migration(client)
    r = await client.post(f"/api/v1/migrations/{mig_id}/steps", json={
        "step_type": "sql", "name": "Doomed", "config": {"expression": "SELECT 1"},
    })
    step_id = r.json()["id"]
    resp = await client.delete(f"/api/v1/migrations/{mig_id}/steps/{step_id}")
    assert resp.status_code == 204


async def test_delete_step_reorders_remaining(client):
    mig_id = await _create_migration(client)
    r1 = await client.post(f"/api/v1/migrations/{mig_id}/steps", json={
        "step_type": "sql", "name": "A", "config": {"expression": "SELECT 1"},
    })
    await client.post(f"/api/v1/migrations/{mig_id}/steps", json={
        "step_type": "sql", "name": "B", "config": {"expression": "SELECT 2"},
    })
    await client.post(f"/api/v1/migrations/{mig_id}/steps", json={
        "step_type": "sql", "name": "C", "config": {"expression": "SELECT 3"},
    })
    # Delete A
    await client.delete(f"/api/v1/migrations/{mig_id}/steps/{r1.json()['id']}")
    mig = await client.get(f"/api/v1/migrations/{mig_id}")
    steps = mig.json()["pipeline_steps"]
    assert [s["name"] for s in steps] == ["B", "C"]
    assert [s["position"] for s in steps] == [0, 1]


async def test_invalid_step_type(client):
    mig_id = await _create_migration(client)
    resp = await client.post(f"/api/v1/migrations/{mig_id}/steps", json={
        "step_type": "invalid", "name": "Bad", "config": {},
    })
    assert resp.status_code == 422


async def test_steps_included_in_migration_response(client):
    mig_id = await _create_migration(client)
    await client.post(f"/api/v1/migrations/{mig_id}/steps", json={
        "step_type": "sql", "name": "Step 1", "config": {"expression": "SELECT 1"},
    })
    mig = await client.get(f"/api/v1/migrations/{mig_id}")
    assert len(mig.json()["pipeline_steps"]) == 1
    assert mig.json()["pipeline_steps"][0]["name"] == "Step 1"
```

- [ ] **Step 2: Run tests — expect FAIL**

Run: `cd backend && uv run pytest tests/test_api_pipeline_steps.py -v`

- [ ] **Step 3: Implement pipeline steps router**

```python
# backend/src/vonnegut/routers/pipeline_steps.py
import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, status

from vonnegut.models.pipeline import PipelineStepCreate, PipelineStepResponse, PipelineStepUpdate

router = APIRouter(tags=["pipeline-steps"])


def _get_db(request: Request):
    return request.app.state.db


@router.post(
    "/migrations/{mig_id}/steps",
    response_model=PipelineStepResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_step(mig_id: str, body: PipelineStepCreate, request: Request):
    db = _get_db(request)
    mig = await db.fetch_one("SELECT id FROM migrations WHERE id = ?", (mig_id,))
    if mig is None:
        raise HTTPException(status_code=404, detail="Migration not found")

    if body.insert_after:
        # Get position of the step we're inserting after
        after_step = await db.fetch_one(
            "SELECT position FROM pipeline_steps WHERE id = ? AND migration_id = ?",
            (body.insert_after, mig_id),
        )
        if after_step is None:
            raise HTTPException(status_code=404, detail="insert_after step not found")
        new_position = after_step["position"] + 1
        # Shift all steps at or after new_position
        await db.execute(
            "UPDATE pipeline_steps SET position = position + 1 WHERE migration_id = ? AND position >= ?",
            (mig_id, new_position),
        )
    else:
        result = await db.fetch_one(
            "SELECT COALESCE(MAX(position), -1) as max_pos FROM pipeline_steps WHERE migration_id = ?",
            (mig_id,),
        )
        new_position = result["max_pos"] + 1

    step_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        """INSERT INTO pipeline_steps (id, migration_id, name, description, position, step_type, config, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (step_id, mig_id, body.name, body.description, new_position,
         body.step_type, json.dumps(body.config), now, now),
    )
    return PipelineStepResponse(
        id=step_id, migration_id=mig_id, name=body.name,
        description=body.description, position=new_position,
        step_type=body.step_type, config=body.config,
        created_at=now, updated_at=now,
    )


@router.put(
    "/migrations/{mig_id}/steps/{step_id}",
    response_model=PipelineStepResponse,
)
async def update_step(mig_id: str, step_id: str, body: PipelineStepUpdate, request: Request):
    db = _get_db(request)
    existing = await db.fetch_one(
        "SELECT * FROM pipeline_steps WHERE id = ? AND migration_id = ?",
        (step_id, mig_id),
    )
    if existing is None:
        raise HTTPException(status_code=404, detail="Pipeline step not found")

    now = datetime.now(timezone.utc).isoformat()
    new_name = body.name if body.name is not None else existing["name"]
    new_desc = body.description if body.description is not None else existing["description"]
    new_type = body.step_type if body.step_type is not None else existing["step_type"]
    new_config = json.dumps(body.config) if body.config is not None else existing["config"]

    await db.execute(
        "UPDATE pipeline_steps SET name=?, description=?, step_type=?, config=?, updated_at=? WHERE id=?",
        (new_name, new_desc, new_type, new_config, now, step_id),
    )
    row = await db.fetch_one("SELECT * FROM pipeline_steps WHERE id = ?", (step_id,))
    return PipelineStepResponse(
        id=row["id"], migration_id=row["migration_id"], name=row["name"],
        description=row["description"], position=row["position"],
        step_type=row["step_type"], config=json.loads(row["config"]),
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


@router.delete(
    "/migrations/{mig_id}/steps/{step_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_step(mig_id: str, step_id: str, request: Request):
    db = _get_db(request)
    existing = await db.fetch_one(
        "SELECT position FROM pipeline_steps WHERE id = ? AND migration_id = ?",
        (step_id, mig_id),
    )
    if existing is None:
        raise HTTPException(status_code=404, detail="Pipeline step not found")

    await db.execute("DELETE FROM pipeline_steps WHERE id = ?", (step_id,))
    # Re-compact positions
    await db.execute(
        "UPDATE pipeline_steps SET position = position - 1 WHERE migration_id = ? AND position > ?",
        (mig_id, existing["position"]),
    )
```

- [ ] **Step 4: Register router in main.py**

Add to `backend/src/vonnegut/main.py`:

```python
from vonnegut.routers.pipeline_steps import router as pipeline_steps_router
# ...
app.include_router(pipeline_steps_router, prefix="/api/v1")
```

- [ ] **Step 5: Run tests — expect PASS**

Run: `cd backend && uv run pytest tests/test_api_pipeline_steps.py -v`

- [ ] **Step 6: Run all tests for regressions**

Run: `cd backend && uv run pytest -v`

- [ ] **Step 7: Commit**

```bash
git add backend/src/vonnegut/routers/pipeline_steps.py backend/src/vonnegut/main.py backend/tests/test_api_pipeline_steps.py
git commit -m "feat: add pipeline steps CRUD router with insert_after support"
```

---

## Chunk 2: Backend Pipeline Engine

### Task 4: CTE Compiler

**Files:**
- Create: `backend/src/vonnegut/services/cte_compiler.py`
- Test: `backend/tests/test_cte_compiler.py`

- [ ] **Step 1: Write tests**

```python
# backend/tests/test_cte_compiler.py
from vonnegut.services.cte_compiler import normalize_cte_name, compile_sql_chain


def test_normalize_simple():
    assert normalize_cte_name("Lower Emails", 0) == "lower_emails_0"


def test_normalize_special_chars():
    assert normalize_cte_name("Hash user's PII!", 2) == "hash_user_s_pii__2"


def test_normalize_truncates():
    long_name = "a" * 100
    result = normalize_cte_name(long_name, 0)
    assert len(result) <= 63


def test_normalize_empty():
    assert normalize_cte_name("", 5) == "step_5"


def test_compile_single_step():
    steps = [
        {"name": "Source Query", "position": 0, "expression": "SELECT a, b FROM users"},
    ]
    sql = compile_sql_chain(steps)
    assert "WITH" in sql
    assert "source_query_0" in sql
    assert "SELECT a, b FROM users" in sql


def test_compile_multiple_steps():
    steps = [
        {"name": "Source", "position": 0, "expression": "SELECT a, b FROM users"},
        {"name": "Lower", "position": 1, "expression": "SELECT a, lower(b) as b FROM source_0"},
        {"name": "Filter", "position": 2, "expression": "SELECT a, b FROM lower_1 WHERE a > 0"},
    ]
    sql = compile_sql_chain(steps)
    assert "source_0 AS" in sql
    assert "lower_1 AS" in sql
    assert "filter_2 AS" in sql


def test_compile_references_previous():
    """Each step should be able to reference the previous CTE name."""
    steps = [
        {"name": "Fetch", "position": 0, "expression": "SELECT id FROM tbl"},
        {"name": "Transform", "position": 1, "expression": "SELECT id * 2 as id FROM {prev}"},
    ]
    sql = compile_sql_chain(steps)
    # {prev} gets replaced with the previous CTE name
    assert "FROM fetch_0" in sql


def test_compile_wrap_limit():
    steps = [
        {"name": "Source", "position": 0, "expression": "SELECT a FROM users"},
    ]
    sql = compile_sql_chain(steps, limit=10)
    assert "LIMIT 10" in sql
```

- [ ] **Step 2: Run tests — expect FAIL**

Run: `cd backend && uv run pytest tests/test_cte_compiler.py -v`

- [ ] **Step 3: Implement CTE compiler**

```python
# backend/src/vonnegut/services/cte_compiler.py
import re


def normalize_cte_name(name: str, position: int) -> str:
    """Normalize a step name into a valid SQL CTE identifier.

    - Lowercase
    - Replace non-alphanumeric with underscores
    - Truncate to fit within 63 chars (PostgreSQL identifier limit) including suffix
    - Append position for uniqueness
    """
    if not name.strip():
        return f"step_{position}"

    normalized = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    suffix = f"_{position}"
    max_base = 63 - len(suffix)
    if len(normalized) > max_base:
        normalized = normalized[:max_base]
    return normalized + suffix


def compile_sql_chain(
    steps: list[dict],
    limit: int | None = None,
) -> str:
    """Compile a list of SQL steps into a single CTE query.

    Each step dict has: name, position, expression.
    In expressions, {prev} is replaced with the previous CTE name.

    Returns a complete SQL string with WITH ... SELECT.
    """
    if not steps:
        raise ValueError("Cannot compile empty SQL chain")

    cte_names: list[str] = []
    cte_parts: list[str] = []

    for i, step in enumerate(steps):
        cte_name = normalize_cte_name(step["name"], step["position"])
        expression = step["expression"]

        # Replace {prev} with previous CTE name
        if i > 0 and "{prev}" in expression:
            expression = expression.replace("{prev}", cte_names[i - 1])

        cte_names.append(cte_name)
        cte_parts.append(f"{cte_name} AS ({expression})")

    last_cte = cte_names[-1]
    ctes = ",\n     ".join(cte_parts)
    final_select = f"SELECT * FROM {last_cte}"
    if limit is not None:
        final_select += f" LIMIT {limit}"

    return f"WITH {ctes}\n{final_select}"
```

- [ ] **Step 4: Run tests — expect PASS**

Run: `cd backend && uv run pytest tests/test_cte_compiler.py -v`

- [ ] **Step 5: Commit**

```bash
git add backend/src/vonnegut/services/cte_compiler.py backend/tests/test_cte_compiler.py
git commit -m "feat: add CTE compiler with name normalization and SQL chain composition"
```

---

### Task 5: Pipeline Engine

**Files:**
- Create: `backend/src/vonnegut/services/pipeline_engine.py`
- Test: `backend/tests/test_pipeline_engine.py`

**Dependencies:** Install duckdb and polars:
```bash
cd backend && uv add duckdb polars
```

- [ ] **Step 1: Write tests**

```python
# backend/tests/test_pipeline_engine.py
import pytest
import pytest_asyncio

from vonnegut.adapters.memory import InMemoryAdapter
from vonnegut.services.pipeline_engine import PipelineEngine


@pytest.fixture
def engine():
    return PipelineEngine()


@pytest.fixture
def source_adapter():
    adapter = InMemoryAdapter()
    adapter.add_table("users", [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
        {"id": 3, "name": "Carol", "email": "carol@example.com"},
    ])
    return adapter


@pytest.fixture
def target_adapter():
    adapter = InMemoryAdapter()
    adapter.add_table("users_copy", [])
    return adapter


async def test_sql_only_pipeline(engine, source_adapter):
    """SQL-only pipeline composes into CTEs and executes against source."""
    source_query = "SELECT id, name, email FROM users"
    steps = [
        {"name": "Lower Names", "position": 0, "step_type": "sql",
         "config": {"expression": "SELECT id, LOWER(name) as name, email FROM {prev}"}},
    ]
    result = await engine.run_test(
        source_adapter=source_adapter,
        source_query=source_query,
        steps=steps,
        limit=10,
    )
    assert len(result["steps"]) == 2  # source + 1 transform
    assert result["steps"][0]["status"] == "ok"
    assert result["steps"][1]["status"] == "ok"


async def test_empty_pipeline(engine, source_adapter):
    """Pipeline with no steps just returns source data."""
    source_query = "SELECT id, name FROM users"
    result = await engine.run_test(
        source_adapter=source_adapter,
        source_query=source_query,
        steps=[],
        limit=10,
    )
    assert len(result["steps"]) == 1  # source only
    assert len(result["steps"][0]["sample_data"]) == 3


async def test_code_pipeline(engine, source_adapter):
    """Code transform pulls data into Python and transforms with polars."""
    source_query = "SELECT id, name FROM users"
    steps = [
        {"name": "Upper Names", "position": 0, "step_type": "code",
         "config": {"function_code": "def transform(df):\n    return df.with_columns(df['name'].str.to_uppercase())"}},
    ]
    result = await engine.run_test(
        source_adapter=source_adapter,
        source_query=source_query,
        steps=steps,
        limit=10,
    )
    assert result["steps"][1]["status"] == "ok"
    rows = result["steps"][1]["sample_data"]
    assert rows[0]["name"] == "ALICE"


async def test_schema_validation_pass(engine, source_adapter, target_adapter):
    """Schema validation passes when output matches target."""
    source_query = "SELECT id, name, email FROM users"
    # Target has same schema
    target_adapter.add_table("target", [
        {"id": 0, "name": "", "email": ""},
    ])
    target_schema = await target_adapter.fetch_schema("target")
    result = await engine.run_test(
        source_adapter=source_adapter,
        source_query=source_query,
        steps=[],
        limit=10,
        target_schema=target_schema,
    )
    final = result["steps"][-1]
    assert final["validation"]["valid"] is True


async def test_pipeline_step_error(engine, source_adapter):
    """Bad SQL in a step produces an error status."""
    source_query = "SELECT id, name FROM users"
    steps = [
        {"name": "Bad SQL", "position": 0, "step_type": "sql",
         "config": {"expression": "SELECT nonexistent_col FROM {prev}"}},
    ]
    result = await engine.run_test(
        source_adapter=source_adapter,
        source_query=source_query,
        steps=steps,
        limit=10,
    )
    # The step with bad SQL should have error status
    assert any(s["status"] == "error" for s in result["steps"])
```

- [ ] **Step 2: Run tests — expect FAIL**

Run: `cd backend && uv run pytest tests/test_pipeline_engine.py -v`

- [ ] **Step 3: Implement pipeline engine**

```python
# backend/src/vonnegut/services/pipeline_engine.py
import polars as pl
import duckdb

from vonnegut.adapters.base import DatabaseAdapter, ColumnSchema
from vonnegut.services.cte_compiler import normalize_cte_name, compile_sql_chain


class PipelineEngine:
    """Executes a migration pipeline: SQL chains via CTEs, code via polars/DuckDB."""

    async def run_test(
        self,
        source_adapter: DatabaseAdapter,
        source_query: str,
        steps: list[dict],
        limit: int = 10,
        target_schema: list[ColumnSchema] | None = None,
    ) -> dict:
        """Run pipeline on sample data, return per-step results."""
        results: list[dict] = []

        # 1. Execute source query with limit
        try:
            wrapped = f"SELECT * FROM ({source_query}) AS _src LIMIT {limit}"
            source_rows = await source_adapter.execute(wrapped)
            source_schema = self._infer_schema(source_rows)
            results.append({
                "node_id": "source",
                "status": "ok",
                "schema": source_schema,
                "sample_data": source_rows,
                "validation": {"valid": True},
            })
        except Exception as e:
            results.append({
                "node_id": "source",
                "status": "error",
                "schema": [],
                "sample_data": [],
                "validation": {"valid": False, "errors": [{"type": "execution_error", "message": str(e)}]},
            })
            return {"steps": results}

        # 2. Process each step
        current_rows = source_rows
        current_schema = source_schema

        # Group consecutive SQL steps for CTE compilation
        sql_chain: list[dict] = []

        for step in steps:
            step_type = step["step_type"]

            if step_type == "sql":
                sql_chain.append(step)
                continue
            elif step_type in ("code", "ai"):
                # First, flush any accumulated SQL chain
                if sql_chain:
                    current_rows, current_schema, step_results = await self._execute_sql_chain(
                        source_adapter, source_query, sql_chain, current_rows, limit,
                    )
                    results.extend(step_results)
                    sql_chain = []

                # Execute code transform
                if step_type == "code" or (step_type == "ai" and step.get("config", {}).get("approved")):
                    code = step["config"].get("function_code") or step["config"].get("generated_code", "")
                    current_rows, current_schema, step_result = self._execute_code(
                        step, current_rows, code,
                    )
                    results.append(step_result)

        # Flush remaining SQL chain
        if sql_chain:
            current_rows, current_schema, step_results = await self._execute_sql_chain(
                source_adapter, source_query, sql_chain, current_rows, limit,
            )
            results.extend(step_results)

        # 3. Validate against target schema if provided
        if target_schema:
            validation = self._validate_schema(current_schema, target_schema)
            results.append({
                "node_id": "target",
                "status": "ok" if validation["valid"] else "error",
                "schema": [{"name": c.name, "type": c.type} for c in target_schema],
                "sample_data": [],
                "validation": validation,
            })

        return {"steps": results}

    async def _execute_sql_chain(
        self,
        adapter: DatabaseAdapter,
        source_query: str,
        sql_steps: list[dict],
        current_rows: list[dict],
        limit: int,
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """Execute a chain of SQL steps as CTEs. Returns (rows, schema, step_results)."""
        step_results = []

        # Build the CTE chain: source query as step 0, then each SQL step
        chain = [{"name": "source", "position": 0, "expression": source_query}]
        for i, step in enumerate(sql_steps):
            chain.append({
                "name": step["name"],
                "position": i + 1,
                "expression": step["config"]["expression"],
            })

        try:
            compiled = compile_sql_chain(chain, limit=limit)
            rows = await adapter.execute(compiled)
            schema = self._infer_schema(rows)

            # One result per SQL step
            for step in sql_steps:
                step_results.append({
                    "node_id": step.get("id", step["name"]),
                    "status": "ok",
                    "schema": schema,
                    "sample_data": rows,
                    "validation": {"valid": True},
                })
            return rows, schema, step_results
        except Exception as e:
            for step in sql_steps:
                step_results.append({
                    "node_id": step.get("id", step["name"]),
                    "status": "error",
                    "schema": [],
                    "sample_data": [],
                    "validation": {"valid": False, "errors": [{"type": "execution_error", "message": str(e)}]},
                })
            return current_rows, self._infer_schema(current_rows), step_results

    def _execute_code(
        self,
        step: dict,
        current_rows: list[dict],
        code: str,
    ) -> tuple[list[dict], list[dict], dict]:
        """Execute a code transform on current rows via polars."""
        try:
            df = pl.DataFrame(current_rows)
            # Execute the user's code
            local_ns: dict = {}
            exec(code, {"pl": pl, "__builtins__": {}}, local_ns)
            transform_fn = local_ns.get("transform")
            if transform_fn is None:
                raise ValueError("Code must define a 'transform(df)' function")
            result_df = transform_fn(df)
            rows = result_df.to_dicts()
            schema = self._infer_schema(rows)
            return rows, schema, {
                "node_id": step.get("id", step["name"]),
                "status": "ok",
                "schema": schema,
                "sample_data": rows,
                "validation": {"valid": True},
            }
        except Exception as e:
            return current_rows, self._infer_schema(current_rows), {
                "node_id": step.get("id", step["name"]),
                "status": "error",
                "schema": [],
                "sample_data": [],
                "validation": {"valid": False, "errors": [{"type": "execution_error", "message": str(e)}]},
            }

    def _infer_schema(self, rows: list[dict]) -> list[dict]:
        """Infer column names and types from row data."""
        if not rows:
            return []
        first = rows[0]
        schema = []
        for key, value in first.items():
            py_type = type(value).__name__ if value is not None else "unknown"
            schema.append({"name": key, "type": py_type})
        return schema

    def _validate_schema(
        self, output_schema: list[dict], target_schema: list[ColumnSchema]
    ) -> dict:
        """Validate output schema against target table schema."""
        errors = []
        target_cols = {c.name: c.type for c in target_schema}
        output_cols = {c["name"]: c["type"] for c in output_schema}

        for col_name, col_type in target_cols.items():
            if col_name not in output_cols:
                errors.append({
                    "type": "missing_column",
                    "column": col_name,
                    "expected": col_type,
                    "actual": None,
                    "message": f"Column '{col_name}' not found in pipeline output",
                })

        return {"valid": len(errors) == 0, "errors": errors}
```

- [ ] **Step 4: Run tests — expect PASS**

Run: `cd backend && uv run pytest tests/test_pipeline_engine.py -v`

- [ ] **Step 5: Commit**

```bash
git add backend/src/vonnegut/services/pipeline_engine.py backend/tests/test_pipeline_engine.py
git commit -m "feat: add pipeline engine with CTE compilation and code transform support"
```

---

### Task 6: Update Migration Test/Run Endpoints

**Files:**
- Modify: `backend/src/vonnegut/routers/migrations.py`

- [ ] **Step 1: Update test endpoint to use PipelineEngine**

Replace the existing `test_migration` endpoint in `backend/src/vonnegut/routers/migrations.py`:

```python
@router.post("/migrations/{mig_id}/test")
async def test_migration(mig_id: str, request: Request):
    db = _get_db(request)
    row = await db.fetch_one("SELECT * FROM migrations WHERE id = ?", (mig_id,))
    if row is None:
        raise HTTPException(status_code=404, detail="Migration not found")

    # Load pipeline steps
    step_rows = await db.fetch_all(
        "SELECT * FROM pipeline_steps WHERE migration_id = ? ORDER BY position",
        (mig_id,),
    )
    steps = [
        {"id": s["id"], "name": s["name"], "position": s["position"],
         "step_type": s["step_type"], "config": json.loads(s["config"])}
        for s in step_rows
    ]

    # Get source adapter
    manager = request.app.state.connection_manager
    source_conn = await manager.get(row["source_connection_id"])
    adapter_factory = _get_adapter_factory(request)
    source_adapter = await adapter_factory.create(source_conn)

    # Get target schema if target is configured
    target_schema = None
    if row["target_connection_id"] and row["target_table"]:
        target_conn = await manager.get(row["target_connection_id"])
        target_adapter = await adapter_factory.create(target_conn)
        try:
            target_schema = await target_adapter.fetch_schema(row["target_table"])
        finally:
            await target_adapter.disconnect()

    try:
        from vonnegut.services.pipeline_engine import PipelineEngine
        engine = PipelineEngine()
        source_query = row["source_query"] or f"SELECT * FROM {row['source_table']}"
        result = await engine.run_test(
            source_adapter=source_adapter,
            source_query=source_query,
            steps=steps,
            limit=10,
            target_schema=target_schema,
        )
        return result
    finally:
        await source_adapter.disconnect()
```

- [ ] **Step 2: Run all tests**

Run: `cd backend && uv run pytest -v`

- [ ] **Step 3: Commit**

```bash
git add backend/src/vonnegut/routers/migrations.py
git commit -m "feat: update test endpoint to use pipeline engine with CTE compilation"
```

---

## Chunk 3: Frontend Types, API & Graph

### Task 7: Frontend Types & API Client

**Files:**
- Create: `frontend/src/types/pipeline.ts`
- Modify: `frontend/src/types/migration.ts`
- Modify: `frontend/src/lib/api.ts`

- [ ] **Step 1: Create pipeline types**

```typescript
// frontend/src/types/pipeline.ts
export type StepType = "sql" | "code" | "ai";

export interface ColumnDef {
  name: string;
  type: string;
}

export interface PipelineStep {
  id: string;
  migration_id: string;
  name: string;
  description: string | null;
  position: number;
  step_type: StepType;
  config: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface PipelineStepCreate {
  step_type: StepType;
  name: string;
  description?: string;
  config: Record<string, unknown>;
  insert_after?: string;
}

export interface PipelineStepUpdate {
  name?: string;
  description?: string;
  step_type?: StepType;
  config?: Record<string, unknown>;
}

export interface ValidationError {
  type: "missing_column" | "type_mismatch" | "execution_error";
  column?: string;
  expected?: string;
  actual?: string | null;
  message: string;
}

export interface StepResult {
  node_id: string;
  status: "ok" | "error";
  schema: ColumnDef[];
  sample_data: Record<string, unknown>[];
  validation: {
    valid: boolean;
    errors?: ValidationError[];
  };
}

export interface PipelineTestResult {
  steps: StepResult[];
}
```

- [ ] **Step 2: Update migration types**

In `frontend/src/types/migration.ts`, add:

```typescript
import type { PipelineStep, ColumnDef } from "./pipeline";

// Add to Migration interface:
  source_query: string;
  source_schema: ColumnDef[];
  pipeline_steps: PipelineStep[];
```

Update `MigrationCreate` to include optional `source_query` and `source_schema`.

Update `EMPTY_MIGRATION` in `MigrationBuilderPage.tsx` to include `source_query: ""`, `source_schema: []`, `pipeline_steps: []`.

- [ ] **Step 3: Update API client**

Add to `frontend/src/lib/api.ts`:

```typescript
import type { PipelineStep, PipelineStepCreate, PipelineStepUpdate, PipelineTestResult } from "@/types/pipeline";

// Add to api object:
  pipelineSteps: {
    list: async (migrationId: string): Promise<PipelineStep[]> => {
      const res = await fetch(`${BASE}/migrations/${migrationId}`);
      const data = await res.json();
      return data.pipeline_steps;
    },
    add: async (migrationId: string, data: PipelineStepCreate): Promise<PipelineStep> => {
      const res = await fetch(`${BASE}/migrations/${migrationId}/steps`, {
        method: "POST",
        headers: HEADERS,
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error(await res.text());
      return res.json();
    },
    update: async (migrationId: string, stepId: string, data: PipelineStepUpdate): Promise<PipelineStep> => {
      const res = await fetch(`${BASE}/migrations/${migrationId}/steps/${stepId}`, {
        method: "PUT",
        headers: HEADERS,
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error(await res.text());
      return res.json();
    },
    delete: async (migrationId: string, stepId: string): Promise<void> => {
      await fetch(`${BASE}/migrations/${migrationId}/steps/${stepId}`, { method: "DELETE" });
    },
  },
```

Update `migrations.test` return type to `Promise<PipelineTestResult>`.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/types/pipeline.ts frontend/src/types/migration.ts frontend/src/lib/api.ts
git commit -m "feat: add frontend pipeline types and API client methods"
```

---

### Task 8: Update Node Theme & Icons

**Files:**
- Modify: `frontend/src/config/nodeTheme.ts`
- Modify: `frontend/src/config/iconRegistry.ts`

- [ ] **Step 1: Update node theme**

Replace `frontend/src/config/nodeTheme.ts`:

```typescript
export const nodeTheme = {
  source: {
    color: "bg-blue-50 border-blue-400",
    accent: "text-blue-600",
    badge: "bg-blue-100 text-blue-700",
  },
  target: {
    color: "bg-green-50 border-green-400",
    accent: "text-green-600",
    badge: "bg-green-100 text-green-700",
  },
  sql: {
    color: "bg-purple-50 border-purple-400",
    accent: "text-purple-600",
    badge: "bg-purple-100 text-purple-700",
  },
  code: {
    color: "bg-amber-50 border-amber-400",
    accent: "text-amber-600",
    badge: "bg-amber-100 text-amber-700",
  },
  ai: {
    color: "bg-teal-50 border-teal-400",
    accent: "text-teal-600",
    badge: "bg-teal-100 text-teal-700",
  },
} as const;

export type NodeType = keyof typeof nodeTheme;
```

- [ ] **Step 2: Add icons for new node types**

In `frontend/src/config/iconRegistry.ts`, add:

```typescript
import { Code2, FunctionSquare, Sparkles } from "lucide-react";

// Add to icons object:
  sql: Code2,
  code: FunctionSquare,
  ai: Sparkles,
```

- [ ] **Step 3: Commit**

```bash
git add frontend/src/config/nodeTheme.ts frontend/src/config/iconRegistry.ts
git commit -m "feat: update node themes and icons for pipeline step types"
```

---

### Task 9: React Flow Graph Redesign

**Files:**
- Rewrite: `frontend/src/components/migration-builder/Canvas.tsx`
- Rewrite: `frontend/src/components/migration-builder/nodes/SourceNode.tsx`
- Rewrite: `frontend/src/components/migration-builder/nodes/TargetNode.tsx`
- Create: `frontend/src/components/migration-builder/nodes/PipelineNode.tsx`
- Create: `frontend/src/components/migration-builder/edges/AddStepEdge.tsx`
- Delete: `frontend/src/components/migration-builder/nodes/TransformNode.tsx`

- [ ] **Step 1: Create AddStepEdge component**

Custom React Flow edge with a "+" button at the midpoint that opens a dropdown.

```typescript
// frontend/src/components/migration-builder/edges/AddStepEdge.tsx
import { BaseEdge, EdgeLabelRenderer, getBezierPath, type EdgeProps } from "@xyflow/react";
import { Plus } from "lucide-react";
import { useState } from "react";
import type { StepType } from "@/types/pipeline";

interface AddStepEdgeData {
  onAddStep: (type: StepType, afterNodeId: string) => void;
  sourceNodeId: string;
}

export function AddStepEdge({ id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data }: EdgeProps) {
  const d = data as unknown as AddStepEdgeData;
  const [showDropdown, setShowDropdown] = useState(false);
  const [edgePath, labelX, labelY] = getBezierPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition });

  const handleAdd = (type: StepType) => {
    d.onAddStep(type, d.sourceNodeId);
    setShowDropdown(false);
  };

  return (
    <>
      <BaseEdge id={id} path={edgePath} />
      <EdgeLabelRenderer>
        <div
          className="absolute flex items-center justify-center"
          style={{ transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`, pointerEvents: "all" }}
        >
          <button
            onClick={() => setShowDropdown(!showDropdown)}
            className="h-6 w-6 rounded-full border bg-background flex items-center justify-center hover:bg-muted"
          >
            <Plus className="h-3 w-3" />
          </button>
          {showDropdown && (
            <div className="absolute top-8 z-50 rounded-md border bg-background shadow-md py-1 min-w-[140px]">
              <button className="w-full text-left px-3 py-1.5 text-sm hover:bg-muted" onClick={() => handleAdd("sql")}>SQL Transform</button>
              <button className="w-full text-left px-3 py-1.5 text-sm hover:bg-muted" onClick={() => handleAdd("code")}>Code Transform</button>
              <button className="w-full text-left px-3 py-1.5 text-sm hover:bg-muted" onClick={() => handleAdd("ai")}>AI Assistant</button>
            </div>
          )}
        </div>
      </EdgeLabelRenderer>
    </>
  );
}
```

- [ ] **Step 2: Create PipelineNode component**

Unified node for SQL/Code/AI transform steps. Shows name, type icon, schema badge, delete X on hover, description tooltip.

```typescript
// frontend/src/components/migration-builder/nodes/PipelineNode.tsx
import { Handle, Position, type NodeProps } from "@xyflow/react";
import { X } from "lucide-react";
import { icons } from "@/config/iconRegistry";
import { nodeTheme, type NodeType } from "@/config/nodeTheme";
import type { StepType } from "@/types/pipeline";

export interface PipelineNodeData {
  name: string;
  stepType: StepType;
  description: string | null;
  schemaCount: number | null;  // number of output columns, null if unknown
  onDelete: (nodeId: string) => void;
}

export function PipelineNode({ id, data, selected }: NodeProps) {
  const d = data as unknown as PipelineNodeData;
  const Icon = icons[d.stepType] || icons.sql_expression;
  const theme = nodeTheme[d.stepType as NodeType];

  return (
    <div
      className={`rounded-lg border-2 p-3 min-w-[160px] relative group ${theme.color} ${selected ? "ring-2 ring-primary" : ""}`}
      title={d.description || undefined}
    >
      <Handle type="target" position={Position.Left} />
      <div className="flex items-center gap-2">
        <Icon className={`h-4 w-4 ${theme.accent}`} />
        <span className="font-medium text-sm">{d.name}</span>
        <button
          onClick={(e) => { e.stopPropagation(); d.onDelete(id); }}
          className="absolute top-1 right-1 opacity-0 group-hover:opacity-100 h-4 w-4 rounded hover:bg-destructive/20"
        >
          <X className="h-3 w-3" />
        </button>
      </div>
      <div className="flex items-center gap-2 mt-1">
        <span className="text-xs text-muted-foreground">{d.stepType}</span>
        {d.schemaCount !== null && (
          <span className={`text-xs px-1.5 py-0.5 rounded ${theme.badge}`}>{d.schemaCount} cols</span>
        )}
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
```

- [ ] **Step 3: Simplify SourceNode**

```typescript
// frontend/src/components/migration-builder/nodes/SourceNode.tsx
import { Handle, Position, type NodeProps } from "@xyflow/react";
import { icons } from "@/config/iconRegistry";
import { nodeTheme } from "@/config/nodeTheme";

export interface SourceNodeData {
  connectionName: string;
  table: string;
  schemaCount: number | null;
}

export function SourceNode({ data, selected }: NodeProps) {
  const d = data as unknown as SourceNodeData;
  const Icon = icons.source;
  const theme = nodeTheme.source;

  return (
    <div className={`rounded-lg border-2 p-3 min-w-[160px] ${theme.color} ${selected ? "ring-2 ring-primary" : ""}`}>
      <div className="flex items-center gap-2">
        <Icon className={`h-4 w-4 ${theme.accent}`} />
        <span className="font-medium text-sm">Source</span>
      </div>
      <div className="text-xs text-muted-foreground mt-1">{d.connectionName || "Not configured"}</div>
      {d.table && <div className="text-xs font-mono mt-0.5">{d.table}</div>}
      {d.schemaCount !== null && (
        <span className={`text-xs px-1.5 py-0.5 rounded mt-1 inline-block ${theme.badge}`}>{d.schemaCount} cols</span>
      )}
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
```

- [ ] **Step 4: Simplify TargetNode**

Same pattern as SourceNode but with target theme, left Handle only, and validation indicator.

```typescript
// frontend/src/components/migration-builder/nodes/TargetNode.tsx
import { Handle, Position, type NodeProps } from "@xyflow/react";
import { icons } from "@/config/iconRegistry";
import { nodeTheme } from "@/config/nodeTheme";
import { CircleCheck, CircleX } from "lucide-react";

export interface TargetNodeData {
  connectionName: string;
  table: string;
  schemaCount: number | null;
  validationStatus: "valid" | "invalid" | "unknown";
}

export function TargetNode({ data, selected }: NodeProps) {
  const d = data as unknown as TargetNodeData;
  const Icon = icons.target;
  const theme = nodeTheme.target;

  return (
    <div className={`rounded-lg border-2 p-3 min-w-[160px] ${theme.color} ${selected ? "ring-2 ring-primary" : ""}`}>
      <Handle type="target" position={Position.Left} />
      <div className="flex items-center gap-2">
        <Icon className={`h-4 w-4 ${theme.accent}`} />
        <span className="font-medium text-sm">Target</span>
        {d.validationStatus === "valid" && <CircleCheck className="h-3.5 w-3.5 text-green-600" />}
        {d.validationStatus === "invalid" && <CircleX className="h-3.5 w-3.5 text-red-600" />}
      </div>
      <div className="text-xs text-muted-foreground mt-1">{d.connectionName || "Not configured"}</div>
      {d.table && <div className="text-xs font-mono mt-0.5">{d.table}</div>}
      {d.schemaCount !== null && (
        <span className={`text-xs px-1.5 py-0.5 rounded mt-1 inline-block ${theme.badge}`}>{d.schemaCount} cols</span>
      )}
    </div>
  );
}
```

- [ ] **Step 5: Rewrite Canvas**

```typescript
// frontend/src/components/migration-builder/Canvas.tsx
import { useCallback, useMemo } from "react";
import {
  ReactFlow, Background, type Node, type Edge, type NodeTypes, type EdgeTypes,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { SourceNode } from "./nodes/SourceNode";
import { TargetNode } from "./nodes/TargetNode";
import { PipelineNode } from "./nodes/PipelineNode";
import { AddStepEdge } from "./edges/AddStepEdge";
import type { Migration } from "@/types/migration";
import type { StepType, PipelineStep, StepResult } from "@/types/pipeline";

interface Props {
  migration: Migration;
  testResults: StepResult[] | null;
  selectedNodeId: string | null;
  onNodeClick: (nodeId: string) => void;
  onCanvasClick: () => void;
  onAddStep: (type: StepType, afterNodeId: string) => void;
  onDeleteStep: (stepId: string) => void;
}

const NODE_SPACING = 250;

export function Canvas({
  migration, testResults, selectedNodeId,
  onNodeClick, onCanvasClick, onAddStep, onDeleteStep,
}: Props) {
  const nodeTypes: NodeTypes = useMemo(() => ({
    source: SourceNode,
    target: TargetNode,
    pipeline: PipelineNode,
  }), []);

  const edgeTypes: EdgeTypes = useMemo(() => ({
    addStep: AddStepEdge,
  }), []);

  const steps = migration.pipeline_steps || [];

  const nodes: Node[] = useMemo(() => {
    const result: Node[] = [];

    // Source node
    result.push({
      id: "source",
      type: "source",
      position: { x: 50, y: 200 },
      selected: selectedNodeId === "source",
      data: {
        connectionName: "", // resolved by parent
        table: migration.source_table || "Not configured",
        schemaCount: migration.source_schema?.length || null,
      },
    });

    // Pipeline step nodes
    steps.forEach((step, i) => {
      result.push({
        id: step.id,
        type: "pipeline",
        position: { x: 50 + (i + 1) * NODE_SPACING, y: 200 },
        selected: selectedNodeId === step.id,
        data: {
          name: step.name,
          stepType: step.step_type,
          description: step.description,
          schemaCount: null, // populated from test results
          onDelete: onDeleteStep,
        },
      });
    });

    // Target node
    result.push({
      id: "target",
      type: "target",
      position: { x: 50 + (steps.length + 1) * NODE_SPACING, y: 200 },
      selected: selectedNodeId === "target",
      data: {
        connectionName: "",
        table: migration.target_table || "Not configured",
        schemaCount: null,
        validationStatus: "unknown",
      },
    });

    return result;
  }, [migration, steps, selectedNodeId, onDeleteStep]);

  const edges: Edge[] = useMemo(() => {
    const result: Edge[] = [];
    const nodeIds = ["source", ...steps.map(s => s.id), "target"];

    for (let i = 0; i < nodeIds.length - 1; i++) {
      result.push({
        id: `${nodeIds[i]}-${nodeIds[i + 1]}`,
        source: nodeIds[i],
        target: nodeIds[i + 1],
        type: "addStep",
        data: { onAddStep, sourceNodeId: nodeIds[i] },
      });
    }

    return result;
  }, [steps, onAddStep]);

  const handleNodeClick = useCallback((_: React.MouseEvent, node: Node) => {
    onNodeClick(node.id);
  }, [onNodeClick]);

  return (
    <div className="h-full w-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodeClick={handleNodeClick}
        onPaneClick={onCanvasClick}
        nodesDraggable={false}
        fitView
      >
        <Background />
      </ReactFlow>
    </div>
  );
}
```

- [ ] **Step 6: Delete old TransformNode**

```bash
rm frontend/src/components/migration-builder/nodes/TransformNode.tsx
```

- [ ] **Step 7: Verify frontend compiles**

Run: `cd frontend && npm run build`

- [ ] **Step 8: Commit**

```bash
git add -A frontend/src/components/migration-builder/ frontend/src/config/
git commit -m "feat: redesign React Flow graph with pipeline nodes and edge add buttons"
```

---

## Chunk 4: Frontend Editor Panel & Page Orchestration

### Task 10: SchemaPanel Component

**Files:**
- Create: `frontend/src/components/migration-builder/SchemaPanel.tsx`

- [ ] **Step 1: Create collapsible schema panel**

```typescript
// frontend/src/components/migration-builder/SchemaPanel.tsx
import { useState } from "react";
import { ChevronLeft, ChevronRight, TableProperties } from "lucide-react";
import type { ColumnDef } from "@/types/pipeline";

interface Props {
  title: "Input Schema" | "Output Schema";
  schema: ColumnDef[];
  side: "left" | "right";
}

export function SchemaPanel({ title, schema, side }: Props) {
  const [collapsed, setCollapsed] = useState(false);
  const CollapseIcon = side === "left" ? ChevronLeft : ChevronRight;
  const ExpandIcon = side === "left" ? ChevronRight : ChevronLeft;

  if (collapsed) {
    return (
      <button
        onClick={() => setCollapsed(false)}
        className="w-8 flex flex-col items-center justify-center border-x hover:bg-muted/50"
        title={`Expand ${title}`}
      >
        <ExpandIcon className="h-3 w-3 mb-1" />
        <span className="text-xs [writing-mode:vertical-lr] text-muted-foreground">{title}</span>
      </button>
    );
  }

  return (
    <div className="w-56 flex flex-col border-x">
      <div className="flex items-center justify-between px-3 py-2 border-b">
        <div className="flex items-center gap-1.5">
          <TableProperties className="h-3.5 w-3.5 text-muted-foreground" />
          <span className="text-xs font-medium">{title}</span>
        </div>
        <button onClick={() => setCollapsed(true)} className="hover:bg-muted rounded p-0.5">
          <CollapseIcon className="h-3 w-3" />
        </button>
      </div>
      <div className="flex-1 overflow-y-auto p-2">
        {schema.length > 0 ? (
          <div className="flex flex-col gap-0.5">
            {schema.map((col) => (
              <div key={col.name} className="flex items-center justify-between px-2 py-1 text-xs rounded hover:bg-muted/50">
                <span className="font-mono truncate">{col.name}</span>
                <span className="text-muted-foreground ml-2 shrink-0">{col.type}</span>
              </div>
            ))}
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center py-8 text-muted-foreground">
            <TableProperties className="h-6 w-6 mb-2 opacity-30" />
            <span className="text-xs">Run test to infer schema</span>
          </div>
        )}
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/migration-builder/SchemaPanel.tsx
git commit -m "feat: add collapsible schema panel component"
```

---

### Task 11: EditorPanel Component

**Files:**
- Create: `frontend/src/components/migration-builder/EditorPanel.tsx`

- [ ] **Step 1: Create slide-up editor panel shell**

Three-column layout: SchemaPanel (input) | Editor content | SchemaPanel (output).
Slides up with CSS transition.

```typescript
// frontend/src/components/migration-builder/EditorPanel.tsx
import { X } from "lucide-react";
import { SchemaPanel } from "./SchemaPanel";
import { SourceEditor } from "./editors/SourceEditor";
import { TargetEditor } from "./editors/TargetEditor";
import { SqlEditor } from "./editors/SqlEditor";
import { CodeEditor } from "./editors/CodeEditor";
import { AiEditor } from "./editors/AiEditor";
import type { Migration } from "@/types/migration";
import type { PipelineStep, ColumnDef, StepResult } from "@/types/pipeline";
import type { Connection } from "@/types/connection";

interface Props {
  nodeId: string;
  migration: Migration;
  connections: Connection[];
  step: PipelineStep | null;  // null for source/target
  inputSchema: ColumnDef[];
  outputSchema: ColumnDef[];
  testResult: StepResult | null;
  onClose: () => void;
  onUpdateMigration: (updates: Partial<Migration>) => void;
  onUpdateStep: (stepId: string, updates: Record<string, unknown>) => void;
}

export function EditorPanel({
  nodeId, migration, connections, step,
  inputSchema, outputSchema, testResult,
  onClose, onUpdateMigration, onUpdateStep,
}: Props) {
  const isSource = nodeId === "source";
  const isTarget = nodeId === "target";
  const nodeName = isSource ? "Source" : isTarget ? "Target" : step?.name || "Step";
  const nodeType = isSource ? "source" : isTarget ? "target" : step?.step_type || "sql";

  return (
    <div className="border-t flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center gap-3 px-4 py-2 border-b shrink-0">
        <span className="text-xs font-medium text-muted-foreground uppercase">{nodeType}</span>
        {step ? (
          <input
            className="font-medium text-sm bg-transparent border-none outline-none flex-1"
            value={step.name}
            onChange={(e) => onUpdateStep(step.id, { name: e.target.value })}
          />
        ) : (
          <span className="font-medium text-sm">{nodeName}</span>
        )}
        {step && (
          <input
            className="text-xs text-muted-foreground bg-transparent border-none outline-none flex-1"
            value={step.description || ""}
            placeholder="Add description..."
            onChange={(e) => onUpdateStep(step.id, { description: e.target.value || null })}
          />
        )}
        <button onClick={onClose} className="hover:bg-muted rounded p-1">
          <X className="h-4 w-4" />
        </button>
      </div>

      {/* Three-column layout */}
      <div className="flex flex-1 min-h-0">
        {/* Input Schema */}
        {!isSource && (
          <SchemaPanel title="Input Schema" schema={inputSchema} side="left" />
        )}

        {/* Main Editor */}
        <div className="flex-1 overflow-auto p-4">
          {isSource && (
            <SourceEditor
              migration={migration}
              connections={connections}
              onUpdate={onUpdateMigration}
            />
          )}
          {isTarget && (
            <TargetEditor
              migration={migration}
              connections={connections}
              onUpdate={onUpdateMigration}
            />
          )}
          {step?.step_type === "sql" && (
            <SqlEditor step={step} onUpdate={(config) => onUpdateStep(step.id, { config })} />
          )}
          {step?.step_type === "code" && (
            <CodeEditor step={step} onUpdate={(config) => onUpdateStep(step.id, { config })} />
          )}
          {step?.step_type === "ai" && (
            <AiEditor
              step={step}
              inputSchema={inputSchema}
              migration={migration}
              onUpdate={(updates) => onUpdateStep(step.id, updates)}
            />
          )}
        </div>

        {/* Output Schema */}
        {!isTarget && (
          <SchemaPanel title="Output Schema" schema={outputSchema} side="right" />
        )}
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/migration-builder/EditorPanel.tsx
git commit -m "feat: add editor panel with three-column layout"
```

---

### Task 12: Node Editors

**Files:**
- Create: `frontend/src/components/migration-builder/editors/SourceEditor.tsx`
- Create: `frontend/src/components/migration-builder/editors/TargetEditor.tsx`
- Create: `frontend/src/components/migration-builder/editors/SqlEditor.tsx`
- Create: `frontend/src/components/migration-builder/editors/CodeEditor.tsx`
- Create: `frontend/src/components/migration-builder/editors/AiEditor.tsx`

- [ ] **Step 1: SourceEditor**

Connection picker + table picker + auto-generated query editor.

```typescript
// frontend/src/components/migration-builder/editors/SourceEditor.tsx
import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import type { Migration } from "@/types/migration";
import type { Connection } from "@/types/connection";

interface Props {
  migration: Migration;
  connections: Connection[];
  onUpdate: (updates: Partial<Migration>) => void;
}

export function SourceEditor({ migration, connections, onUpdate }: Props) {
  const [tables, setTables] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!migration.source_connection_id) return;
    setLoading(true);
    api.connections.tables(migration.source_connection_id)
      .then(setTables)
      .finally(() => setLoading(false));
  }, [migration.source_connection_id]);

  const handleConnectionChange = (connId: string) => {
    onUpdate({ source_connection_id: connId, source_table: "", source_query: "", source_schema: [] });
  };

  const handleTableChange = async (table: string) => {
    onUpdate({ source_table: table });
    // Auto-generate query from schema
    if (migration.source_connection_id && table) {
      const schema = await api.connections.schema(migration.source_connection_id, table);
      const columns = schema.map((c) => c.name).join(", ");
      const query = `SELECT ${columns} FROM ${table}`;
      onUpdate({
        source_table: table,
        source_query: query,
        source_schema: schema.map((c) => ({ name: c.name, type: c.type })),
      });
    }
  };

  return (
    <div className="flex flex-col gap-4">
      <div>
        <label className="text-xs font-medium text-muted-foreground block mb-1">Connection</label>
        <select
          className="w-full border rounded px-3 py-2 text-sm"
          value={migration.source_connection_id}
          onChange={(e) => handleConnectionChange(e.target.value)}
        >
          <option value="">Select connection...</option>
          {connections.map((c) => (
            <option key={c.id} value={c.id}>{c.name}</option>
          ))}
        </select>
      </div>

      <div>
        <label className="text-xs font-medium text-muted-foreground block mb-1">Table</label>
        <select
          className="w-full border rounded px-3 py-2 text-sm"
          value={migration.source_table}
          onChange={(e) => handleTableChange(e.target.value)}
          disabled={!migration.source_connection_id || loading}
        >
          <option value="">Select table...</option>
          {tables.map((t) => (
            <option key={t} value={t}>{t}</option>
          ))}
        </select>
      </div>

      <div>
        <label className="text-xs font-medium text-muted-foreground block mb-1">Source Query</label>
        <textarea
          className="w-full border rounded px-3 py-2 text-sm font-mono min-h-[120px] resize-y"
          value={migration.source_query}
          onChange={(e) => onUpdate({ source_query: e.target.value })}
          placeholder="SELECT col1, col2 FROM table"
        />
      </div>
    </div>
  );
}
```

- [ ] **Step 2: TargetEditor**

Connection picker + table picker + target schema display + validation.

```typescript
// frontend/src/components/migration-builder/editors/TargetEditor.tsx
import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import { CircleCheck, CircleX } from "lucide-react";
import type { Migration } from "@/types/migration";
import type { Connection, ColumnSchema } from "@/types/connection";

interface Props {
  migration: Migration;
  connections: Connection[];
  onUpdate: (updates: Partial<Migration>) => void;
}

export function TargetEditor({ migration, connections, onUpdate }: Props) {
  const [tables, setTables] = useState<string[]>([]);
  const [schema, setSchema] = useState<ColumnSchema[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!migration.target_connection_id) return;
    setLoading(true);
    api.connections.tables(migration.target_connection_id)
      .then(setTables)
      .finally(() => setLoading(false));
  }, [migration.target_connection_id]);

  useEffect(() => {
    if (!migration.target_connection_id || !migration.target_table) return;
    api.connections.schema(migration.target_connection_id, migration.target_table).then(setSchema);
  }, [migration.target_connection_id, migration.target_table]);

  return (
    <div className="flex flex-col gap-4">
      <div>
        <label className="text-xs font-medium text-muted-foreground block mb-1">Connection</label>
        <select
          className="w-full border rounded px-3 py-2 text-sm"
          value={migration.target_connection_id}
          onChange={(e) => onUpdate({ target_connection_id: e.target.value, target_table: "" })}
        >
          <option value="">Select connection...</option>
          {connections.map((c) => (
            <option key={c.id} value={c.id}>{c.name}</option>
          ))}
        </select>
      </div>

      <div>
        <label className="text-xs font-medium text-muted-foreground block mb-1">Table</label>
        <select
          className="w-full border rounded px-3 py-2 text-sm"
          value={migration.target_table}
          onChange={(e) => onUpdate({ target_table: e.target.value })}
          disabled={!migration.target_connection_id || loading}
        >
          <option value="">Select table...</option>
          {tables.map((t) => (
            <option key={t} value={t}>{t}</option>
          ))}
        </select>
      </div>

      {schema.length > 0 && (
        <div>
          <label className="text-xs font-medium text-muted-foreground block mb-1">Target Schema</label>
          <div className="border rounded p-2 text-xs font-mono max-h-[200px] overflow-y-auto">
            {schema.map((col) => (
              <div key={col.name} className="flex justify-between py-0.5">
                <span>{col.name}</span>
                <span className="text-muted-foreground">{col.type}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 3: SqlEditor**

```typescript
// frontend/src/components/migration-builder/editors/SqlEditor.tsx
import type { PipelineStep } from "@/types/pipeline";

interface Props {
  step: PipelineStep;
  onUpdate: (config: Record<string, unknown>) => void;
}

export function SqlEditor({ step, onUpdate }: Props) {
  const expression = (step.config.expression as string) || "";

  return (
    <div className="flex flex-col gap-3">
      <div>
        <label className="text-xs font-medium text-muted-foreground block mb-1">SQL Expression</label>
        <p className="text-xs text-muted-foreground mb-2">
          Write a SELECT that transforms the input. Use <code className="bg-muted px-1 rounded">{"{prev}"}</code> to reference the previous step.
        </p>
        <textarea
          className="w-full border rounded px-3 py-2 text-sm font-mono min-h-[200px] resize-y"
          value={expression}
          onChange={(e) => onUpdate({ expression: e.target.value })}
          placeholder="SELECT col1, lower(col2) as col2 FROM {prev}"
        />
      </div>
    </div>
  );
}
```

- [ ] **Step 4: CodeEditor**

```typescript
// frontend/src/components/migration-builder/editors/CodeEditor.tsx
import type { PipelineStep } from "@/types/pipeline";

interface Props {
  step: PipelineStep;
  onUpdate: (config: Record<string, unknown>) => void;
}

const DEFAULT_CODE = `def transform(df):
    """Transform the input DataFrame.

    Args:
        df: polars DataFrame with input data
    Returns:
        polars DataFrame with transformed data
    """
    return df
`;

export function CodeEditor({ step, onUpdate }: Props) {
  const code = (step.config.function_code as string) || DEFAULT_CODE;

  return (
    <div className="flex flex-col gap-3">
      <div>
        <label className="text-xs font-medium text-muted-foreground block mb-1">Python Transform</label>
        <p className="text-xs text-muted-foreground mb-2">
          Define a <code className="bg-muted px-1 rounded">transform(df)</code> function that takes and returns a polars DataFrame.
          Available: <code className="bg-muted px-1 rounded">pl</code> (polars), hashlib, cryptography, datetime, json, math, re.
        </p>
        <textarea
          className="w-full border rounded px-3 py-2 text-sm font-mono min-h-[250px] resize-y"
          value={code}
          onChange={(e) => onUpdate({ function_code: e.target.value })}
        />
      </div>
    </div>
  );
}
```

- [ ] **Step 5: AiEditor**

```typescript
// frontend/src/components/migration-builder/editors/AiEditor.tsx
import { useState } from "react";
import { Sparkles, Check } from "lucide-react";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api";
import type { PipelineStep, ColumnDef } from "@/types/pipeline";
import type { Migration } from "@/types/migration";

interface Props {
  step: PipelineStep;
  inputSchema: ColumnDef[];
  migration: Migration;
  onUpdate: (updates: Record<string, unknown>) => void;
}

export function AiEditor({ step, inputSchema, migration, onUpdate }: Props) {
  const [generating, setGenerating] = useState(false);
  const prompt = (step.config.prompt as string) || "";
  const generatedCode = (step.config.generated_code as string) || "";
  const generatedType = (step.config.generated_type as string) || null;
  const approved = (step.config.approved as boolean) || false;

  const handleGenerate = async () => {
    setGenerating(true);
    try {
      const result = await api.ai.suggest({
        prompt,
        source_schema: inputSchema.map((c) => ({ column: c.name, type: c.type })),
        sample_data: [],
        target_schema: null,
      });
      onUpdate({
        config: {
          ...step.config,
          generated_type: "sql",
          generated_code: result.expression,
          approved: false,
        },
      });
    } finally {
      setGenerating(false);
    }
  };

  const handleApprove = () => {
    // Convert AI node to its generated type
    onUpdate({
      step_type: generatedType === "code" ? "code" : "sql",
      config: generatedType === "code"
        ? { function_code: generatedCode }
        : { expression: generatedCode },
    });
  };

  return (
    <div className="flex flex-col gap-4">
      <div>
        <label className="text-xs font-medium text-muted-foreground block mb-1">Prompt</label>
        <textarea
          className="w-full border rounded px-3 py-2 text-sm min-h-[80px] resize-y"
          value={prompt}
          onChange={(e) => onUpdate({ config: { ...step.config, prompt: e.target.value } })}
          placeholder="Describe the transformation you want, e.g. 'Hash the email column with SHA256'"
        />
        <Button
          size="sm"
          className="mt-2"
          onClick={handleGenerate}
          disabled={!prompt || generating}
        >
          <Sparkles className="h-3 w-3 mr-1" />
          {generating ? "Generating..." : "Generate"}
        </Button>
      </div>

      {generatedCode && (
        <div>
          <label className="text-xs font-medium text-muted-foreground block mb-1">
            Generated {generatedType === "code" ? "Python" : "SQL"}
          </label>
          <textarea
            className="w-full border rounded px-3 py-2 text-sm font-mono min-h-[150px] resize-y"
            value={generatedCode}
            onChange={(e) => onUpdate({
              config: { ...step.config, generated_code: e.target.value },
            })}
          />
          {!approved && (
            <Button size="sm" variant="outline" className="mt-2" onClick={handleApprove}>
              <Check className="h-3 w-3 mr-1" /> Approve & Convert
            </Button>
          )}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 6: Commit**

```bash
git add frontend/src/components/migration-builder/editors/
git commit -m "feat: add source, target, SQL, code, and AI node editors"
```

---

### Task 13: MigrationBuilderPage Orchestration

**Files:**
- Rewrite: `frontend/src/pages/MigrationBuilderPage.tsx`

- [ ] **Step 1: Rewrite page to orchestrate graph + editor**

```typescript
// frontend/src/pages/MigrationBuilderPage.tsx
import { useEffect, useState, useCallback } from "react";
import { useParams, useNavigate } from "react-router";
import { api } from "@/lib/api";
import { Canvas } from "@/components/migration-builder/Canvas";
import { EditorPanel } from "@/components/migration-builder/EditorPanel";
import { Button } from "@/components/ui/button";
import { Play, FlaskConical, Save } from "lucide-react";
import type { Migration } from "@/types/migration";
import type { Connection } from "@/types/connection";
import type { StepType, PipelineStep, PipelineTestResult, ColumnDef } from "@/types/pipeline";

const EMPTY_MIGRATION: Migration = {
  id: "", name: "Untitled Migration",
  source_connection_id: "", target_connection_id: "",
  source_table: "", target_table: "",
  source_query: "", source_schema: [],
  status: "draft", truncate_target: false,
  rows_processed: null, total_rows: null, error_message: null,
  created_at: "", updated_at: "",
  transformations: [], pipeline_steps: [],
};

export function MigrationBuilderPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const isNew = id === "new";

  const [migration, setMigration] = useState<Migration | null>(isNew ? { ...EMPTY_MIGRATION } : null);
  const [connections, setConnections] = useState<Connection[]>([]);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [testResults, setTestResults] = useState<PipelineTestResult | null>(null);

  const load = useCallback(async () => {
    if (!id || isNew) return;
    setMigration(await api.migrations.get(id));
  }, [id, isNew]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { api.connections.list().then(setConnections); }, []);

  // --- Handlers ---

  const handleSave = async () => {
    if (!migration) return;
    if (isNew) {
      const created = await api.migrations.create({
        name: migration.name,
        source_connection_id: migration.source_connection_id,
        target_connection_id: migration.target_connection_id,
        source_table: migration.source_table,
        target_table: migration.target_table,
        truncate_target: migration.truncate_target,
      });
      navigate(`/migrations/${created.id}`, { replace: true });
    } else if (id) {
      await api.migrations.update(id, {
        name: migration.name,
        source_table: migration.source_table,
        target_table: migration.target_table,
        truncate_target: migration.truncate_target,
      });
      load();
    }
  };

  const handleTest = async () => {
    if (!id || isNew) return;
    const result = await api.migrations.test(id);
    setTestResults(result as PipelineTestResult);
  };

  const handleRun = async () => {
    if (!id || isNew) return;
    await api.migrations.run(id);
    load();
  };

  const handleAddStep = async (type: StepType, afterNodeId: string) => {
    if (!id || isNew) return;
    const stepCount = migration?.pipeline_steps?.length || 0;
    const defaultNames: Record<StepType, string> = {
      sql: `SQL Transform ${stepCount + 1}`,
      code: `Code Transform ${stepCount + 1}`,
      ai: `AI Assistant ${stepCount + 1}`,
    };
    const defaultConfigs: Record<StepType, Record<string, unknown>> = {
      sql: { expression: "SELECT * FROM {prev}" },
      code: { function_code: "def transform(df):\n    return df\n" },
      ai: { prompt: "", generated_type: null, generated_code: null, approved: false },
    };

    // Determine insert_after: if afterNodeId is "source", pass null (append after source = position 0)
    // Otherwise pass the step ID
    const insertAfter = afterNodeId === "source" ? undefined : afterNodeId;

    await api.pipelineSteps.add(id, {
      step_type: type,
      name: defaultNames[type],
      config: defaultConfigs[type],
      insert_after: insertAfter,
    });
    load();
  };

  const handleDeleteStep = async (stepId: string) => {
    if (!id || isNew) return;
    await api.pipelineSteps.delete(id, stepId);
    if (selectedNodeId === stepId) setSelectedNodeId(null);
    load();
  };

  const handleUpdateMigration = (updates: Partial<Migration>) => {
    if (!migration) return;
    setMigration({ ...migration, ...updates });
  };

  const handleUpdateStep = async (stepId: string, updates: Record<string, unknown>) => {
    if (!id || isNew) return;
    await api.pipelineSteps.update(id, stepId, updates);
    load();
  };

  // --- Derived state ---

  const selectedStep: PipelineStep | null =
    selectedNodeId && selectedNodeId !== "source" && selectedNodeId !== "target"
      ? (migration?.pipeline_steps?.find((s) => s.id === selectedNodeId) || null)
      : null;

  const getInputSchema = (): ColumnDef[] => {
    if (!selectedNodeId || selectedNodeId === "source") return [];
    if (!migration) return [];
    const steps = migration.pipeline_steps || [];
    if (selectedNodeId === "target") {
      // Input to target = output of last step, or source schema
      if (steps.length === 0) return migration.source_schema || [];
      // Use test results if available
      const lastStepResult = testResults?.steps?.find((r) => r.node_id === steps[steps.length - 1].id);
      return lastStepResult?.schema || migration.source_schema || [];
    }
    const stepIdx = steps.findIndex((s) => s.id === selectedNodeId);
    if (stepIdx === 0) return migration.source_schema || [];
    const prevResult = testResults?.steps?.find((r) => r.node_id === steps[stepIdx - 1].id);
    return prevResult?.schema || migration.source_schema || [];
  };

  const getOutputSchema = (): ColumnDef[] => {
    if (!selectedNodeId || selectedNodeId === "target") return [];
    if (selectedNodeId === "source") return migration?.source_schema || [];
    const stepResult = testResults?.steps?.find((r) => r.node_id === selectedNodeId);
    return stepResult?.schema || [];
  };

  // --- Render ---

  if (!migration) return <div className="p-6">Loading...</div>;

  return (
    <div className="flex flex-1 flex-col">
      {/* Toolbar */}
      <div className="flex items-center gap-2 border-b px-4 py-2">
        <input
          className="font-semibold mr-4 bg-transparent border-none outline-none"
          value={migration.name}
          onChange={(e) => handleUpdateMigration({ name: e.target.value })}
        />
        <div className="flex-1" />
        <Button variant="outline" size="sm" onClick={handleSave}>
          <Save className="h-3 w-3 mr-1" /> Save
        </Button>
        {!isNew && (
          <>
            <Button variant="outline" size="sm" onClick={handleTest}>
              <FlaskConical className="h-3 w-3 mr-1" /> Test
            </Button>
            <Button size="sm" onClick={handleRun}>
              <Play className="h-3 w-3 mr-1" /> Run
            </Button>
          </>
        )}
      </div>

      {/* Graph */}
      <div className={`flex-1 ${selectedNodeId ? "h-[60%]" : ""}`}>
        <Canvas
          migration={migration}
          testResults={testResults?.steps || null}
          selectedNodeId={selectedNodeId}
          onNodeClick={setSelectedNodeId}
          onCanvasClick={() => setSelectedNodeId(null)}
          onAddStep={handleAddStep}
          onDeleteStep={handleDeleteStep}
        />
      </div>

      {/* Editor Panel — slides up when node selected */}
      {selectedNodeId && (
        <div className="h-[40%] border-t">
          <EditorPanel
            nodeId={selectedNodeId}
            migration={migration}
            connections={connections}
            step={selectedStep}
            inputSchema={getInputSchema()}
            outputSchema={getOutputSchema()}
            testResult={testResults?.steps?.find((r) => r.node_id === selectedNodeId) || null}
            onClose={() => setSelectedNodeId(null)}
            onUpdateMigration={handleUpdateMigration}
            onUpdateStep={handleUpdateStep}
          />
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Delete old transformation types**

```bash
rm frontend/src/types/transformation.ts
```

Update any remaining imports that reference `transformation.ts` — the only consumer should be the old `TransformNode.tsx` which was already deleted.

- [ ] **Step 3: Verify frontend compiles**

Run: `cd frontend && npm run build`

- [ ] **Step 4: Run backend tests for regressions**

Run: `cd backend && uv run pytest -v`

- [ ] **Step 5: Commit**

```bash
git add -A frontend/src/pages/MigrationBuilderPage.tsx frontend/src/types/
git commit -m "feat: complete migration builder v2 with graph + editor panel orchestration"
```

---

## Post-Implementation Checklist

- [ ] Delete unused files: `frontend/src/types/transformation.ts`, `frontend/src/components/migration-builder/nodes/TransformNode.tsx`
- [ ] Verify the old `transformations` router still works (backwards compat) or remove it if no longer needed
- [ ] Install backend deps: `cd backend && uv add duckdb polars`
- [ ] Run full test suite: `cd backend && uv run pytest -v`
- [ ] Run frontend build: `cd frontend && npm run build`
- [ ] Manual smoke test: create migration → add steps → test → verify graph + editor work
