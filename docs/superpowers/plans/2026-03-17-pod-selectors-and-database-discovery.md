# Pod Selectors & Database Discovery Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace static pod names with label selectors in pod connections, add database discovery, and make container optional.

**Architecture:** Update the `PostgresPodConfig` model to use `pod_selector` + `pick_strategy` instead of `pod_name`. Add `fetch_databases()` to the adapter interface. Add a database discovery endpoint. Update the frontend connection form to a two-stage flow with database dropdown.

**Tech Stack:** Python/FastAPI/Pydantic (backend), React/TypeScript (frontend), pytest

---

## Chunk 1: Backend Changes

### Task 1: Update Connection Model — Pod Selector Fields

**Files:**
- Modify: `backend/src/vonnegut/models/connection.py`
- Modify: `backend/tests/test_models_connection.py`

- [ ] **Step 1: Write failing tests for new PostgresPodConfig**

```python
# Add to tests/test_models_connection.py

def test_postgres_pod_config_with_selector():
    config = PostgresPodConfig(
        namespace="production",
        pod_selector="app=postgres",
        user="admin",
        password="secret",
    )
    assert config.pod_selector == "app=postgres"
    assert config.pick_strategy == "first_ready"
    assert config.pick_filter is None
    assert config.container is None
    assert config.local_port is None


def test_postgres_pod_config_with_name_contains_strategy():
    config = PostgresPodConfig(
        namespace="staging",
        pod_selector="app=postgres,release=v2",
        pick_strategy="name_contains",
        pick_filter="primary",
        container="pg",
        user="admin",
        password="secret",
    )
    assert config.pick_strategy == "name_contains"
    assert config.pick_filter == "primary"
    assert config.container == "pg"


def test_postgres_pod_config_rejects_invalid_strategy():
    from pydantic import ValidationError
    import pytest
    with pytest.raises(ValidationError):
        PostgresPodConfig(
            namespace="default",
            pod_selector="app=postgres",
            pick_strategy="invalid",
            user="admin",
            password="secret",
        )
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/dannymor/mydev/vonnegut/backend
uv run pytest tests/test_models_connection.py -v -k "pod_config"
```

Expected: New tests FAIL (old ones may still pass)

- [ ] **Step 3: Update PostgresPodConfig model**

Replace `PostgresPodConfig` in `backend/src/vonnegut/models/connection.py`:

```python
class PostgresPodConfig(BaseModel):
    namespace: str
    pod_selector: str
    pick_strategy: Literal["first_ready", "name_contains"] = "first_ready"
    pick_filter: str | None = None
    container: str | None = None
    database: str = ""
    user: str
    password: str
    local_port: int | None = None
```

- [ ] **Step 4: Update or remove old pod config tests that reference `pod_name`**

The existing tests `test_postgres_pod_config_optional_local_port`, `test_postgres_pod_config_with_local_port`, and `test_connection_create_pod` reference `pod_name` and `container` as required. Update them to use the new fields:

```python
def test_postgres_pod_config_optional_local_port():
    config = PostgresPodConfig(
        namespace="default",
        pod_selector="app=postgres",
        user="admin",
        password="secret",
    )
    assert config.local_port is None
    assert config.container is None


def test_postgres_pod_config_with_local_port():
    config = PostgresPodConfig(
        namespace="default",
        pod_selector="app=postgres",
        local_port=15432,
        user="admin",
        password="secret",
    )
    assert config.local_port == 15432


def test_connection_create_pod():
    conn = ConnectionCreate(
        name="K8s Prod",
        type="postgres_pod",
        config={"namespace": "production", "pod_selector": "app=postgres", "user": "admin", "password": "secret"},
    )
    assert conn.parsed_config.namespace == "production"
    assert conn.parsed_config.pod_selector == "app=postgres"
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd /Users/dannymor/mydev/vonnegut/backend
uv run pytest tests/test_models_connection.py -v
```

Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add backend/src/vonnegut/models/connection.py backend/tests/test_models_connection.py
git commit -m "feat: replace pod_name with pod_selector in PostgresPodConfig"
```

---

### Task 2: Add `fetch_databases()` to Adapter Interface & Implementations

**Files:**
- Modify: `backend/src/vonnegut/adapters/base.py`
- Modify: `backend/src/vonnegut/adapters/postgres_direct.py`
- Modify: `backend/src/vonnegut/adapters/memory.py`
- Modify: `backend/tests/test_adapter_base.py`
- Modify: `backend/tests/test_adapter_memory.py`

- [ ] **Step 1: Write failing test for InMemoryAdapter.fetch_databases**

```python
# Add to tests/test_adapter_memory.py

async def test_fetch_databases():
    adapter = InMemoryAdapter()
    adapter.add_database("analytics")
    adapter.add_database("production")
    result = await adapter.fetch_databases()
    assert result == ["analytics", "production"]


async def test_fetch_databases_empty():
    adapter = InMemoryAdapter()
    result = await adapter.fetch_databases()
    assert result == []
```

- [ ] **Step 2: Write failing test for interface**

```python
# Add to tests/test_adapter_base.py

def test_database_adapter_defines_fetch_databases():
    assert hasattr(DatabaseAdapter, "fetch_databases")
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cd /Users/dannymor/mydev/vonnegut/backend
uv run pytest tests/test_adapter_base.py tests/test_adapter_memory.py -v -k "database"
```

- [ ] **Step 4: Add `fetch_databases` to DatabaseAdapter ABC**

Add to `backend/src/vonnegut/adapters/base.py`:

```python
@abstractmethod
async def fetch_databases(self) -> list[str]:
    """Return list of database names on this server."""
```

- [ ] **Step 5: Implement in PostgresDirectAdapter**

Add to `backend/src/vonnegut/adapters/postgres_direct.py`:

```python
async def fetch_databases(self) -> list[str]:
    cursor = await self._conn.execute(
        "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
    )
    rows = await cursor.fetchall()
    return [row[0] for row in rows]
```

- [ ] **Step 6: Implement in InMemoryAdapter**

Add a `_databases` list to `InMemoryAdapter.__init__` and implement:

```python
def __init__(self):
    # ... existing init ...
    self._databases: list[str] = []

def add_database(self, name: str):
    if name not in self._databases:
        self._databases.append(name)

async def fetch_databases(self) -> list[str]:
    return list(self._databases)
```

- [ ] **Step 7: Run tests to verify they pass**

```bash
cd /Users/dannymor/mydev/vonnegut/backend
uv run pytest tests/test_adapter_base.py tests/test_adapter_memory.py -v
```

Expected: All PASS

- [ ] **Step 8: Commit**

```bash
git add backend/src/vonnegut/adapters/ backend/tests/test_adapter_base.py backend/tests/test_adapter_memory.py
git commit -m "feat: add fetch_databases() to adapter interface and implementations"
```

---

### Task 3: Add Database Discovery Endpoint

**Files:**
- Modify: `backend/src/vonnegut/routers/explorer.py`
- Modify: `backend/tests/test_api_explorer.py`

- [ ] **Step 1: Write failing test**

```python
# Add to tests/test_api_explorer.py

async def test_list_databases(client, saved_connection_id, memory_adapter):
    memory_adapter.add_database("analytics")
    memory_adapter.add_database("production")
    resp = await client.get(f"/api/v1/connections/{saved_connection_id}/databases")
    assert resp.status_code == 200
    assert resp.json() == ["analytics", "production"]


async def test_databases_nonexistent_connection(client):
    resp = await client.get("/api/v1/connections/nonexistent/databases")
    assert resp.status_code == 404
```

Note: The test fixtures (`client`, `saved_connection_id`, `memory_adapter`) should already exist from the explorer tests. If the `memory_adapter` fixture doesn't expose `add_database` yet, update the fixture after implementing Task 2.

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/dannymor/mydev/vonnegut/backend
uv run pytest tests/test_api_explorer.py -v -k "database"
```

- [ ] **Step 3: Add the endpoint**

Add to `backend/src/vonnegut/routers/explorer.py`:

```python
@router.get("/connections/{conn_id}/databases")
async def list_databases(conn_id: str, request: Request):
    manager = _get_manager(request)
    conn = await manager.get(conn_id)
    if conn is None:
        raise HTTPException(status_code=404, detail="Connection not found")
    adapter = await _get_adapter_factory(request).create(conn)
    try:
        return await adapter.fetch_databases()
    finally:
        await adapter.disconnect()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/dannymor/mydev/vonnegut/backend
uv run pytest tests/test_api_explorer.py -v
```

Expected: All PASS

- [ ] **Step 5: Run all backend tests**

```bash
cd /Users/dannymor/mydev/vonnegut/backend
uv run pytest -v
```

Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add backend/src/vonnegut/routers/explorer.py backend/tests/test_api_explorer.py
git commit -m "feat: add database discovery endpoint GET /connections/{id}/databases"
```

---

## Chunk 2: Frontend Changes

### Task 4: Update TypeScript Types & API Client

**Files:**
- Modify: `frontend/src/types/connection.ts`
- Modify: `frontend/src/lib/api.ts`

- [ ] **Step 1: Update ConnectionConfig type**

```typescript
// frontend/src/types/connection.ts
export interface ConnectionConfig {
  // Direct connection
  host?: string;
  port?: number;
  // Pod connection
  namespace?: string;
  pod_selector?: string;
  pick_strategy?: "first_ready" | "name_contains";
  pick_filter?: string;
  container?: string;
  local_port?: number;
  // Shared
  database?: string;
  user?: string;
  password?: string;
}

export interface Connection {
  id: string;
  name: string;
  type: "postgres_direct" | "postgres_pod";
  config: ConnectionConfig;
  created_at: string;
  updated_at: string;
}

export interface ConnectionCreate {
  name: string;
  type: "postgres_direct" | "postgres_pod";
  config: ConnectionConfig;
}

export interface ConnectionTestResult {
  status: "ok" | "error";
  message: string;
}
```

- [ ] **Step 2: Add databases API method**

Add to the `connections` object in `frontend/src/lib/api.ts`:

```typescript
databases: (id: string) => request<string[]>(`/connections/${id}/databases`),
```

- [ ] **Step 3: Verify types compile**

```bash
cd /Users/dannymor/mydev/vonnegut/frontend
npx tsc --noEmit
```

- [ ] **Step 4: Commit**

```bash
git add frontend/src/types/connection.ts frontend/src/lib/api.ts
git commit -m "feat: update connection types for pod selectors and add databases API"
```

---

### Task 5: Update Connection Form — Two-Stage with Database Discovery

**Files:**
- Modify: `frontend/src/components/connections/ConnectionForm.tsx`

- [ ] **Step 1: Rewrite ConnectionForm with pod selector fields and database discovery**

```tsx
// frontend/src/components/connections/ConnectionForm.tsx
import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { api } from "@/lib/api";
import { icons } from "@/config/iconRegistry";
import type { Connection, ConnectionCreate } from "@/types/connection";

interface Props {
  open: boolean;
  onClose: () => void;
  onSave: (data: ConnectionCreate) => void;
  initial?: Connection | null;
}

export function ConnectionForm({ open, onClose, onSave, initial }: Props) {
  const [name, setName] = useState(initial?.name ?? "");
  const [type, setType] = useState<"postgres_direct" | "postgres_pod">(
    initial?.type ?? "postgres_direct"
  );
  // Direct fields
  const [host, setHost] = useState(initial?.config.host ?? "localhost");
  const [port, setPort] = useState(String(initial?.config.port ?? 5432));
  // Pod fields
  const [namespace, setNamespace] = useState(initial?.config.namespace ?? "default");
  const [podSelector, setPodSelector] = useState(initial?.config.pod_selector ?? "");
  const [pickStrategy, setPickStrategy] = useState<"first_ready" | "name_contains">(
    initial?.config.pick_strategy ?? "first_ready"
  );
  const [pickFilter, setPickFilter] = useState(initial?.config.pick_filter ?? "");
  const [container, setContainer] = useState(initial?.config.container ?? "");
  // Shared fields
  const [database, setDatabase] = useState(initial?.config.database ?? "");
  const [user, setUser] = useState(initial?.config.user ?? "");
  const [password, setPassword] = useState("");
  // Database discovery
  const [databases, setDatabases] = useState<string[]>([]);
  const [discovering, setDiscovering] = useState(false);
  const [discoverError, setDiscoverError] = useState<string | null>(null);

  const handleDiscover = async () => {
    // Save a temporary connection to test against, then discover databases
    // For now, we need a saved connection to call the API.
    // If editing an existing connection, use its ID directly.
    if (!initial?.id) {
      setDiscoverError("Save the connection first, then use Test & Discover.");
      return;
    }
    setDiscovering(true);
    setDiscoverError(null);
    try {
      const result = await api.connections.databases(initial.id);
      setDatabases(result);
    } catch (e: unknown) {
      setDiscoverError(e instanceof Error ? e.message : "Discovery failed");
    } finally {
      setDiscovering(false);
    }
  };

  const handleSubmit = () => {
    const config =
      type === "postgres_direct"
        ? { host, port: Number(port), database, user, password }
        : {
            namespace,
            pod_selector: podSelector,
            pick_strategy: pickStrategy,
            ...(pickStrategy === "name_contains" && pickFilter ? { pick_filter: pickFilter } : {}),
            ...(container ? { container } : {}),
            database,
            user,
            password,
          };
    onSave({ name, type, config });
    onClose();
  };

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>{initial ? "Edit Connection" : "New Connection"}</DialogTitle>
        </DialogHeader>
        <div className="grid gap-4 py-4">
          <div className="grid gap-2">
            <Label>Name</Label>
            <Input value={name} onChange={(e) => setName(e.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label>Type</Label>
            <select
              className="border rounded px-3 py-2 text-sm"
              value={type}
              onChange={(e) => setType(e.target.value as typeof type)}
            >
              <option value="postgres_direct">Direct</option>
              <option value="postgres_pod">Kubernetes Pod</option>
            </select>
          </div>

          {type === "postgres_direct" ? (
            <div className="grid grid-cols-2 gap-2">
              <div><Label>Host</Label><Input value={host} onChange={(e) => setHost(e.target.value)} /></div>
              <div><Label>Port</Label><Input value={port} onChange={(e) => setPort(e.target.value)} /></div>
            </div>
          ) : (
            <>
              <div className="grid grid-cols-2 gap-2">
                <div><Label>Namespace</Label><Input value={namespace} onChange={(e) => setNamespace(e.target.value)} /></div>
                <div>
                  <Label>Pod Selector</Label>
                  <Input
                    value={podSelector}
                    onChange={(e) => setPodSelector(e.target.value)}
                    placeholder="app=postgres"
                  />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <Label>Pick Strategy</Label>
                  <select
                    className="w-full border rounded px-3 py-2 text-sm"
                    value={pickStrategy}
                    onChange={(e) => setPickStrategy(e.target.value as typeof pickStrategy)}
                  >
                    <option value="first_ready">First Ready</option>
                    <option value="name_contains">Name Contains</option>
                  </select>
                </div>
                {pickStrategy === "name_contains" && (
                  <div>
                    <Label>Name Filter</Label>
                    <Input
                      value={pickFilter}
                      onChange={(e) => setPickFilter(e.target.value)}
                      placeholder="primary"
                    />
                  </div>
                )}
              </div>
              <div>
                <Label>Container <span className="text-xs text-muted-foreground">(optional, for multi-container pods)</span></Label>
                <Input
                  value={container}
                  onChange={(e) => setContainer(e.target.value)}
                  placeholder="Leave empty for single-container pods"
                />
              </div>
            </>
          )}

          <div className="grid grid-cols-2 gap-2">
            <div><Label>User</Label><Input value={user} onChange={(e) => setUser(e.target.value)} /></div>
            <div><Label>Password</Label><Input type="password" value={password} onChange={(e) => setPassword(e.target.value)} /></div>
          </div>

          {/* Database field with discovery */}
          <div className="grid gap-2">
            <div className="flex items-center justify-between">
              <Label>Database</Label>
              {initial?.id && (
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  onClick={handleDiscover}
                  disabled={discovering}
                  className="text-xs h-6"
                >
                  {discovering ? "Discovering..." : "Discover databases"}
                </Button>
              )}
            </div>
            {databases.length > 0 ? (
              <select
                className="border rounded px-3 py-2 text-sm"
                value={database}
                onChange={(e) => setDatabase(e.target.value)}
              >
                <option value="">Select a database...</option>
                {databases.map((db) => (
                  <option key={db} value={db}>{db}</option>
                ))}
              </select>
            ) : (
              <Input
                value={database}
                onChange={(e) => setDatabase(e.target.value)}
                placeholder="Enter database name or save first to discover"
              />
            )}
            {discoverError && (
              <p className="text-xs text-destructive">{discoverError}</p>
            )}
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={handleSubmit}>Save</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
```

- [ ] **Step 2: Verify build succeeds**

```bash
cd /Users/dannymor/mydev/vonnegut/frontend
npm run build
```

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/connections/ConnectionForm.tsx
git commit -m "feat: update connection form with pod selectors and database discovery"
```

---

### Task 6: Update Connection List Display

**Files:**
- Modify: `frontend/src/components/connections/ConnectionList.tsx`

- [ ] **Step 1: Update subtitle to show pod selector instead of pod name**

In the connection list, change the pod subtitle from `${conn.config.namespace}/${conn.config.pod_name}` to `${conn.config.namespace} | ${conn.config.pod_selector}`.

Replace the subtitle section:

```tsx
<div className="text-sm text-muted-foreground">
  {conn.type === "postgres_direct"
    ? `${conn.config.host}:${conn.config.port}/${conn.config.database}`
    : `${conn.config.namespace} | ${conn.config.pod_selector}`}
</div>
```

- [ ] **Step 2: Verify build succeeds**

```bash
cd /Users/dannymor/mydev/vonnegut/frontend
npm run build
```

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/connections/ConnectionList.tsx
git commit -m "feat: show pod selector in connection list subtitle"
```

---

### Task 7: Verify Full Build & Tests

**Files:** None (verification only)

- [ ] **Step 1: Run all backend tests**

```bash
cd /Users/dannymor/mydev/vonnegut/backend
uv run pytest -v
```

Expected: All tests PASS

- [ ] **Step 2: Run full frontend build**

```bash
cd /Users/dannymor/mydev/vonnegut/frontend
npm run build
```

Expected: Build succeeds
