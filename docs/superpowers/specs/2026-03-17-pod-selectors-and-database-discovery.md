# Pod Selectors & Database Discovery

## Problem

1. **Pod connections are fragile.** The current `postgres_pod` config stores a literal `pod_name`, but pods are ephemeral — every deployment creates new pod names. Users must recreate the connection each time a pod is recycled.

2. **Database must be typed manually.** Users need to know the exact database name upfront. There's no way to discover which databases exist on a server before committing to one.

3. **Container field is over-specified.** The current config requires `container` with a default of `"postgres"`, but most pods have a single container. `kubectl port-forward` targets the pod, not a container — it only needs a container name when there are multiple.

## Design

### 1. Replace `pod_name` with `pod_selector`

The `PostgresPodConfig` model changes from storing a specific pod name to storing a Kubernetes label selector string. At connection time, the system queries for matching pods and picks one.

**Config changes:**

| Field | Before | After |
|---|---|---|
| `pod_name` | required string | removed |
| `pod_selector` | — | required string, e.g. `"app=postgres"` |
| `pick_strategy` | — | `"first_ready"` (default) or `"name_contains"` |
| `pick_filter` | — | optional string, used with `name_contains` strategy |
| `container` | required, default `"postgres"` | optional, no default (only needed for multi-container pods) |
| `local_port` | optional | optional (auto-assigned if omitted) |

**Pick strategies:**
- `first_ready`: Select the first pod in `Running` phase with all containers `Ready`. This is the default and covers most cases.
- `name_contains`: Filter matched pods by substring (e.g. `"primary"` to find the primary in a replica set), then pick the first ready one.

**Pod resolution flow:**
1. Run `kubectl get pods -n <namespace> -l <pod_selector> -o json`
2. Filter to pods in `Running` phase with `Ready` condition
3. If `pick_strategy` is `name_contains`, further filter by `pick_filter` substring in pod name
4. Pick the first matching pod
5. Port-forward to it (`kubectl port-forward` to localhost on `local_port` or an auto-assigned port)
6. Connect to Postgres via the forwarded port

### 2. Database Discovery Endpoint

Add a new endpoint that connects to a Postgres server (using the default `postgres` database) and returns the list of available databases.

**Endpoint:** `GET /api/v1/connections/{conn_id}/databases`

**Behavior:**
- Connects using the saved connection config, overriding `database` to `"postgres"` (the default database that always exists)
- Queries: `SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname`
- Returns: `["analytics", "mydb", "production"]`

**Frontend UX change:**
The connection form becomes two-stage:
1. Fill in connection details (host/port or pod selector + credentials) — database field is empty
2. Click "Test & Discover" — tests the connection and fetches available databases
3. A dropdown appears with discovered databases — user picks one
4. Save

The database field also accepts free-text typing as a fallback (in case discovery fails or user already knows the name).

### 3. DatabaseAdapter Interface Addition

Add `fetch_databases()` to the `DatabaseAdapter` ABC:

```python
@abstractmethod
async def fetch_databases(self) -> list[str]:
    """Return list of database names on this server."""
```

This keeps discovery as a first-class adapter capability alongside `fetch_tables()` and `fetch_schema()`.

## Files Changed

### Backend
- `backend/src/vonnegut/adapters/base.py` — add `fetch_databases()` to ABC
- `backend/src/vonnegut/adapters/postgres_direct.py` — implement `fetch_databases()`
- `backend/src/vonnegut/adapters/memory.py` — implement `fetch_databases()` for tests
- `backend/src/vonnegut/adapters/factory.py` — handle pod resolution in `create()` for `postgres_pod` type
- `backend/src/vonnegut/models/connection.py` — update `PostgresPodConfig` (selector fields, optional container)
- `backend/src/vonnegut/routers/explorer.py` — add `GET /connections/{id}/databases` endpoint
- `backend/src/vonnegut/database.py` — update CHECK constraint for connection type validation (no schema change needed since config is JSON)
- Tests for pod config validation, database discovery endpoint, pick strategy logic

### Frontend
- `frontend/src/types/connection.ts` — update `ConnectionConfig` fields
- `frontend/src/lib/api.ts` — add `connections.databases()` method
- `frontend/src/components/connections/ConnectionForm.tsx` — two-stage form with database discovery dropdown, optional container field
- `frontend/src/components/connections/ConnectionList.tsx` — show selector instead of pod name in subtitle

## Out of Scope

- Actually running `kubectl` commands (the pod adapter implementation itself is a separate task — this spec only covers the config model and API changes)
- Pod health monitoring / reconnection on pod failure
- Multi-cluster support
