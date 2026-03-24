# Kubectl Exec Adapter & Schema Display

## Problem

1. **Pod connections use the wrong mechanism.** The current `postgres_pod` type uses `kubectl port-forward` to tunnel a local port to the pod, then connects via psycopg3. But the actual use case is different: users have a pod with `psql` installed (a jump box) and need to `kubectl exec` into it to reach a Postgres server on the internal network. The port-forward approach is redundant — if you already have port-forwarding set up, you'd just use a direct connection.

2. **Schema display is too bare.** The current `ColumnSchema` only has `column`, `type`, `nullable`, and `is_primary_key`. It's missing foreign keys, unique constraints, defaults, and there's no visual distinction by type in the UI. Users exploring a database need to quickly understand the shape of a table — types, constraints, relationships.

3. **Connection config uses untyped dicts.** While `PostgresDirectConfig` and `PostgresPodConfig` Pydantic models exist, `ConnectionCreate.config` is typed as `dict` with a manual `@model_validator` and `parsed_config` property to validate. The top-level `type` field is separate from the config. This loses Pydantic's discriminated union capabilities, makes the OpenAPI schema opaque, and requires if/else chains in the factory to validate and dispatch.

## Design

### 1. Replace Port-Forward with Kubectl Exec

Remove the current `postgres_port_forward` connection type entirely. Replace it with a new `postgres_pod` type that uses `kubectl exec` + `psql`.

**How it works:**
1. Resolve a pod using label selectors (same mechanism as before)
2. `kubectl exec` into the pod
3. From inside the pod, run `psql -h <host> -p <port> -U <user> -d <database> -c "<query>"`
4. Parse the output back into structured data

The pod is the **execution environment** (a jump box with `psql`), not the database server. The database is a separate host reachable from within the cluster (managed service, another pod, etc.).

**Pod config fields:**

| Field | Type | Description |
|---|---|---|
| `type` | `Literal["postgres_pod"]` | Discriminator |
| `namespace` | `str` | K8s namespace |
| `pod_selector` | `str` | Label selector, e.g., `app=myservice` |
| `pick_strategy` | `Literal["first_ready", "name_contains"]` | How to pick from multiple matching pods (default: `first_ready`) |
| `pick_filter` | `str \| None` | Substring filter for `name_contains` strategy |
| `container` | `str \| None` | Only needed for multi-container pods |
| `host` | `str` | Postgres host reachable from inside the pod |
| `port` | `int` | Postgres port (default: 5432) |
| `database` | `str` | Database name |
| `user` | `str` | Database user |
| `password` | `str` | Database password |

**Direct config fields (unchanged except `type` moves inside):**

| Field | Type | Description |
|---|---|---|
| `type` | `Literal["postgres_direct"]` | Discriminator |
| `host` | `str` | Postgres host |
| `port` | `int` | Postgres port (default: 5432) |
| `database` | `str` | Database name |
| `user` | `str` | Database user |
| `password` | `str` | Database password |

**Removed:** `local_port` field (was for port-forwarding).

### 2. Discriminated Union for Connection Config

Replace untyped `config: dict` with Pydantic discriminated unions. The `type` field moves into the config (it's an attribute of the config, not the connection).

```python
from typing import Annotated, Literal, Union
from pydantic import Discriminator, Tag

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
    database: str
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
    config: ConnectionConfig  # Pydantic resolves automatically

class ConnectionResponse(BaseModel):
    id: str
    name: str
    config: ConnectionConfig
    created_at: str
    updated_at: str
```

This eliminates:
- Manual `@model_validator` for config parsing
- The `parsed_config` property
- The top-level `type` field on `ConnectionCreate` (it's inside `config`)
- If/else chains for config validation

FastAPI auto-generates accurate OpenAPI schemas for each connection type.

### 3. Registry-Based Adapter Factory

Replace if/else dispatch with a registry pattern:

```python
_adapter_registry: dict[str, type[DatabaseAdapter]] = {
    "postgres_direct": PostgresDirectAdapter,
    "postgres_pod": PostgresExecAdapter,
}

class DefaultAdapterFactory:
    async def create(self, connection: dict) -> DatabaseAdapter:
        config = connection["config"]
        adapter_cls = _adapter_registry.get(config["type"])
        if not adapter_cls:
            raise ValueError(f"Unsupported: {config['type']}")
        adapter = adapter_cls.from_config(config)
        await adapter.connect()
        return adapter
```

Each adapter implements a `from_config()` class method that accepts the Pydantic config model:

```python
class PostgresDirectAdapter(DatabaseAdapter):
    @classmethod
    def from_config(cls, config: PostgresDirectConfig) -> "PostgresDirectAdapter":
        return cls(host=config.host, port=config.port, database=config.database,
                   user=config.user, password=config.password)

class PostgresExecAdapter(DatabaseAdapter):
    @classmethod
    def from_config(cls, config: PostgresPodConfig) -> "PostgresExecAdapter":
        return cls(namespace=config.namespace, pod_selector=config.pod_selector,
                   pick_strategy=config.pick_strategy, pick_filter=config.pick_filter,
                   container=config.container, host=config.host, port=config.port,
                   database=config.database, user=config.user, password=config.password)
```

Adding a new database type means: add a config model, add an adapter class with `from_config()`, register it. No touching factory logic.

### 4. PostgresExecAdapter

New adapter implementing `DatabaseAdapter` via `kubectl exec` + `psql`. Each method call is a one-shot subprocess — no persistent connection.

**Lifecycle:**
- `connect()` — resolves the pod (kubectl get pods with selectors, filter Running+Ready, apply pick strategy), validates it's reachable
- `execute(query)` — `kubectl exec ... -- psql ... -c "<query>"`, parses output into `list[dict]`
- `disconnect()` — no-op (no persistent connection)

**Pod resolution flow (inside `connect()`):**
1. `kubectl get pods -n <namespace> -l <pod_selector> -o json`
2. Filter to pods in Running phase with Ready condition
3. If `pick_strategy` is `name_contains`, further filter by `pick_filter` substring
4. Pick the first matching pod, store its name for subsequent `execute()` calls
5. If no pod matches, raise an error

**Query execution:**
- Data queries and schema queries use the same mechanism
- Output format: `psql --csv -t` (CSV mode, tuples-only). This gives clean CSV output without headers or footers, which is trivial to parse with Python's `csv` module. Column names come from the query itself (known at call site). For schema/metadata queries that return simple values, single-column CSV is just one value per line.
- The adapter returns `list[dict]` like every other adapter
- Password is passed to psql via the connection URI format: `psql "postgresql://user:pass@host:port/db"` — avoids command-line arguments visible in process lists

**Error handling:**
- `connect()` raises `ConnectionError` if:
  - `kubectl` is not found on PATH
  - kubeconfig is invalid or cluster unreachable
  - No pods match the selector
  - No pods are in Running+Ready state
- `execute()` raises `RuntimeError` if:
  - The resolved pod is no longer available (stale pod name)
  - psql returns a non-zero exit code (syntax error, permission denied, etc.)
  - The subprocess times out (configurable, default 30s)
- All errors include the stderr output from kubectl/psql for diagnostics
- These exceptions surface as HTTP 502 (bad gateway) from the API layer, since the backend is proxying to an external system

### 5. Expanded ColumnSchema

Expand from 4 fields to 8:

```python
@dataclass
class ColumnSchema:
    name: str              # renamed from "column"
    type: str              # raw db type: "int4", "varchar", "timestamptz"
    category: str          # normalized: "number", "text", "datetime", etc.
    nullable: bool
    default: str | None
    is_primary_key: bool
    foreign_key: str | None  # "table.column" reference or None
    is_unique: bool
```

**Standard type categories** (adapter-provided, UI-consumed):

| Category | Postgres types | Icon |
|---|---|---|
| `number` | int2, int4, int8, float4, float8, numeric | Hash |
| `text` | varchar, text, char, bpchar | Type |
| `datetime` | timestamp, timestamptz, date, time, timetz | Calendar |
| `boolean` | bool | ToggleLeft |
| `json` | json, jsonb | Braces |
| `uuid` | uuid | Fingerprint |
| `array` | _int4, _text, etc. (any type prefixed with _) | List |
| `binary` | bytea | FileDigit |
| `unknown` | anything else | CircleHelp |

Each adapter maps its own type system to these categories. Adding a new database type (SingleStore, etc.) means implementing the mapping in that adapter — the frontend never changes.

**Shared mapping module:** `pg_types.py` contains the Postgres-specific type-to-category mapping, shared by both `PostgresDirectAdapter` and `PostgresExecAdapter`.

**Type field:** Uses `udt_name` from `information_schema.columns` (not `data_type`). `udt_name` gives the short Postgres type names (`int4`, `varchar`, `timestamptz`) which map cleanly to categories. `data_type` gives verbose names (`integer`, `character varying`, `timestamp with time zone`) that are harder to map and less useful for display.

**Schema query** — enhanced to fetch all properties in one query:

```sql
SELECT
    c.column_name,
    c.udt_name,
    c.is_nullable,
    c.column_default,
    CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_pk,
    CASE WHEN fk.column_name IS NOT NULL
         THEN fk.foreign_table || '.' || fk.foreign_column END as fk_ref,
    CASE WHEN uq.column_name IS NOT NULL THEN true ELSE false END as is_unique
FROM information_schema.columns c
LEFT JOIN (
    SELECT ku.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
    WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = <table>
) pk ON c.column_name = pk.column_name
LEFT JOIN (
    SELECT ku.column_name,
           ccu.table_name AS foreign_table,
           ccu.column_name AS foreign_column
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
    JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY' AND ku.table_name = <table>
) fk ON c.column_name = fk.column_name
LEFT JOIN (
    SELECT ku.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
    WHERE tc.constraint_type = 'UNIQUE' AND tc.table_name = <table>
) uq ON c.column_name = uq.column_name
WHERE c.table_schema = 'public' AND c.table_name = <table>
ORDER BY c.ordinal_position
```

### 6. Frontend Schema Display

**Explorer page** — richer schema view when a table is selected:

Each column row shows:
- Type icon (based on `category` from backend) with the standard icon mapping
- Column name — bold if NOT NULL, normal weight if nullable
- Raw type as a badge (e.g., `int4`, `varchar`)
- Default value — shown as muted text if present (e.g., `now()`, `0`, `gen_random_uuid()`)
- Primary key indicator: Key icon
- Foreign key indicator: Link icon with tooltip showing `-> table.column`
- Unique indicator: small badge

**Icon registry additions:**

```typescript
// Type category icons
type_number: Hash,
type_text: Type,
type_datetime: Calendar,
type_boolean: ToggleLeft,
type_json: Braces,
type_uuid: Fingerprint,
type_array: List,
type_binary: FileDigit,
type_unknown: CircleHelp,
// Constraint icons
constraint_pk: Key,
constraint_fk: Link,
constraint_unique: Snowflake,
```

**Connection form** — two visual sections for pod type:

```
+-- Pod Access ---------------------------------+
|  Namespace        Pod Selector                |
|  Pick Strategy    Pick Filter                 |
|  Container (optional)                         |
+-----------------------------------------------+
+-- Database -----------------------------------+
|  Host             Port                        |
|  User             Password                    |
|  Database (with discover button)              |
+-----------------------------------------------+
```

For `postgres_direct`, only the Database section shows. The sections have visible borders and labels to make the distinction clear.

### 7. Frontend Type Changes

Mirror the discriminated union on the frontend:

```typescript
interface PostgresDirectConfig {
  type: "postgres_direct";
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

interface PostgresPodConfig {
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

type ConnectionConfig = PostgresDirectConfig | PostgresPodConfig;
```

Remove `local_port`. The `type` field inside config acts as the discriminator for TypeScript narrowing.

## Files Changed

### Backend
- `backend/src/vonnegut/models/connection.py` — discriminated union, `type` moves into config, remove manual validators, remove `local_port`
- `backend/src/vonnegut/adapters/base.py` — expand `ColumnSchema` (8 fields, rename `column` to `name`)
- `backend/src/vonnegut/adapters/pg_types.py` (new) — shared Postgres type-to-category mapping
- `backend/src/vonnegut/adapters/postgres_direct.py` — update `fetch_schema()` for new fields, use `pg_types`
- `backend/src/vonnegut/adapters/postgres_exec.py` (new) — `PostgresExecAdapter` via kubectl exec + psql
- `backend/src/vonnegut/adapters/factory.py` — registry-based dispatch, `from_config()` class methods on adapters
- `backend/src/vonnegut/adapters/memory.py` — update for new `ColumnSchema` fields
- `backend/src/vonnegut/routers/connections.py` — use typed Pydantic models, remove top-level `type` field
- `backend/src/vonnegut/routers/explorer.py` — update for new schema shape
- `backend/src/vonnegut/database.py` — update schema if connection table stored `type` separately
- Tests for exec adapter, discriminated union validation, expanded schema, pod resolution

### Frontend
- `frontend/src/types/connection.ts` — discriminated union types, remove `local_port`
- `frontend/src/components/connections/ConnectionForm.tsx` — two-section layout, `type` inside config
- `frontend/src/components/connections/ConnectionList.tsx` — read type from `config.type`
- `frontend/src/config/iconRegistry.ts` — type category + constraint icons
- `frontend/src/pages/ExplorerPage.tsx` — type icons, constraint indicators, richer schema display
- `frontend/src/lib/api.ts` — update types if needed

## Out of Scope

- Schema caching (add later if slow)
- Memory limits on query results (later)
- Other database types (SingleStore, Iceberg, etc. — the design supports them but we only build Postgres adapters now)
- Pod health monitoring / reconnection
- Interactive psql sessions
