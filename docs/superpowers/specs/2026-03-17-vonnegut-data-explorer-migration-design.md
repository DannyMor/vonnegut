# Vonnegut v1 — Data Explorer & Migration Platform

## Purpose

A single-user tool for exploring, transforming, and migrating data between Postgres databases, including databases running on Kubernetes pods. Provides a visual pipeline builder for defining transformations with AI-assisted suggestions via Claude.

## Tech Stack

### Backend
- **Runtime:** Python 3.14.3
- **Package manager:** uv
- **Framework:** FastAPI 0.135.1
- **Validation:** Pydantic 2.12.5
- **Testing:** pytest 9.0.2
- **Database driver:** psycopg 3.3.3
- **App state:** SQLite (via aiosqlite 0.22.1)
- **Secret encryption:** cryptography 46.0.5 (Fernet)
- **AI integration:** Anthropic Python SDK 0.85.0
- **K8s access:** subprocess (kubectl commands)

### Frontend
- **Runtime:** Node.js 24.14.0 (LTS)
- **Framework:** React 19.2.4 + TypeScript 5.9.3
- **Build tool:** Vite 8.0.0
- **Routing:** React Router 7.13.1
- **UI components:** shadcn/ui
- **Styling:** Tailwind CSS 4.2.1
- **Data tables:** TanStack React Table 8.21.3 (via shadcn/ui)
- **Visual pipeline:** React Flow (@xyflow/react) 12.10.1
- **Icons:** Lucide React 0.577.0 (default, swappable via registry)

## Architecture

```
Frontend (React + Vite)
    │
    ▼ REST API
FastAPI Backend
    │
    ├──► SQLite (app state + encrypted secrets)
    ├──► Source Postgres (direct or via kubectl)
    └──► Target Postgres (direct or via kubectl)
```

Single-user, no authentication in v1. Frontend and backend communicate via REST. No WebSockets — polling for migration progress (recommended interval: 1 second during active runs).

CORS middleware is configured on the FastAPI backend to allow requests from the Vite dev server during development (default: `http://localhost:5173`).

## Data Model

### Connection

| Field        | Type                                    | Notes                          |
|--------------|-----------------------------------------|--------------------------------|
| id           | UUID                                    | Primary key                    |
| name         | str                                     | User-friendly label            |
| type         | "postgres_direct" \| "postgres_pod"     | Discriminator                  |
| config       | ConnectionConfig (JSON)                 | Sensitive fields encrypted     |
| created_at   | datetime                                |                                |
| updated_at   | datetime                                |                                |

**PostgresDirectConfig:** host, port, database, user, password (encrypted)

**PostgresPodConfig:** namespace, pod_name, container, database, user, password (encrypted), local_port

### Migration

| Field                | Type                                                        | Notes              |
|----------------------|-------------------------------------------------------------|--------------------|
| id                   | UUID                                                        | Primary key        |
| name                 | str                                                         |                    |
| source_connection_id | UUID                                                        | FK to Connection   |
| target_connection_id | UUID                                                        | FK to Connection   |
| source_table         | str                                                         |                    |
| target_table         | str                                                         |                    |
| status               | "draft" \| "testing" \| "running" \| "completed" \| "failed" \| "cancelled" |                    |
| rows_processed       | int \| null                                                  | Updated during run |
| total_rows           | int \| null                                                  | Set before run     |
| error_message        | str \| null                                                  | Set on failure     |
| created_at           | datetime                                                    |                    |
| updated_at           | datetime                                                    |                    |

### Transformation

| Field        | Type                                                    | Notes                        |
|--------------|---------------------------------------------------------|------------------------------|
| id           | UUID                                                    | Primary key                  |
| migration_id | UUID                                                    | FK to Migration              |
| order        | int                                                     | Position in pipeline         |
| type         | "column_mapping" \| "sql_expression" \| "ai_generated"  | Extensible for future types  |
| config       | dict (JSON)                                             | Shape depends on type        |
| created_at   | datetime                                                |                              |
| updated_at   | datetime                                                |                              |

**Config shapes by type:**

- `column_mapping`: `{ mappings: [{ source_col: str, target_col: str | null, drop: bool }] }` — one transformation holds all column mappings for the migration. When `drop: true`, the source column is excluded from the output and `target_col` is ignored (should be null).
- `sql_expression`: `{ expression: str, output_column: str }`
- `ai_generated`: `{ prompt: str, generated_expression: str, approved: bool }`

The `type` field is extensible — Python-based transformations can be added later as a new type + config shape.

## API Design

All endpoints are prefixed with `/api/v1/`.

### Connections

| Method | Path                            | Description                                    |
|--------|---------------------------------|------------------------------------------------|
| POST   | /connections                    | Create connection (validates connectivity)      |
| GET    | /connections                    | List all connections                            |
| GET    | /connections/{id}               | Get connection details (passwords masked)       |
| PUT    | /connections/{id}               | Update a connection                             |
| DELETE | /connections/{id}               | Delete a connection                             |
| POST   | /connections/{id}/test          | Test connectivity                               |

### Schema & Data Exploration

| Method | Path                                       | Description                          |
|--------|--------------------------------------------|--------------------------------------|
| GET    | /connections/{id}/tables                   | List all tables in the database      |
| GET    | /connections/{id}/tables/{table}/schema    | Column names, types, constraints, PKs|
| GET    | /connections/{id}/tables/{table}/sample    | Sample rows (query param: rows=10)   |

### Migrations

| Method | Path                                        | Description                              |
|--------|---------------------------------------------|------------------------------------------|
| POST   | /migrations                                 | Create migration (draft)                 |
| GET    | /migrations                                 | List all migrations                      |
| GET    | /migrations/{id}                            | Get migration with transformations       |
| PUT    | /migrations/{id}                            | Update migration config                  |
| DELETE | /migrations/{id}                            | Delete migration                         |
| POST   | /migrations/{id}/test                       | Run on sample data, return before/after  |
| POST   | /migrations/{id}/run                        | Execute the migration                    |
| POST   | /migrations/{id}/cancel                     | Cancel a running migration               |
| GET    | /migrations/{id}/status                     | Poll progress (rows processed, errors)   |

### Transformations

| Method | Path                                                  | Description                    |
|--------|-------------------------------------------------------|--------------------------------|
| POST   | /migrations/{id}/transformations                      | Add transformation to pipeline |
| PUT    | /migrations/{id}/transformations/{t_id}               | Update a transformation        |
| DELETE | /migrations/{id}/transformations/{t_id}               | Remove from pipeline           |
| PUT    | /migrations/{id}/transformations/reorder              | Reorder pipeline (drag-and-drop)|

**Reorder request body:** `{ "order": [UUID, UUID, ...] }` — ordered list of transformation IDs. All transformation IDs for the migration must be included.

### AI Assistant

| Method | Path                          | Description                                        |
|--------|-------------------------------|----------------------------------------------------|
| POST   | /ai/suggest-transformation    | Send sample data + prompt, get suggested expression |

**AI suggest-transformation request/response:**

Request body:
- `prompt`: str — natural language description of desired transformation
- `source_schema`: list of `{ column: str, type: str }` — source table schema
- `sample_data`: list of dicts — sample rows from source (up to 10)
- `target_schema`: list of `{ column: str, type: str }` | null — target table schema if known

Response body:
- `expression`: str — suggested SQL expression
- `output_column`: str — suggested output column name
- `explanation`: str — human-readable explanation of what the expression does

### Error Responses

All error responses use a consistent shape:

- `400` — bad request (invalid input, transformation syntax error)
- `404` — resource not found (connection, migration, transformation)
- `409` — conflict (e.g., deleting a connection used by an active migration)
- `422` — validation error (Pydantic, returned automatically by FastAPI)
- `500` — internal server error (unexpected failures)

Response body: `{ "detail": str }` — human-readable error message.

## Postgres Connectivity

Three access patterns, abstracted behind a common interface:

1. **Direct connection:** Standard psycopg connection using host/port/credentials.
2. **kubectl port-forward:** Spawn a `kubectl port-forward` subprocess to expose the pod's Postgres port locally, then connect via psycopg to localhost on the forwarded port.
3. **kubectl exec:** Run SQL commands via `kubectl exec` into the pod, piping through `psql`.

A `ConnectionManager` class handles lifecycle (opening/closing connections, starting/stopping port-forwards). Each connection type implements a common `DatabaseAdapter` interface with methods: `connect()`, `disconnect()`, `execute(query)`, `fetch_tables()`, `fetch_schema(table)`, `fetch_sample(table, rows)`.

The `postgres_pod.py` adapter handles both kubectl access patterns (port-forward and exec). It uses port-forward by default (better performance, full psycopg support) and falls back to exec for environments where port-forwarding is not available. When using exec fallback, `psql` is invoked with `--csv` flag for machine-readable output. Known limitations of exec fallback: no binary data support, potential encoding issues with non-UTF8 data.

The `local_port` field in `PostgresPodConfig` is optional. When omitted, the adapter auto-assigns an available ephemeral port for port-forwarding. If a specified port is already in use, the connection test fails with a clear error message.

## Migration Execution

**Read strategy:** Full `SELECT` from source table into memory (v1). A pre-flight `SELECT COUNT(*)` populates `total_rows` on the Migration model. If `total_rows` exceeds 100,000 rows, the backend rejects the run with a 400 error and a clear message suggesting the user filter or wait for future batching support.

**Write strategy:** `INSERT INTO` target table using parameterized queries via psycopg. Rows are inserted in batches of 1,000. `rows_processed` is updated after each batch.

**Target table handling:** The target table must already exist. The backend does not auto-create tables. If the target table does not exist, the run fails with a 404 error.

**Conflict behavior:** By default, the target table is **not** truncated — rows are appended. The migration config includes an optional `truncate_target` boolean (default `false`). When `true`, the target table is truncated within the same transaction before inserting.

**Transaction semantics:** The entire migration runs in a single transaction on the target database. If the migration fails or is cancelled, the transaction is rolled back — no partial data is written.

**Cancellation:** The migration runner checks a cancellation flag between each batch. When `POST /migrations/{id}/cancel` is called, the flag is set, the current batch completes, and the transaction is rolled back. The migration status is set to `"cancelled"`.

**Transformation application:** Transformations are applied in Python after reading from source and before writing to target. The `transformation_engine` processes each row through the ordered pipeline (column mappings first, then SQL expressions evaluated via simple expression parsing, then AI-generated expressions which are stored as SQL expressions once approved).

## Secret Encryption

Sensitive fields (passwords) are encrypted at rest in SQLite using Fernet symmetric encryption.

- Master key is loaded from the `VONNEGUT_SECRET_KEY` environment variable.
- If not set, a key is auto-generated and stored in `~/.vonnegut/secret.key` on first run.
- Encryption/decryption happens in the Pydantic model layer — the rest of the app works with plaintext values in memory.

## Frontend Structure

### Navigation

Sidebar with three main sections, each with an icon (from Lucide, swappable via icon registry):

- **Connections** (Plug icon) — manage database connections
- **Explorer** (Search icon) — browse schemas and preview data
- **Migrations** (Workflow icon) — build and run migration pipelines

Sidebar is collapsible to icon-only mode.

### Pages

**Connections page:**
- List saved connections with status indicators (green/red)
- Add/edit/delete connections via modal forms
- "Test Connection" button with visual feedback

**Explorer page:**
- Select a connection from a dropdown
- Left sidebar: table list for the selected database
- Main area: schema view (columns, types, constraints) + sample data table
- Data table uses shadcn/ui (TanStack Table) with sorting and filtering

**Migrations list page:**
- List all migrations with name, status (color-coded badge), source/target info, and timestamps
- Actions per migration: open in builder, duplicate, delete
- "New Migration" button to create and open a fresh draft

**Migration Builder page (core experience):**
- Full-screen React Flow canvas
- Source node (left): select connection + table, shows schema preview
- Target node (right): select connection + table
- Transformation nodes (middle): chained between source and target
- Toolbar: add transformations (column mapping, SQL expression, AI-assisted)
- Click any node: opens config panel (slide-out drawer)
- "Test" button: runs pipeline on sample data, shows before/after split table
- "Run" button: executes migration with progress indicator

**AI Transformation dialog:**
- Opens within the migration builder
- Text input for natural language prompt
- Shows suggested SQL expression
- Preview effect on sample data
- Accept/edit/reject actions

### Node Design in React Flow

**Colors (configurable via `nodeTheme.ts`):**
- Source nodes: blue
- Target nodes: green
- Column mapping transforms: amber
- SQL expression transforms: purple
- AI-generated transforms: teal (with sparkle icon)

**View modes:**
- Full view: icon + label + details
- Compact view: icon + short label only
- Toggle via toolbar or driven by zoom level

**Hover tooltips (shadcn/ui HoverCard, read-only):**
- Source/Target: connection name, database, table, row count, first ~8 columns with types ("... +N more")
- Transformations: type, expression/mapping config, mini preview (2-3 rows)

### Icon System

Central icon registry (`iconRegistry.ts`) maps semantic names to Lucide components:

- `source` → Database
- `target` → DatabaseZap
- `column_mapping` → ArrowRightLeft
- `sql_expression` → Code
- `ai_generated` → Sparkles
- `connection_ok` → CircleCheck
- `connection_error` → CircleX
- `nav_connections` → Plug
- `nav_explorer` → Search
- `nav_migrations` → Workflow

Icons appear in: navigation, page headers, node headers, list items, dropdowns, action buttons, status indicators.

Swappable: update the registry to use custom SVGs or a different icon library without touching components.

### Theming

- Tailwind theme config (`tailwind.config.ts`) defines the color palette
- shadcn/ui CSS variables in `globals.css` control component colors
- Node colors in `nodeTheme.ts` are independent from the UI theme
- All configurable — change config files to update the entire look without touching components
- Dark/light mode supported via Tailwind

## Scope Boundaries

**In v1:**
- Single user, no auth
- Postgres only (direct + K8s pods)
- Column mapping + SQL expression + AI-generated transformations
- Small-scale data (loads into memory)
- Visual pipeline builder with React Flow
- Before/after test preview
- AI-assisted transformation suggestions via Claude SDK

**Designed for but not in v1:**
- Multi-user auth
- Additional database types (MySQL, Snowflake, etc.)
- Python-based transformations (new type in the extensible system)
- Large-scale batching/streaming/resumability
- UI-based theme editor
- Reusable migration templates
- Logging/audit trail

## Project Structure

```
vonnegut/
├── backend/
│   ├── pyproject.toml          # uv project config
│   ├── src/
│   │   └── vonnegut/
│   │       ├── __init__.py
│   │       ├── main.py         # FastAPI app entry
│   │       ├── config.py       # App settings
│   │       ├── database.py     # SQLite setup
│   │       ├── encryption.py   # Fernet encrypt/decrypt
│   │       ├── models/         # Pydantic models
│   │       │   ├── connection.py
│   │       │   ├── migration.py
│   │       │   └── transformation.py
│   │       ├── routers/        # FastAPI routers (v1)
│   │       │   ├── connections.py
│   │       │   ├── explorer.py
│   │       │   ├── migrations.py
│   │       │   ├── transformations.py
│   │       │   └── ai.py
│   │       ├── services/       # Business logic
│   │       │   ├── connection_manager.py
│   │       │   ├── migration_runner.py
│   │       │   ├── transformation_engine.py
│   │       │   └── ai_assistant.py
│   │       └── adapters/       # Database adapters
│   │           ├── base.py     # DatabaseAdapter interface
│   │           ├── postgres_direct.py
│   │           └── postgres_pod.py
│   └── tests/
│       ├── conftest.py
│       ├── test_connections.py
│       ├── test_explorer.py
│       ├── test_migrations.py
│       ├── test_transformations.py
│       └── test_ai.py
├── frontend/
│   ├── package.json
│   ├── tsconfig.json
│   ├── vite.config.ts
│   ├── tailwind.config.ts
│   ├── src/
│   │   ├── main.tsx
│   │   ├── App.tsx
│   │   ├── globals.css
│   │   ├── config/
│   │   │   ├── nodeTheme.ts
│   │   │   └── iconRegistry.ts
│   │   ├── components/
│   │   │   ├── ui/             # shadcn/ui components
│   │   │   ├── layout/
│   │   │   │   ├── Sidebar.tsx
│   │   │   │   └── PageHeader.tsx
│   │   │   ├── connections/
│   │   │   ├── explorer/
│   │   │   └── migration-builder/
│   │   │       ├── Canvas.tsx
│   │   │       ├── nodes/
│   │   │       │   ├── SourceNode.tsx
│   │   │       │   ├── TargetNode.tsx
│   │   │       │   └── TransformNode.tsx
│   │   │       ├── panels/
│   │   │       └── tooltips/
│   │   ├── pages/
│   │   │   ├── ConnectionsPage.tsx
│   │   │   ├── ExplorerPage.tsx
│   │   │   ├── MigrationsListPage.tsx
│   │   │   └── MigrationBuilderPage.tsx
│   │   ├── hooks/
│   │   ├── lib/
│   │   │   └── api.ts          # API client
│   │   └── types/
│   │       ├── connection.ts
│   │       ├── migration.ts
│   │       └── transformation.ts
│   └── tests/
└── docs/
    └── superpowers/
        └── specs/
```
