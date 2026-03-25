# Vonnegut Backlog

Living document tracking planned work, completed items, and priorities.
Updated as work progresses.

---

## Completed

### Infrastructure & Foundation
- [x] Backend project setup (FastAPI, SQLite, psycopg3, uv)
- [x] Frontend project setup (React, TypeScript, Vite, shadcn/ui, React Flow)
- [x] Database layer with SQLite (aiosqlite)
- [x] Encryption for connection secrets (Fernet)
- [x] Connection CRUD with encrypted config storage
- [x] Explorer — list databases, tables, schemas, sample data
- [x] Migration CRUD — create, update, delete, list
- [x] Pipeline steps CRUD — add, update, delete with position management
- [x] Transformations CRUD — add, update, delete, reorder
- [x] Adapter pattern — DatabaseAdapter ABC with Postgres direct, kubectl exec, in-memory implementations
- [x] AI integration — Claude-powered code/SQL generation for pipeline steps

### Pipeline Validation Framework
- [x] DAG model — nodes dict + edges list, topological sort (Kahn's), cycle detection
- [x] Canonical schema types — DataType enum, Column, Schema with adapters (Arrow, Polars, Postgres)
- [x] Node executors — SourceExecutor, SqlExecutor, CodeExecutor, TargetExecutor (stateless, Arrow-based)
- [x] Validation rules — SyntaxCheckRule, ColumnNameRule, SqlParseRule
- [x] Node validator — pre/post execution rule composition
- [x] Pipeline validator — SchemaCompatibilityRule for cross-edge checks
- [x] Orchestrator — DAG walk with per-node validation
- [x] Graph builder — converts migration + steps to PipelineGraph
- [x] PipelineRunner — bridges new framework to existing SSE/API contract
- [x] Control plane — pipeline hashing (SHA-256), state management, PipelineManager
- [x] SSE Reporter — bridges Reporter interface to async callbacks
- [x] Old PipelineEngine removed (replaced by PipelineRunner)

### Storage Layer Refactor
- [x] AppDatabase protocol — abstract interface for metadata DB
- [x] SqliteDatabase — renamed concrete implementation
- [x] Repository pattern — ConnectionRepository, MigrationRepository, PipelineStepRepository, TransformationRepository
- [x] Routers are SQL-free — all raw SQL moved to repositories
- [x] ConnectionManager refactored to use ConnectionRepository
- [x] Transaction support (context manager on SqliteDatabase)

### Pipeline Metadata Persistence (P0)
- [x] `pipeline_metadata` table — validation_status, validated_hash, node_schemas, timestamps
- [x] PipelineMetadataRepository — get_or_create, update_validation, reset_to_draft
- [x] PipelineRunner persists validation results after test runs
- [x] Auto-invalidation — step CRUD resets metadata to DRAFT
- [x] GET /migrations/{mig_id}/validation endpoint
- [x] 5 metadata repository tests

### Test Suite
- [x] 255 tests, all green
- [x] Fixed InMemoryAdapter quoted identifier handling
- [x] Repository unit tests (23 tests)
- [x] Pipeline framework tests (82 tests)
- [x] API integration tests
- [x] Adapter tests (direct, exec, memory)

---

## In Progress

(nothing currently)

---

## Up Next — Prioritized

### P0 — Core Functionality Gaps

- [x] **Persist pipeline metadata to DB** — ~~inferred schemas, validation status, validated_hash are currently in-memory only.~~ Done in PR #13.

- [x] **Wire PipelineManager into API endpoints** — ~~currently PipelineRunner is called directly from routers.~~ Validation gate added to run/run-stream endpoints. Done in PR #14.

### P1 — Validation & Safety

- [x] **Source validation rules** — check_source_connection, check_source_query pre-flight checks. Done in PR #16.
- [x] **Target validation rules** — check_target_connection, check_target_table pre-flight checks. Done in PR #16.
- [x] **Execution-based validation rules** — SqlExecutionRule, CodeExecutionRule, SchemaStabilityRule. Done in PR #15.
- [x] **SchemaStabilityRule** — re-runs code transforms on data subsets to detect unstable schemas. Done in PR #15.

### P2 — Optimizer

- [ ] **MergeSqlNodesRule** — merge consecutive SQL nodes into a single CTE chain (uses sqlglot)
- [ ] **NoOpRemovalRule** — remove `SELECT * FROM {prev}` passthrough nodes
- [ ] **PredicatePushdownRule** — push SQL filters closer to source
- [ ] **ColumnPruningRule** — remove unused columns early

### P3 — Frontend Improvements

- [ ] **Richer validation display** — show structured validation results (per-rule pass/fail) in the pipeline builder UI
- [ ] **Better error messages** — surface validation rule details (not just generic error strings)
- [ ] **Pipeline status indicators** — show DRAFT/VALID/INVALID status in the UI

### P4 — Cleanup & Quality

- [ ] **Remove TransformationEngine + MigrationRunner** — these are legacy (pre-pipeline framework). The old `/run` endpoint still uses them. Once run-stream fully replaces run, they can go.
- [ ] **Code execution safety** — restricted builtins, timeout protection, no network access in CodeExecutor sandbox
- [ ] **LogReporter** — writes pipeline events to Python logger (useful for debugging)

### P5 — Storage & Multi-DB

- [ ] **PostgresDatabase implementation** — implement AppDatabase protocol for Postgres, enabling Postgres as the metadata store
- [ ] **DuckDBSchemaAdapter** — schema adapter for DuckDB column types

---

## Future / Deferred

- Multi-input nodes (joins, unions) — DAG model already supports this
- Cost-based optimizer (table statistics)
- Parallel execution of independent DAG branches
- Caching intermediate results
- Incremental pipelines (process only changed data)
- Scheduling system
- Extended observability (OpenTelemetry)
- Multi-user support with authentication
- Target write transaction safety (truncate + insert in one transaction)
