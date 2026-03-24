# Migration Builder v2 — Design Spec

## Goal

Redesign the migration builder page into a visual pipeline editor with a React Flow graph (top) and a context-aware editor panel (bottom), backed by a pipeline execution engine that composes SQL via CTEs and bridges to DuckDB for programmatic transforms.

## Architecture Overview

The migration builder is a single page with two zones:

- **Pipeline Graph** (top, ~60% height) — React Flow canvas, horizontal left-to-right layout. Nodes: Source → Transform(s) → Target, connected by edges with "+" insert buttons.
- **Editor Panel** (bottom, ~40% height) — Slides up when a node is clicked. Three-column layout: collapsible Input Schema | Main Editor | collapsible Output Schema. Closable via X or clicking empty canvas.

### Node Types

| Type | Purpose | Editor Content |
|------|---------|---------------|
| **Source** | Defines data origin | Connection picker, table picker, auto-generated SELECT with column names, editable query editor |
| **Target** | Defines data destination | Connection picker, table picker, target schema display (read-only), validation indicator |
| **SQL Transform** | SQL-based transformation | Named, SQL expression editor, composable as CTEs |
| **Code Transform** | Python-based transformation | Named, Python code editor, pre-filled function signature `def transform(df: pl.DataFrame) -> pl.DataFrame:` |
| **AI Assistant** | Generates SQL or Code via LLM | Prompt textarea, "Generate" button, code preview, "Approve" converts to SQL or Code node |

## Pipeline Graph (React Flow)

### Layout

- Horizontal left-to-right, auto-positioned (no manual dragging)
- Source node on the far left, target on the far right, transforms in between
- Edges connect nodes in pipeline order

### Node Rendering

- **Source node** — connection name + table name, database icon, green accent
- **Target node** — connection name + table name, target icon, distinct accent color
- **SQL Transform** — user-given name (default "SQL Transform 1"), code icon
- **Code Transform** — user-given name (default "Code Transform 1"), function icon
- **AI Transform** — "AI Transform" label with AI badge, converts to SQL/Code after approval
- All transform nodes show a small schema badge (e.g., "5 cols") for quick visibility
- Transform nodes show a delete X on hover
- Nodes with a description show a tooltip on hover with the description text

### Edge "+" Buttons

- Each edge has a "+" button at its midpoint
- Clicking opens a small dropdown: "SQL Transform", "Code Transform", "AI Assistant"
- Selecting inserts a new node at that position: backend increments `position` of all subsequent steps, frontend re-fetches migration, new node is auto-selected opening its editor

### Node Interactions

- Click → opens editor panel for that node
- Selected node gets a highlight border
- Click empty canvas or X → closes editor panel

## Editor Panel

### Layout

Three-column layout within the slide-up panel:

```
[Input Schema] | [Main Editor] | [Output Schema]
```

- **Input Schema column** — collapsible to a thin strip (icon + label). Shows columns from the previous node: name, type, constraint. Title: "Input Schema".
- **Main Editor column** — always takes remaining space. Content varies by node type.
- **Output Schema column** — collapsible to a thin strip. Shows this node's output columns. Title: "Output Schema". If not yet inferred, shows a subtle muted icon with "Run test to infer schema" in grayed-out text (not error-styled).

Header bar: editable node name (for transforms), optional description field (small text input below name), node type indicator, X close button.

### Source Node Editor

- Connection dropdown (from saved connections)
- Table dropdown (populated after connection selected)
- On table selection: fetch schema via existing `GET /connections/{id}/tables/{table}/schema`, auto-generate `SELECT col1, col2, ... FROM table` with actual column names, store schema as `[{ name, type }]`
- Editable code editor for the query (user can modify after generation)
- If user changes table, query and schema are regenerated (overwriting edits)
- No input schema (first node). Output schema = source table columns.

### Target Node Editor

- Connection dropdown
- Table dropdown
- Target schema display (read-only, fetched from table)
- Validation indicator: green checkmark if incoming schema is compatible, red X with mismatch details
- Input schema = output of last transform. No output schema.

### SQL Transform Editor

- Editable name field
- SQL expression editor — user writes a SELECT transforming the input
- Default template: `SELECT * FROM previous`
- Input schema from previous node. Output schema inferred at test time (future: sqlglot static inference).

### Code Transform Editor

- Editable name field
- Python code editor
- Pre-filled: `def transform(df: pl.DataFrame) -> pl.DataFrame:`
- Input schema from previous node. Output schema inferred at test time by running on sample data.

### AI Assistant Editor

- Editable name field
- Natural language prompt textarea
- "Generate" button — calls backend with full pipeline context
- Shows generated code (SQL or Python) in a preview editor
- User can edit the generated code
- "Approve" button — converts node to SQL or Code type with the generated code

## Backend Pipeline Engine

### Execution Model

The engine executes the pipeline as an ordered sequence of steps:

1. **SQL chain compilation** — consecutive SQL nodes (including source query) compose into a single query using CTEs. CTE names are derived from node names: normalized to lowercase, spaces/special chars replaced with underscores, truncated to 63 chars (PostgreSQL identifier limit), with a position suffix for uniqueness (e.g., `_0`). Example with nodes named "Source Query", "Lower Emails", "Filter Active":
   ```sql
   WITH source_query_0 AS (SELECT a, b, c FROM source_table),
        lower_emails_1 AS (SELECT a, lower(b) as b, c FROM source_query_0),
        filter_active_2 AS (SELECT a, b FROM lower_emails_1 WHERE a > 0)
   SELECT * FROM filter_active_2
   ```

2. **Code node bridge** — when a Code node is encountered:
   - Execute accumulated SQL chain against source DB (with LIMIT for test)
   - Load results into a polars DataFrame
   - Run the Python transform function
   - Capture output DataFrame

3. **SQL after Code** — load the transformed DataFrame into DuckDB (in-memory `:memory:`, fresh instance per execution). Continue composing SQL CTEs against DuckDB. Execute when hitting another Code node or the pipeline end. DuckDB instance is discarded after execution completes.

4. **Repeat** — any mix of SQL and Code nodes is supported. DuckDB bridges the gap seamlessly.

### Schema Validation

At each node boundary:
- Compare output schema of node N against expected input of node N+1
- At the final step, compare against target table schema
- Any mismatch → fail with structured errors:
  ```json
  {
    "valid": false,
    "errors": [{
      "type": "missing_column | type_mismatch",
      "column": "email_hash",
      "expected": "varchar",
      "actual": null,
      "message": "Column 'email_hash' not found in target schema"
    }]
  }
  ```

### Test Endpoint

`POST /api/v1/migrations/{id}/test`

- Wraps source query for sampling: `SELECT * FROM ({source_query}) AS _src LIMIT 10`
- The source query itself is never modified — user's LIMIT, ORDER BY, etc. are preserved as-is
- PostgreSQL's demand-pull executor stops scanning after 10 rows for simple queries; queries with ORDER BY/GROUP BY/DISTINCT may still process fully before the outer LIMIT applies
- Runs full pipeline on the sampled rows
- Returns per-node results:
  ```json
  {
    "steps": [
      {
        "node_id": "source_1",
        "status": "ok",
        "schema": [{ "name": "id", "type": "integer" }, ...],
        "sample_data": [{ "id": 1, ... }, ...],
        "validation": { "valid": true }
      },
      {
        "node_id": "sql_transform_1",
        "status": "ok",
        "schema": [...],
        "sample_data": [...],
        "validation": { "valid": true }
      },
      {
        "node_id": "target_1",
        "status": "error",
        "validation": {
          "valid": false,
          "errors": ["Column 'email_hash' not found in target schema"]
        }
      }
    ]
  }
  ```

### Run Endpoint

`POST /api/v1/migrations/{id}/run`

- Executes source query as-is (no wrapping — user's own LIMIT applies if present)
- Writes to target table
- Reports progress (rows processed / total)

## Data Model

### Migration (updated)

Existing fields remain. Source and target stay on the migration record:

```python
class Migration:
    id: str
    name: str
    source_connection_id: str
    target_connection_id: str
    source_table: str
    target_table: str
    source_query: str  # NEW — the editable SELECT statement
    source_schema: list[ColumnDef]  # NEW — [{ name, type }]
    status: MigrationStatusType
    truncate_target: bool
    # ... existing fields
```

### Pipeline Step (replaces current Transformation)

> **Migration from existing model:** The current `transformations` table (types: `column_mapping`, `sql_expression`, `ai_generated`) is replaced by `pipeline_steps`. Existing `sql_expression` transforms map to `sql` steps, `ai_generated` to `ai` steps. `column_mapping` is dropped (superseded by SQL projections). The `order` field becomes `position`.

```python
StepType = Literal["sql", "code", "ai"]

class PipelineStep:
    id: str
    migration_id: str
    name: str  # default "SQL Transform 1", "Code Transform 2", etc.
    description: str | None  # optional, shown as tooltip on graph node
    position: int  # order in pipeline
    step_type: StepType
    config: SQLConfig | CodeConfig | AIConfig  # discriminated union on step_type

class SQLConfig:
    expression: str  # SQL SELECT statement

class CodeConfig:
    function_code: str  # Python function body

class AIConfig:
    prompt: str
    generated_type: "sql" | "code" | None
    generated_code: str | None
    approved: bool
```

### Pipeline Session (transient, not persisted)

Built during test runs:

```python
class PipelineSession:
    steps: list[StepResult]

class StepResult:
    node_id: str
    status: "ok" | "error"
    input_schema: list[ColumnDef]
    output_schema: list[ColumnDef]
    sample_data: list[dict]
    validation: ValidationResult
    error: str | None
```

## AI Assistant Context

When generating a transformation, the backend assembles a compact context payload for the LLM:

```python
{
    "input_schema": [{ "name": "email", "type": "varchar" }, ...],
    "target_schema": [{ "name": "email_hash", "type": "varchar" }, ...],
    "previous_transforms": ["SQL: SELECT id, email FROM step_0"],
    "sample_data": [{ "email": "user@example.com" }, ...],
    "available_tools": {
        "sql": "CTE-composable SELECT statement",
        "python": "polars DataFrame transform, libraries: polars, hashlib, cryptography, ..."
    },
    "constraints": [
        "Output schema must be compatible with next node's input",
        "Must be deterministic",
        "No side effects"
    ]
}
```

This context is stored as a reference document in the backend that can evolve as capabilities are added.

**Endpoint:** `POST /api/v1/migrations/{id}/steps/{step_id}/generate`
- Body: `{ "prompt": string }`
- Backend assembles context from pipeline state (schemas, sample data, available tools)
- Returns: `{ "type": "sql" | "code", "code": string, "reasoning": string }`
- Stored in `AIConfig.generated_type` and `generated_code`, user approves before it takes effect

## Code Transform Safety

- Execution timeout: 30 seconds per transform
- Available packages: polars, hashlib, cryptography, datetime, json, math, re
- No network access, no file system access
- Executed in isolated subprocess

## Future Enhancements (Not in v1)

- **sqlglot static schema inference** — infer SQL transform output schema without executing
- **Code transform static inference** — type hints / dry-run inference
- **Visual diff** — show schema changes between nodes as colored diff
- **Transform library** — reusable transform templates (e.g., "Hash column", "Encrypt column")
- **Parallel branches** — multiple source nodes merging into a join node
