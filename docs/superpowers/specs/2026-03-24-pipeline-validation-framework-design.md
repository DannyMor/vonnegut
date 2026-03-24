# Pipeline Validation Framework — Design Specification

## 1. Overview

This document defines the architecture of a pipeline processing system that separates **pipeline definition (WHAT)** from **execution (HOW)**. The system replaces the current monolithic `PipelineEngine` with a modular, extensible framework.

### Design Goals

- **Extensible** — new node types, optimization rules, and validation checks can be added without modifying existing code
- **Maintainable** — clear separation of concerns across well-defined layers
- **Safe** — schema validation before execution, sandboxed code transforms
- **Performant** — optimized execution plans with SQL merging and predicate pushdown
- **Observable** — structured reporting of all validation and execution events

### Current State

The existing `PipelineEngine` (in `services/pipeline_engine.py`) is a single ~300-line class that handles source querying, SQL CTE compilation, code execution, schema inference, target validation, and progress streaming. Validation, execution, and schema inference are interleaved in a single `run_test()` method with no clear boundaries. This makes it difficult to test components independently, add new node types, or change the execution strategy.

---

## 2. Glossary

| Term | Definition |
|------|-----------|
| **Pipeline** | A user-defined ordered sequence of nodes representing data flow and transformations |
| **Node** | A unit of work in the pipeline. Pure configuration — no behavior |
| **Logical Plan** | The pipeline as defined by the user. Stored in the database. Not optimized |
| **Execution Plan** | An optimized version of the logical plan, produced by the optimizer |
| **Execution Context** | Immutable, per-node runtime configuration passed to an executor. Derived from node config + pipeline context (schemas, connection details) |
| **Schema** | A structured definition of data columns, types, and nullability. Custom abstraction, not tied to any execution engine |
| **Schema Inference** | The process of determining a node's output schema — a byproduct of validation, not a separate operation |
| **Validation** | Verifying correctness at node level (can it execute? is the output sane?) and pipeline level (do adjacent schemas match?) |
| **Optimization** | Transformation of a logical plan into a more efficient execution plan via composable rules |
| **Control Plane** | Manages pipeline definitions, lifecycle, metadata. Does not process data |
| **Execution Engine** | Executes pipelines on data. Does not manage lifecycle or persistence |
| **Reporter** | Receives events from execution and validation. Decoupled from producers via a pub/sub interface |

---

## 3. High-Level Architecture

```
[ UI / API Layer ]
        |
[ Control Plane ]
        |
[ Execution Engine ]
```

### 3.1 Control Plane

**Responsibilities:**
- Store pipeline definitions (logical plans)
- Store derived metadata (inferred schemas, validation results, run history)
- Manage pipeline lifecycle and validation status
- Enforce preconditions via hash-based validation tracking
- Trigger the execution engine

**Does NOT:** process actual data.

### 3.2 Execution Engine

**Responsibilities:**
- Execute pipelines on data
- Validate nodes via composable rules
- Optimize logical plans into execution plans
- Handle data flow between nodes
- Report progress and results

**Does NOT:** persist state or manage lifecycle.

### 3.3 Interface Layer

**Responsibilities:**
- API endpoints (FastAPI routers)
- SSE streaming for real-time progress
- User interaction

**Does NOT:** contain business logic.

---

## 4. Core Data Models

### 4.1 Schema

A custom, engine-independent schema abstraction. All schema comparison and storage happens through this model. Execution engines (Polars, DuckDB, PostgreSQL) convert to/from this representation via adapters.

```python
class Column:
    name: str
    dtype: DataType       # canonical type enum (Int64, Utf8, Boolean, Float64, Timestamp, etc.)
    nullable: bool

class Schema:
    columns: list[Column]
```

**Schema Adapters** translate between the canonical schema and engine-specific types:

| Adapter | Converts |
|---------|----------|
| `ArrowSchemaAdapter` | Schema <-> `pyarrow.Schema` |
| `PolarsSchemaAdapter` | Schema <-> Polars DataFrame schema |
| `PostgresSchemaAdapter` | Schema <-> PostgreSQL column metadata |
| `DuckDBSchemaAdapter` | Schema <-> DuckDB column types |

Adding support for a new database means adding one schema adapter. No validation or execution logic changes.

### 4.2 Node

Pure configuration. No behavior. One config type per node type.

```python
class Node:
    id: str
    type: NodeType          # source | sql | code | target
    position: int
    config: NodeConfig      # discriminated union by type
```

**Node config types:**

```python
class SourceNodeConfig:
    connection_id: str
    table: str
    query: str | None       # custom source query, overrides table

class SqlNodeConfig:
    expression: str         # SQL expression, {prev} placeholder for input

class CodeNodeConfig:
    function_code: str      # Python code defining transform(df)

class TargetNodeConfig:
    connection_id: str
    table: str
    truncate: bool
```

### 4.3 Pipeline Definition

The top-level container. Stored in the database.

```python
class PipelineDefinition:
    id: str
    name: str
    nodes: list[Node]       # ordered sequence
    current_hash: str       # hash of pipeline structure + all node configs
    created_at: datetime
    updated_at: datetime
```

**`current_hash`** is a deterministic hash computed from the pipeline's structural content: the ordered node types, their configs, and connection settings. It changes whenever the user modifies any node or reorders the pipeline. It does NOT change on metadata updates (name, timestamps). The hash is recomputed on every save.

**Note:** The pipeline is an ordered list, not a DAG. Branching/merging (multiple sources or targets) is a future enhancement. The current design assumes a linear chain: source → transform_1 → ... → transform_n → target.

### 4.4 Pipeline Metadata (Derived State)

Stored separately from the pipeline definition. Inferred schemas and validation results are derived data — they should not pollute the user-defined configuration.

```python
class NodeMetadata:
    node_id: str
    input_schema: Schema | None
    output_schema: Schema | None
    validation_status: str          # pending | passed | failed
    last_validated_at: datetime | None

class PipelineMetadata:
    pipeline_id: str
    node_metadata: dict[str, NodeMetadata]
    validated_hash: str | None      # hash of pipeline at time of last successful validation
    validation_status: ValidationStatus  # DRAFT | VALIDATING | VALID | INVALID
    last_validated_at: datetime | None
```

**Validation status transitions:**

```
DRAFT → VALIDATING → VALID
                   → INVALID → DRAFT (on edit)
VALID → DRAFT (on edit, i.e. hash mismatch)
```

- **DRAFT** — pipeline has never been validated, or has been modified since last validation (`current_hash != validated_hash`)
- **VALIDATING** — validation is currently in progress
- **VALID** — all nodes validated successfully. `validated_hash` matches `current_hash` at the time validation passed
- **INVALID** — validation failed. Must edit and re-validate

**Hash comparison determines freshness:** When `current_hash == validated_hash` and `validation_status == VALID`, the pipeline is considered validated and can be run without re-validating. Editing any node changes `current_hash`, causing a mismatch, which resets `validation_status` to `DRAFT`.

### 4.5 Plans

**Logical Plan** — derived from the pipeline definition. Decoupled from persistence — no database IDs, timestamps, or ORM concerns. Used as input to the optimizer and validator.

```python
class LogicalPlan:
    nodes: list[PlanNode]       # ordered sequence

class PlanNode:
    id: str                     # stable identifier for reporting/tracking
    type: NodeType
    config: NodeConfig
```

**Execution Plan** — produced by the optimizer. May have fewer nodes than the logical plan (e.g., consecutive SQL nodes merged into one). Each node in the execution plan has an associated execution context.

```python
class ExecutionPlan:
    contexts: list[ExecutionContext]   # ordered, optimized sequence

class ExecutionContext:
    node_id: str                # maps back to original node(s) for reporting
    node_type: NodeType
    config: NodeConfig          # the node's config (or merged config for optimized SQL)
    input_schema: Schema        # canonical schema from previous node
    connection_info: dict | None  # resolved connection details if needed
```

The execution context is immutable. The executor receives it and cannot mutate the pipeline or node configuration.

### 4.6 Data Interchange Format

Data flowing between nodes uses **PyArrow Tables** (`pyarrow.Table`) as the canonical interchange format. This is distinct from the schema model (which is our own `Schema` abstraction).

Why PyArrow Tables:
- Polars converts to/from Arrow zero-copy (`df.to_arrow()` / `pl.from_arrow()`)
- DuckDB reads Arrow tables natively
- PostgreSQL adapters can produce Arrow via result conversion
- Efficient columnar format suitable for both small samples and full datasets

Executors receive `pa.Table` and return `pa.Table`. Internal to an executor, data may be in any format (Polars DataFrame for code nodes, query results for SQL nodes), but the boundary is always Arrow.

---

## 5. Execution Components

### 5.1 Executor

Runs a single node. Takes an execution context + input data, returns output data. Stateless — all context comes from the execution context.

```python
class NodeExecutor(ABC):
    async def execute(self, context: ExecutionContext, input_data: pa.Table) -> pa.Table
```

Executors are async because database operations (source queries, target writes) are inherently async in the FastAPI stack. All executors, validators, and the orchestrator use `async/await` throughout.

**Implementations:**

| Executor | Behavior |
|----------|----------|
| `SourceExecutor` | Connects to DB, executes source query, returns rows |
| `SqlExecutor` | Executes SQL expression against input data (via DuckDB or source DB) |
| `CodeExecutor` | Runs Python transform in sandboxed environment using Polars |
| `TargetExecutor` | Writes data to target table (only in run mode; no-op in dry-run) |

### 5.2 Executor Registry

Maps node types to executors. Used by the orchestrator to look up the right executor for each node.

```python
class ExecutorRegistry:
    def get_executor(self, node_type: NodeType) -> NodeExecutor
```

---

## 6. Validation System

Validation is **rule-based and composable**. This follows the same pattern as the optimizer (section 7), giving the system a consistent extension model.

### 6.1 Validation Rule

A single, focused check. Purely observational — does not mutate data or configuration.

```python
class ValidationRule(ABC):
    name: str
    critical: bool = True   # if True, a failure stops subsequent checks

    def check(self, node: Node, context: ExecutionContext,
              input_data, output_data,
              input_schema: Schema, output_schema: Schema | None) -> CheckResult
```

A rule receives the node, its context, the input/output data and schemas, and returns a result. Not all rules use all parameters — a syntax check ignores data, an execution check inspects output. Rules are purely observational — they do not execute the node or mutate data. The executor is not passed to rules; only the validator calls the executor (see section 6.3).

```python
class CheckResult:
    rule_name: str
    status: str             # passed | failed | warning
    message: str
    details: dict | None    # optional structured data (e.g., mismatched columns)
```

### 6.2 Example Validation Rules

**Code node rules:**
- `SyntaxCheckRule` — does the code compile without errors?
- `ExecutionCheckRule` — does the code execute on sample data without crashing? Does it return a DataFrame?
- `SchemaStabilityRule` — run the transform multiple times with varied input; does the output schema remain consistent?
- `ColumnNameRule` — no duplicate or empty column names in output?

**SQL node rules:**
- `SqlParseRule` — does the SQL parse via sqlglot? Is it a single SELECT?
- `SqlExecutionRule` — does the SQL execute against sample data?
- `ColumnExistenceRule` — do referenced columns exist in the input schema?

**Source node rules:**
- `ConnectionRule` — can we connect to the source database?
- `QueryExecutionRule` — does the source query execute?

**Target node rules:**
- `ConnectionRule` — can we connect to the target database?
- `SchemaAvailabilityRule` — does the target table exist and have a discoverable schema?

New rules can be added to any node type without modifying existing code.

### 6.3 Node Validator

One per node type. Composed of an executor + a list of validation rules. Orchestrates: execute the node, then run checks on the result.

```python
class NodeValidator:
    def __init__(self, executor: NodeExecutor, rules: list[ValidationRule]):
        self.executor = executor
        self.rules = rules

    async def validate(self, node: Node, context: ExecutionContext,
                       input_data: pa.Table) -> ValidationResult:
        # 1. Execute the node (catch failures)
        output_data = None
        output_schema = None
        try:
            output_data = await self.executor.execute(context, input_data)
            # Infer schema from output using the appropriate schema adapter
            # (e.g., PolarsSchemaAdapter for code nodes, ArrowSchemaAdapter for SQL)
            output_schema = schema_adapter.from_engine(output_data)
        except Exception as exec_error:
            # Execution failure is captured — checks will inspect it
            check_results = [CheckResult(
                rule_name="execution",
                status="failed",
                message=str(exec_error),
            )]
            return ValidationResult(
                output_schema=None, output_data=None,
                checks=check_results, success=False,
            )

        # 2. Run each check
        check_results = []
        for rule in self.rules:
            result = rule.check(node, context, input_data, output_data,
                                context.input_schema, output_schema)
            check_results.append(result)
            if result.status == "failed" and rule.critical:
                break  # stop on critical failure

        return ValidationResult(
            output_schema=output_schema,
            output_data=output_data,
            checks=check_results,
            success=all(r.status != "failed" for r in check_results),
        )
```

### 6.4 Pipeline Validator

Cross-node validation that runs at the boundaries between nodes. Also rule-based.

```python
class PipelineValidationRule(ABC):
    def check(self, from_node: Node, to_node: Node,
              from_schema: Schema, to_schema: Schema) -> CheckResult

class PipelineValidator:
    def __init__(self, rules: list[PipelineValidationRule]):
        self.rules = rules

    def validate_boundary(self, from_node, to_node,
                          from_schema, to_schema) -> list[CheckResult]
```

**Example pipeline validation rules:**
- `SchemaCompatibilityRule` — does the output of node N have all columns expected by node N+1?
- `TypeCompatibilityRule` — are column types compatible across the boundary? (supports strict and lenient modes)
- `NullabilityRule` — does a non-nullable target column receive data from a nullable source column?

### 6.5 Validation Result

```python
class ValidationResult:
    output_schema: Schema | None    # inferred output schema (canonical)
    output_data: Any | None         # output data (for passing to next node)
    checks: list[CheckResult]       # all check results
    success: bool                   # True if no critical failures
```

---

## 7. Optimizer

Transforms a logical plan into an optimized execution plan via composable, stateless rules.

### 7.1 Optimization Rule

```python
class OptimizationRule(ABC):
    def apply(self, plan: LogicalPlan, context: OptimizationContext) -> LogicalPlan
```

Each rule receives the full plan and returns a (potentially modified) plan. Rules are stateless and independent — they can be reordered, added, or removed without affecting each other.

### 7.2 Optimizer

```python
class Optimizer:
    def __init__(self, rules: list[OptimizationRule]):
        self.rules = rules

    def optimize(self, plan: LogicalPlan, context: OptimizationContext) -> ExecutionPlan:
        for rule in self.rules:
            plan = rule.apply(plan, context)
        return ExecutionPlan(plan)
```

### 7.3 Optimization Context

```python
class OptimizationContext:
    schemas: dict[str, Schema]      # per-node schemas (from validation)
    statistics: dict | None         # optional table statistics for cost-based optimization
```

### 7.4 Example Optimization Rules

| Rule | Effect |
|------|--------|
| `MergeSqlNodesRule` | Merge consecutive SQL nodes into a single CTE chain |
| `PredicatePushdownRule` | Push SQL filter predicates closer to the source |
| `ColumnPruningRule` | Remove unused columns early in the pipeline |
| `NoOpRemovalRule` | Remove nodes that don't transform data (e.g., `SELECT * FROM {prev}`) |

These rules use sqlglot for SQL analysis and rewriting.

### 7.5 Key Principles

- Rules are independent and composable
- Rules are stateless — no side effects
- Plan is treated as immutable (each rule returns a new plan)
- Optimization requires validated schemas (from test mode)

---

## 8. Pipeline Orchestrator

The orchestrator walks a plan (logical or execution), passing data from node to node, and reporting progress. It does not decide *when* to run — that's the pipeline manager's job.

### 8.1 Interface

```python
class PipelineOrchestrator:
    def run_test(self, plan: LogicalPlan, sample_data, reporter) -> TestResult
    def run_execute(self, plan: ExecutionPlan, data, reporter, allow_writes: bool) -> ExecutionResult
```

### 8.2 Test Mode Walk

```python
async def run_test(self, plan, sample_data, reporter):
    input_data = sample_data        # pa.Table
    input_schema = None             # Schema (canonical)
    prev_node = None
    results = []

    for node in plan.nodes:
        context = build_execution_context(node, input_schema)
        validator = self.validator_registry.get(node.type)

        await reporter.emit("node_start", node_id=node.id, name=node.config.name)
        result = await validator.validate(node, context, input_data)
        await reporter.emit("node_complete", node_id=node.id, checks=result.checks)

        results.append(result)
        if not result.success:
            await reporter.emit("pipeline_failed", node_id=node.id)
            break

        # Pipeline-level validation at boundary
        if prev_node is not None and input_schema is not None:
            boundary_checks = self.pipeline_validator.validate_boundary(
                prev_node, node, input_schema, result.output_schema)
            results[-1].checks.extend(boundary_checks)
            if any(c.status == "failed" for c in boundary_checks):
                await reporter.emit("pipeline_failed", node_id=node.id)
                break

        input_data = result.output_data
        input_schema = result.output_schema
        prev_node = node

    return TestResult(node_results=results)
```

### 8.3 Execute Mode Walk (Dry-Run and Run)

```python
async def run_execute(self, plan, data, reporter, allow_writes):
    input_data = data               # pa.Table

    for exec_context in plan.contexts:
        executor = self.executor_registry.get(exec_context.node_type)

        # Skip target writes in dry-run
        if exec_context.node_type == "target" and not allow_writes:
            await reporter.emit("node_skipped", node_id=exec_context.node_id,
                                reason="dry-run: writes disabled")
            continue

        await reporter.emit("node_start", node_id=exec_context.node_id)
        try:
            output_data = await executor.execute(exec_context, input_data)
            await reporter.emit("node_complete", node_id=exec_context.node_id)
        except Exception as e:
            await reporter.emit("node_failed", node_id=exec_context.node_id,
                                error=str(e))
            return ExecutionResult(output_data=None, success=False, error=str(e))

        input_data = output_data

    return ExecutionResult(output_data=input_data, success=True, error=None)
```

```python
class ExecutionResult:
    output_data: pa.Table | None
    success: bool
    error: str | None
```

---

## 9. Pipeline Manager (Control Plane)

The single entry point for all pipeline actions. Enforces lifecycle preconditions and coordinates between the control plane and execution engine.

### 9.1 Validation Status vs Execution Status

The pipeline has two independent status dimensions:

**Validation status** (`PipelineMetadata.validation_status`) — tracks whether the pipeline definition is validated:

```
DRAFT → VALIDATING → VALID
                   → INVALID
VALID → DRAFT (on edit — hash mismatch)
INVALID → DRAFT (on edit)
```

**Execution status** (`PipelineDefinition` or run record) — tracks the current execution state:

```
idle → running → completed
                → failed
```

These are independent. A pipeline can be `VALID` (validation passed) and `idle` (not currently running). A pipeline can be run multiple times while remaining `VALID` — validation is not re-run unless the pipeline definition changes.

**Hash-based validation freshness:** When the user edits any node, `current_hash` changes. On the next action that requires validation, the manager compares `current_hash` to `validated_hash`. If they differ, `validation_status` is set to `DRAFT` and validation must run before execution.

Note: optimization is not a persisted state. It runs inline as part of `dry_run()` and `run()` — the optimizer produces an ephemeral execution plan that is used immediately and not stored.

### 9.2 Actions

```python
class PipelineManager:
    def can_run(self, pipeline_id) -> bool:
        """Check if the pipeline can run without re-validation."""
        pipeline = self.load_pipeline(pipeline_id)
        metadata = self.load_metadata(pipeline_id)
        return (
            metadata.validation_status == ValidationStatus.VALID
            and metadata.validated_hash == pipeline.current_hash
        )

    async def test(self, pipeline_id, reporter) -> TestResult:
        """Validate pipeline, infer schemas, store metadata."""
        pipeline = self.load_pipeline(pipeline_id)
        metadata = self.load_metadata(pipeline_id)
        metadata.validation_status = ValidationStatus.VALIDATING
        self.store_metadata(pipeline_id, metadata)

        plan = build_logical_plan(pipeline)
        result = self.orchestrator.run_test(plan, sample_data, reporter)

        # Store inferred schemas and update validation status
        metadata.node_metadata = result.node_metadata
        if result.success:
            metadata.validation_status = ValidationStatus.VALID
            metadata.validated_hash = pipeline.current_hash
        else:
            metadata.validation_status = ValidationStatus.INVALID
            metadata.validated_hash = None
        metadata.last_validated_at = datetime.utcnow()
        self.store_metadata(pipeline_id, metadata)
        return result

    async def dry_run(self, pipeline_id, reporter) -> ExecutionResult:
        """Optimize and execute on sample data without writing to target."""
        pipeline = self.load_pipeline(pipeline_id)
        await self.ensure_valid(pipeline, reporter)

        metadata = self.load_metadata(pipeline_id)
        plan = build_logical_plan(pipeline)
        opt_context = OptimizationContext(schemas=metadata.schemas)
        exec_plan = self.optimizer.optimize(plan, opt_context)

        result = await self.orchestrator.run_execute(
            exec_plan, sample_data=None, reporter=reporter, allow_writes=False)
        return result

    async def run(self, pipeline_id, reporter) -> ExecutionResult:
        """Execute the optimized pipeline with writes enabled."""
        pipeline = self.load_pipeline(pipeline_id)
        await self.ensure_valid(pipeline, reporter)

        metadata = self.load_metadata(pipeline_id)
        plan = build_logical_plan(pipeline)
        opt_context = OptimizationContext(schemas=metadata.schemas)
        exec_plan = self.optimizer.optimize(plan, opt_context)

        result = await self.orchestrator.run_execute(
            exec_plan, sample_data=None, reporter=reporter, allow_writes=True)
        return result

    async def ensure_valid(self, pipeline, reporter):
        """Ensure pipeline is validated. Only re-validates if hash has changed."""
        metadata = self.load_metadata(pipeline.id)
        if (metadata.validation_status == ValidationStatus.VALID
                and metadata.validated_hash == pipeline.current_hash):
            return  # still valid, no work needed
        # Hash changed or never validated — run test
        result = await self.test(pipeline.id, reporter)
        if not result.success:
            raise PipelineValidationError("Pipeline validation failed")
```

### 9.3 Precondition Enforcement

**Core invariant:** `run()` and `dry_run()` are allowed only when `validated_hash == current_hash AND validation_status == VALID`.

- `dry_run()` and `run()` call `ensure_valid()`, which checks the hash. If the pipeline hasn't changed since last successful validation, it proceeds immediately — no re-validation.
- If the hash has changed (user edited a node), `ensure_valid()` triggers `test()` automatically. If test fails, the action is aborted.
- A validated pipeline can be run as many times as needed without re-validation, as long as its definition hasn't changed.
- `current_hash` is recomputed on every pipeline save. If any node config, node order, or connection setting changes, the hash changes and `validation_status` effectively becomes stale (DRAFT).

---

## 10. Reporter (Event System)

The reporter is a pub/sub interface that receives events from the orchestrator, validators, and executors. It decouples event production from consumption.

### 10.1 Interface

```python
class Reporter(ABC):
    async def emit(self, event_type: str, **data: Any) -> None:
        """Emit a named event with keyword data. e.g., reporter.emit("node_start", node_id="abc", name="Source")"""
```

### 10.2 Event Types

| Event | Emitted By | Data |
|-------|-----------|------|
| `node_start` | orchestrator | node id, name |
| `node_complete` | orchestrator | node id, duration, row count, schema |
| `node_failed` | orchestrator | node id, error |
| `check_passed` | validator | rule name, message |
| `check_failed` | validator | rule name, message, details |
| `check_warning` | validator | rule name, message |
| `pipeline_failed` | orchestrator | failing node, reason |
| `pipeline_complete` | orchestrator | overall result |

### 10.3 Listeners

| Listener | Purpose |
|----------|---------|
| `SSEReporter` | Streams events to the frontend via Server-Sent Events (current UI) |
| `LogReporter` | Writes events to Python logger |
| Future: `SlackReporter`, `WebhookReporter`, etc. |

### 10.4 Implementation Note

The reporter is passed as an optional dependency. All components accept `reporter: Reporter | None`. If None, events are silently discarded. This means the full system works without any reporter (useful for testing), and reporters can be added without changing any execution logic.

---

## 11. Code Execution Safety

All user-defined code (code nodes) runs in a sandboxed environment:

- **Restricted builtins** — no `__import__`, no `eval`/`exec`, no file I/O
- **Pre-injected modules** — `pl` (polars), `math`, `re`, `json`, `hashlib`, `datetime`
- **Timeout protection** — execution is time-bounded
- **No network access** — code cannot make HTTP calls or open sockets
- **Compiled before execution** — `compile()` for better error messages

The `CodeExecutor` encapsulates all sandbox logic. Validation rules for code nodes (like `SchemaStabilityRule`) may run the transform multiple times with varied input to detect edge cases — this happens internally within the rule, not at the orchestrator level.

---

## 12. Schema Inference Strategy

Schema inference is not a separate operation — it's a byproduct of validation. When a node is validated, its output schema is inferred and returned as part of the `ValidationResult`.

**Per node type:**

| Node Type | Inference Method |
|-----------|-----------------|
| Source | `fetch_schema(table)` from DB metadata, or execute query with `LIMIT 0` |
| SQL | Static analysis via sqlglot AST + input schema. Optionally confirmed by execution with `LIMIT 0` via DuckDB |
| Code | Must execute on sample data. Output schema inferred from resulting DataFrame. Multiple runs recommended for stability |
| Target | `fetch_schema(table)` from DB metadata. This is the expected schema, not inferred from data flow |

Schema inference for code nodes uses synthetic data generated from the input schema (one row with default values per type) when real sample data is not available. This enables fast schema resolution during authoring without requiring a database connection.

---

## 13. Extensibility

### Adding a New Node Type

1. Define a new `NodeConfig` (data model)
2. Implement a `NodeExecutor` (execution logic)
3. Implement validation rules specific to the node type
4. Register in the executor and validator registries

No changes to: orchestrator, optimizer, pipeline manager, reporter, or existing node types.

### Adding a New Validation Rule

1. Implement `ValidationRule` with a `check()` method
2. Add to the appropriate node validator's rule list

No changes to: orchestrator, executor, other rules.

### Adding a New Optimization Rule

1. Implement `OptimizationRule` with an `apply()` method
2. Add to the optimizer's rule list

No changes to: orchestrator, executor, validator.

### Adding a New Execution Mode

1. Extend `PipelineManager` with a new action method
2. Define preconditions and status transitions

No changes to: orchestrator, executor, validator, optimizer.

### Adding a New Reporter Listener

1. Implement `Reporter` interface
2. Pass to the orchestrator when invoking actions

No changes to: any execution or validation logic.

---

## 14. Component Responsibility Summary

### Control Plane

| Component | Responsibility |
|-----------|---------------|
| PipelineManager | Lifecycle orchestration, precondition enforcement |
| Metadata Store | Inferred schemas, validation results, run history |
| Pipeline Repository | CRUD for pipeline definitions |

### Execution Engine

| Component | Responsibility |
|-----------|---------------|
| PipelineOrchestrator | Walk plan, pass data between nodes, report progress |
| NodeExecutor (per type) | Execute a single node |
| NodeValidator (per type) | Run composable validation rules using executor |
| ValidationRule | Single, focused check (composable) |
| PipelineValidator | Cross-node boundary validation |
| Optimizer | Transform logical plan → execution plan via rules |
| OptimizationRule | Single plan transformation (composable) |
| Reporter | Pub/sub event system |

### Schema Layer

| Component | Responsibility |
|-----------|---------------|
| Schema / Column / DataType | Canonical schema representation |
| Schema Adapters | Translate between canonical and engine-specific types |

---

## 15. Design Principles

1. **Separate WHAT from HOW** — pipeline definition is data; execution is behavior
2. **Nodes are pure configuration** — no behavior on node objects
3. **Separate node validation from pipeline validation** — node checks and boundary checks are distinct concerns
4. **Rule-based systems for validation and optimization** — consistent, composable, extensible
5. **Derived data stored separately** — inferred schemas and validation results are not part of the pipeline definition
6. **Execution contexts are immutable** — executors cannot mutate configuration
7. **Reporter is optional** — the system works without any observer
8. **Prefer composition over inheritance** — validators compose rules, optimizers compose rules
9. **Linear pipeline only (for now)** — DAG support is a future enhancement

---

## 16. Concurrency and Safety

### 16.1 Concurrent Access

The system is single-user for v1. The pipeline manager enforces that only one action runs per pipeline at a time — if a pipeline is currently executing or validating, subsequent `test()`, `dry_run()`, or `run()` calls are rejected. Future versions may add explicit locking for multi-user scenarios.

### 16.2 Target Write Safety

When `run()` writes to the target, the write should be wrapped in a database transaction where supported. If the pipeline fails partway through writing:
- If `truncate` is enabled: the truncate and all inserts are in the same transaction, so a failure rolls back to the pre-truncate state
- If `truncate` is disabled: the inserts are in a transaction, so a failure rolls back all new rows

The `TargetExecutor` is responsible for transaction management. This is critical for data safety — a truncate followed by a failed write must not leave the target empty.

### 16.3 Testing Strategy

Key components are tested with real implementations and in-memory test doubles:

- **Orchestrator tests**: use in-memory executors and validators to verify the walk logic, data passing, and failure handling without any database
- **Executor tests**: `CodeExecutor` tested with real Polars; `SqlExecutor` tested with real DuckDB; `SourceExecutor` and `TargetExecutor` tested with `InMemoryAdapter`
- **Validation rule tests**: each rule tested independently with constructed input/output data
- **Reporter tests**: in-memory reporter that collects events for assertion
- **Schema adapter tests**: round-trip tests (canonical → engine → canonical) for each adapter
- **Optimizer rule tests**: input plan → apply rule → assert output plan structure

---

## 17. Migration from Current System

The current `PipelineEngine` will be gradually replaced:

1. Implement the new schema layer (Schema, Column, DataType, adapters)
2. Implement executors per node type (extracting logic from PipelineEngine)
3. Implement validation rules (extracting and improving current validation logic)
4. Implement the orchestrator (replacing the monolithic `run_test()`)
5. Implement the optimizer (new capability, starting with SQL merge rule)
6. Wire up the pipeline manager (replacing current router-level orchestration)
7. Update API endpoints to use the new system
8. Remove old PipelineEngine, TransformationEngine, and MigrationRunner

The existing SSE streaming becomes an `SSEReporter` listener. The existing CTE compiler becomes part of the `MergeSqlNodesRule` optimization.

---

## 18. Future Enhancements

- DAG support (branching/merging pipelines)
- Cost-based optimizer (using table statistics)
- Parallel execution of independent branches
- Caching intermediate results
- Incremental pipelines (process only changed data)
- Scheduling system
- Extended observability (metrics, tracing, OpenTelemetry)

---

## 19. Recommended Libraries

| Library | Purpose |
|---------|---------|
| polars | DataFrame execution for code transforms |
| pyarrow | Data interchange format between engines |
| sqlglot | SQL parsing, schema inference, optimization, dialect transpilation |
| duckdb | In-process SQL execution for validation and optimization |
| pydantic | Schema and configuration models |
