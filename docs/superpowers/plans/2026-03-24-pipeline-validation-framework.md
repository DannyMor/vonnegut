# Pipeline Validation Framework — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the monolithic PipelineEngine with a modular, extensible pipeline framework featuring DAG-based execution, rule-based validation, hash-based lifecycle management, and composable optimization.

**Architecture:** Bottom-up build: schema types → DAG models → reporter → executors → validators → orchestrator → optimizer → pipeline manager → API integration. Each layer is independently testable. The new code lives under `backend/src/vonnegut/pipeline/` alongside the existing `services/`, `models/`, `routers/` which are gradually migrated.

**Tech Stack:** Python 3.12+, polars, pyarrow, sqlglot, duckdb, pydantic, pytest, FastAPI

**Spec:** `docs/superpowers/specs/2026-03-24-pipeline-validation-framework-design.md`
**Conventions:** `docs/CODE_CONVENTIONS.md` — discriminated unions, pattern matching, fully typed signatures, no `Any`

---

## File Structure

### New files (under `backend/src/vonnegut/pipeline/`)

```
pipeline/
├── __init__.py
├── schema/
│   ├── __init__.py
│   ├── types.py                   # DataType enum, Column, Schema
│   └── adapters.py                # ArrowSchemaAdapter, PolarsSchemaAdapter, PostgresSchemaAdapter
├── dag/
│   ├── __init__.py
│   ├── node.py                    # NodeType, NodeConfig types, Node
│   ├── edge.py                    # Edge
│   ├── graph.py                   # PipelineGraph, topological_sort, collect_inputs
│   └── plan.py                    # LogicalPlan, PlanNode, PlanEdge, ExecutionPlan, ExecutionContext
├── engine/
│   ├── __init__.py
│   ├── orchestrator.py            # PipelineOrchestrator
│   ├── executor/
│   │   ├── __init__.py
│   │   ├── base.py                # NodeExecutor ABC, ExecutorRegistry
│   │   ├── source_executor.py     # SourceExecutor
│   │   ├── sql_executor.py        # SqlExecutor
│   │   ├── code_executor.py       # CodeExecutor (sandbox logic extracted from PipelineEngine)
│   │   └── target_executor.py     # TargetExecutor
│   ├── validator/
│   │   ├── __init__.py
│   │   ├── node_validator.py      # NodeValidator
│   │   ├── pipeline_validator.py  # PipelineValidator, PipelineValidationRule
│   │   └── rules/
│   │       ├── __init__.py
│   │       ├── base.py            # ValidationRule ABC, CheckResult
│   │       ├── code_rules.py      # SyntaxCheckRule, ExecutionCheckRule, ColumnNameRule
│   │       ├── sql_rules.py       # SqlParseRule, SqlExecutionRule
│   │       ├── source_rules.py    # ConnectionRule, QueryExecutionRule
│   │       └── target_rules.py    # ConnectionRule, SchemaAvailabilityRule
│   └── optimizer/
│       ├── __init__.py
│       ├── optimizer.py           # Optimizer, OptimizationContext
│       └── rules/
│           ├── __init__.py
│           ├── base.py            # OptimizationRule ABC
│           └── merge_sql.py       # MergeSqlNodesRule
├── reporter/
│   ├── __init__.py
│   ├── base.py                    # Reporter ABC, NullReporter, CollectorReporter
│   └── sse_reporter.py            # SSEReporter
├── control_plane/
│   ├── __init__.py
│   ├── pipeline_manager.py        # PipelineManager
│   ├── pipeline_state.py          # ValidationStatus, PipelineMetadata, NodeMetadata
│   └── hashing.py                 # compute_pipeline_hash
└── results.py                     # ValidationSuccess, ValidationFailure, ExecutionSuccess, ExecutionFailure, CheckResult
```

### New test files (under `backend/tests/pipeline/`)

```
tests/pipeline/
├── __init__.py
├── test_schema_types.py
├── test_schema_adapters.py
├── test_dag_graph.py
├── test_reporter.py
├── test_executor_code.py
├── test_executor_sql.py
├── test_validator_rules.py
├── test_node_validator.py
├── test_pipeline_validator.py
├── test_orchestrator.py
├── test_optimizer.py
├── test_pipeline_manager.py
└── test_hashing.py
```

### Modified existing files

- `backend/src/vonnegut/routers/migrations.py` — wire new pipeline manager into test/run endpoints
- `backend/src/vonnegut/database.py` — add pipeline_metadata table schema
- `backend/pyproject.toml` — add pyarrow dependency

---

## Chunk 1: Foundation — Schema, DAG, Reporter, Results

### Task 1: Add pyarrow dependency

**Files:**
- Modify: `backend/pyproject.toml`

- [ ] **Step 1: Add pyarrow to dependencies**

In `backend/pyproject.toml`, add `pyarrow>=18.0.0` to the dependencies list (after polars).

- [ ] **Step 2: Install and verify**

Run: `cd backend && uv sync`
Expected: installs pyarrow successfully

- [ ] **Step 3: Commit**

```bash
git add backend/pyproject.toml backend/uv.lock
git commit -m "chore: add pyarrow dependency"
```

---

### Task 2: Schema types — DataType, Column, Schema

**Files:**
- Create: `backend/src/vonnegut/pipeline/__init__.py`
- Create: `backend/src/vonnegut/pipeline/schema/__init__.py`
- Create: `backend/src/vonnegut/pipeline/schema/types.py`
- Test: `backend/tests/pipeline/__init__.py`
- Test: `backend/tests/pipeline/test_schema_types.py`

- [ ] **Step 1: Create package init files**

Create empty `__init__.py` for `pipeline/`, `pipeline/schema/`, and `tests/pipeline/`.

- [ ] **Step 2: Write failing tests for Schema types**

```python
# backend/tests/pipeline/test_schema_types.py
import pytest
from vonnegut.pipeline.schema.types import DataType, Column, Schema


class TestDataType:
    def test_all_canonical_types_exist(self):
        assert DataType.INT64
        assert DataType.FLOAT64
        assert DataType.UTF8
        assert DataType.BOOLEAN
        assert DataType.TIMESTAMP
        assert DataType.DATE
        assert DataType.BINARY
        assert DataType.NULL

    def test_enum_values_are_strings(self):
        assert DataType.INT64.value == "int64"
        assert DataType.UTF8.value == "utf8"


class TestColumn:
    def test_create_column(self):
        col = Column(name="age", dtype=DataType.INT64, nullable=False)
        assert col.name == "age"
        assert col.dtype == DataType.INT64
        assert col.nullable is False

    def test_column_defaults_nullable(self):
        col = Column(name="name", dtype=DataType.UTF8)
        assert col.nullable is True


class TestSchema:
    def test_create_schema(self):
        schema = Schema(columns=[
            Column(name="id", dtype=DataType.INT64, nullable=False),
            Column(name="name", dtype=DataType.UTF8),
        ])
        assert len(schema.columns) == 2

    def test_column_names(self):
        schema = Schema(columns=[
            Column(name="id", dtype=DataType.INT64),
            Column(name="name", dtype=DataType.UTF8),
        ])
        assert schema.column_names == ["id", "name"]

    def test_get_column(self):
        col = Column(name="id", dtype=DataType.INT64)
        schema = Schema(columns=[col])
        assert schema.get_column("id") == col
        assert schema.get_column("missing") is None

    def test_empty_schema(self):
        schema = Schema(columns=[])
        assert len(schema.columns) == 0
        assert schema.column_names == []

    def test_schema_equality(self):
        s1 = Schema(columns=[Column(name="id", dtype=DataType.INT64)])
        s2 = Schema(columns=[Column(name="id", dtype=DataType.INT64)])
        assert s1 == s2
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd backend && uv run pytest tests/pipeline/test_schema_types.py -v`
Expected: FAIL — module not found

- [ ] **Step 4: Implement Schema types**

```python
# backend/src/vonnegut/pipeline/schema/types.py
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum


class DataType(str, Enum):
    INT64 = "int64"
    INT32 = "int32"
    INT16 = "int16"
    INT8 = "int8"
    UINT64 = "uint64"
    UINT32 = "uint32"
    FLOAT64 = "float64"
    FLOAT32 = "float32"
    UTF8 = "utf8"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    TIME = "time"
    BINARY = "binary"
    NULL = "null"


@dataclass(frozen=True)
class Column:
    name: str
    dtype: DataType
    nullable: bool = True


@dataclass(frozen=True)
class Schema:
    columns: tuple[Column, ...] | list[Column] = field(default_factory=list)

    def __post_init__(self):
        if isinstance(self.columns, list):
            object.__setattr__(self, "columns", tuple(self.columns))

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def get_column(self, name: str) -> Column | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd backend && uv run pytest tests/pipeline/test_schema_types.py -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add backend/src/vonnegut/pipeline/ backend/tests/pipeline/
git commit -m "feat(pipeline): add Schema, Column, DataType canonical types"
```

---

### Task 3: Schema adapters — Arrow, Polars, Postgres

**Files:**
- Create: `backend/src/vonnegut/pipeline/schema/adapters.py`
- Test: `backend/tests/pipeline/test_schema_adapters.py`

- [ ] **Step 1: Write failing tests for schema adapters**

```python
# backend/tests/pipeline/test_schema_adapters.py
import pyarrow as pa
import polars as pl
import pytest
from vonnegut.pipeline.schema.types import DataType, Column, Schema
from vonnegut.pipeline.schema.adapters import (
    ArrowSchemaAdapter,
    PolarsSchemaAdapter,
    PostgresSchemaAdapter,
)


class TestArrowSchemaAdapter:
    def test_from_arrow(self):
        arrow_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.utf8()),
        ])
        schema = ArrowSchemaAdapter.from_arrow(arrow_schema)
        assert len(schema.columns) == 2
        assert schema.columns[0] == Column("id", DataType.INT64, nullable=False)
        assert schema.columns[1] == Column("name", DataType.UTF8, nullable=True)

    def test_to_arrow(self):
        schema = Schema(columns=[
            Column("id", DataType.INT64, nullable=False),
            Column("name", DataType.UTF8),
        ])
        arrow_schema = ArrowSchemaAdapter.to_arrow(schema)
        assert arrow_schema.field("id").type == pa.int64()
        assert arrow_schema.field("name").type == pa.utf8()

    def test_roundtrip(self):
        original = Schema(columns=[
            Column("id", DataType.INT64, nullable=False),
            Column("score", DataType.FLOAT64),
            Column("active", DataType.BOOLEAN),
        ])
        roundtripped = ArrowSchemaAdapter.from_arrow(ArrowSchemaAdapter.to_arrow(original))
        assert roundtripped == original


class TestPolarsSchemaAdapter:
    def test_from_dataframe(self):
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        schema = PolarsSchemaAdapter.from_dataframe(df)
        assert schema.get_column("id").dtype == DataType.INT64
        assert schema.get_column("name").dtype == DataType.UTF8

    def test_from_polars_schema(self):
        polars_schema = {"id": pl.Int64, "name": pl.Utf8}
        schema = PolarsSchemaAdapter.from_polars_schema(polars_schema)
        assert len(schema.columns) == 2


class TestPostgresSchemaAdapter:
    def test_from_column_metadata(self):
        metadata = [
            {"name": "id", "type": "integer", "nullable": False},
            {"name": "email", "type": "varchar", "nullable": True},
            {"name": "created_at", "type": "timestamp", "nullable": True},
        ]
        schema = PostgresSchemaAdapter.from_column_metadata(metadata)
        assert schema.get_column("id").dtype == DataType.INT64
        assert schema.get_column("email").dtype == DataType.UTF8
        assert schema.get_column("created_at").dtype == DataType.TIMESTAMP
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd backend && uv run pytest tests/pipeline/test_schema_adapters.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Implement schema adapters**

```python
# backend/src/vonnegut/pipeline/schema/adapters.py
from __future__ import annotations
import pyarrow as pa
import polars as pl
from vonnegut.pipeline.schema.types import DataType, Column, Schema

# Arrow type mappings
_ARROW_TO_CANONICAL: dict[pa.DataType, DataType] = {
    pa.int8(): DataType.INT8,
    pa.int16(): DataType.INT16,
    pa.int32(): DataType.INT32,
    pa.int64(): DataType.INT64,
    pa.uint32(): DataType.UINT32,
    pa.uint64(): DataType.UINT64,
    pa.float32(): DataType.FLOAT32,
    pa.float64(): DataType.FLOAT64,
    pa.utf8(): DataType.UTF8,
    pa.large_utf8(): DataType.UTF8,
    pa.bool_(): DataType.BOOLEAN,
    pa.date32(): DataType.DATE,
    pa.binary(): DataType.BINARY,
}

_CANONICAL_TO_ARROW: dict[DataType, pa.DataType] = {
    DataType.INT8: pa.int8(),
    DataType.INT16: pa.int16(),
    DataType.INT32: pa.int32(),
    DataType.INT64: pa.int64(),
    DataType.UINT32: pa.uint32(),
    DataType.UINT64: pa.uint64(),
    DataType.FLOAT32: pa.float32(),
    DataType.FLOAT64: pa.float64(),
    DataType.UTF8: pa.utf8(),
    DataType.BOOLEAN: pa.bool_(),
    DataType.TIMESTAMP: pa.timestamp("us"),
    DataType.DATE: pa.date32(),
    DataType.TIME: pa.time64("us"),
    DataType.BINARY: pa.binary(),
    DataType.NULL: pa.null(),
}

# Polars type mappings
_POLARS_TO_CANONICAL: dict[type, DataType] = {
    pl.Int8: DataType.INT8,
    pl.Int16: DataType.INT16,
    pl.Int32: DataType.INT32,
    pl.Int64: DataType.INT64,
    pl.UInt32: DataType.UINT32,
    pl.UInt64: DataType.UINT64,
    pl.Float32: DataType.FLOAT32,
    pl.Float64: DataType.FLOAT64,
    pl.Utf8: DataType.UTF8,
    pl.String: DataType.UTF8,
    pl.Boolean: DataType.BOOLEAN,
    pl.Date: DataType.DATE,
    pl.Binary: DataType.BINARY,
}

# Postgres type string mappings
_PG_TYPE_TO_CANONICAL: dict[str, DataType] = {
    "integer": DataType.INT64,
    "int": DataType.INT64,
    "int4": DataType.INT64,
    "bigint": DataType.INT64,
    "int8": DataType.INT64,
    "smallint": DataType.INT32,
    "int2": DataType.INT32,
    "real": DataType.FLOAT32,
    "float4": DataType.FLOAT32,
    "double precision": DataType.FLOAT64,
    "float8": DataType.FLOAT64,
    "numeric": DataType.FLOAT64,
    "decimal": DataType.FLOAT64,
    "text": DataType.UTF8,
    "varchar": DataType.UTF8,
    "character varying": DataType.UTF8,
    "char": DataType.UTF8,
    "boolean": DataType.BOOLEAN,
    "bool": DataType.BOOLEAN,
    "timestamp": DataType.TIMESTAMP,
    "timestamp without time zone": DataType.TIMESTAMP,
    "timestamp with time zone": DataType.TIMESTAMP,
    "timestamptz": DataType.TIMESTAMP,
    "date": DataType.DATE,
    "time": DataType.TIME,
    "bytea": DataType.BINARY,
    "uuid": DataType.UTF8,
    "json": DataType.UTF8,
    "jsonb": DataType.UTF8,
}


class ArrowSchemaAdapter:
    @staticmethod
    def from_arrow(arrow_schema: pa.Schema) -> Schema:
        columns = []
        for field in arrow_schema:
            dtype = _ARROW_TO_CANONICAL.get(field.type)
            if dtype is None and pa.types.is_timestamp(field.type):
                dtype = DataType.TIMESTAMP
            if dtype is None and pa.types.is_time(field.type):
                dtype = DataType.TIME
            columns.append(Column(
                name=field.name,
                dtype=dtype or DataType.UTF8,
                nullable=field.nullable,
            ))
        return Schema(columns=columns)

    @staticmethod
    def to_arrow(schema: Schema) -> pa.Schema:
        fields = []
        for col in schema.columns:
            arrow_type = _CANONICAL_TO_ARROW.get(col.dtype, pa.utf8())
            fields.append(pa.field(col.name, arrow_type, nullable=col.nullable))
        return pa.schema(fields)


class PolarsSchemaAdapter:
    @staticmethod
    def from_dataframe(df: pl.DataFrame) -> Schema:
        columns = []
        for name, dtype in zip(df.columns, df.dtypes):
            canonical = _POLARS_TO_CANONICAL.get(type(dtype), DataType.UTF8)
            columns.append(Column(name=name, dtype=canonical, nullable=True))
        return Schema(columns=columns)

    @staticmethod
    def from_polars_schema(polars_schema: dict) -> Schema:
        columns = []
        for name, dtype in polars_schema.items():
            canonical = _POLARS_TO_CANONICAL.get(dtype, DataType.UTF8)
            columns.append(Column(name=name, dtype=canonical, nullable=True))
        return Schema(columns=columns)


class PostgresSchemaAdapter:
    @staticmethod
    def from_column_metadata(metadata: list[dict]) -> Schema:
        columns = []
        for col in metadata:
            pg_type = col.get("type", "text").lower().strip()
            dtype = _PG_TYPE_TO_CANONICAL.get(pg_type, DataType.UTF8)
            columns.append(Column(
                name=col["name"],
                dtype=dtype,
                nullable=col.get("nullable", True),
            ))
        return Schema(columns=columns)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && uv run pytest tests/pipeline/test_schema_adapters.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add backend/src/vonnegut/pipeline/schema/adapters.py backend/tests/pipeline/test_schema_adapters.py
git commit -m "feat(pipeline): add Arrow, Polars, Postgres schema adapters"
```

---

### Task 4: Result types — discriminated unions

**Files:**
- Create: `backend/src/vonnegut/pipeline/results.py`

- [ ] **Step 1: Write failing test**

```python
# Add to backend/tests/pipeline/test_schema_types.py (or new file)
from vonnegut.pipeline.results import (
    CheckResult, CheckStatus,
    ValidationSuccess, ValidationFailure, NodeValidationResult,
    ExecutionSuccess, ExecutionFailure, ExecutionResult,
)

def test_check_result_creation():
    cr = CheckResult(rule_name="syntax", status=CheckStatus.PASSED, message="OK")
    assert cr.status == CheckStatus.PASSED

def test_validation_success_pattern_match():
    result: NodeValidationResult = ValidationSuccess(
        output_schema=Schema(columns=[]),
        output_data=None,
        checks=[],
    )
    match result:
        case ValidationSuccess(output_schema=s):
            assert s is not None
        case ValidationFailure():
            pytest.fail("Should not match failure")

def test_execution_failure_pattern_match():
    result: ExecutionResult = ExecutionFailure(node_id="abc", error="boom")
    match result:
        case ExecutionFailure(node_id=nid, error=err):
            assert nid == "abc"
            assert err == "boom"
        case ExecutionSuccess():
            pytest.fail("Should not match success")
```

- [ ] **Step 2: Implement result types**

```python
# backend/src/vonnegut/pipeline/results.py
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa
    from vonnegut.pipeline.schema.types import Schema


class CheckStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"


@dataclass(frozen=True)
class CheckResult:
    rule_name: str
    status: CheckStatus
    message: str
    details: dict | None = None


@dataclass
class ValidationSuccess:
    output_schema: Schema
    output_data: pa.Table | None
    checks: list[CheckResult] = field(default_factory=list)


@dataclass
class ValidationFailure:
    errors: list[CheckResult]
    output_schema: Schema | None = None
    output_data: pa.Table | None = None


NodeValidationResult = ValidationSuccess | ValidationFailure


@dataclass
class ExecutionSuccess:
    pass


@dataclass
class ExecutionFailure:
    node_id: str
    error: str


ExecutionResult = ExecutionSuccess | ExecutionFailure
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
cd backend && uv run pytest tests/pipeline/ -v
git add backend/src/vonnegut/pipeline/results.py backend/tests/pipeline/
git commit -m "feat(pipeline): add discriminated union result types"
```

---

### Task 5: DAG models — Node, Edge, Graph with topological sort

**Files:**
- Create: `backend/src/vonnegut/pipeline/dag/__init__.py`
- Create: `backend/src/vonnegut/pipeline/dag/node.py`
- Create: `backend/src/vonnegut/pipeline/dag/edge.py`
- Create: `backend/src/vonnegut/pipeline/dag/graph.py`
- Test: `backend/tests/pipeline/test_dag_graph.py`

- [ ] **Step 1: Write failing tests for DAG**

```python
# backend/tests/pipeline/test_dag_graph.py
import pytest
from vonnegut.pipeline.dag.node import Node, NodeType, SourceNodeConfig, SqlNodeConfig, CodeNodeConfig, TargetNodeConfig
from vonnegut.pipeline.dag.edge import Edge
from vonnegut.pipeline.dag.graph import PipelineGraph, topological_sort, collect_inputs, CycleError


class TestTopologicalSort:
    def test_linear_chain(self):
        nodes = {
            "src": Node(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1")),
            "sql": Node(id="sql", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT * FROM {prev}")),
            "tgt": Node(id="tgt", type=NodeType.TARGET, config=TargetNodeConfig(connection_id="c2", table="t2", truncate=False)),
        }
        edges = [
            Edge(id="e1", from_node_id="src", to_node_id="sql"),
            Edge(id="e2", from_node_id="sql", to_node_id="tgt"),
        ]
        order = topological_sort(nodes, edges)
        assert order == ["src", "sql", "tgt"]

    def test_cycle_raises(self):
        nodes = {
            "a": Node(id="a", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT 1")),
            "b": Node(id="b", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT 1")),
        }
        edges = [
            Edge(id="e1", from_node_id="a", to_node_id="b"),
            Edge(id="e2", from_node_id="b", to_node_id="a"),
        ]
        with pytest.raises(CycleError):
            topological_sort(nodes, edges)

    def test_single_node(self):
        nodes = {"src": Node(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1"))}
        order = topological_sort(nodes, [])
        assert order == ["src"]


class TestCollectInputs:
    def test_collects_default_input(self):
        edges = [Edge(id="e1", from_node_id="src", to_node_id="sql")]
        outputs = {"src": "fake_table"}
        inputs = collect_inputs("sql", edges, outputs)
        assert inputs == {"default": "fake_table"}

    def test_collects_named_inputs(self):
        edges = [
            Edge(id="e1", from_node_id="a", to_node_id="join", input_name="left"),
            Edge(id="e2", from_node_id="b", to_node_id="join", input_name="right"),
        ]
        outputs = {"a": "table_a", "b": "table_b"}
        inputs = collect_inputs("join", edges, outputs)
        assert inputs == {"left": "table_a", "right": "table_b"}

    def test_source_has_no_inputs(self):
        edges = [Edge(id="e1", from_node_id="src", to_node_id="sql")]
        inputs = collect_inputs("src", edges, {})
        assert inputs == {}


class TestPipelineGraph:
    def test_validate_linear_chain(self):
        graph = PipelineGraph(
            nodes={
                "src": Node(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1")),
                "tgt": Node(id="tgt", type=NodeType.TARGET, config=TargetNodeConfig(connection_id="c2", table="t2", truncate=False)),
            },
            edges=[Edge(id="e1", from_node_id="src", to_node_id="tgt")],
        )
        graph.validate()  # should not raise

    def test_validate_rejects_cycle(self):
        graph = PipelineGraph(
            nodes={
                "a": Node(id="a", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT 1")),
                "b": Node(id="b", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT 1")),
            },
            edges=[
                Edge(id="e1", from_node_id="a", to_node_id="b"),
                Edge(id="e2", from_node_id="b", to_node_id="a"),
            ],
        )
        with pytest.raises(CycleError):
            graph.validate()

    def test_validate_rejects_orphaned_node(self):
        graph = PipelineGraph(
            nodes={
                "src": Node(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1")),
                "orphan": Node(id="orphan", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT 1")),
            },
            edges=[],
        )
        with pytest.raises(ValueError, match="orphan"):
            graph.validate()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd backend && uv run pytest tests/pipeline/test_dag_graph.py -v`
Expected: FAIL

- [ ] **Step 3: Implement Node types**

```python
# backend/src/vonnegut/pipeline/dag/node.py
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum


class NodeType(str, Enum):
    SOURCE = "source"
    SQL = "sql"
    CODE = "code"
    TARGET = "target"


@dataclass(frozen=True)
class SourceNodeConfig:
    connection_id: str
    table: str
    query: str | None = None


@dataclass(frozen=True)
class SqlNodeConfig:
    expression: str


@dataclass(frozen=True)
class CodeNodeConfig:
    function_code: str


@dataclass(frozen=True)
class TargetNodeConfig:
    connection_id: str
    table: str
    truncate: bool


NodeConfig = SourceNodeConfig | SqlNodeConfig | CodeNodeConfig | TargetNodeConfig


@dataclass(frozen=True)
class Node:
    id: str
    type: NodeType
    config: NodeConfig
```

- [ ] **Step 4: Implement Edge**

```python
# backend/src/vonnegut/pipeline/dag/edge.py
from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class Edge:
    id: str
    from_node_id: str
    to_node_id: str
    input_name: str | None = None
```

- [ ] **Step 5: Implement PipelineGraph with topological sort**

```python
# backend/src/vonnegut/pipeline/dag/graph.py
from __future__ import annotations
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import TypeVar

from vonnegut.pipeline.dag.node import Node, NodeType
from vonnegut.pipeline.dag.edge import Edge

T = TypeVar("T")


class CycleError(Exception):
    pass


def topological_sort(nodes: dict[str, Node], edges: list[Edge]) -> list[str]:
    in_degree: dict[str, int] = {nid: 0 for nid in nodes}
    adjacency: dict[str, list[str]] = defaultdict(list)

    for edge in edges:
        adjacency[edge.from_node_id].append(edge.to_node_id)
        in_degree[edge.to_node_id] = in_degree.get(edge.to_node_id, 0) + 1

    queue = deque(nid for nid, deg in in_degree.items() if deg == 0)
    result: list[str] = []

    while queue:
        nid = queue.popleft()
        result.append(nid)
        for neighbor in adjacency[nid]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(result) != len(nodes):
        raise CycleError("Pipeline contains a cycle")

    return result


def collect_inputs(
    node_id: str,
    edges: list[Edge],
    outputs: dict[str, T],
) -> dict[str, T]:
    inputs: dict[str, T] = {}
    for edge in edges:
        if edge.to_node_id == node_id and edge.from_node_id in outputs:
            key = edge.input_name or "default"
            inputs[key] = outputs[edge.from_node_id]
    return inputs


def get_incoming_edges(node_id: str, edges: list[Edge]) -> list[Edge]:
    return [e for e in edges if e.to_node_id == node_id]


@dataclass
class PipelineGraph:
    nodes: dict[str, Node]
    edges: list[Edge] = field(default_factory=list)

    def validate(self) -> None:
        # Check for cycles (topological sort will raise CycleError)
        topological_sort(self.nodes, self.edges)

        # Check for orphaned nodes (not connected by any edge)
        if len(self.nodes) > 1:
            connected = set()
            for edge in self.edges:
                connected.add(edge.from_node_id)
                connected.add(edge.to_node_id)
            orphans = set(self.nodes.keys()) - connected
            if orphans:
                raise ValueError(f"Orphaned nodes not connected by any edge: {orphans}")

    def execution_order(self) -> list[str]:
        return topological_sort(self.nodes, self.edges)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd backend && uv run pytest tests/pipeline/test_dag_graph.py -v`
Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add backend/src/vonnegut/pipeline/dag/ backend/tests/pipeline/test_dag_graph.py
git commit -m "feat(pipeline): add DAG models — Node, Edge, Graph with topological sort"
```

---

### Task 6: Plan models — LogicalPlan, ExecutionPlan, ExecutionContext

**Files:**
- Create: `backend/src/vonnegut/pipeline/dag/plan.py`

- [ ] **Step 1: Implement plan models**

```python
# backend/src/vonnegut/pipeline/dag/plan.py
from __future__ import annotations
from dataclasses import dataclass, field

from vonnegut.pipeline.dag.node import NodeType, NodeConfig
from vonnegut.pipeline.schema.types import Schema


@dataclass(frozen=True)
class PlanNode:
    id: str
    type: NodeType
    config: NodeConfig


@dataclass(frozen=True)
class PlanEdge:
    from_node_id: str
    to_node_id: str
    input_name: str | None = None


@dataclass
class LogicalPlan:
    nodes: dict[str, PlanNode]
    edges: list[PlanEdge] = field(default_factory=list)


@dataclass(frozen=True)
class ExecutionContext:
    node_id: str
    node_type: NodeType
    config: NodeConfig
    input_schemas: dict[str, Schema] = field(default_factory=dict)
    connection_info: dict | None = None


@dataclass
class ExecutionPlan:
    contexts: list[ExecutionContext] = field(default_factory=list)
    edges: list[PlanEdge] = field(default_factory=list)
```

- [ ] **Step 2: Quick smoke test and commit**

```bash
cd backend && uv run python -c "from vonnegut.pipeline.dag.plan import LogicalPlan, ExecutionPlan; print('OK')"
git add backend/src/vonnegut/pipeline/dag/plan.py
git commit -m "feat(pipeline): add LogicalPlan, ExecutionPlan, ExecutionContext models"
```

---

### Task 7: Reporter — base, collector, null

**Files:**
- Create: `backend/src/vonnegut/pipeline/reporter/__init__.py`
- Create: `backend/src/vonnegut/pipeline/reporter/base.py`
- Test: `backend/tests/pipeline/test_reporter.py`

- [ ] **Step 1: Write failing tests**

```python
# backend/tests/pipeline/test_reporter.py
import asyncio
import pytest
from vonnegut.pipeline.reporter.base import Reporter, NullReporter, CollectorReporter


class TestNullReporter:
    @pytest.mark.asyncio
    async def test_emit_does_nothing(self):
        reporter = NullReporter()
        await reporter.emit("test_event", node_id="abc")  # should not raise


class TestCollectorReporter:
    @pytest.mark.asyncio
    async def test_collects_events(self):
        reporter = CollectorReporter()
        await reporter.emit("node_start", node_id="src", name="Source")
        await reporter.emit("node_complete", node_id="src", duration_ms=42)
        assert len(reporter.events) == 2
        assert reporter.events[0] == {"type": "node_start", "node_id": "src", "name": "Source"}
        assert reporter.events[1] == {"type": "node_complete", "node_id": "src", "duration_ms": 42}

    @pytest.mark.asyncio
    async def test_events_of_type(self):
        reporter = CollectorReporter()
        await reporter.emit("node_start", node_id="a")
        await reporter.emit("node_complete", node_id="a")
        await reporter.emit("node_start", node_id="b")
        starts = reporter.events_of_type("node_start")
        assert len(starts) == 2
```

- [ ] **Step 2: Implement Reporter**

```python
# backend/src/vonnegut/pipeline/reporter/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


class Reporter(ABC):
    @abstractmethod
    async def emit(self, event_type: str, **data: Any) -> None: ...


class NullReporter(Reporter):
    async def emit(self, event_type: str, **data: Any) -> None:
        pass


class CollectorReporter(Reporter):
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    async def emit(self, event_type: str, **data: Any) -> None:
        self.events.append({"type": event_type, **data})

    def events_of_type(self, event_type: str) -> list[dict[str, Any]]:
        return [e for e in self.events if e["type"] == event_type]
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
cd backend && uv run pytest tests/pipeline/test_reporter.py -v
git add backend/src/vonnegut/pipeline/reporter/ backend/tests/pipeline/test_reporter.py
git commit -m "feat(pipeline): add Reporter ABC, NullReporter, CollectorReporter"
```

---

## Chunk 2: Executors

### Task 8: Executor base + registry

**Files:**
- Create: `backend/src/vonnegut/pipeline/engine/__init__.py`
- Create: `backend/src/vonnegut/pipeline/engine/executor/__init__.py`
- Create: `backend/src/vonnegut/pipeline/engine/executor/base.py`

- [ ] **Step 1: Implement NodeExecutor ABC and ExecutorRegistry**

```python
# backend/src/vonnegut/pipeline/engine/executor/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
import pyarrow as pa

from vonnegut.pipeline.dag.node import NodeType
from vonnegut.pipeline.dag.plan import ExecutionContext


class NodeExecutor(ABC):
    @abstractmethod
    async def execute(
        self, context: ExecutionContext, inputs: dict[str, pa.Table]
    ) -> pa.Table: ...


class ExecutorRegistry:
    def __init__(self) -> None:
        self._executors: dict[NodeType, NodeExecutor] = {}

    def register(self, node_type: NodeType, executor: NodeExecutor) -> None:
        self._executors[node_type] = executor

    def get(self, node_type: NodeType) -> NodeExecutor:
        executor = self._executors.get(node_type)
        if executor is None:
            raise KeyError(f"No executor registered for node type: {node_type}")
        return executor
```

- [ ] **Step 2: Commit**

```bash
git add backend/src/vonnegut/pipeline/engine/
git commit -m "feat(pipeline): add NodeExecutor ABC and ExecutorRegistry"
```

---

### Task 9: CodeExecutor — extract from PipelineEngine

**Files:**
- Create: `backend/src/vonnegut/pipeline/engine/executor/code_executor.py`
- Test: `backend/tests/pipeline/test_executor_code.py`

This extracts the sandbox logic from the existing `services/pipeline_engine.py:_execute_code` into a proper executor.

- [ ] **Step 1: Write failing tests**

```python
# backend/tests/pipeline/test_executor_code.py
import pyarrow as pa
import pytest
from vonnegut.pipeline.engine.executor.code_executor import CodeExecutor
from vonnegut.pipeline.dag.node import NodeType, CodeNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.schema.types import Schema, Column, DataType


def _make_context(code: str) -> ExecutionContext:
    return ExecutionContext(
        node_id="code1",
        node_type=NodeType.CODE,
        config=CodeNodeConfig(function_code=code),
    )


def _make_input_table() -> pa.Table:
    return pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})


class TestCodeExecutor:
    @pytest.mark.asyncio
    async def test_identity_transform(self):
        code = "def transform(df):\n    return df\n"
        executor = CodeExecutor()
        result = await executor.execute(_make_context(code), {"default": _make_input_table()})
        assert isinstance(result, pa.Table)
        assert result.num_rows == 3

    @pytest.mark.asyncio
    async def test_adds_column(self):
        code = "def transform(df):\n    return df.with_columns(pl.col('id') * 2)\n"
        executor = CodeExecutor()
        result = await executor.execute(_make_context(code), {"default": _make_input_table()})
        assert result.column("id").to_pylist() == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_missing_transform_function_raises(self):
        code = "x = 42\n"
        executor = CodeExecutor()
        with pytest.raises(ValueError, match="transform"):
            await executor.execute(_make_context(code), {"default": _make_input_table()})

    @pytest.mark.asyncio
    async def test_import_blocked(self):
        code = "import os\ndef transform(df):\n    return df\n"
        executor = CodeExecutor()
        with pytest.raises(ValueError, match="Import"):
            await executor.execute(_make_context(code), {"default": _make_input_table()})

    @pytest.mark.asyncio
    async def test_polars_available(self):
        code = "def transform(df):\n    return df.select(pl.col('id'))\n"
        executor = CodeExecutor()
        result = await executor.execute(_make_context(code), {"default": _make_input_table()})
        assert result.column_names == ["id"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd backend && uv run pytest tests/pipeline/test_executor_code.py -v`

- [ ] **Step 3: Implement CodeExecutor**

```python
# backend/src/vonnegut/pipeline/engine/executor/code_executor.py
from __future__ import annotations
import builtins
import datetime
import hashlib
import json
import math
import re

import polars as pl
import pyarrow as pa

from vonnegut.pipeline.dag.node import CodeNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.engine.executor.base import NodeExecutor

_SAFE_BUILTIN_NAMES = [
    "abs", "all", "any", "bool", "bytes", "chr", "dict", "divmod",
    "enumerate", "filter", "float", "format", "frozenset", "hasattr",
    "hash", "int", "isinstance", "issubclass", "iter", "len", "list",
    "map", "max", "min", "next", "ord", "pow", "print", "range",
    "repr", "reversed", "round", "set", "slice", "sorted", "str",
    "sum", "tuple", "type", "zip",
    "True", "False", "None",
    "ValueError", "TypeError", "KeyError", "IndexError", "RuntimeError",
    "Exception", "StopIteration", "AttributeError", "ZeroDivisionError",
]
_SAFE_BUILTINS = {name: getattr(builtins, name) for name in _SAFE_BUILTIN_NAMES if hasattr(builtins, name)}

_CODE_GLOBALS = {
    "pl": pl, "polars": pl,
    "math": math, "re": re, "json": json,
    "hashlib": hashlib, "datetime": datetime,
    "__builtins__": _SAFE_BUILTINS,
}


class CodeExecutor(NodeExecutor):
    async def execute(
        self, context: ExecutionContext, inputs: dict[str, pa.Table]
    ) -> pa.Table:
        config = context.config
        assert isinstance(config, CodeNodeConfig)

        input_table = inputs.get("default")
        df = pl.from_arrow(input_table) if input_table is not None else pl.DataFrame()

        code = config.function_code
        compiled = compile(code, f"<{context.node_id}>", "exec")
        local_ns: dict = {}

        try:
            exec(compiled, {**_CODE_GLOBALS}, local_ns)
        except ImportError as e:
            if "__import__" in str(e):
                raise ValueError(
                    "Import statements are not allowed in code transforms. "
                    "Available modules: pl (polars), math, re, json, hashlib, datetime."
                ) from None
            raise

        transform_fn = local_ns.get("transform")
        if transform_fn is None:
            raise ValueError("Code must define a 'transform(df)' function")

        result_df = transform_fn(df)
        if not isinstance(result_df, pl.DataFrame):
            raise ValueError(f"transform() must return a polars DataFrame, got {type(result_df).__name__}")

        return result_df.to_arrow()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && uv run pytest tests/pipeline/test_executor_code.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add backend/src/vonnegut/pipeline/engine/executor/code_executor.py backend/tests/pipeline/test_executor_code.py
git commit -m "feat(pipeline): add CodeExecutor with sandbox (extracted from PipelineEngine)"
```

---

### Task 10: SqlExecutor — execute SQL via DuckDB on Arrow tables

**Files:**
- Create: `backend/src/vonnegut/pipeline/engine/executor/sql_executor.py`
- Test: `backend/tests/pipeline/test_executor_sql.py`

- [ ] **Step 1: Write failing tests**

```python
# backend/tests/pipeline/test_executor_sql.py
import pyarrow as pa
import pytest
from vonnegut.pipeline.engine.executor.sql_executor import SqlExecutor
from vonnegut.pipeline.dag.node import NodeType, SqlNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext


def _make_context(expression: str) -> ExecutionContext:
    return ExecutionContext(
        node_id="sql1",
        node_type=NodeType.SQL,
        config=SqlNodeConfig(expression=expression),
    )


def _make_input_table() -> pa.Table:
    return pa.table({"id": [1, 2, 3], "name": ["alice", "bob", "charlie"], "age": [30, 25, 35]})


class TestSqlExecutor:
    @pytest.mark.asyncio
    async def test_select_all(self):
        executor = SqlExecutor()
        result = await executor.execute(
            _make_context("SELECT * FROM {prev}"),
            {"default": _make_input_table()},
        )
        assert isinstance(result, pa.Table)
        assert result.num_rows == 3

    @pytest.mark.asyncio
    async def test_filter(self):
        executor = SqlExecutor()
        result = await executor.execute(
            _make_context("SELECT * FROM {prev} WHERE age > 28"),
            {"default": _make_input_table()},
        )
        assert result.num_rows == 2

    @pytest.mark.asyncio
    async def test_add_column(self):
        executor = SqlExecutor()
        result = await executor.execute(
            _make_context("SELECT *, age * 2 AS double_age FROM {prev}"),
            {"default": _make_input_table()},
        )
        assert "double_age" in result.column_names

    @pytest.mark.asyncio
    async def test_invalid_sql_raises(self):
        executor = SqlExecutor()
        with pytest.raises(Exception):
            await executor.execute(
                _make_context("NOT VALID SQL"),
                {"default": _make_input_table()},
            )
```

- [ ] **Step 2: Implement SqlExecutor**

```python
# backend/src/vonnegut/pipeline/engine/executor/sql_executor.py
from __future__ import annotations
import duckdb
import pyarrow as pa

from vonnegut.pipeline.dag.node import SqlNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.engine.executor.base import NodeExecutor


class SqlExecutor(NodeExecutor):
    async def execute(
        self, context: ExecutionContext, inputs: dict[str, pa.Table]
    ) -> pa.Table:
        config = context.config
        assert isinstance(config, SqlNodeConfig)

        expression = config.expression
        input_table = inputs.get("default")

        conn = duckdb.connect()
        try:
            if input_table is not None:
                conn.register("prev", input_table)
                # Replace {prev} placeholder with the registered table name
                sql = expression.replace("{prev}", "prev")
            else:
                sql = expression

            result = conn.execute(sql).fetch_arrow_table()
            return result
        finally:
            conn.close()
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
cd backend && uv run pytest tests/pipeline/test_executor_sql.py -v
git add backend/src/vonnegut/pipeline/engine/executor/sql_executor.py backend/tests/pipeline/test_executor_sql.py
git commit -m "feat(pipeline): add SqlExecutor using DuckDB on Arrow tables"
```

---

### Task 11: SourceExecutor and TargetExecutor

**Files:**
- Create: `backend/src/vonnegut/pipeline/engine/executor/source_executor.py`
- Create: `backend/src/vonnegut/pipeline/engine/executor/target_executor.py`

These interact with real databases via the existing `DatabaseAdapter` interface. For now, implement with adapter dependency injection — full integration tests come later.

- [ ] **Step 1: Implement SourceExecutor**

```python
# backend/src/vonnegut/pipeline/engine/executor/source_executor.py
from __future__ import annotations
import pyarrow as pa

from vonnegut.pipeline.dag.node import SourceNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.engine.executor.base import NodeExecutor
from vonnegut.adapters.base import DatabaseAdapter


class SourceExecutor(NodeExecutor):
    def __init__(self, adapter_factory: object) -> None:
        self._adapter_factory = adapter_factory

    async def execute(
        self, context: ExecutionContext, inputs: dict[str, pa.Table]
    ) -> pa.Table:
        config = context.config
        assert isinstance(config, SourceNodeConfig)

        # Resolve adapter from connection_info in context
        adapter: DatabaseAdapter = context.connection_info["adapter"]
        query = config.query or f"SELECT * FROM {config.table}"

        # Apply limit if present in connection_info (test mode)
        limit = context.connection_info.get("limit")
        if limit:
            query = f"SELECT * FROM ({query}) AS _src LIMIT {limit}"

        rows = await adapter.execute(query)
        if not rows:
            return pa.table({})
        # Convert list[dict] to Arrow table
        return pa.Table.from_pylist(rows)


class TargetExecutor(NodeExecutor):
    async def execute(
        self, context: ExecutionContext, inputs: dict[str, pa.Table]
    ) -> pa.Table:
        config = context.config
        assert isinstance(config, SourceNodeConfig.__class__)  # TargetNodeConfig
        from vonnegut.pipeline.dag.node import TargetNodeConfig
        assert isinstance(config, TargetNodeConfig)

        input_table = inputs.get("default")
        if input_table is None:
            return pa.table({})

        adapter: DatabaseAdapter = context.connection_info["adapter"]
        allow_writes = context.connection_info.get("allow_writes", False)

        if not allow_writes:
            # Dry-run or test mode — just pass through
            return input_table

        rows = input_table.to_pylist()
        if config.truncate:
            await adapter.execute(f"TRUNCATE TABLE {config.table}")

        if rows:
            columns = list(rows[0].keys())
            placeholders = ", ".join(["%s"] * len(columns))
            col_names = ", ".join(columns)
            insert_sql = f"INSERT INTO {config.table} ({col_names}) VALUES ({placeholders})"
            for row in rows:
                await adapter.execute(insert_sql, tuple(row[c] for c in columns))

        return input_table
```

- [ ] **Step 2: Commit**

```bash
git add backend/src/vonnegut/pipeline/engine/executor/source_executor.py backend/src/vonnegut/pipeline/engine/executor/target_executor.py
git commit -m "feat(pipeline): add SourceExecutor and TargetExecutor"
```

---

## Chunk 3: Validation System

### Task 12: Validation rules — base + CheckResult

**Files:**
- Create: `backend/src/vonnegut/pipeline/engine/validator/__init__.py`
- Create: `backend/src/vonnegut/pipeline/engine/validator/rules/__init__.py`
- Create: `backend/src/vonnegut/pipeline/engine/validator/rules/base.py`

- [ ] **Step 1: Implement ValidationRule ABC**

```python
# backend/src/vonnegut/pipeline/engine/validator/rules/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from vonnegut.pipeline.results import CheckResult

if TYPE_CHECKING:
    import pyarrow as pa
    from vonnegut.pipeline.dag.node import Node
    from vonnegut.pipeline.dag.plan import ExecutionContext
    from vonnegut.pipeline.schema.types import Schema


class ValidationRule(ABC):
    name: str
    critical: bool = True

    @abstractmethod
    def check(
        self,
        node: Node,
        context: ExecutionContext,
        input_data: dict[str, pa.Table],
        output_data: pa.Table | None,
        input_schemas: dict[str, Schema],
        output_schema: Schema | None,
    ) -> CheckResult: ...
```

- [ ] **Step 2: Commit**

```bash
git add backend/src/vonnegut/pipeline/engine/validator/
git commit -m "feat(pipeline): add ValidationRule ABC"
```

---

### Task 13: Code validation rules — SyntaxCheck, ExecutionCheck, ColumnNameCheck

**Files:**
- Create: `backend/src/vonnegut/pipeline/engine/validator/rules/code_rules.py`
- Test: `backend/tests/pipeline/test_validator_rules.py`

- [ ] **Step 1: Write failing tests**

```python
# backend/tests/pipeline/test_validator_rules.py
import pyarrow as pa
import pytest
from vonnegut.pipeline.results import CheckStatus
from vonnegut.pipeline.dag.node import Node, NodeType, CodeNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.schema.types import Schema, Column, DataType
from vonnegut.pipeline.engine.validator.rules.code_rules import (
    SyntaxCheckRule,
    ColumnNameRule,
)


def _node(code: str) -> Node:
    return Node(id="c1", type=NodeType.CODE, config=CodeNodeConfig(function_code=code))


def _ctx(code: str) -> ExecutionContext:
    return ExecutionContext(node_id="c1", node_type=NodeType.CODE, config=CodeNodeConfig(function_code=code))


class TestSyntaxCheckRule:
    def test_valid_code_passes(self):
        rule = SyntaxCheckRule()
        result = rule.check(
            _node("def transform(df):\n    return df\n"),
            _ctx("def transform(df):\n    return df\n"),
            {}, None, {}, None,
        )
        assert result.status == CheckStatus.PASSED

    def test_syntax_error_fails(self):
        rule = SyntaxCheckRule()
        result = rule.check(
            _node("def transform(df)\n    return df\n"),  # missing colon
            _ctx("def transform(df)\n    return df\n"),
            {}, None, {}, None,
        )
        assert result.status == CheckStatus.FAILED

    def test_no_transform_function_fails(self):
        rule = SyntaxCheckRule()
        result = rule.check(
            _node("x = 42\n"),
            _ctx("x = 42\n"),
            {}, None, {}, None,
        )
        assert result.status == CheckStatus.FAILED


class TestColumnNameRule:
    def test_unique_columns_pass(self):
        rule = ColumnNameRule()
        schema = Schema(columns=[Column("a", DataType.INT64), Column("b", DataType.UTF8)])
        result = rule.check(_node(""), _ctx(""), {}, None, {}, schema)
        assert result.status == CheckStatus.PASSED

    def test_duplicate_columns_fail(self):
        rule = ColumnNameRule()
        schema = Schema(columns=[Column("a", DataType.INT64), Column("a", DataType.UTF8)])
        result = rule.check(_node(""), _ctx(""), {}, None, {}, schema)
        assert result.status == CheckStatus.FAILED

    def test_empty_column_name_fails(self):
        rule = ColumnNameRule()
        schema = Schema(columns=[Column("", DataType.INT64)])
        result = rule.check(_node(""), _ctx(""), {}, None, {}, schema)
        assert result.status == CheckStatus.FAILED
```

- [ ] **Step 2: Implement code validation rules**

```python
# backend/src/vonnegut/pipeline/engine/validator/rules/code_rules.py
from __future__ import annotations
import ast
from typing import TYPE_CHECKING

from vonnegut.pipeline.results import CheckResult, CheckStatus
from vonnegut.pipeline.engine.validator.rules.base import ValidationRule

if TYPE_CHECKING:
    import pyarrow as pa
    from vonnegut.pipeline.dag.node import Node, CodeNodeConfig
    from vonnegut.pipeline.dag.plan import ExecutionContext
    from vonnegut.pipeline.schema.types import Schema


class SyntaxCheckRule(ValidationRule):
    name = "syntax_check"
    critical = True

    def check(self, node, context, input_data, output_data, input_schemas, output_schema):
        from vonnegut.pipeline.dag.node import CodeNodeConfig
        config = node.config
        assert isinstance(config, CodeNodeConfig)
        code = config.function_code

        # Check syntax
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            return CheckResult(
                rule_name=self.name, status=CheckStatus.FAILED,
                message=f"Syntax error: {e}",
            )

        # Check that transform() function exists
        func_names = [
            n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
        ]
        if "transform" not in func_names:
            return CheckResult(
                rule_name=self.name, status=CheckStatus.FAILED,
                message="Code must define a 'transform(df)' function",
            )

        return CheckResult(
            rule_name=self.name, status=CheckStatus.PASSED,
            message="Syntax valid, transform() function found",
        )


class ColumnNameRule(ValidationRule):
    name = "column_name_check"
    critical = False

    def check(self, node, context, input_data, output_data, input_schemas, output_schema):
        if output_schema is None:
            return CheckResult(
                rule_name=self.name, status=CheckStatus.PASSED,
                message="No output schema to check",
            )

        names = output_schema.column_names
        # Check for empty names
        empty = [i for i, n in enumerate(names) if not n.strip()]
        if empty:
            return CheckResult(
                rule_name=self.name, status=CheckStatus.FAILED,
                message=f"Empty column name(s) at positions: {empty}",
            )

        # Check for duplicates
        seen = set()
        dupes = []
        for n in names:
            if n in seen:
                dupes.append(n)
            seen.add(n)
        if dupes:
            return CheckResult(
                rule_name=self.name, status=CheckStatus.FAILED,
                message=f"Duplicate column names: {dupes}",
            )

        return CheckResult(
            rule_name=self.name, status=CheckStatus.PASSED,
            message="All column names valid and unique",
        )
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
cd backend && uv run pytest tests/pipeline/test_validator_rules.py -v
git add backend/src/vonnegut/pipeline/engine/validator/rules/code_rules.py backend/tests/pipeline/test_validator_rules.py
git commit -m "feat(pipeline): add SyntaxCheckRule and ColumnNameRule for code nodes"
```

---

### Task 14: SQL validation rules — SqlParseRule

**Files:**
- Create: `backend/src/vonnegut/pipeline/engine/validator/rules/sql_rules.py`

- [ ] **Step 1: Add SQL rule tests to test_validator_rules.py**

```python
# Append to backend/tests/pipeline/test_validator_rules.py
from vonnegut.pipeline.dag.node import SqlNodeConfig
from vonnegut.pipeline.engine.validator.rules.sql_rules import SqlParseRule


def _sql_node(expr: str) -> Node:
    return Node(id="s1", type=NodeType.SQL, config=SqlNodeConfig(expression=expr))

def _sql_ctx(expr: str) -> ExecutionContext:
    return ExecutionContext(node_id="s1", node_type=NodeType.SQL, config=SqlNodeConfig(expression=expr))


class TestSqlParseRule:
    def test_valid_select_passes(self):
        rule = SqlParseRule()
        result = rule.check(_sql_node("SELECT * FROM {prev}"), _sql_ctx("SELECT * FROM {prev}"),
                           {}, None, {}, None)
        assert result.status == CheckStatus.PASSED

    def test_invalid_sql_fails(self):
        rule = SqlParseRule()
        result = rule.check(_sql_node("NOT VALID SQL AT ALL"), _sql_ctx("NOT VALID SQL AT ALL"),
                           {}, None, {}, None)
        assert result.status == CheckStatus.FAILED

    def test_multiple_statements_fails(self):
        rule = SqlParseRule()
        result = rule.check(_sql_node("SELECT 1; SELECT 2"), _sql_ctx("SELECT 1; SELECT 2"),
                           {}, None, {}, None)
        assert result.status == CheckStatus.FAILED

    def test_non_select_fails(self):
        rule = SqlParseRule()
        result = rule.check(_sql_node("DROP TABLE users"), _sql_ctx("DROP TABLE users"),
                           {}, None, {}, None)
        assert result.status == CheckStatus.FAILED
```

- [ ] **Step 2: Implement SqlParseRule**

```python
# backend/src/vonnegut/pipeline/engine/validator/rules/sql_rules.py
from __future__ import annotations
import sqlglot
from sqlglot import exp

from vonnegut.pipeline.results import CheckResult, CheckStatus
from vonnegut.pipeline.engine.validator.rules.base import ValidationRule


class SqlParseRule(ValidationRule):
    name = "sql_parse"
    critical = True

    def check(self, node, context, input_data, output_data, input_schemas, output_schema):
        from vonnegut.pipeline.dag.node import SqlNodeConfig
        config = node.config
        assert isinstance(config, SqlNodeConfig)

        expression = config.expression.strip()
        # Replace {prev} with a dummy table name for parsing
        parse_expr = expression.replace("{prev}", "__prev__")

        try:
            statements = sqlglot.parse(parse_expr, error_level=sqlglot.ErrorLevel.WARN)
        except sqlglot.errors.ParseError as e:
            return CheckResult(
                rule_name=self.name, status=CheckStatus.FAILED,
                message=f"SQL parse error: {e}",
            )

        if len(statements) > 1:
            return CheckResult(
                rule_name=self.name, status=CheckStatus.FAILED,
                message="SQL must be a single statement (no semicolons)",
            )

        if not statements or statements[0] is None:
            return CheckResult(
                rule_name=self.name, status=CheckStatus.FAILED,
                message="SQL expression is empty or could not be parsed",
            )

        if not isinstance(statements[0], exp.Select):
            return CheckResult(
                rule_name=self.name, status=CheckStatus.FAILED,
                message=f"SQL must be a SELECT statement, got: {type(statements[0]).__name__}",
            )

        return CheckResult(
            rule_name=self.name, status=CheckStatus.PASSED,
            message="SQL is a valid single SELECT statement",
        )
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
cd backend && uv run pytest tests/pipeline/test_validator_rules.py -v
git add backend/src/vonnegut/pipeline/engine/validator/rules/sql_rules.py backend/tests/pipeline/test_validator_rules.py
git commit -m "feat(pipeline): add SqlParseRule for SQL node validation"
```

---

### Task 15: NodeValidator — compose executor + rules

**Files:**
- Create: `backend/src/vonnegut/pipeline/engine/validator/node_validator.py`
- Test: `backend/tests/pipeline/test_node_validator.py`

- [ ] **Step 1: Write failing tests**

```python
# backend/tests/pipeline/test_node_validator.py
import pyarrow as pa
import pytest
from vonnegut.pipeline.results import ValidationSuccess, ValidationFailure
from vonnegut.pipeline.dag.node import Node, NodeType, CodeNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.engine.executor.code_executor import CodeExecutor
from vonnegut.pipeline.engine.validator.node_validator import NodeValidator
from vonnegut.pipeline.engine.validator.rules.code_rules import SyntaxCheckRule, ColumnNameRule


def _make_validator() -> NodeValidator:
    return NodeValidator(
        executor=CodeExecutor(),
        rules=[SyntaxCheckRule(), ColumnNameRule()],
    )


class TestNodeValidator:
    @pytest.mark.asyncio
    async def test_valid_code_returns_success(self):
        validator = _make_validator()
        node = Node(id="c1", type=NodeType.CODE, config=CodeNodeConfig(function_code="def transform(df):\n    return df\n"))
        ctx = ExecutionContext(node_id="c1", node_type=NodeType.CODE, config=node.config)
        input_table = pa.table({"id": [1, 2], "name": ["a", "b"]})

        result = await validator.validate(node, ctx, {"default": input_table})

        match result:
            case ValidationSuccess(output_schema=schema):
                assert schema is not None
                assert len(schema.columns) == 2
            case ValidationFailure():
                pytest.fail("Expected ValidationSuccess")

    @pytest.mark.asyncio
    async def test_syntax_error_returns_failure(self):
        validator = _make_validator()
        node = Node(id="c1", type=NodeType.CODE, config=CodeNodeConfig(function_code="def transform(df)\n    return df\n"))
        ctx = ExecutionContext(node_id="c1", node_type=NodeType.CODE, config=node.config)

        result = await validator.validate(node, ctx, {"default": pa.table({"x": [1]})})

        match result:
            case ValidationFailure(errors=errors):
                assert len(errors) > 0
                assert errors[0].rule_name == "syntax_check"
            case ValidationSuccess():
                pytest.fail("Expected ValidationFailure")

    @pytest.mark.asyncio
    async def test_execution_error_returns_failure(self):
        validator = _make_validator()
        code = "def transform(df):\n    raise RuntimeError('boom')\n"
        node = Node(id="c1", type=NodeType.CODE, config=CodeNodeConfig(function_code=code))
        ctx = ExecutionContext(node_id="c1", node_type=NodeType.CODE, config=node.config)

        result = await validator.validate(node, ctx, {"default": pa.table({"x": [1]})})

        match result:
            case ValidationFailure(errors=errors):
                assert any("boom" in e.message for e in errors)
            case ValidationSuccess():
                pytest.fail("Expected ValidationFailure")
```

- [ ] **Step 2: Implement NodeValidator**

```python
# backend/src/vonnegut/pipeline/engine/validator/node_validator.py
from __future__ import annotations
import pyarrow as pa

from vonnegut.pipeline.dag.node import Node
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.engine.executor.base import NodeExecutor
from vonnegut.pipeline.engine.validator.rules.base import ValidationRule
from vonnegut.pipeline.results import (
    CheckResult, CheckStatus,
    ValidationSuccess, ValidationFailure, NodeValidationResult,
)
from vonnegut.pipeline.schema.adapters import ArrowSchemaAdapter


class NodeValidator:
    def __init__(self, executor: NodeExecutor, rules: list[ValidationRule]) -> None:
        self.executor = executor
        self.rules = rules

    async def validate(
        self,
        node: Node,
        context: ExecutionContext,
        inputs: dict[str, pa.Table],
    ) -> NodeValidationResult:
        # 1. Run pre-execution rules (rules that don't need output)
        pre_checks: list[CheckResult] = []
        for rule in self.rules:
            result = rule.check(node, context, inputs, None, context.input_schemas, None)
            pre_checks.append(result)
            if result.status == CheckStatus.FAILED and rule.critical:
                return ValidationFailure(errors=[r for r in pre_checks if r.status == CheckStatus.FAILED])

        # 2. Execute the node
        try:
            output_data = await self.executor.execute(context, inputs)
            output_schema = ArrowSchemaAdapter.from_arrow(output_data.schema)
        except Exception as exec_error:
            return ValidationFailure(
                errors=[CheckResult(
                    rule_name="execution",
                    status=CheckStatus.FAILED,
                    message=str(exec_error),
                )],
            )

        # 3. Run post-execution rules (rules that inspect output)
        post_checks: list[CheckResult] = []
        for rule in self.rules:
            result = rule.check(node, context, inputs, output_data, context.input_schemas, output_schema)
            post_checks.append(result)
            if result.status == CheckStatus.FAILED and rule.critical:
                failed = [r for r in pre_checks + post_checks if r.status == CheckStatus.FAILED]
                return ValidationFailure(
                    errors=failed,
                    output_schema=output_schema,
                    output_data=output_data,
                )

        all_checks = pre_checks + post_checks
        failed = [r for r in all_checks if r.status == CheckStatus.FAILED]
        if failed:
            return ValidationFailure(errors=failed, output_schema=output_schema, output_data=output_data)

        return ValidationSuccess(
            output_schema=output_schema,
            output_data=output_data,
            checks=all_checks,
        )
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
cd backend && uv run pytest tests/pipeline/test_node_validator.py -v
git add backend/src/vonnegut/pipeline/engine/validator/node_validator.py backend/tests/pipeline/test_node_validator.py
git commit -m "feat(pipeline): add NodeValidator — composes executor + validation rules"
```

---

### Task 16: PipelineValidator — cross-edge schema compatibility

**Files:**
- Create: `backend/src/vonnegut/pipeline/engine/validator/pipeline_validator.py`
- Test: `backend/tests/pipeline/test_pipeline_validator.py`

- [ ] **Step 1: Write failing tests**

```python
# backend/tests/pipeline/test_pipeline_validator.py
import pytest
from vonnegut.pipeline.results import CheckStatus
from vonnegut.pipeline.schema.types import Schema, Column, DataType
from vonnegut.pipeline.dag.plan import PlanNode, PlanEdge
from vonnegut.pipeline.dag.node import NodeType, SqlNodeConfig
from vonnegut.pipeline.engine.validator.pipeline_validator import (
    PipelineValidator, SchemaCompatibilityRule,
)


class TestSchemaCompatibilityRule:
    def test_compatible_schemas_pass(self):
        rule = SchemaCompatibilityRule()
        from_schema = Schema(columns=[Column("id", DataType.INT64), Column("name", DataType.UTF8)])
        edge = PlanEdge(from_node_id="a", to_node_id="b")
        from_node = PlanNode(id="a", type=NodeType.SQL, config=SqlNodeConfig(expression=""))
        to_node = PlanNode(id="b", type=NodeType.SQL, config=SqlNodeConfig(expression=""))
        result = rule.check(edge, from_node, to_node, from_schema, None)
        assert result.status == CheckStatus.PASSED

    def test_empty_schema_warns(self):
        rule = SchemaCompatibilityRule()
        from_schema = Schema(columns=[])
        edge = PlanEdge(from_node_id="a", to_node_id="b")
        from_node = PlanNode(id="a", type=NodeType.SQL, config=SqlNodeConfig(expression=""))
        to_node = PlanNode(id="b", type=NodeType.SQL, config=SqlNodeConfig(expression=""))
        result = rule.check(edge, from_node, to_node, from_schema, None)
        assert result.status == CheckStatus.WARNING
```

- [ ] **Step 2: Implement PipelineValidator**

```python
# backend/src/vonnegut/pipeline/engine/validator/pipeline_validator.py
from __future__ import annotations
from abc import ABC, abstractmethod

from vonnegut.pipeline.results import CheckResult, CheckStatus
from vonnegut.pipeline.schema.types import Schema
from vonnegut.pipeline.dag.plan import PlanNode, PlanEdge


class PipelineValidationRule(ABC):
    name: str

    @abstractmethod
    def check(
        self,
        edge: PlanEdge,
        from_node: PlanNode,
        to_node: PlanNode,
        from_schema: Schema,
        to_input_name: str | None,
    ) -> CheckResult: ...


class SchemaCompatibilityRule(PipelineValidationRule):
    name = "schema_compatibility"

    def check(self, edge, from_node, to_node, from_schema, to_input_name):
        if not from_schema.columns:
            return CheckResult(
                rule_name=self.name, status=CheckStatus.WARNING,
                message=f"Empty schema from node '{from_node.id}' — cannot verify compatibility",
            )
        return CheckResult(
            rule_name=self.name, status=CheckStatus.PASSED,
            message=f"Schema from '{from_node.id}' has {len(from_schema.columns)} columns",
        )


class PipelineValidator:
    def __init__(self, rules: list[PipelineValidationRule] | None = None) -> None:
        self.rules = rules or [SchemaCompatibilityRule()]

    def validate_edge(
        self,
        edge: PlanEdge,
        from_node: PlanNode,
        to_node: PlanNode,
        from_schema: Schema,
        to_input_name: str | None,
    ) -> list[CheckResult]:
        results = []
        for rule in self.rules:
            results.append(rule.check(edge, from_node, to_node, from_schema, to_input_name))
        return results
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
cd backend && uv run pytest tests/pipeline/test_pipeline_validator.py -v
git add backend/src/vonnegut/pipeline/engine/validator/pipeline_validator.py backend/tests/pipeline/test_pipeline_validator.py
git commit -m "feat(pipeline): add PipelineValidator with SchemaCompatibilityRule"
```

---

## Chunk 4: Orchestrator, Optimizer, Pipeline Manager

### Task 17: PipelineOrchestrator — test mode walk

**Files:**
- Create: `backend/src/vonnegut/pipeline/engine/orchestrator.py`
- Test: `backend/tests/pipeline/test_orchestrator.py`

- [ ] **Step 1: Write failing tests**

```python
# backend/tests/pipeline/test_orchestrator.py
import pyarrow as pa
import pytest
from vonnegut.pipeline.engine.orchestrator import PipelineOrchestrator
from vonnegut.pipeline.engine.executor.base import NodeExecutor, ExecutorRegistry
from vonnegut.pipeline.engine.validator.node_validator import NodeValidator
from vonnegut.pipeline.engine.validator.pipeline_validator import PipelineValidator
from vonnegut.pipeline.dag.node import Node, NodeType, SourceNodeConfig, SqlNodeConfig, TargetNodeConfig
from vonnegut.pipeline.dag.edge import Edge
from vonnegut.pipeline.dag.plan import LogicalPlan, PlanNode, PlanEdge
from vonnegut.pipeline.results import ValidationSuccess, ValidationFailure, ExecutionSuccess, ExecutionFailure
from vonnegut.pipeline.reporter.base import CollectorReporter


class StubExecutor(NodeExecutor):
    """Returns input unchanged (or empty table for source)."""
    def __init__(self, output: pa.Table | None = None):
        self._output = output

    async def execute(self, context, inputs):
        if self._output is not None:
            return self._output
        default = inputs.get("default")
        return default if default is not None else pa.table({"id": [1, 2, 3]})


class FailingExecutor(NodeExecutor):
    async def execute(self, context, inputs):
        raise RuntimeError("Executor failed")


def _make_linear_plan() -> LogicalPlan:
    return LogicalPlan(
        nodes={
            "src": PlanNode(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1")),
            "sql": PlanNode(id="sql", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT * FROM {prev}")),
            "tgt": PlanNode(id="tgt", type=NodeType.TARGET, config=TargetNodeConfig(connection_id="c2", table="t2", truncate=False)),
        },
        edges=[
            PlanEdge(from_node_id="src", to_node_id="sql"),
            PlanEdge(from_node_id="sql", to_node_id="tgt"),
        ],
    )


class TestOrchestratorTestMode:
    @pytest.mark.asyncio
    async def test_successful_linear_pipeline(self):
        stub = StubExecutor()
        registry = {}
        for nt in NodeType:
            registry[nt] = NodeValidator(executor=stub, rules=[])

        orchestrator = PipelineOrchestrator(
            validator_registry=registry,
            pipeline_validator=PipelineValidator(),
        )
        reporter = CollectorReporter()
        result = await orchestrator.run_test(_make_linear_plan(), reporter)

        assert result.success
        assert len(result.node_results) == 3
        starts = reporter.events_of_type("node_start")
        assert len(starts) == 3

    @pytest.mark.asyncio
    async def test_stops_on_node_failure(self):
        stub = StubExecutor()
        fail = FailingExecutor()
        registry = {
            NodeType.SOURCE: NodeValidator(executor=stub, rules=[]),
            NodeType.SQL: NodeValidator(executor=fail, rules=[]),
            NodeType.TARGET: NodeValidator(executor=stub, rules=[]),
        }

        orchestrator = PipelineOrchestrator(
            validator_registry=registry,
            pipeline_validator=PipelineValidator(),
        )
        reporter = CollectorReporter()
        result = await orchestrator.run_test(_make_linear_plan(), reporter)

        assert not result.success
        # Should have stopped after sql node failed — target not reached
        assert len(result.node_results) == 2
        failed_events = reporter.events_of_type("pipeline_failed")
        assert len(failed_events) == 1
```

- [ ] **Step 2: Implement PipelineOrchestrator**

```python
# backend/src/vonnegut/pipeline/engine/orchestrator.py
from __future__ import annotations
from dataclasses import dataclass, field
import pyarrow as pa

from vonnegut.pipeline.dag.node import NodeType
from vonnegut.pipeline.dag.plan import LogicalPlan, ExecutionPlan, ExecutionContext, PlanEdge
from vonnegut.pipeline.dag.graph import topological_sort, collect_inputs, get_incoming_edges
from vonnegut.pipeline.engine.validator.node_validator import NodeValidator
from vonnegut.pipeline.engine.validator.pipeline_validator import PipelineValidator
from vonnegut.pipeline.engine.executor.base import NodeExecutor, ExecutorRegistry
from vonnegut.pipeline.reporter.base import Reporter, NullReporter
from vonnegut.pipeline.results import (
    ValidationSuccess, ValidationFailure, NodeValidationResult,
    ExecutionSuccess, ExecutionFailure, ExecutionResult,
    CheckStatus,
)
from vonnegut.pipeline.schema.types import Schema


@dataclass
class TestResult:
    node_results: list[NodeValidationResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return all(isinstance(r, ValidationSuccess) for r in self.node_results)


class PipelineOrchestrator:
    def __init__(
        self,
        validator_registry: dict[NodeType, NodeValidator],
        pipeline_validator: PipelineValidator,
        executor_registry: ExecutorRegistry | None = None,
    ) -> None:
        self._validators = validator_registry
        self._pipeline_validator = pipeline_validator
        self._executors = executor_registry

    async def run_test(
        self, plan: LogicalPlan, reporter: Reporter | None = None,
    ) -> TestResult:
        reporter = reporter or NullReporter()
        # Build node/edge dicts compatible with topological_sort
        from vonnegut.pipeline.dag.node import Node
        nodes_for_sort = {
            nid: Node(id=nid, type=pn.type, config=pn.config)
            for nid, pn in plan.nodes.items()
        }
        from vonnegut.pipeline.dag.edge import Edge
        edges_for_sort = [
            Edge(id=f"e_{i}", from_node_id=pe.from_node_id, to_node_id=pe.to_node_id, input_name=pe.input_name)
            for i, pe in enumerate(plan.edges)
        ]

        order = topological_sort(nodes_for_sort, edges_for_sort)
        node_outputs: dict[str, pa.Table] = {}
        node_schemas: dict[str, Schema] = {}
        results: list[NodeValidationResult] = []

        for node_id in order:
            plan_node = plan.nodes[node_id]
            node = nodes_for_sort[node_id]

            inputs = collect_inputs(node_id, edges_for_sort, node_outputs)
            input_schemas = collect_inputs(node_id, edges_for_sort, node_schemas)

            context = ExecutionContext(
                node_id=node_id,
                node_type=plan_node.type,
                config=plan_node.config,
                input_schemas=input_schemas,
            )

            validator = self._validators.get(plan_node.type)
            if validator is None:
                raise KeyError(f"No validator for node type: {plan_node.type}")

            await reporter.emit("node_start", node_id=node_id)
            result = await validator.validate(node, context, inputs)
            results.append(result)

            match result:
                case ValidationFailure():
                    await reporter.emit("pipeline_failed", node_id=node_id)
                    return TestResult(node_results=results)
                case ValidationSuccess(output_schema=schema, output_data=data):
                    await reporter.emit("node_complete", node_id=node_id)
                    # Edge validation
                    for edge in get_incoming_edges(node_id, edges_for_sort):
                        from_schema = node_schemas.get(edge.from_node_id)
                        if from_schema is not None:
                            edge_checks = self._pipeline_validator.validate_edge(
                                PlanEdge(from_node_id=edge.from_node_id, to_node_id=edge.to_node_id, input_name=edge.input_name),
                                plan.nodes[edge.from_node_id], plan_node, from_schema, edge.input_name,
                            )
                            if any(c.status == CheckStatus.FAILED for c in edge_checks):
                                await reporter.emit("pipeline_failed", node_id=node_id)
                                return TestResult(node_results=results)

                    if data is not None:
                        node_outputs[node_id] = data
                    if schema is not None:
                        node_schemas[node_id] = schema

        return TestResult(node_results=results)

    async def run_execute(
        self,
        plan: ExecutionPlan,
        reporter: Reporter | None = None,
        allow_writes: bool = False,
    ) -> ExecutionResult:
        reporter = reporter or NullReporter()
        if self._executors is None:
            raise RuntimeError("ExecutorRegistry required for run_execute")

        node_outputs: dict[str, pa.Table] = {}
        from vonnegut.pipeline.dag.edge import Edge
        edges_for_collect = [
            Edge(id=f"e_{i}", from_node_id=pe.from_node_id, to_node_id=pe.to_node_id, input_name=pe.input_name)
            for i, pe in enumerate(plan.edges)
        ]

        for exec_context in plan.contexts:
            if exec_context.node_type == NodeType.TARGET and not allow_writes:
                await reporter.emit("node_skipped", node_id=exec_context.node_id, reason="dry-run")
                continue

            inputs = collect_inputs(exec_context.node_id, edges_for_collect, node_outputs)
            executor = self._executors.get(exec_context.node_type)

            await reporter.emit("node_start", node_id=exec_context.node_id)
            try:
                output = await executor.execute(exec_context, inputs)
                await reporter.emit("node_complete", node_id=exec_context.node_id)
                node_outputs[exec_context.node_id] = output
            except Exception as e:
                await reporter.emit("node_failed", node_id=exec_context.node_id, error=str(e))
                return ExecutionFailure(node_id=exec_context.node_id, error=str(e))

        return ExecutionSuccess()
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
cd backend && uv run pytest tests/pipeline/test_orchestrator.py -v
git add backend/src/vonnegut/pipeline/engine/orchestrator.py backend/tests/pipeline/test_orchestrator.py
git commit -m "feat(pipeline): add PipelineOrchestrator — DAG walk with validation"
```

---

### Task 18: Optimizer — pass-through for v1

**Files:**
- Create: `backend/src/vonnegut/pipeline/engine/optimizer/__init__.py`
- Create: `backend/src/vonnegut/pipeline/engine/optimizer/optimizer.py`
- Create: `backend/src/vonnegut/pipeline/engine/optimizer/rules/__init__.py`
- Create: `backend/src/vonnegut/pipeline/engine/optimizer/rules/base.py`
- Test: `backend/tests/pipeline/test_optimizer.py`

- [ ] **Step 1: Write failing tests**

```python
# backend/tests/pipeline/test_optimizer.py
import pytest
from vonnegut.pipeline.engine.optimizer.optimizer import Optimizer, OptimizationContext
from vonnegut.pipeline.dag.plan import LogicalPlan, PlanNode, PlanEdge, ExecutionPlan
from vonnegut.pipeline.dag.node import NodeType, SourceNodeConfig, SqlNodeConfig, TargetNodeConfig


def _make_plan() -> LogicalPlan:
    return LogicalPlan(
        nodes={
            "src": PlanNode(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1")),
            "sql": PlanNode(id="sql", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT * FROM {prev}")),
            "tgt": PlanNode(id="tgt", type=NodeType.TARGET, config=TargetNodeConfig(connection_id="c2", table="t2", truncate=False)),
        },
        edges=[
            PlanEdge(from_node_id="src", to_node_id="sql"),
            PlanEdge(from_node_id="sql", to_node_id="tgt"),
        ],
    )


class TestOptimizer:
    def test_pass_through_preserves_all_nodes(self):
        optimizer = Optimizer(rules=[])
        ctx = OptimizationContext(schemas={})
        exec_plan = optimizer.optimize(_make_plan(), ctx)
        assert isinstance(exec_plan, ExecutionPlan)
        assert len(exec_plan.contexts) == 3
        assert exec_plan.contexts[0].node_id == "src"
        assert exec_plan.contexts[1].node_id == "sql"
        assert exec_plan.contexts[2].node_id == "tgt"

    def test_edges_preserved(self):
        optimizer = Optimizer(rules=[])
        ctx = OptimizationContext(schemas={})
        exec_plan = optimizer.optimize(_make_plan(), ctx)
        assert len(exec_plan.edges) == 2
```

- [ ] **Step 2: Implement Optimizer**

```python
# backend/src/vonnegut/pipeline/engine/optimizer/rules/base.py
from __future__ import annotations
from abc import ABC, abstractmethod

from vonnegut.pipeline.dag.plan import LogicalPlan


class OptimizationContext:
    def __init__(self, schemas: dict | None = None, statistics: dict | None = None):
        self.schemas = schemas or {}
        self.statistics = statistics


class OptimizationRule(ABC):
    @abstractmethod
    def apply(self, plan: LogicalPlan, context: OptimizationContext) -> LogicalPlan: ...
```

```python
# backend/src/vonnegut/pipeline/engine/optimizer/optimizer.py
from __future__ import annotations

from vonnegut.pipeline.dag.plan import LogicalPlan, ExecutionPlan, ExecutionContext, PlanEdge
from vonnegut.pipeline.dag.graph import topological_sort
from vonnegut.pipeline.dag.node import Node
from vonnegut.pipeline.dag.edge import Edge
from vonnegut.pipeline.engine.optimizer.rules.base import OptimizationRule, OptimizationContext


class Optimizer:
    def __init__(self, rules: list[OptimizationRule] | None = None) -> None:
        self.rules = rules or []

    def optimize(self, plan: LogicalPlan, context: OptimizationContext) -> ExecutionPlan:
        # Apply optimization rules
        current = plan
        for rule in self.rules:
            current = rule.apply(current, context)

        # Convert LogicalPlan → ExecutionPlan via topological sort
        nodes_for_sort = {
            nid: Node(id=nid, type=pn.type, config=pn.config)
            for nid, pn in current.nodes.items()
        }
        edges_for_sort = [
            Edge(id=f"e_{i}", from_node_id=pe.from_node_id, to_node_id=pe.to_node_id, input_name=pe.input_name)
            for i, pe in enumerate(current.edges)
        ]
        order = topological_sort(nodes_for_sort, edges_for_sort)

        contexts = []
        for node_id in order:
            pn = current.nodes[node_id]
            contexts.append(ExecutionContext(
                node_id=node_id,
                node_type=pn.type,
                config=pn.config,
                input_schemas=context.schemas.get(node_id, {}),
            ))

        return ExecutionPlan(contexts=contexts, edges=list(current.edges))
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
cd backend && uv run pytest tests/pipeline/test_optimizer.py -v
git add backend/src/vonnegut/pipeline/engine/optimizer/ backend/tests/pipeline/test_optimizer.py
git commit -m "feat(pipeline): add Optimizer with pass-through (no rules for v1)"
```

---

### Task 19: Pipeline hashing

**Files:**
- Create: `backend/src/vonnegut/pipeline/control_plane/__init__.py`
- Create: `backend/src/vonnegut/pipeline/control_plane/hashing.py`
- Test: `backend/tests/pipeline/test_hashing.py`

- [ ] **Step 1: Write failing tests**

```python
# backend/tests/pipeline/test_hashing.py
from vonnegut.pipeline.control_plane.hashing import compute_pipeline_hash
from vonnegut.pipeline.dag.node import Node, NodeType, SourceNodeConfig, SqlNodeConfig, TargetNodeConfig
from vonnegut.pipeline.dag.edge import Edge


def test_same_pipeline_same_hash():
    nodes = {"src": Node(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1"))}
    edges = [Edge(id="e1", from_node_id="src", to_node_id="tgt")]
    h1 = compute_pipeline_hash(nodes, edges)
    h2 = compute_pipeline_hash(nodes, edges)
    assert h1 == h2

def test_different_config_different_hash():
    n1 = {"src": Node(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1"))}
    n2 = {"src": Node(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t2"))}
    h1 = compute_pipeline_hash(n1, [])
    h2 = compute_pipeline_hash(n2, [])
    assert h1 != h2

def test_different_edges_different_hash():
    nodes = {
        "a": Node(id="a", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT 1")),
        "b": Node(id="b", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT 2")),
    }
    e1 = [Edge(id="e1", from_node_id="a", to_node_id="b")]
    e2 = [Edge(id="e1", from_node_id="b", to_node_id="a")]
    h1 = compute_pipeline_hash(nodes, e1)
    h2 = compute_pipeline_hash(nodes, e2)
    assert h1 != h2

def test_hash_is_deterministic_string():
    nodes = {"src": Node(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1"))}
    h = compute_pipeline_hash(nodes, [])
    assert isinstance(h, str)
    assert len(h) == 64  # SHA-256 hex
```

- [ ] **Step 2: Implement hashing**

```python
# backend/src/vonnegut/pipeline/control_plane/hashing.py
from __future__ import annotations
import hashlib
import json
from dataclasses import asdict

from vonnegut.pipeline.dag.node import Node
from vonnegut.pipeline.dag.edge import Edge


def compute_pipeline_hash(nodes: dict[str, Node], edges: list[Edge]) -> str:
    # Build a deterministic representation
    node_data = {}
    for nid in sorted(nodes.keys()):
        node = nodes[nid]
        node_data[nid] = {
            "type": node.type.value,
            "config": asdict(node.config),
        }

    edge_data = [
        {"from": e.from_node_id, "to": e.to_node_id, "input_name": e.input_name}
        for e in sorted(edges, key=lambda e: (e.from_node_id, e.to_node_id, e.input_name or ""))
    ]

    payload = json.dumps({"nodes": node_data, "edges": edge_data}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
cd backend && uv run pytest tests/pipeline/test_hashing.py -v
git add backend/src/vonnegut/pipeline/control_plane/ backend/tests/pipeline/test_hashing.py
git commit -m "feat(pipeline): add deterministic pipeline hashing for validation tracking"
```

---

### Task 20: PipelineState and PipelineManager

**Files:**
- Create: `backend/src/vonnegut/pipeline/control_plane/pipeline_state.py`
- Create: `backend/src/vonnegut/pipeline/control_plane/pipeline_manager.py`
- Test: `backend/tests/pipeline/test_pipeline_manager.py`

- [ ] **Step 1: Implement PipelineState**

```python
# backend/src/vonnegut/pipeline/control_plane/pipeline_state.py
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from vonnegut.pipeline.schema.types import Schema


class ValidationStatus(str, Enum):
    DRAFT = "DRAFT"
    VALIDATING = "VALIDATING"
    VALID = "VALID"
    INVALID = "INVALID"


@dataclass
class NodeMetadata:
    node_id: str
    input_schemas: dict[str, Schema] = field(default_factory=dict)
    output_schema: Schema | None = None
    validation_status: str = "pending"
    last_validated_at: datetime | None = None


@dataclass
class PipelineMetadata:
    pipeline_id: str
    node_metadata: dict[str, NodeMetadata] = field(default_factory=dict)
    validated_hash: str | None = None
    validation_status: ValidationStatus = ValidationStatus.DRAFT
    last_validated_at: datetime | None = None
```

- [ ] **Step 2: Write failing tests for PipelineManager**

```python
# backend/tests/pipeline/test_pipeline_manager.py
import pyarrow as pa
import pytest
from vonnegut.pipeline.control_plane.pipeline_manager import PipelineManager
from vonnegut.pipeline.control_plane.pipeline_state import PipelineMetadata, ValidationStatus
from vonnegut.pipeline.dag.node import Node, NodeType, SourceNodeConfig, SqlNodeConfig, TargetNodeConfig
from vonnegut.pipeline.dag.edge import Edge
from vonnegut.pipeline.dag.graph import PipelineGraph
from vonnegut.pipeline.engine.executor.base import NodeExecutor
from vonnegut.pipeline.reporter.base import CollectorReporter


class StubExecutor(NodeExecutor):
    async def execute(self, context, inputs):
        return inputs.get("default", pa.table({"id": [1, 2, 3]}))


class TestPipelineManager:
    def _make_graph(self) -> PipelineGraph:
        return PipelineGraph(
            nodes={
                "src": Node(id="src", type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1")),
                "tgt": Node(id="tgt", type=NodeType.TARGET, config=TargetNodeConfig(connection_id="c2", table="t2", truncate=False)),
            },
            edges=[Edge(id="e1", from_node_id="src", to_node_id="tgt")],
        )

    def _make_manager(self) -> PipelineManager:
        return PipelineManager.create_default(stub_executor=StubExecutor())

    @pytest.mark.asyncio
    async def test_validate_sets_status_valid(self):
        manager = self._make_manager()
        graph = self._make_graph()
        metadata = PipelineMetadata(pipeline_id="p1")
        reporter = CollectorReporter()

        result = await manager.validate(graph, metadata, reporter)

        assert result.success
        assert metadata.validation_status == ValidationStatus.VALID
        assert metadata.validated_hash is not None

    @pytest.mark.asyncio
    async def test_can_run_after_validation(self):
        manager = self._make_manager()
        graph = self._make_graph()
        metadata = PipelineMetadata(pipeline_id="p1")

        await manager.validate(graph, metadata, CollectorReporter())
        assert manager.can_run(graph, metadata)

    @pytest.mark.asyncio
    async def test_cannot_run_without_validation(self):
        manager = self._make_manager()
        graph = self._make_graph()
        metadata = PipelineMetadata(pipeline_id="p1")
        assert not manager.can_run(graph, metadata)

    @pytest.mark.asyncio
    async def test_hash_change_invalidates(self):
        manager = self._make_manager()
        graph = self._make_graph()
        metadata = PipelineMetadata(pipeline_id="p1")

        await manager.validate(graph, metadata, CollectorReporter())
        assert manager.can_run(graph, metadata)

        # Modify graph — hash changes
        graph.nodes["sql"] = Node(id="sql", type=NodeType.SQL, config=SqlNodeConfig(expression="SELECT 1"))
        graph.edges.append(Edge(id="e2", from_node_id="src", to_node_id="sql"))

        assert not manager.can_run(graph, metadata)
```

- [ ] **Step 3: Implement PipelineManager**

```python
# backend/src/vonnegut/pipeline/control_plane/pipeline_manager.py
from __future__ import annotations
from datetime import datetime, timezone

from vonnegut.pipeline.control_plane.hashing import compute_pipeline_hash
from vonnegut.pipeline.control_plane.pipeline_state import (
    PipelineMetadata, ValidationStatus,
)
from vonnegut.pipeline.dag.graph import PipelineGraph
from vonnegut.pipeline.dag.plan import LogicalPlan, PlanNode, PlanEdge
from vonnegut.pipeline.dag.node import NodeType
from vonnegut.pipeline.engine.orchestrator import PipelineOrchestrator, TestResult
from vonnegut.pipeline.engine.validator.node_validator import NodeValidator
from vonnegut.pipeline.engine.validator.pipeline_validator import PipelineValidator
from vonnegut.pipeline.engine.optimizer.optimizer import Optimizer
from vonnegut.pipeline.engine.optimizer.rules.base import OptimizationContext
from vonnegut.pipeline.engine.executor.base import NodeExecutor, ExecutorRegistry
from vonnegut.pipeline.reporter.base import Reporter, NullReporter
from vonnegut.pipeline.results import ExecutionResult, ExecutionFailure


class PipelineValidationError(Exception):
    pass


class PipelineManager:
    def __init__(
        self,
        orchestrator: PipelineOrchestrator,
        optimizer: Optimizer,
    ) -> None:
        self._orchestrator = orchestrator
        self._optimizer = optimizer

    @classmethod
    def create_default(cls, stub_executor: NodeExecutor | None = None) -> PipelineManager:
        """Create a PipelineManager with default configuration. Pass stub_executor for testing."""
        from vonnegut.pipeline.engine.validator.rules.code_rules import SyntaxCheckRule, ColumnNameRule
        from vonnegut.pipeline.engine.validator.rules.sql_rules import SqlParseRule

        executor = stub_executor
        validators: dict[NodeType, NodeValidator] = {}
        for nt in NodeType:
            rules = []
            if nt == NodeType.CODE:
                rules = [SyntaxCheckRule(), ColumnNameRule()]
            elif nt == NodeType.SQL:
                rules = [SqlParseRule()]
            validators[nt] = NodeValidator(executor=executor, rules=rules)

        orchestrator = PipelineOrchestrator(
            validator_registry=validators,
            pipeline_validator=PipelineValidator(),
        )
        return cls(orchestrator=orchestrator, optimizer=Optimizer())

    def can_run(self, graph: PipelineGraph, metadata: PipelineMetadata) -> bool:
        current_hash = compute_pipeline_hash(graph.nodes, graph.edges)
        return (
            metadata.validation_status == ValidationStatus.VALID
            and metadata.validated_hash == current_hash
        )

    async def validate(
        self, graph: PipelineGraph, metadata: PipelineMetadata,
        reporter: Reporter | None = None,
    ) -> TestResult:
        reporter = reporter or NullReporter()
        metadata.validation_status = ValidationStatus.VALIDATING

        plan = self._build_logical_plan(graph)
        result = await self._orchestrator.run_test(plan, reporter)

        if result.success:
            metadata.validation_status = ValidationStatus.VALID
            metadata.validated_hash = compute_pipeline_hash(graph.nodes, graph.edges)
        else:
            metadata.validation_status = ValidationStatus.INVALID
            metadata.validated_hash = None
        metadata.last_validated_at = datetime.now(timezone.utc)
        return result

    async def ensure_valid(
        self, graph: PipelineGraph, metadata: PipelineMetadata,
        reporter: Reporter | None = None,
    ) -> None:
        if self.can_run(graph, metadata):
            return
        result = await self.validate(graph, metadata, reporter)
        if not result.success:
            raise PipelineValidationError("Pipeline validation failed")

    async def run(
        self, graph: PipelineGraph, metadata: PipelineMetadata,
        reporter: Reporter | None = None, allow_writes: bool = True,
    ) -> ExecutionResult:
        reporter = reporter or NullReporter()
        await self.ensure_valid(graph, metadata, reporter)

        plan = self._build_logical_plan(graph)
        ctx = OptimizationContext(schemas={})
        exec_plan = self._optimizer.optimize(plan, ctx)

        return await self._orchestrator.run_execute(exec_plan, reporter, allow_writes=allow_writes)

    def _build_logical_plan(self, graph: PipelineGraph) -> LogicalPlan:
        nodes = {
            nid: PlanNode(id=nid, type=n.type, config=n.config)
            for nid, n in graph.nodes.items()
        }
        edges = [
            PlanEdge(from_node_id=e.from_node_id, to_node_id=e.to_node_id, input_name=e.input_name)
            for e in graph.edges
        ]
        return LogicalPlan(nodes=nodes, edges=edges)
```

- [ ] **Step 4: Run tests, verify pass, commit**

```bash
cd backend && uv run pytest tests/pipeline/test_pipeline_manager.py -v
git add backend/src/vonnegut/pipeline/control_plane/ backend/tests/pipeline/test_pipeline_manager.py
git commit -m "feat(pipeline): add PipelineManager with hash-based validation tracking"
```

---

## Chunk 5: API Integration and SSE Reporter

### Task 21: SSE Reporter

**Files:**
- Create: `backend/src/vonnegut/pipeline/reporter/sse_reporter.py`

- [ ] **Step 1: Implement SSEReporter**

```python
# backend/src/vonnegut/pipeline/reporter/sse_reporter.py
from __future__ import annotations
import json
from collections.abc import Callable, Awaitable
from typing import Any

from vonnegut.pipeline.reporter.base import Reporter


class SSEReporter(Reporter):
    """Bridges the new Reporter interface to the existing SSE callback pattern."""

    def __init__(self, callback: Callable[[dict], Awaitable[None]]) -> None:
        self._callback = callback

    async def emit(self, event_type: str, **data: Any) -> None:
        event = {"type": event_type, **data}
        await self._callback(event)
```

- [ ] **Step 2: Commit**

```bash
git add backend/src/vonnegut/pipeline/reporter/sse_reporter.py
git commit -m "feat(pipeline): add SSEReporter bridging to existing callback pattern"
```

---

### Task 22: Wire new pipeline framework into migration test-stream endpoint

**Files:**
- Modify: `backend/src/vonnegut/routers/migrations.py`

This is the integration point. The existing `test_migration_stream` endpoint currently creates a `PipelineEngine` and calls `run_test()`. We'll add a parallel code path using the new framework, gated behind a query parameter `?engine=v2` for gradual rollout.

- [ ] **Step 1: Add v2 test path**

In `routers/migrations.py`, add an import block and a new helper function:

```python
# Add these imports at the top of migrations.py
from vonnegut.pipeline.control_plane.pipeline_manager import PipelineManager
from vonnegut.pipeline.control_plane.pipeline_state import PipelineMetadata
from vonnegut.pipeline.dag.node import Node, NodeType, SourceNodeConfig, SqlNodeConfig, CodeNodeConfig, TargetNodeConfig
from vonnegut.pipeline.dag.edge import Edge
from vonnegut.pipeline.dag.graph import PipelineGraph
from vonnegut.pipeline.reporter.sse_reporter import SSEReporter
from vonnegut.pipeline.engine.executor.code_executor import CodeExecutor
from vonnegut.pipeline.engine.executor.sql_executor import SqlExecutor
from vonnegut.pipeline.engine.executor.source_executor import SourceExecutor
```

Then add a helper to convert existing migration + pipeline_steps to the new DAG model:

```python
def _build_graph_from_migration(mig: dict, steps: list[dict]) -> PipelineGraph:
    """Convert existing migration + steps to PipelineGraph."""
    nodes: dict[str, Node] = {}
    edges: list[Edge] = []

    # Source node
    source_query = mig.get("source_query") or f"SELECT * FROM {mig['source_table']}"
    nodes["source"] = Node(
        id="source",
        type=NodeType.SOURCE,
        config=SourceNodeConfig(
            connection_id=mig["source_connection_id"],
            table=mig["source_table"],
            query=source_query,
        ),
    )

    # Transform nodes
    prev_id = "source"
    for step in steps:
        node_id = step["id"]
        step_type = step["step_type"]
        config_data = json.loads(step["config"]) if isinstance(step["config"], str) else step["config"]

        if step_type == "sql":
            config = SqlNodeConfig(expression=config_data.get("expression", ""))
        elif step_type == "code":
            config = CodeNodeConfig(function_code=config_data.get("function_code", ""))
        elif step_type == "ai" and config_data.get("approved"):
            code = config_data.get("generated_code", "")
            config = CodeNodeConfig(function_code=code)
        else:
            continue  # Skip unapproved AI steps

        nodes[node_id] = Node(id=node_id, type=NodeType(step_type) if step_type != "ai" else NodeType.CODE, config=config)
        edges.append(Edge(id=f"e_{prev_id}_{node_id}", from_node_id=prev_id, to_node_id=node_id))
        prev_id = node_id

    # Target node
    nodes["target"] = Node(
        id="target",
        type=NodeType.TARGET,
        config=TargetNodeConfig(
            connection_id=mig["target_connection_id"],
            table=mig["target_table"],
            truncate=bool(mig.get("truncate_target")),
        ),
    )
    edges.append(Edge(id=f"e_{prev_id}_target", from_node_id=prev_id, to_node_id="target"))

    return PipelineGraph(nodes=nodes, edges=edges)
```

- [ ] **Step 2: Test the integration manually**

Run the backend, create a migration with pipeline steps, and hit the test-stream endpoint. Verify the new graph construction works.

- [ ] **Step 3: Commit**

```bash
git add backend/src/vonnegut/routers/migrations.py
git commit -m "feat(pipeline): add graph builder for migration → DAG conversion"
```

---

### Task 23: Run all tests and verify nothing is broken

- [ ] **Step 1: Run full test suite**

```bash
cd backend && uv run pytest -v
```

Expected: All existing tests pass, all new pipeline tests pass.

- [ ] **Step 2: Fix any failures**

If any existing tests break, fix the import issues or compatibility problems.

- [ ] **Step 3: Final commit**

```bash
git add -A
git commit -m "test: verify full test suite passes with new pipeline framework"
```

---

## Summary

This plan builds the pipeline validation framework in 23 tasks across 5 chunks:

| Chunk | Tasks | What it builds |
|-------|-------|---------------|
| 1: Foundation | 1-7 | Schema types, adapters, result types, DAG models, reporter |
| 2: Executors | 8-11 | NodeExecutor ABC, CodeExecutor, SqlExecutor, Source/TargetExecutor |
| 3: Validation | 12-16 | ValidationRule ABC, code/SQL rules, NodeValidator, PipelineValidator |
| 4: Orchestration | 17-20 | PipelineOrchestrator, Optimizer, pipeline hashing, PipelineManager |
| 5: Integration | 21-23 | SSEReporter, migration→DAG converter, full test verification |

**What's deferred (future tasks, not in this plan):**
- MergeSqlNodesRule optimization (first real optimizer rule)
- Source/Target validation rules (ConnectionRule, SchemaAvailabilityRule) — need real DB
- Database persistence for PipelineMetadata (currently in-memory)
- Full API endpoint migration (replacing old PipelineEngine calls with new PipelineManager)
- Frontend updates to use new validation status / pipeline actions
- `dry_run()` endpoint
