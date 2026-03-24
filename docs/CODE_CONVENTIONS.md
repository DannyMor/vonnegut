# Code Conventions

Living document — updated as patterns emerge.

## Type System

- **Fully typed function signatures** — every parameter and return type annotated
- **Strict type annotations** — always pick the most accurate type. Avoid `Any` unless there is genuinely no other option
- **Discriminated unions over boolean flags** — use `Union[Success, Failure]` with pattern matching, not `Result(success=bool, errors=Optional[...])`
- **Pattern matching over isinstance** — prefer `match ... case` for branching on union types (Python 3.10+)
- **Pydantic models for data** — use pydantic `BaseModel` for all data models that cross boundaries (API, persistence, config)
- **Dataclasses for internal types** — use `@dataclass` for internal value types that don't need validation

## Discriminated Union Pattern

```python
from dataclasses import dataclass

@dataclass
class ValidationSuccess:
    output_schema: Schema
    output_data: pa.Table

@dataclass
class ValidationFailure:
    errors: list[ValidationError]

ValidationResult = ValidationSuccess | ValidationFailure

# Usage — pattern matching, not isinstance
match result:
    case ValidationSuccess(output_schema=schema):
        # proceed
    case ValidationFailure(errors=errors):
        # handle errors
```

This pattern applies to all result types in the system — validation, execution, optimization.

## Naming

- **Modules** — snake_case, descriptive (`pipeline_manager.py`, not `manager.py`)
- **Classes** — PascalCase, noun-based (`CodeExecutor`, `SchemaCompatibilityRule`)
- **Functions** — snake_case, verb-based (`validate_node`, `build_execution_context`)
- **Constants** — UPPER_SNAKE_CASE

## Architecture

- **Nodes are pure configuration** — no behavior on node objects
- **Executors are stateless** — all context passed via `ExecutionContext`
- **Rules are composable** — validation rules and optimization rules are independent, focused units
- **Interfaces before implementations** — define ABCs, then implement. Allows swapping and testing
- **Minimize mocks in tests** — use real implementations and in-memory test doubles that implement the same interface. Only mock when truly necessary (external APIs)

## Testing

- **Interfaces over mocks** — define abstract interfaces (ABCs), then build real in-memory implementations for tests. Test doubles should implement the same interface as production code
- **Real implementations first** — prefer real implementations over mocks whenever possible. Only mock when there is no realistic alternative (external APIs, network calls)
- **Test doubles behave like the real system** — test implementations should exercise real logic, not just return hardcoded values. An in-memory database adapter should parse and execute queries, not return canned results
- **Integration-style tests** — test components working together through their interfaces. A test for `PipelineOrchestrator` should use real executors with real data, not stubs
- **If a test is hard to write, the design is wrong** — difficulty writing tests signals that the code under test has too many dependencies, unclear boundaries, or hidden coupling. Fix the design, not the test
- **Don't test the language or tooling** — no tests for "does dataclass work" or "does pydantic validate." Test your logic and behavior, not framework guarantees
- **No low-value tests** — skip tests that only assert constructor assignment or trivial getters. Every test should validate meaningful behavior
- **Specific assertions** — assert on the exact values and structure you expect, not just "it didn't throw"

## Style

- No unnecessary comments — code should be self-documenting
- No docstrings on obvious methods — add them only when the behavior isn't clear from the signature
- Prefer raising specific exceptions over returning error codes
- Prefer early returns over nested conditionals
