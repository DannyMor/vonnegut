import builtins
import datetime
import hashlib
import json
import logging
import math
import re
import time
from collections.abc import Callable, Awaitable

logger = logging.getLogger(__name__)

import polars as pl

from vonnegut.adapters.base import DatabaseAdapter, ColumnSchema
from vonnegut.services.cte_compiler import normalize_cte_name, compile_sql_chain

# Safe builtins for code transforms — no file I/O, no eval/exec, no imports
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

# Modules available inside code transforms
_CODE_TRANSFORM_GLOBALS = {
    "pl": pl,
    "polars": pl,
    "math": math,
    "re": re,
    "json": json,
    "hashlib": hashlib,
    "datetime": datetime,
    "__builtins__": _SAFE_BUILTINS,
}

# Type for the optional progress callback: async fn(event_dict) -> None
ProgressCallback = Callable[[dict], Awaitable[None]] | None


class PipelineEngine:
    """Executes a migration pipeline: SQL chains via CTEs, code via polars/DuckDB."""

    async def run_test(
        self,
        source_adapter: DatabaseAdapter,
        source_query: str,
        steps: list[dict],
        limit: int = 10,
        target_schema: list[ColumnSchema] | None = None,
        on_progress: ProgressCallback = None,
    ) -> dict:
        """Run pipeline on sample data, return per-step results."""
        logger.info("Running pipeline test with %d steps, limit=%d", len(steps), limit)
        results: list[dict] = []

        async def _emit(event: dict):
            if on_progress:
                await on_progress(event)

        # 1. Execute source query with limit
        await _emit({"type": "step_start", "node_id": "source", "name": "Source"})
        t0 = time.monotonic()
        try:
            wrapped = f"SELECT * FROM ({source_query}) AS _src LIMIT {limit}"
            source_rows = await source_adapter.execute(wrapped)
            source_schema = self._infer_schema(source_rows)
            duration_ms = round((time.monotonic() - t0) * 1000)
            result = {
                "node_id": "source",
                "status": "ok",
                "schema": source_schema,
                "sample_data": source_rows,
                "validation": {"valid": True},
            }
            results.append(result)
            await _emit({"type": "step_complete", "node_id": "source", "name": "Source",
                         "status": "ok", "duration_ms": duration_ms,
                         "row_count": len(source_rows), "col_count": len(source_schema)})
        except Exception as e:
            duration_ms = round((time.monotonic() - t0) * 1000)
            results.append({
                "node_id": "source",
                "status": "error",
                "schema": [],
                "sample_data": [],
                "validation": {"valid": False, "errors": [{"type": "execution_error", "message": str(e)}]},
            })
            await _emit({"type": "step_error", "node_id": "source", "name": "Source",
                         "duration_ms": duration_ms, "error": str(e)})
            return {"steps": results}

        # 2. Process steps
        current_rows = source_rows
        sql_chain: list[dict] = []

        pipeline_failed = False

        for step in steps:
            step_type = step["step_type"]

            if step_type == "sql":
                sql_chain.append(step)
                continue

            # Flush SQL chain before code/ai step
            if sql_chain:
                current_rows, step_results = await self._execute_sql_chain(
                    source_adapter, source_query, sql_chain, current_rows, limit,
                    on_progress=on_progress,
                )
                results.extend(step_results)
                sql_chain = []
                if any(r["status"] == "error" for r in step_results):
                    pipeline_failed = True
                    break

            if step_type == "code" or (step_type == "ai" and step.get("config", {}).get("approved")):
                step_name = step.get("name", step_type)
                await _emit({"type": "step_start", "node_id": step.get("id", step_name), "name": step_name})
                t0 = time.monotonic()
                code = step["config"].get("function_code") or step["config"].get("generated_code", "")
                current_rows, step_result = self._execute_code(step, current_rows, code)
                duration_ms = round((time.monotonic() - t0) * 1000)
                results.append(step_result)
                if step_result["status"] == "ok":
                    await _emit({"type": "step_complete", "node_id": step_result["node_id"],
                                 "name": step_name, "status": "ok", "duration_ms": duration_ms,
                                 "row_count": len(current_rows), "col_count": len(step_result["schema"])})
                else:
                    error_msg = step_result["validation"].get("errors", [{}])[0].get("message", "Unknown error")
                    await _emit({"type": "step_error", "node_id": step_result["node_id"],
                                 "name": step_name, "duration_ms": duration_ms, "error": error_msg})
                    pipeline_failed = True
                    break

        # Flush remaining SQL chain (only if pipeline hasn't failed)
        if sql_chain and not pipeline_failed:
            current_rows, step_results = await self._execute_sql_chain(
                source_adapter, source_query, sql_chain, current_rows, limit,
                on_progress=on_progress,
            )
            results.extend(step_results)
            if any(r["status"] == "error" for r in step_results):
                pipeline_failed = True

        # 3. Validate against target schema (only if pipeline hasn't failed)
        if target_schema and not pipeline_failed:
            await _emit({"type": "step_start", "node_id": "target", "name": "Target Validation"})
            t0 = time.monotonic()
            output_schema = self._infer_schema(current_rows)
            validation = self._validate_schema(output_schema, target_schema)
            duration_ms = round((time.monotonic() - t0) * 1000)
            results.append({
                "node_id": "target",
                "status": "ok" if validation["valid"] else "error",
                "schema": [{"name": c.name, "type": c.type} for c in target_schema],
                "sample_data": [],
                "validation": validation,
            })
            if validation["valid"]:
                await _emit({"type": "step_complete", "node_id": "target", "name": "Target Validation",
                             "status": "ok", "duration_ms": duration_ms})
            else:
                errors = validation.get("errors", [])
                msg = "; ".join(e.get("message", "") for e in errors)
                await _emit({"type": "step_error", "node_id": "target", "name": "Target Validation",
                             "duration_ms": duration_ms, "error": msg})

        return {"steps": results}

    async def _execute_sql_chain(
        self,
        adapter: DatabaseAdapter,
        source_query: str,
        sql_steps: list[dict],
        current_rows: list[dict],
        limit: int,
        on_progress: ProgressCallback = None,
    ) -> tuple[list[dict], list[dict]]:
        """Execute a chain of SQL steps as CTEs."""
        step_results = []

        async def _emit(event: dict):
            if on_progress:
                await on_progress(event)

        for step in sql_steps:
            await _emit({"type": "step_start", "node_id": step.get("id", step["name"]),
                         "name": step.get("name", "SQL")})

        chain = [{"name": "source", "position": 0, "expression": source_query}]
        for i, step in enumerate(sql_steps):
            chain.append({
                "name": step["name"],
                "position": i + 1,
                "expression": step["config"]["expression"],
            })

        t0 = time.monotonic()
        try:
            compiled = compile_sql_chain(chain, limit=limit)
            rows = await adapter.execute(compiled)
            schema = self._infer_schema(rows)
            duration_ms = round((time.monotonic() - t0) * 1000)

            for step in sql_steps:
                step_results.append({
                    "node_id": step.get("id", step["name"]),
                    "status": "ok",
                    "schema": schema,
                    "sample_data": rows,
                    "validation": {"valid": True},
                })
                await _emit({"type": "step_complete", "node_id": step.get("id", step["name"]),
                             "name": step.get("name", "SQL"), "status": "ok", "duration_ms": duration_ms,
                             "row_count": len(rows), "col_count": len(schema)})
            return rows, step_results
        except Exception as e:
            duration_ms = round((time.monotonic() - t0) * 1000)
            for step in sql_steps:
                step_results.append({
                    "node_id": step.get("id", step["name"]),
                    "status": "error",
                    "schema": [],
                    "sample_data": [],
                    "validation": {"valid": False, "errors": [{"type": "execution_error", "message": str(e)}]},
                })
                await _emit({"type": "step_error", "node_id": step.get("id", step["name"]),
                             "name": step.get("name", "SQL"), "duration_ms": duration_ms, "error": str(e)})
            return current_rows, step_results

    def _execute_code(
        self,
        step: dict,
        current_rows: list[dict],
        code: str,
    ) -> tuple[list[dict], dict]:
        """Execute a code transform on current rows via polars."""
        step_name = step.get("name", "transform")
        logger.info("Executing code transform '%s' on %d rows", step_name, len(current_rows))
        try:
            df = pl.DataFrame(current_rows)
            local_ns: dict = {}
            compiled = compile(code, f"<{step_name}>", "exec")
            try:
                exec(compiled, {**_CODE_TRANSFORM_GLOBALS}, local_ns)
            except ImportError as e:
                if "__import__" in str(e):
                    raise ValueError(
                        "Import statements are not allowed in code transforms. "
                        "The following modules are pre-injected and available directly: "
                        "pl (polars), math, re, json, hashlib, datetime."
                    ) from None
                raise
            transform_fn = local_ns.get("transform")
            if transform_fn is None:
                raise ValueError("Code must define a 'transform(df)' function")
            result_df = transform_fn(df)
            rows = result_df.to_dicts()
            schema = [{"name": name, "type": str(dtype)} for name, dtype in zip(result_df.columns, result_df.dtypes)]
            return rows, {
                "node_id": step.get("id", step["name"]),
                "status": "ok",
                "schema": schema,
                "sample_data": rows,
                "validation": {"valid": True},
            }
        except Exception as e:
            logger.warning("Code transform '%s' failed: %s", step_name, e)
            return current_rows, {
                "node_id": step.get("id", step["name"]),
                "status": "error",
                "schema": [],
                "sample_data": [],
                "validation": {"valid": False, "errors": [{"type": "execution_error", "message": str(e)}]},
            }

    def _infer_schema(self, rows: list[dict]) -> list[dict]:
        if not rows:
            return []
        first = rows[0]
        return [{"name": key, "type": type(value).__name__ if value is not None else "unknown"} for key, value in first.items()]

    def _validate_schema(
        self, output_schema: list[dict], target_schema: list[ColumnSchema]
    ) -> dict:
        errors = []
        output_cols = {c["name"] for c in output_schema}

        for col in target_schema:
            if col.name not in output_cols:
                errors.append({
                    "type": "missing_column",
                    "column": col.name,
                    "expected": col.type,
                    "actual": None,
                    "message": f"Column '{col.name}' not found in pipeline output",
                })

        return {"valid": len(errors) == 0, "errors": errors}
