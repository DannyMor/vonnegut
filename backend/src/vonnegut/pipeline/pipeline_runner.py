"""High-level pipeline runner that bridges the new framework to the existing API contract.

Produces SSE events and result dicts matching the frontend's PipelineTestResult shape.
"""
from __future__ import annotations
import json
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

import pyarrow as pa

from vonnegut.adapters.base import DatabaseAdapter, ColumnSchema
from vonnegut.pipeline.dag.node import Node, NodeType, SourceNodeConfig
from vonnegut.pipeline.dag.edge import Edge
from vonnegut.pipeline.dag.graph import PipelineGraph, topological_sort, collect_inputs
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.engine.executor.code_executor import CodeExecutor
from vonnegut.pipeline.engine.executor.source_executor import SourceExecutor
from vonnegut.pipeline.engine.executor.sql_executor import SqlExecutor
from vonnegut.pipeline.engine.executor.target_executor import TargetExecutor
from vonnegut.pipeline.engine.executor.base import NodeExecutor
from vonnegut.pipeline.graph_builder import build_graph_from_migration

ProgressCallback = Callable[[dict], Awaitable[None]] | None


def _arrow_to_schema(table: pa.Table) -> list[dict]:
    """Convert Arrow schema to the frontend ColumnDef[] format."""
    return [
        {"name": field.name, "type": str(field.type)}
        for field in table.schema
    ]


def _arrow_to_rows(table: pa.Table) -> list[dict]:
    """Convert Arrow table to list of dicts for JSON serialization."""
    return table.to_pylist()


def _build_node_names(graph: PipelineGraph, steps: list[dict]) -> dict[str, str]:
    """Build a node_id → display name mapping."""
    names: dict[str, str] = {"source": "Source", "target": "Target Validation"}
    step_by_id = {s["id"]: s for s in steps}
    for node_id in graph.nodes:
        if node_id in names:
            continue
        step = step_by_id.get(node_id)
        if step:
            names[node_id] = step.get("name", step.get("step_type", node_id))
        else:
            names[node_id] = node_id
    return names


class PipelineRunner:
    """Runs a pipeline using the new modular framework,
    emitting events compatible with the existing SSE frontend."""

    def __init__(self) -> None:
        self._executors: dict[NodeType, NodeExecutor] = {
            NodeType.SOURCE: SourceExecutor(adapter_factory=None),
            NodeType.SQL: SqlExecutor(),
            NodeType.CODE: CodeExecutor(),
            NodeType.TARGET: TargetExecutor(),
        }

    async def run_test(
        self,
        source_adapter: DatabaseAdapter,
        source_query: str,
        steps: list[dict],
        limit: int = 10,
        target_schema: list[ColumnSchema] | None = None,
        on_progress: ProgressCallback = None,
    ) -> dict:
        """Run pipeline test, returning PipelineTestResult-shaped dict."""
        mig = self._build_migration_dict(source_query, steps)
        graph = build_graph_from_migration(mig, steps)
        node_names = _build_node_names(graph, steps)
        connection_info = {"adapter": source_adapter, "limit": limit}

        # Build execution order
        nodes_for_sort = {
            nid: Node(id=nid, type=n.type, config=n.config)
            for nid, n in graph.nodes.items()
        }
        edges = graph.edges
        order = topological_sort(nodes_for_sort, edges)

        results: list[dict] = []
        node_outputs: dict[str, pa.Table] = {}
        pipeline_failed = False

        for node_id in order:
            node = graph.nodes[node_id]
            name = node_names.get(node_id, node_id)
            executor = self._executors.get(node.type)

            if executor is None:
                continue

            # Build inputs from upstream outputs
            inputs = collect_inputs(node_id, edges, node_outputs)

            # Build execution context
            ctx = ExecutionContext(
                node_id=node_id,
                node_type=node.type,
                config=node.config,
                connection_info=connection_info if node.type in (NodeType.SOURCE, NodeType.TARGET) else None,
            )

            # For target nodes, do schema validation instead of execution
            if node.type == NodeType.TARGET:
                step_result = self._validate_target(
                    node_id, name, inputs, target_schema,
                )
                results.append(step_result)
                if on_progress:
                    if step_result["status"] == "ok":
                        await on_progress({
                            "type": "step_complete", "node_id": node_id,
                            "name": name, "status": "ok", "duration_ms": 0,
                        })
                    else:
                        errors = step_result["validation"].get("errors", [])
                        msg = "; ".join(e.get("message", "") for e in errors)
                        await on_progress({
                            "type": "step_error", "node_id": node_id,
                            "name": name, "duration_ms": 0, "error": msg,
                        })
                continue

            # Emit step_start
            if on_progress:
                await on_progress({
                    "type": "step_start", "node_id": node_id, "name": name,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

            t0 = time.monotonic()
            try:
                output = await executor.execute(ctx, inputs)
                duration_ms = round((time.monotonic() - t0) * 1000)

                schema = _arrow_to_schema(output)
                sample_data = _arrow_to_rows(output)
                node_outputs[node_id] = output

                results.append({
                    "node_id": node_id,
                    "status": "ok",
                    "schema": schema,
                    "sample_data": sample_data,
                    "validation": {"valid": True},
                })

                if on_progress:
                    await on_progress({
                        "type": "step_complete", "node_id": node_id,
                        "name": name, "status": "ok",
                        "duration_ms": duration_ms,
                        "row_count": len(sample_data),
                        "col_count": len(schema),
                    })

            except Exception as e:
                duration_ms = round((time.monotonic() - t0) * 1000)
                results.append({
                    "node_id": node_id,
                    "status": "error",
                    "schema": [],
                    "sample_data": [],
                    "validation": {
                        "valid": False,
                        "errors": [{"type": "execution_error", "message": str(e)}],
                    },
                })

                if on_progress:
                    await on_progress({
                        "type": "step_error", "node_id": node_id,
                        "name": name, "duration_ms": duration_ms,
                        "error": str(e),
                    })

                pipeline_failed = True
                break

        return {"steps": results}

    def _validate_target(
        self,
        node_id: str,
        name: str,
        inputs: dict[str, pa.Table],
        target_schema: list[ColumnSchema] | None,
    ) -> dict:
        """Validate pipeline output against target schema."""
        if not target_schema:
            return {
                "node_id": node_id,
                "status": "ok",
                "schema": [],
                "sample_data": [],
                "validation": {"valid": True},
            }

        input_table = inputs.get("default")
        if input_table is None:
            return {
                "node_id": node_id,
                "status": "ok",
                "schema": [{"name": c.name, "type": c.type} for c in target_schema],
                "sample_data": [],
                "validation": {"valid": True},
            }

        output_cols = set(input_table.column_names)
        errors = []
        for col in target_schema:
            if col.name not in output_cols:
                errors.append({
                    "type": "missing_column",
                    "column": col.name,
                    "expected": col.type,
                    "actual": None,
                    "message": f"Column '{col.name}' not found in pipeline output",
                })

        return {
            "node_id": node_id,
            "status": "ok" if not errors else "error",
            "schema": [{"name": c.name, "type": c.type} for c in target_schema],
            "sample_data": [],
            "validation": {"valid": len(errors) == 0, "errors": errors},
        }

    def _build_migration_dict(
        self, source_query: str, steps: list[dict],
    ) -> dict:
        """Build a minimal migration dict for graph_builder."""
        return {
            "source_connection_id": "_test",
            "source_table": "_source",
            "source_query": source_query,
            "target_connection_id": "_test",
            "target_table": "_target",
            "truncate_target": False,
        }
