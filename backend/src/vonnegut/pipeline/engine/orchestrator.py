from __future__ import annotations
from dataclasses import dataclass, field

import pyarrow as pa

from vonnegut.pipeline.dag.node import Node, NodeType
from vonnegut.pipeline.dag.edge import Edge
from vonnegut.pipeline.dag.plan import (
    LogicalPlan,
    ExecutionPlan,
    ExecutionContext,
    PlanEdge,
)
from vonnegut.pipeline.dag.graph import (
    topological_sort,
    collect_inputs,
    get_incoming_edges,
)
from vonnegut.pipeline.engine.executor.base import ExecutorRegistry
from vonnegut.pipeline.engine.validator.node_validator import NodeValidator
from vonnegut.pipeline.engine.validator.pipeline_validator import PipelineValidator
from vonnegut.pipeline.reporter.base import Reporter, NullReporter
from vonnegut.pipeline.results import (
    ValidationSuccess,
    ValidationFailure,
    NodeValidationResult,
    ExecutionSuccess,
    ExecutionFailure,
    ExecutionResult,
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
        connection_info: dict | None = None,
    ) -> None:
        self._validators = validator_registry
        self._pipeline_validator = pipeline_validator
        self._executors = executor_registry
        self._connection_info = connection_info or {}

    async def run_test(
        self,
        plan: LogicalPlan,
        reporter: Reporter | None = None,
    ) -> TestResult:
        reporter = reporter or NullReporter()

        nodes_for_sort = {
            nid: Node(id=nid, type=pn.type, config=pn.config)
            for nid, pn in plan.nodes.items()
        }
        edges_for_sort = [
            Edge(
                id=f"e_{i}",
                from_node_id=pe.from_node_id,
                to_node_id=pe.to_node_id,
                input_name=pe.input_name,
            )
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
                connection_info=self._connection_info,
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
                                PlanEdge(
                                    from_node_id=edge.from_node_id,
                                    to_node_id=edge.to_node_id,
                                    input_name=edge.input_name,
                                ),
                                plan.nodes[edge.from_node_id],
                                plan_node,
                                from_schema,
                                edge.input_name,
                            )
                            if any(
                                c.status == CheckStatus.FAILED for c in edge_checks
                            ):
                                await reporter.emit(
                                    "pipeline_failed", node_id=node_id,
                                )
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
        edges_for_collect = [
            Edge(
                id=f"e_{i}",
                from_node_id=pe.from_node_id,
                to_node_id=pe.to_node_id,
                input_name=pe.input_name,
            )
            for i, pe in enumerate(plan.edges)
        ]

        for exec_context in plan.contexts:
            if exec_context.node_type == NodeType.TARGET and not allow_writes:
                await reporter.emit(
                    "node_skipped",
                    node_id=exec_context.node_id,
                    reason="dry-run",
                )
                continue

            inputs = collect_inputs(
                exec_context.node_id, edges_for_collect, node_outputs,
            )
            executor = self._executors.get(exec_context.node_type)

            await reporter.emit("node_start", node_id=exec_context.node_id)
            try:
                output = await executor.execute(exec_context, inputs)
                await reporter.emit(
                    "node_complete", node_id=exec_context.node_id,
                )
                node_outputs[exec_context.node_id] = output
            except Exception as e:
                await reporter.emit(
                    "node_failed",
                    node_id=exec_context.node_id,
                    error=str(e),
                )
                return ExecutionFailure(
                    node_id=exec_context.node_id, error=str(e),
                )

        return ExecutionSuccess()
