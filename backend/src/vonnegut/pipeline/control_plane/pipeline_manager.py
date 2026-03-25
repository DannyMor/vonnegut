from __future__ import annotations
from datetime import datetime, timezone

from vonnegut.pipeline.control_plane.hashing import compute_pipeline_hash
from vonnegut.pipeline.control_plane.pipeline_state import (
    NodeMetadata,
    PipelineMetadata,
    ValidationStatus,
)
from vonnegut.pipeline.dag.graph import PipelineGraph
from vonnegut.pipeline.dag.plan import LogicalPlan, PlanNode, PlanEdge
from vonnegut.pipeline.dag.node import NodeType
from vonnegut.pipeline.engine.orchestrator import PipelineOrchestrator, TestResult
from vonnegut.pipeline.engine.optimizer.optimizer import Optimizer
from vonnegut.pipeline.engine.optimizer.rules.base import OptimizationContext
from vonnegut.pipeline.reporter.base import Reporter, NullReporter
from vonnegut.pipeline.results import ExecutionResult


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

    def can_run(self, graph: PipelineGraph, metadata: PipelineMetadata) -> bool:
        current_hash = compute_pipeline_hash(graph.nodes, graph.edges)
        return (
            metadata.validation_status == ValidationStatus.VALID
            and metadata.validated_hash == current_hash
        )

    async def validate(
        self,
        graph: PipelineGraph,
        metadata: PipelineMetadata,
        reporter: Reporter | None = None,
    ) -> TestResult:
        reporter = reporter or NullReporter()
        metadata.validation_status = ValidationStatus.VALIDATING

        plan = self._build_logical_plan(graph)
        result = await self._orchestrator.run_test(plan, reporter)

        if result.success:
            metadata.validation_status = ValidationStatus.VALID
            metadata.validated_hash = compute_pipeline_hash(graph.nodes, graph.edges)
            for nid, schema in result.node_schemas.items():
                if nid not in metadata.node_metadata:
                    metadata.node_metadata[nid] = NodeMetadata(node_id=nid)
                metadata.node_metadata[nid].output_schema = schema
        else:
            metadata.validation_status = ValidationStatus.INVALID
            metadata.validated_hash = None
        metadata.last_validated_at = datetime.now(timezone.utc)
        return result

    async def ensure_valid(
        self,
        graph: PipelineGraph,
        metadata: PipelineMetadata,
        reporter: Reporter | None = None,
    ) -> None:
        if self.can_run(graph, metadata):
            return
        result = await self.validate(graph, metadata, reporter)
        if not result.success:
            raise PipelineValidationError("Pipeline validation failed")

    async def run(
        self,
        graph: PipelineGraph,
        metadata: PipelineMetadata,
        reporter: Reporter | None = None,
        allow_writes: bool = True,
    ) -> ExecutionResult:
        reporter = reporter or NullReporter()
        await self.ensure_valid(graph, metadata, reporter)

        plan = self._build_logical_plan(graph)
        node_schemas = {
            nid: nm.output_schema
            for nid, nm in metadata.node_metadata.items()
            if nm.output_schema is not None
        }
        ctx = OptimizationContext(node_schemas=node_schemas)
        exec_plan = self._optimizer.optimize(plan, ctx)

        return await self._orchestrator.run_execute(
            exec_plan, reporter, allow_writes=allow_writes,
        )

    def _build_logical_plan(self, graph: PipelineGraph) -> LogicalPlan:
        nodes = {
            nid: PlanNode(id=nid, type=n.type, config=n.config)
            for nid, n in graph.nodes.items()
        }
        edges = [
            PlanEdge(
                from_node_id=e.from_node_id,
                to_node_id=e.to_node_id,
                input_name=e.input_name,
            )
            for e in graph.edges
        ]
        return LogicalPlan(nodes=nodes, edges=edges)
