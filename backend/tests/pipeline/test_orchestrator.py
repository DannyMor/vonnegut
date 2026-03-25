import pyarrow as pa
import pytest
from vonnegut.pipeline.engine.orchestrator import PipelineOrchestrator
from vonnegut.pipeline.engine.executor.base import NodeExecutor
from vonnegut.pipeline.engine.executor.code_executor import CodeExecutor
from vonnegut.pipeline.engine.executor.sql_executor import SqlExecutor
from vonnegut.pipeline.engine.executor.source_executor import SourceExecutor
from vonnegut.pipeline.engine.executor.target_executor import TargetExecutor
from vonnegut.pipeline.engine.validator.node_validator import NodeValidator
from vonnegut.pipeline.engine.validator.pipeline_validator import PipelineValidator
from vonnegut.pipeline.dag.node import NodeType, SourceNodeConfig, SqlNodeConfig, TargetNodeConfig
from vonnegut.pipeline.dag.plan import LogicalPlan, PlanNode, PlanEdge, ExecutionContext
from vonnegut.pipeline.results import ValidationSuccess, ValidationFailure
from vonnegut.pipeline.reporter.base import CollectorReporter
from tests.pipeline.helpers import InMemoryDatabaseAdapter


class FailingExecutor(NodeExecutor):
    async def execute(
        self, context: ExecutionContext, inputs: dict[str, pa.Table],
    ) -> pa.Table:
        raise RuntimeError("Executor failed")


@pytest.fixture
def source_adapter() -> InMemoryDatabaseAdapter:
    adapter = InMemoryDatabaseAdapter()
    adapter.seed_table(
        "t1",
        [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
            {"id": 3, "name": "charlie"},
        ],
    )
    return adapter


def _make_linear_plan() -> LogicalPlan:
    return LogicalPlan(
        nodes={
            "src": PlanNode(
                id="src",
                type=NodeType.SOURCE,
                config=SourceNodeConfig(connection_id="c1", table="t1"),
            ),
            "sql": PlanNode(
                id="sql",
                type=NodeType.SQL,
                config=SqlNodeConfig(expression="SELECT * FROM {prev}"),
            ),
            "tgt": PlanNode(
                id="tgt",
                type=NodeType.TARGET,
                config=TargetNodeConfig(
                    connection_id="c2", table="t2", truncate=False,
                ),
            ),
        },
        edges=[
            PlanEdge(from_node_id="src", to_node_id="sql"),
            PlanEdge(from_node_id="sql", to_node_id="tgt"),
        ],
    )


def _make_orchestrator(
    source_adapter: InMemoryDatabaseAdapter,
    sql_executor: NodeExecutor | None = None,
) -> PipelineOrchestrator:
    validators = {
        NodeType.SOURCE: NodeValidator(
            executor=SourceExecutor(adapter_factory=None), rules=[],
        ),
        NodeType.SQL: NodeValidator(
            executor=sql_executor or SqlExecutor(), rules=[],
        ),
        NodeType.CODE: NodeValidator(executor=CodeExecutor(), rules=[]),
        NodeType.TARGET: NodeValidator(executor=TargetExecutor(), rules=[]),
    }
    return PipelineOrchestrator(
        validator_registry=validators,
        pipeline_validator=PipelineValidator(),
        connection_info={"adapter": source_adapter},
    )


class TestOrchestratorTestMode:
    @pytest.mark.asyncio
    async def test_successful_linear_pipeline(self, source_adapter):
        orchestrator = _make_orchestrator(source_adapter)
        reporter = CollectorReporter()
        result = await orchestrator.run_test(_make_linear_plan(), reporter)

        assert result.success
        assert len(result.node_results) == 3
        starts = reporter.events_of_type("node_start")
        assert len(starts) == 3

    @pytest.mark.asyncio
    async def test_stops_on_node_failure(self, source_adapter):
        orchestrator = _make_orchestrator(
            source_adapter, sql_executor=FailingExecutor(),
        )
        reporter = CollectorReporter()
        result = await orchestrator.run_test(_make_linear_plan(), reporter)

        assert not result.success
        assert len(result.node_results) == 2
        failed_events = reporter.events_of_type("pipeline_failed")
        assert len(failed_events) == 1
