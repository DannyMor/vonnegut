import pytest
from vonnegut.pipeline.control_plane.pipeline_manager import PipelineManager
from vonnegut.pipeline.control_plane.pipeline_state import (
    PipelineMetadata,
    ValidationStatus,
)
from vonnegut.pipeline.dag.node import (
    Node,
    NodeType,
    SourceNodeConfig,
    SqlNodeConfig,
    TargetNodeConfig,
)
from vonnegut.pipeline.dag.edge import Edge
from vonnegut.pipeline.dag.graph import PipelineGraph
from vonnegut.pipeline.engine.executor.source_executor import SourceExecutor
from vonnegut.pipeline.engine.executor.sql_executor import SqlExecutor
from vonnegut.pipeline.engine.executor.code_executor import CodeExecutor
from vonnegut.pipeline.engine.executor.target_executor import TargetExecutor
from vonnegut.pipeline.engine.validator.node_validator import NodeValidator
from vonnegut.pipeline.engine.validator.pipeline_validator import PipelineValidator
from vonnegut.pipeline.engine.optimizer.optimizer import Optimizer
from vonnegut.pipeline.engine.orchestrator import PipelineOrchestrator
from vonnegut.pipeline.reporter.base import CollectorReporter
from tests.pipeline.helpers import InMemoryDatabaseAdapter


@pytest.fixture
def source_adapter() -> InMemoryDatabaseAdapter:
    adapter = InMemoryDatabaseAdapter()
    adapter.seed_table(
        "t1",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )
    return adapter


def _make_graph() -> PipelineGraph:
    return PipelineGraph(
        nodes={
            "src": Node(
                id="src",
                type=NodeType.SOURCE,
                config=SourceNodeConfig(connection_id="c1", table="t1"),
            ),
            "tgt": Node(
                id="tgt",
                type=NodeType.TARGET,
                config=TargetNodeConfig(
                    connection_id="c2", table="t2", truncate=False,
                ),
            ),
        },
        edges=[Edge(id="e1", from_node_id="src", to_node_id="tgt")],
    )


def _make_manager(source_adapter: InMemoryDatabaseAdapter) -> PipelineManager:
    validators = {
        NodeType.SOURCE: NodeValidator(
            executor=SourceExecutor(adapter_factory=None), rules=[],
        ),
        NodeType.SQL: NodeValidator(executor=SqlExecutor(), rules=[]),
        NodeType.CODE: NodeValidator(executor=CodeExecutor(), rules=[]),
        NodeType.TARGET: NodeValidator(executor=TargetExecutor(), rules=[]),
    }
    orchestrator = PipelineOrchestrator(
        validator_registry=validators,
        pipeline_validator=PipelineValidator(),
        connection_info={"adapter": source_adapter},
    )
    return PipelineManager(orchestrator=orchestrator, optimizer=Optimizer())


class TestPipelineManager:
    @pytest.mark.asyncio
    async def test_validate_sets_status_valid(self, source_adapter):
        manager = _make_manager(source_adapter)
        graph = _make_graph()
        metadata = PipelineMetadata(pipeline_id="p1")
        reporter = CollectorReporter()

        result = await manager.validate(graph, metadata, reporter)

        assert result.success
        assert metadata.validation_status == ValidationStatus.VALID
        assert metadata.validated_hash is not None

    @pytest.mark.asyncio
    async def test_can_run_after_validation(self, source_adapter):
        manager = _make_manager(source_adapter)
        graph = _make_graph()
        metadata = PipelineMetadata(pipeline_id="p1")

        await manager.validate(graph, metadata, CollectorReporter())
        assert manager.can_run(graph, metadata)

    @pytest.mark.asyncio
    async def test_cannot_run_without_validation(self, source_adapter):
        manager = _make_manager(source_adapter)
        graph = _make_graph()
        metadata = PipelineMetadata(pipeline_id="p1")
        assert not manager.can_run(graph, metadata)

    @pytest.mark.asyncio
    async def test_hash_change_invalidates(self, source_adapter):
        manager = _make_manager(source_adapter)
        graph = _make_graph()
        metadata = PipelineMetadata(pipeline_id="p1")

        await manager.validate(graph, metadata, CollectorReporter())
        assert manager.can_run(graph, metadata)

        # Modify graph — hash changes
        graph.nodes["sql"] = Node(
            id="sql",
            type=NodeType.SQL,
            config=SqlNodeConfig(expression="SELECT 1"),
        )
        graph.edges.append(
            Edge(id="e2", from_node_id="src", to_node_id="sql"),
        )

        assert not manager.can_run(graph, metadata)
