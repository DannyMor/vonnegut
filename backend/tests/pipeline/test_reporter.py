import pytest
from vonnegut.pipeline.reporter.base import Reporter, NullReporter, CollectorReporter


class TestNullReporter:
    @pytest.mark.asyncio
    async def test_emit_does_nothing(self):
        reporter = NullReporter()
        await reporter.emit("test_event", node_id="abc")


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
