import logging

import pytest
from vonnegut.pipeline.reporter.base import Reporter, NullReporter, CollectorReporter
from vonnegut.pipeline.reporter.log_reporter import LogReporter


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


class TestLogReporter:
    @pytest.mark.asyncio
    async def test_logs_info_for_normal_events(self, caplog):
        reporter = LogReporter()
        with caplog.at_level(logging.INFO, logger="vonnegut.pipeline"):
            await reporter.emit("node_start", node_id="src")
        assert any("node_start" in r.message and "node_id=src" in r.message for r in caplog.records)
        assert all(r.levelno == logging.INFO for r in caplog.records)

    @pytest.mark.asyncio
    async def test_logs_error_for_failure_events(self, caplog):
        reporter = LogReporter()
        with caplog.at_level(logging.ERROR, logger="vonnegut.pipeline"):
            await reporter.emit("pipeline_failed", node_id="step1")
        assert any("pipeline_failed" in r.message for r in caplog.records)
        assert any(r.levelno == logging.ERROR for r in caplog.records)

    @pytest.mark.asyncio
    async def test_logs_error_for_node_error(self, caplog):
        reporter = LogReporter()
        with caplog.at_level(logging.ERROR, logger="vonnegut.pipeline"):
            await reporter.emit("node_error", node_id="x", error="boom")
        assert any(r.levelno == logging.ERROR for r in caplog.records)
        assert any("error=boom" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_custom_logger(self, caplog):
        custom = logging.getLogger("my.custom.logger")
        reporter = LogReporter(logger=custom)
        with caplog.at_level(logging.INFO, logger="my.custom.logger"):
            await reporter.emit("node_complete", node_id="a")
        assert any("node_complete" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_emit_with_no_data(self, caplog):
        reporter = LogReporter()
        with caplog.at_level(logging.INFO, logger="vonnegut.pipeline"):
            await reporter.emit("done")
        assert any("done" in r.message for r in caplog.records)
