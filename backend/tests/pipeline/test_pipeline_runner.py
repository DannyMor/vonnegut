import json
import pytest
from vonnegut.pipeline.pipeline_runner import PipelineRunner
from tests.pipeline.helpers import InMemoryDatabaseAdapter


@pytest.fixture
def adapter() -> InMemoryDatabaseAdapter:
    db = InMemoryDatabaseAdapter()
    db.seed_table("users", [
        {"id": 1, "name": "alice", "age": 30},
        {"id": 2, "name": "bob", "age": 25},
        {"id": 3, "name": "charlie", "age": 35},
    ])
    return db


class TestPipelineRunnerTest:
    @pytest.mark.asyncio
    async def test_source_only(self, adapter):
        runner = PipelineRunner()
        result = await runner.run_test(
            source_adapter=adapter,
            source_query="SELECT * FROM users",
            steps=[],
            limit=10,
        )
        assert len(result["steps"]) == 2  # source + target
        source_step = result["steps"][0]
        assert source_step["node_id"] == "source"
        assert source_step["status"] == "ok"
        assert len(source_step["sample_data"]) == 3
        assert len(source_step["schema"]) == 3

    @pytest.mark.asyncio
    async def test_with_sql_step(self, adapter):
        runner = PipelineRunner()
        result = await runner.run_test(
            source_adapter=adapter,
            source_query="SELECT * FROM users",
            steps=[{
                "id": "s1",
                "step_type": "sql",
                "name": "Filter Adults",
                "config": json.dumps({"expression": "SELECT * FROM {prev} WHERE age > 28"}),
            }],
            limit=10,
        )
        steps = result["steps"]
        assert len(steps) == 3  # source + sql + target
        sql_step = steps[1]
        assert sql_step["node_id"] == "s1"
        assert sql_step["status"] == "ok"
        assert len(sql_step["sample_data"]) == 2  # alice(30) + charlie(35)

    @pytest.mark.asyncio
    async def test_with_code_step(self, adapter):
        runner = PipelineRunner()
        result = await runner.run_test(
            source_adapter=adapter,
            source_query="SELECT * FROM users",
            steps=[{
                "id": "c1",
                "step_type": "code",
                "name": "Double Age",
                "config": json.dumps({
                    "function_code": "def transform(df):\n    return df.with_columns(pl.col('age') * 2)\n",
                }),
            }],
            limit=10,
        )
        code_step = result["steps"][1]
        assert code_step["status"] == "ok"
        ages = [r["age"] for r in code_step["sample_data"]]
        assert ages == [60, 50, 70]

    @pytest.mark.asyncio
    async def test_error_stops_pipeline(self, adapter):
        runner = PipelineRunner()
        result = await runner.run_test(
            source_adapter=adapter,
            source_query="SELECT * FROM users",
            steps=[{
                "id": "bad",
                "step_type": "code",
                "name": "Bad Code",
                "config": json.dumps({
                    "function_code": "def transform(df):\n    raise RuntimeError('boom')\n",
                }),
            }],
            limit=10,
        )
        # source succeeded, code failed, target not reached
        assert len(result["steps"]) == 2
        assert result["steps"][0]["status"] == "ok"
        assert result["steps"][1]["status"] == "error"
        assert "boom" in result["steps"][1]["validation"]["errors"][0]["message"]

    @pytest.mark.asyncio
    async def test_emits_sse_events(self, adapter):
        events: list[dict] = []

        async def on_event(event: dict):
            events.append(event)

        runner = PipelineRunner()
        await runner.run_test(
            source_adapter=adapter,
            source_query="SELECT * FROM users",
            steps=[{
                "id": "s1",
                "step_type": "sql",
                "name": "Filter",
                "config": json.dumps({"expression": "SELECT * FROM {prev}"}),
            }],
            limit=10,
            on_progress=on_event,
        )
        types = [e["type"] for e in events]
        assert "step_start" in types
        assert "step_complete" in types
        # Should have start+complete for source and sql (target emits only complete)
        starts = [e for e in events if e["type"] == "step_start"]
        completes = [e for e in events if e["type"] == "step_complete"]
        assert len(starts) >= 2  # source + sql
        assert len(completes) >= 2

    @pytest.mark.asyncio
    async def test_multi_step_pipeline(self, adapter):
        runner = PipelineRunner()
        result = await runner.run_test(
            source_adapter=adapter,
            source_query="SELECT * FROM users",
            steps=[
                {
                    "id": "s1", "step_type": "sql", "name": "Filter",
                    "config": json.dumps({"expression": "SELECT * FROM {prev} WHERE age > 20"}),
                },
                {
                    "id": "c1", "step_type": "code", "name": "Add Column",
                    "config": json.dumps({
                        "function_code": "def transform(df):\n    return df.with_columns(pl.lit('active').alias('status'))\n",
                    }),
                },
            ],
            limit=10,
        )
        assert len(result["steps"]) == 4  # source + sql + code + target
        assert all(s["status"] == "ok" for s in result["steps"])
        code_step = result["steps"][2]
        assert "status" in code_step["sample_data"][0]
