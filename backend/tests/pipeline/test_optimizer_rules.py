import pytest
from vonnegut.pipeline.dag.node import NodeType, SourceNodeConfig, SqlNodeConfig, TargetNodeConfig
from vonnegut.pipeline.dag.plan import LogicalPlan, PlanNode, PlanEdge
from vonnegut.pipeline.engine.optimizer.rules.base import OptimizationContext
from vonnegut.pipeline.engine.optimizer.rules.noop_removal import NoOpRemovalRule
from vonnegut.pipeline.engine.optimizer.rules.merge_sql import MergeSqlNodesRule


def _src_node(nid="src"):
    return PlanNode(id=nid, type=NodeType.SOURCE, config=SourceNodeConfig(connection_id="c1", table="t1"))


def _sql_node(nid, expr):
    return PlanNode(id=nid, type=NodeType.SQL, config=SqlNodeConfig(expression=expr))


def _tgt_node(nid="tgt"):
    return PlanNode(id=nid, type=NodeType.TARGET, config=TargetNodeConfig(connection_id="c2", table="t2", truncate=False))


_CTX = OptimizationContext()


class TestNoOpRemovalRule:
    def test_removes_select_star_from_prev(self):
        plan = LogicalPlan(
            nodes={
                "src": _src_node(),
                "noop": _sql_node("noop", "SELECT * FROM {prev}"),
                "tgt": _tgt_node(),
            },
            edges=[
                PlanEdge(from_node_id="src", to_node_id="noop"),
                PlanEdge(from_node_id="noop", to_node_id="tgt"),
            ],
        )
        result = NoOpRemovalRule().apply(plan, _CTX)
        assert "noop" not in result.nodes
        assert len(result.nodes) == 2
        # Edge should be src → tgt
        assert len(result.edges) == 1
        assert result.edges[0].from_node_id == "src"
        assert result.edges[0].to_node_id == "tgt"

    def test_preserves_non_noop_sql(self):
        plan = LogicalPlan(
            nodes={
                "src": _src_node(),
                "sql": _sql_node("sql", "SELECT id, UPPER(name) AS name FROM {prev}"),
                "tgt": _tgt_node(),
            },
            edges=[
                PlanEdge(from_node_id="src", to_node_id="sql"),
                PlanEdge(from_node_id="sql", to_node_id="tgt"),
            ],
        )
        result = NoOpRemovalRule().apply(plan, _CTX)
        assert "sql" in result.nodes
        assert len(result.nodes) == 3

    def test_no_changes_when_no_noops(self):
        plan = LogicalPlan(
            nodes={"src": _src_node(), "tgt": _tgt_node()},
            edges=[PlanEdge(from_node_id="src", to_node_id="tgt")],
        )
        result = NoOpRemovalRule().apply(plan, _CTX)
        assert result is plan  # No changes, returns same plan

    def test_case_insensitive(self):
        plan = LogicalPlan(
            nodes={
                "src": _src_node(),
                "noop": _sql_node("noop", "select * from {prev}"),
                "tgt": _tgt_node(),
            },
            edges=[
                PlanEdge(from_node_id="src", to_node_id="noop"),
                PlanEdge(from_node_id="noop", to_node_id="tgt"),
            ],
        )
        result = NoOpRemovalRule().apply(plan, _CTX)
        assert "noop" not in result.nodes

    def test_removes_multiple_noops(self):
        plan = LogicalPlan(
            nodes={
                "src": _src_node(),
                "noop1": _sql_node("noop1", "SELECT * FROM {prev}"),
                "sql": _sql_node("sql", "SELECT id FROM {prev}"),
                "noop2": _sql_node("noop2", "SELECT * FROM {prev}"),
                "tgt": _tgt_node(),
            },
            edges=[
                PlanEdge(from_node_id="src", to_node_id="noop1"),
                PlanEdge(from_node_id="noop1", to_node_id="sql"),
                PlanEdge(from_node_id="sql", to_node_id="noop2"),
                PlanEdge(from_node_id="noop2", to_node_id="tgt"),
            ],
        )
        result = NoOpRemovalRule().apply(plan, _CTX)
        assert "noop1" not in result.nodes
        assert "noop2" not in result.nodes
        assert "sql" in result.nodes
        assert len(result.nodes) == 3


class TestMergeSqlNodesRule:
    def test_merges_two_consecutive_sql_nodes(self):
        plan = LogicalPlan(
            nodes={
                "src": _src_node(),
                "sql1": _sql_node("sql1", "SELECT id, name FROM {prev}"),
                "sql2": _sql_node("sql2", "SELECT id, UPPER(name) AS name FROM {prev}"),
                "tgt": _tgt_node(),
            },
            edges=[
                PlanEdge(from_node_id="src", to_node_id="sql1"),
                PlanEdge(from_node_id="sql1", to_node_id="sql2"),
                PlanEdge(from_node_id="sql2", to_node_id="tgt"),
            ],
        )
        result = MergeSqlNodesRule().apply(plan, _CTX)

        # sql2 should be removed, sql1 should be the merged node
        assert "sql2" not in result.nodes
        assert "sql1" in result.nodes

        merged = result.nodes["sql1"]
        assert isinstance(merged.config, SqlNodeConfig)
        assert "WITH" in merged.config.expression
        assert "_step_0" in merged.config.expression
        assert "_step_1" in merged.config.expression

        # Edges: src → sql1 → tgt
        assert len(result.edges) == 2

    def test_merges_three_consecutive_sql_nodes(self):
        plan = LogicalPlan(
            nodes={
                "src": _src_node(),
                "sql1": _sql_node("sql1", "SELECT * FROM {prev} WHERE id > 0"),
                "sql2": _sql_node("sql2", "SELECT id, name FROM {prev}"),
                "sql3": _sql_node("sql3", "SELECT *, 'done' AS status FROM {prev}"),
                "tgt": _tgt_node(),
            },
            edges=[
                PlanEdge(from_node_id="src", to_node_id="sql1"),
                PlanEdge(from_node_id="sql1", to_node_id="sql2"),
                PlanEdge(from_node_id="sql2", to_node_id="sql3"),
                PlanEdge(from_node_id="sql3", to_node_id="tgt"),
            ],
        )
        result = MergeSqlNodesRule().apply(plan, _CTX)
        assert "sql2" not in result.nodes
        assert "sql3" not in result.nodes
        assert "sql1" in result.nodes

        merged = result.nodes["sql1"]
        assert "_step_2" in merged.config.expression

    def test_no_merge_with_single_sql_node(self):
        plan = LogicalPlan(
            nodes={
                "src": _src_node(),
                "sql": _sql_node("sql", "SELECT id FROM {prev}"),
                "tgt": _tgt_node(),
            },
            edges=[
                PlanEdge(from_node_id="src", to_node_id="sql"),
                PlanEdge(from_node_id="sql", to_node_id="tgt"),
            ],
        )
        result = MergeSqlNodesRule().apply(plan, _CTX)
        assert len(result.nodes) == 3
        assert result is plan  # No changes

    def test_no_merge_when_non_sql_between(self):
        from vonnegut.pipeline.dag.node import CodeNodeConfig
        plan = LogicalPlan(
            nodes={
                "src": _src_node(),
                "sql1": _sql_node("sql1", "SELECT id FROM {prev}"),
                "code": PlanNode(id="code", type=NodeType.CODE, config=CodeNodeConfig(function_code="def transform(df): return df")),
                "sql2": _sql_node("sql2", "SELECT * FROM {prev}"),
                "tgt": _tgt_node(),
            },
            edges=[
                PlanEdge(from_node_id="src", to_node_id="sql1"),
                PlanEdge(from_node_id="sql1", to_node_id="code"),
                PlanEdge(from_node_id="code", to_node_id="sql2"),
                PlanEdge(from_node_id="sql2", to_node_id="tgt"),
            ],
        )
        result = MergeSqlNodesRule().apply(plan, _CTX)
        # No merge: sql1 and sql2 are separated by code node
        assert "sql1" in result.nodes
        assert "sql2" in result.nodes
        assert len(result.nodes) == 5

    def test_does_not_merge_aggregation_nodes(self):
        """Nodes with GROUP BY / aggregation should not be merged."""
        plan = LogicalPlan(
            nodes={
                "src": _src_node(),
                "sql1": _sql_node("sql1", "SELECT id, name FROM {prev}"),
                "sql2": _sql_node("sql2", "SELECT name, COUNT(*) AS cnt FROM {prev} GROUP BY name"),
                "tgt": _tgt_node(),
            },
            edges=[
                PlanEdge(from_node_id="src", to_node_id="sql1"),
                PlanEdge(from_node_id="sql1", to_node_id="sql2"),
                PlanEdge(from_node_id="sql2", to_node_id="tgt"),
            ],
        )
        result = MergeSqlNodesRule().apply(plan, _CTX)
        # Should NOT merge: sql2 has aggregation
        assert "sql1" in result.nodes
        assert "sql2" in result.nodes

    def test_does_not_merge_window_function_nodes(self):
        plan = LogicalPlan(
            nodes={
                "src": _src_node(),
                "sql1": _sql_node("sql1", "SELECT id, name FROM {prev}"),
                "sql2": _sql_node("sql2", "SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM {prev}"),
                "tgt": _tgt_node(),
            },
            edges=[
                PlanEdge(from_node_id="src", to_node_id="sql1"),
                PlanEdge(from_node_id="sql1", to_node_id="sql2"),
                PlanEdge(from_node_id="sql2", to_node_id="tgt"),
            ],
        )
        result = MergeSqlNodesRule().apply(plan, _CTX)
        assert "sql1" in result.nodes
        assert "sql2" in result.nodes

    def test_does_not_merge_distinct_nodes(self):
        plan = LogicalPlan(
            nodes={
                "src": _src_node(),
                "sql1": _sql_node("sql1", "SELECT id, name FROM {prev}"),
                "sql2": _sql_node("sql2", "SELECT DISTINCT name FROM {prev}"),
                "tgt": _tgt_node(),
            },
            edges=[
                PlanEdge(from_node_id="src", to_node_id="sql1"),
                PlanEdge(from_node_id="sql1", to_node_id="sql2"),
                PlanEdge(from_node_id="sql2", to_node_id="tgt"),
            ],
        )
        result = MergeSqlNodesRule().apply(plan, _CTX)
        assert "sql1" in result.nodes
        assert "sql2" in result.nodes

    def test_merged_sql_resolves_prev_references(self):
        plan = LogicalPlan(
            nodes={
                "src": _src_node(),
                "sql1": _sql_node("sql1", "SELECT id, name FROM {prev}"),
                "sql2": _sql_node("sql2", "SELECT id FROM {prev} WHERE name IS NOT NULL"),
                "tgt": _tgt_node(),
            },
            edges=[
                PlanEdge(from_node_id="src", to_node_id="sql1"),
                PlanEdge(from_node_id="sql1", to_node_id="sql2"),
                PlanEdge(from_node_id="sql2", to_node_id="tgt"),
            ],
        )
        result = MergeSqlNodesRule().apply(plan, _CTX)
        merged = result.nodes["sql1"]
        expr = merged.config.expression

        # sql1 should keep {prev} (references upstream source)
        assert "{prev}" in expr
        # sql2's {prev} should be replaced with _step_0
        assert "_step_0" in expr
        # The original {prev} in sql2 should NOT remain
        # The merged SQL should reference _step_0 instead of {prev} for the second step
        assert "FROM _step_0 WHERE" in expr
