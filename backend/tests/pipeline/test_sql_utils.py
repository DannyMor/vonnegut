import pytest
from sqlglot import exp
from vonnegut.pipeline.sql_utils import (
    parse_sql,
    parse_sql_strict,
    is_select_star_from_single_table,
    is_safe_to_merge,
    resolve_prev_reference,
    build_cte_chain,
)


class TestParseSql:
    def test_parses_simple_select(self):
        ast = parse_sql("SELECT * FROM {prev}")
        assert ast is not None
        assert isinstance(ast, exp.Select)

    def test_parses_with_where(self):
        ast = parse_sql("SELECT id, name FROM {prev} WHERE id > 0")
        assert ast is not None
        assert isinstance(ast, exp.Select)

    def test_returns_none_for_empty(self):
        assert parse_sql("") is None

    def test_returns_ast_for_partial_parse(self):
        # sqlglot with WARN level partially parses some garbage SQL
        # The strict variant catches non-SELECT results
        ast = parse_sql("NOT VALID SQL AT ALL BLAH")
        # It may parse something, but it won't be a Select
        assert not isinstance(ast, exp.Select)


class TestParseSqlStrict:
    def test_valid_select(self):
        ast, error = parse_sql_strict("SELECT * FROM {prev}")
        assert ast is not None
        assert error is None

    def test_multiple_statements(self):
        ast, error = parse_sql_strict("SELECT 1; SELECT 2")
        assert ast is None
        assert "single statement" in error

    def test_non_select(self):
        ast, error = parse_sql_strict("DROP TABLE users")
        assert ast is None
        assert "SELECT" in error

    def test_empty(self):
        ast, error = parse_sql_strict("")
        assert ast is None


class TestIsSelectStarFromSingleTable:
    def test_simple_select_star(self):
        assert is_select_star_from_single_table("SELECT * FROM {prev}") is True

    def test_case_insensitive(self):
        assert is_select_star_from_single_table("select * from {prev}") is True

    def test_with_extra_whitespace(self):
        assert is_select_star_from_single_table("  SELECT  *  FROM  {prev}  ") is True

    def test_select_columns_is_not_noop(self):
        assert is_select_star_from_single_table("SELECT id, name FROM {prev}") is False

    def test_with_where_is_not_noop(self):
        assert is_select_star_from_single_table("SELECT * FROM {prev} WHERE id > 0") is False

    def test_with_order_is_not_noop(self):
        assert is_select_star_from_single_table("SELECT * FROM {prev} ORDER BY id") is False

    def test_with_limit_is_not_noop(self):
        assert is_select_star_from_single_table("SELECT * FROM {prev} LIMIT 10") is False

    def test_with_join_is_not_noop(self):
        assert is_select_star_from_single_table("SELECT * FROM {prev} JOIN other ON 1=1") is False

    def test_with_group_by_is_not_noop(self):
        assert is_select_star_from_single_table("SELECT * FROM {prev} GROUP BY id") is False

    def test_empty_returns_false(self):
        assert is_select_star_from_single_table("") is False


class TestIsSafeToMerge:
    def test_simple_select_is_safe(self):
        assert is_safe_to_merge("SELECT id, name FROM {prev}") is True

    def test_select_with_where_is_safe(self):
        assert is_safe_to_merge("SELECT * FROM {prev} WHERE id > 0") is True

    def test_select_with_cast_is_safe(self):
        assert is_safe_to_merge("SELECT CAST(id AS TEXT) FROM {prev}") is True

    def test_aggregation_is_unsafe(self):
        assert is_safe_to_merge("SELECT user_id, COUNT(*) FROM {prev} GROUP BY user_id") is False

    def test_window_function_is_unsafe(self):
        assert is_safe_to_merge("SELECT *, ROW_NUMBER() OVER (ORDER BY id) FROM {prev}") is False

    def test_distinct_is_unsafe(self):
        assert is_safe_to_merge("SELECT DISTINCT name FROM {prev}") is False

    def test_subquery_is_unsafe(self):
        assert is_safe_to_merge("SELECT * FROM {prev} WHERE id IN (SELECT id FROM other)") is False

    def test_empty_is_unsafe(self):
        assert is_safe_to_merge("") is False


class TestResolvePrevReference:
    def test_replaces_prev_with_table_name(self):
        result = resolve_prev_reference("SELECT * FROM {prev}", "my_table")
        assert "my_table" in result
        assert "{prev}" not in result

    def test_replaces_in_where_clause(self):
        result = resolve_prev_reference(
            "SELECT id FROM {prev} WHERE name IS NOT NULL", "step_0"
        )
        assert "step_0" in result

    def test_handles_non_standard_sql(self):
        # Even with unusual SQL, the function should not crash
        result = resolve_prev_reference("SELECT 1 FROM {prev}", "my_table")
        assert "my_table" in result


class TestBuildCteChain:
    def test_two_step_chain(self):
        steps = [
            ("_step_0", "SELECT id, name FROM {prev}"),
            ("_step_1", "SELECT id FROM {prev} WHERE name IS NOT NULL"),
        ]
        result = build_cte_chain(steps)
        assert "WITH" in result
        assert "_step_0" in result
        assert "_step_1" in result
        assert "SELECT * FROM _step_1" in result

    def test_three_step_chain(self):
        steps = [
            ("_step_0", "SELECT * FROM {prev} WHERE id > 0"),
            ("_step_1", "SELECT id, name FROM {prev}"),
            ("_step_2", "SELECT *, 'done' AS status FROM {prev}"),
        ]
        result = build_cte_chain(steps)
        assert "_step_2" in result
        assert "SELECT * FROM _step_2" in result

    def test_first_step_keeps_prev(self):
        steps = [
            ("_step_0", "SELECT * FROM {prev}"),
            ("_step_1", "SELECT id FROM {prev}"),
        ]
        result = build_cte_chain(steps)
        # First step should still reference {prev} (resolved at execution time)
        assert "{prev}" in result

    def test_subsequent_steps_resolve_prev(self):
        steps = [
            ("_step_0", "SELECT id, name FROM {prev}"),
            ("_step_1", "SELECT id FROM {prev} WHERE name IS NOT NULL"),
        ]
        result = build_cte_chain(steps)
        # _step_1 should reference _step_0, not {prev}
        assert "_step_0" in result
