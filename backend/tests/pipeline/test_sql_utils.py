import pytest
from sqlglot import exp
from vonnegut.pipeline.sql_utils import (
    parse_sql,
    parse_sql_strict,
    is_select_star_from_single_table,
    is_safe_to_merge,
    resolve_prev_reference,
    build_cte_chain,
    SafetyTier,
    classify_safety,
    get_produced_columns,
    get_consumed_columns,
    prune_columns,
    optimize_sql,
    extract_where_predicate,
    get_predicate_columns,
    add_where_to_sql,
    remove_where,
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


class TestClassifySafety:
    def test_simple_select_is_safe(self):
        assert classify_safety("SELECT id, name FROM {prev}") == SafetyTier.SAFE

    def test_where_is_safe(self):
        assert classify_safety("SELECT * FROM {prev} WHERE id > 0") == SafetyTier.SAFE

    def test_aggregation_is_unsafe(self):
        assert classify_safety("SELECT COUNT(*) FROM {prev} GROUP BY id") == SafetyTier.UNSAFE

    def test_window_is_unsafe(self):
        assert classify_safety("SELECT ROW_NUMBER() OVER (ORDER BY id) FROM {prev}") == SafetyTier.UNSAFE

    def test_distinct_is_unsafe(self):
        assert classify_safety("SELECT DISTINCT name FROM {prev}") == SafetyTier.UNSAFE

    def test_limit_is_partial(self):
        assert classify_safety("SELECT * FROM {prev} LIMIT 10") == SafetyTier.PARTIAL

    def test_empty_is_unsafe(self):
        assert classify_safety("") == SafetyTier.UNSAFE

    def test_is_safe_to_merge_allows_partial(self):
        # PARTIAL tier nodes can still be CTE-merged
        assert is_safe_to_merge("SELECT * FROM {prev} LIMIT 10") is True


class TestGetProducedColumns:
    def test_explicit_columns(self):
        cols = get_produced_columns("SELECT id, name FROM {prev}")
        assert cols == {"id", "name"}

    def test_aliased_columns(self):
        cols = get_produced_columns("SELECT id, UPPER(name) AS uname FROM {prev}")
        assert "uname" in cols
        assert "id" in cols

    def test_star_returns_wildcard(self):
        cols = get_produced_columns("SELECT * FROM {prev}")
        assert cols == {"*"}

    def test_empty_returns_empty(self):
        assert get_produced_columns("") == set()


class TestGetConsumedColumns:
    def test_where_clause_columns(self):
        cols = get_consumed_columns("SELECT id FROM {prev} WHERE name = 'test'")
        assert "name" in cols
        assert "id" in cols

    def test_all_references(self):
        cols = get_consumed_columns("SELECT id, UPPER(name) AS uname FROM {prev} WHERE age > 0")
        assert "id" in cols
        assert "name" in cols
        assert "age" in cols


class TestPruneColumns:
    def test_removes_unused_columns(self):
        result = prune_columns("SELECT id, name, age FROM {prev}", {"id", "name"})
        assert "id" in result
        assert "name" in result
        assert "age" not in result

    def test_preserves_needed_columns(self):
        result = prune_columns("SELECT id, name FROM {prev}", {"id", "name"})
        assert "id" in result
        assert "name" in result

    def test_no_prune_on_select_star(self):
        original = "SELECT * FROM {prev}"
        result = prune_columns(original, {"id"})
        # SELECT * can't be pruned without schema info
        assert "*" in result

    def test_no_prune_with_wildcard_needed(self):
        original = "SELECT id, name FROM {prev}"
        result = prune_columns(original, {"*"})
        assert "id" in result
        assert "name" in result

    def test_no_empty_select(self):
        # If pruning would remove all columns, keep them
        result = prune_columns("SELECT id, name FROM {prev}", {"other"})
        assert "SELECT" in result


class TestExtractWherePredicate:
    def test_extracts_simple_where(self):
        pred = extract_where_predicate("SELECT * FROM {prev} WHERE id > 10")
        assert pred is not None

    def test_returns_none_without_where(self):
        pred = extract_where_predicate("SELECT * FROM {prev}")
        assert pred is None

    def test_returns_none_for_empty(self):
        assert extract_where_predicate("") is None

    def test_extracts_compound_where(self):
        pred = extract_where_predicate(
            "SELECT * FROM {prev} WHERE id > 0 AND name IS NOT NULL"
        )
        assert pred is not None


class TestGetPredicateColumns:
    def test_simple_comparison(self):
        pred = extract_where_predicate("SELECT * FROM {prev} WHERE id > 10")
        cols = get_predicate_columns(pred)
        assert "id" in cols

    def test_compound_predicate(self):
        pred = extract_where_predicate(
            "SELECT * FROM {prev} WHERE id > 0 AND name = 'test'"
        )
        cols = get_predicate_columns(pred)
        assert "id" in cols
        assert "name" in cols


class TestAddWhereToSql:
    def test_adds_where_to_sql_without_where(self):
        pred = extract_where_predicate("SELECT * FROM {prev} WHERE id > 10")
        result = add_where_to_sql("SELECT id, name FROM {prev}", pred)
        assert "WHERE" in result
        assert "id > 10" in result
        assert "{prev}" in result

    def test_combines_with_existing_where(self):
        pred = extract_where_predicate("SELECT * FROM {prev} WHERE id > 10")
        result = add_where_to_sql(
            "SELECT * FROM {prev} WHERE name IS NOT NULL", pred
        )
        assert "AND" in result
        assert "10" in result
        # sqlglot normalizes IS NOT NULL to NOT ... IS NULL
        assert "NULL" in result


class TestRemoveWhere:
    def test_removes_where_clause(self):
        result = remove_where("SELECT id, name FROM {prev} WHERE id > 10")
        assert "WHERE" not in result
        assert "id" in result
        assert "name" in result
        assert "{prev}" in result

    def test_no_change_without_where(self):
        original = "SELECT id FROM {prev}"
        result = remove_where(original)
        assert "WHERE" not in result
        assert "id" in result

    def test_empty_returns_empty(self):
        assert remove_where("") == ""


class TestOptimizeSql:
    def test_basic_optimization(self):
        result = optimize_sql("SELECT * FROM {prev} WHERE 1 = 1")
        assert "SELECT" in result

    def test_fallback_on_failure(self):
        result = optimize_sql("")
        assert result == ""
