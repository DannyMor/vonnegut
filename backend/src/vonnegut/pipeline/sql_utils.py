"""Shared sqlglot-based SQL analysis utilities.

All SQL parsing, analysis, and transformation in the pipeline should go through
these functions. No regex for SQL handling.
"""
from __future__ import annotations

import sqlglot
from sqlglot import exp

# Placeholder used in pipeline SQL expressions to reference the upstream table.
PREV_PLACEHOLDER = "{prev}"
_PREV_TABLE = "__prev__"


def parse_sql(expression: str) -> exp.Expression | None:
    """Parse a pipeline SQL expression, replacing {prev} with a parseable name.

    Returns the parsed AST or None if parsing fails.
    """
    sanitized = expression.strip().replace(PREV_PLACEHOLDER, _PREV_TABLE)
    try:
        statements = sqlglot.parse(sanitized, error_level=sqlglot.ErrorLevel.WARN)
    except sqlglot.errors.ParseError:
        return None

    if not statements or statements[0] is None:
        return None
    return statements[0]


def parse_sql_strict(expression: str) -> tuple[exp.Expression | None, str | None]:
    """Parse and validate a pipeline SQL expression.

    Returns (ast, error_message). If parsing succeeds, error_message is None.
    """
    sanitized = expression.strip().replace(PREV_PLACEHOLDER, _PREV_TABLE)
    try:
        statements = sqlglot.parse(sanitized, error_level=sqlglot.ErrorLevel.WARN)
    except sqlglot.errors.ParseError as e:
        return None, f"SQL parse error: {e}"

    if len(statements) > 1:
        return None, "SQL must be a single statement (no semicolons)"
    if not statements or statements[0] is None:
        return None, "SQL expression is empty or could not be parsed"
    if not isinstance(statements[0], exp.Select):
        return None, f"SQL must be a SELECT statement, got: {type(statements[0]).__name__}"

    return statements[0], None


def is_select_star_from_single_table(expression: str) -> bool:
    """Check if a SQL expression is a simple `SELECT * FROM table` with no transforms.

    Uses sqlglot AST — no regex.
    """
    ast = parse_sql(expression)
    if ast is None or not isinstance(ast, exp.Select):
        return False

    # Must select only star (*)
    projections = ast.expressions
    if len(projections) != 1 or not isinstance(projections[0], exp.Star):
        return False

    # Must have a simple FROM with one table, no joins
    from_clause = ast.find(exp.From)
    if from_clause is None:
        return False

    tables = list(ast.find_all(exp.Table))
    if len(tables) != 1:
        return False

    # Must not have WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, etc.
    if ast.find(exp.Where):
        return False
    if ast.find(exp.Group):
        return False
    if ast.find(exp.Having):
        return False
    if ast.find(exp.Order):
        return False
    if ast.find(exp.Limit):
        return False
    if ast.find(exp.Join):
        return False

    return True


def is_safe_to_merge(expression: str) -> bool:
    """Check if a SQL expression is safe to merge/inline with other SQL nodes.

    Returns False if the expression contains constructs that could change semantics
    when inlined: aggregations, window functions, DISTINCT, subqueries, CTEs,
    or non-deterministic functions.
    """
    ast = parse_sql(expression)
    if ast is None:
        return False

    for node in ast.walk():
        # Aggregation boundary
        if isinstance(node, (exp.Group, exp.AggFunc)):
            return False
        # Window functions
        if isinstance(node, exp.Window):
            return False
        # DISTINCT
        if isinstance(node, exp.Distinct):
            return False
        # Subqueries (could have side effects or different semantics when inlined)
        if isinstance(node, exp.Subquery):
            return False
        # CTEs (already complex, don't nest further)
        if isinstance(node, exp.CTE):
            return False
        # Non-deterministic functions
        if isinstance(node, exp.Anonymous):
            func_name = node.name.upper() if hasattr(node, 'name') else ""
            if func_name in ("NOW", "RANDOM", "RAND", "UUID", "GEN_RANDOM_UUID"):
                return False

    return True


def resolve_prev_reference(expression: str, replacement: str) -> str:
    """Replace {prev} table reference in SQL with a different table name using sqlglot AST.

    This is safer than string replacement because it only replaces table references,
    not occurrences of '{prev}' inside string literals or comments.
    """
    ast = parse_sql(expression)
    if ast is None:
        # Fallback to string replacement if parsing fails
        return expression.replace(PREV_PLACEHOLDER, replacement)

    def transformer(node):
        if isinstance(node, exp.Table) and node.name == _PREV_TABLE:
            return exp.Table(this=exp.to_identifier(replacement))
        return node

    transformed = ast.transform(transformer)
    return transformed.sql()


def build_cte_chain(steps: list[tuple[str, str]], keep_first_prev: bool = True) -> str:
    """Build a CTE chain from a list of (step_name, sql_expression) pairs.

    Each step after the first has its {prev} reference resolved to the previous step name.
    If keep_first_prev is True, the first step keeps {prev} as-is (resolved at execution time).

    Returns the merged SQL string with CTEs.
    """
    cte_parts: list[str] = []

    for i, (step_name, sql_expr) in enumerate(steps):
        if i == 0:
            if keep_first_prev:
                cte_parts.append(f"{step_name} AS ({sql_expr.strip()})")
            else:
                cte_parts.append(f"{step_name} AS ({sql_expr.strip()})")
        else:
            prev_step = steps[i - 1][0]
            resolved = resolve_prev_reference(sql_expr, prev_step)
            cte_parts.append(f"{step_name} AS ({resolved})")

    last_step = steps[-1][0]
    return f"WITH {', '.join(cte_parts)} SELECT * FROM {last_step}"
