"""Shared sqlglot-based SQL analysis utilities.

All SQL parsing, analysis, and transformation in the pipeline should go through
these functions. No regex for SQL handling.

Architecture layers:
1. Parse layer — parse_sql, parse_sql_strict
2. Analysis layer — column lineage, safety classification
3. Rewrite layer — resolve references, CTE building, column pruning
"""
from __future__ import annotations
from enum import Enum

import sqlglot
from sqlglot import exp
from sqlglot.optimizer import optimize as sqlglot_optimize

# Placeholder used in pipeline SQL expressions to reference the upstream table.
PREV_PLACEHOLDER = "{prev}"
_PREV_TABLE = "__prev__"

_NON_DETERMINISTIC = {"NOW", "RANDOM", "RAND", "UUID", "GEN_RANDOM_UUID", "CURRENT_TIMESTAMP"}


# ---------------------------------------------------------------------------
# 1. Parse layer
# ---------------------------------------------------------------------------

def parse_sql(expression: str) -> exp.Expression | None:
    """Parse a pipeline SQL expression, replacing {prev} with a parseable name."""
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


# ---------------------------------------------------------------------------
# 2. Analysis layer — safety, lineage, classification
# ---------------------------------------------------------------------------

class SafetyTier(str, Enum):
    """Three-tier safety classification for SQL optimization.

    SAFE: Full fusion — alias inlining, column pruning, flattening.
    PARTIAL: CTE merge only — no inlining or pruning across boundaries.
    UNSAFE: No merge — wrap in CTE, apply sqlglot.optimize only.
    """
    SAFE = "safe"
    PARTIAL = "partial"
    UNSAFE = "unsafe"


def _has_aggregation(ast: exp.Expression) -> bool:
    for node in ast.walk():
        if isinstance(node, (exp.Group, exp.AggFunc)):
            return True
    return False


def _has_window(ast: exp.Expression) -> bool:
    for node in ast.walk():
        if isinstance(node, exp.Window):
            return True
    return False


def _has_distinct(ast: exp.Expression) -> bool:
    for node in ast.walk():
        if isinstance(node, exp.Distinct):
            return True
    return False


def _has_risky_join(ast: exp.Expression) -> bool:
    for join in ast.find_all(exp.Join):
        kind = join.args.get("kind")
        if kind and kind.upper() in ("LEFT", "RIGHT", "FULL"):
            return True
    return False


def _has_nondeterministic(ast: exp.Expression) -> bool:
    for func in ast.find_all(exp.Anonymous):
        if hasattr(func, "name") and func.name.upper() in _NON_DETERMINISTIC:
            return True
    return False


def _has_subquery(ast: exp.Expression) -> bool:
    for node in ast.walk():
        if isinstance(node, (exp.Subquery, exp.CTE)):
            return True
    return False


def _has_limit(ast: exp.Expression) -> bool:
    return ast.find(exp.Limit) is not None


def classify_safety(expression: str) -> SafetyTier:
    """Classify a SQL expression into a safety tier for optimization.

    SAFE: Simple selects, filters, casts, renames — full optimization allowed.
    PARTIAL: Has LIMIT or risky joins — CTE merge only.
    UNSAFE: Aggregation, window, DISTINCT, subqueries, non-deterministic — no merge.
    """
    ast = parse_sql(expression)
    if ast is None:
        return SafetyTier.UNSAFE

    # UNSAFE tier: semantic-changing constructs
    if _has_aggregation(ast):
        return SafetyTier.UNSAFE
    if _has_window(ast):
        return SafetyTier.UNSAFE
    if _has_distinct(ast):
        return SafetyTier.UNSAFE
    if _has_subquery(ast):
        return SafetyTier.UNSAFE
    if _has_nondeterministic(ast):
        return SafetyTier.UNSAFE

    # PARTIAL tier: can CTE-merge but don't inline across
    if _has_limit(ast):
        return SafetyTier.PARTIAL
    if _has_risky_join(ast):
        return SafetyTier.PARTIAL

    return SafetyTier.SAFE


def is_safe_to_merge(expression: str) -> bool:
    """Check if a SQL expression is safe to merge/inline with other SQL nodes.

    Returns True for SAFE and PARTIAL tiers (both can be CTE-merged).
    Only UNSAFE nodes are excluded from merging.
    """
    return classify_safety(expression) != SafetyTier.UNSAFE


def is_select_star_from_single_table(expression: str) -> bool:
    """Check if a SQL expression is a simple `SELECT * FROM table` with no transforms."""
    ast = parse_sql(expression)
    if ast is None or not isinstance(ast, exp.Select):
        return False

    projections = ast.expressions
    if len(projections) != 1 or not isinstance(projections[0], exp.Star):
        return False

    from_clause = ast.find(exp.From)
    if from_clause is None:
        return False

    tables = list(ast.find_all(exp.Table))
    if len(tables) != 1:
        return False

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


# ---------------------------------------------------------------------------
# Column lineage
# ---------------------------------------------------------------------------

def get_produced_columns(expression: str) -> set[str]:
    """Extract column names produced (projected) by a SQL expression.

    For `SELECT id, UPPER(name) AS uname FROM t`, returns {"id", "uname"}.
    For `SELECT * FROM t`, returns {"*"} (unknown projection).
    """
    ast = parse_sql(expression)
    if ast is None or not isinstance(ast, exp.Select):
        return set()

    cols: set[str] = set()
    for proj in ast.expressions:
        if isinstance(proj, exp.Star):
            return {"*"}  # Can't statically determine columns
        elif isinstance(proj, exp.Alias):
            cols.add(proj.alias)
        elif isinstance(proj, exp.Column):
            cols.add(proj.name)
    return cols


def get_consumed_columns(expression: str) -> set[str]:
    """Extract column names consumed (referenced) by a SQL expression.

    Looks at WHERE, JOIN, ORDER BY, expressions — everywhere columns are read.
    """
    ast = parse_sql(expression)
    if ast is None:
        return set()

    cols: set[str] = set()
    for col in ast.find_all(exp.Column):
        cols.add(col.name)
    return cols


# ---------------------------------------------------------------------------
# 3. Rewrite layer — transformation, pruning, CTE building
# ---------------------------------------------------------------------------

def resolve_prev_reference(expression: str, replacement: str) -> str:
    """Replace {prev} table reference in SQL with a different table name using sqlglot AST."""
    ast = parse_sql(expression)
    if ast is None:
        return expression.replace(PREV_PLACEHOLDER, replacement)

    def transformer(node):
        if isinstance(node, exp.Table) and node.name == _PREV_TABLE:
            return exp.Table(this=exp.to_identifier(replacement))
        return node

    transformed = ast.transform(transformer)
    return transformed.sql()


def prune_columns(expression: str, needed_columns: set[str]) -> str:
    """Remove unused columns from a SELECT projection list.

    If the expression has `SELECT *` or if needed_columns contains "*",
    returns the expression unchanged (can't prune without knowing all columns).

    Only prunes when all needed columns are explicitly listed in projections.
    """
    ast = parse_sql(expression)
    if ast is None or not isinstance(ast, exp.Select):
        return expression

    if "*" in needed_columns:
        return expression

    # Check for SELECT *
    if any(isinstance(p, exp.Star) for p in ast.expressions):
        return expression

    new_projections = []
    for proj in ast.expressions:
        name = None
        if isinstance(proj, exp.Alias):
            name = proj.alias
        elif isinstance(proj, exp.Column):
            name = proj.name

        if name is not None and name in needed_columns:
            new_projections.append(proj)
        elif name is None:
            # Keep expressions we can't analyze (function calls without aliases, etc.)
            new_projections.append(proj)

    if not new_projections:
        return expression  # Don't produce empty SELECT

    ast.set("expressions", new_projections)
    return ast.sql()


def build_cte_chain(steps: list[tuple[str, str]], keep_first_prev: bool = True) -> str:
    """Build a CTE chain from a list of (step_name, sql_expression) pairs.

    Each step after the first has its {prev} reference resolved to the previous step name.
    """
    cte_parts: list[str] = []

    for i, (step_name, sql_expr) in enumerate(steps):
        if i == 0:
            cte_parts.append(f"{step_name} AS ({sql_expr.strip()})")
        else:
            prev_step = steps[i - 1][0]
            resolved = resolve_prev_reference(sql_expr, prev_step)
            cte_parts.append(f"{step_name} AS ({resolved})")

    last_step = steps[-1][0]
    return f"WITH {', '.join(cte_parts)} SELECT * FROM {last_step}"


def optimize_sql(expression: str, schema: dict[str, dict[str, str]] | None = None) -> str:
    """Apply sqlglot's built-in optimizer as a final cleanup pass.

    This handles constant folding, simplification, and other safe rewrites.
    Falls back to the original expression if optimization fails.

    Args:
        expression: SQL expression with {prev} placeholders.
        schema: Optional mapping of table_name → {column_name: type_str} for
                schema-aware optimization (column resolution, type inference).
    """
    ast = parse_sql(expression)
    if ast is None:
        return expression

    try:
        kwargs: dict = {}
        if schema:
            kwargs["schema"] = schema
        optimized = sqlglot_optimize.optimize(ast, **kwargs)
        return optimized.sql()
    except Exception:
        return expression
