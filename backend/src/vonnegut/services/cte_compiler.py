import re

import sqlglot
from sqlglot import exp


def _validate_expression(expression: str) -> None:
    """Validate that a SQL expression is a single SELECT statement using AST parsing."""
    stripped = expression.strip()

    # Parse with WARN level so unknown identifiers (like CTE refs) don't hard-fail
    try:
        statements = sqlglot.parse(stripped, error_level=sqlglot.ErrorLevel.WARN)
    except sqlglot.errors.ParseError as e:
        raise ValueError(f"Invalid SQL expression: {e}") from e

    if len(statements) > 1:
        raise ValueError("SQL expression must be a single statement (no semicolons)")

    if not statements or statements[0] is None:
        raise ValueError("SQL expression is empty or could not be parsed")

    if not isinstance(statements[0], exp.Select):
        raise ValueError(
            f"SQL expression must be a SELECT statement, got: {type(statements[0]).__name__}"
        )


def normalize_cte_name(name: str, position: int) -> str:
    """Normalize a step name into a valid SQL CTE identifier.

    - Lowercase, non-alphanumeric replaced with underscores
    - Truncated to fit within 63 chars (PostgreSQL identifier limit)
    - Position suffix for uniqueness
    """
    if not name.strip():
        return f"step_{position}"

    normalized = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    suffix = f"_{position}"
    max_base = 63 - len(suffix)
    if len(normalized) > max_base:
        normalized = normalized[:max_base]
    return normalized + suffix


def compile_sql_chain(
    steps: list[dict],
    limit: int | None = None,
) -> str:
    """Compile a list of SQL steps into a single CTE query.

    Each step dict has: name, position, expression.
    In expressions, {prev} is replaced with the previous CTE name.
    """
    if not steps:
        raise ValueError("Cannot compile empty SQL chain")

    cte_names: list[str] = []
    cte_parts: list[str] = []

    for i, step in enumerate(steps):
        cte_name = normalize_cte_name(step["name"], step["position"])
        expression = step["expression"]

        if i > 0 and "{prev}" in expression:
            expression = expression.replace("{prev}", cte_names[i - 1])

        _validate_expression(expression)
        cte_names.append(cte_name)
        cte_parts.append(f"{cte_name} AS ({expression})")

    last_cte = cte_names[-1]
    ctes = ",\n     ".join(cte_parts)
    final_select = f"SELECT * FROM {last_cte}"
    if limit is not None:
        final_select += f" LIMIT {limit}"

    return f"WITH {ctes}\n{final_select}"
