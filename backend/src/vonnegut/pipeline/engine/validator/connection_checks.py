"""Pre-flight connection and schema checks for source/target databases.

These are async checks that run before the pipeline to validate connectivity
and table availability. They return CheckResult for consistency with the
rule-based validation system.
"""
from __future__ import annotations

from vonnegut.adapters.base import AdapterFactory, ColumnSchema
from vonnegut.pipeline.results import CheckResult, CheckStatus


async def check_source_connection(
    adapter_factory: AdapterFactory,
    connection: dict,
) -> CheckResult:
    """Test if we can connect to the source database."""
    try:
        adapter = await adapter_factory.create(connection)
        await adapter.disconnect()
        return CheckResult(
            rule_name="source_connection",
            status=CheckStatus.PASSED,
            message="Successfully connected to source database",
        )
    except Exception as e:
        return CheckResult(
            rule_name="source_connection",
            status=CheckStatus.FAILED,
            message=f"Cannot connect to source database: {e}",
        )


async def check_target_connection(
    adapter_factory: AdapterFactory,
    connection: dict,
) -> CheckResult:
    """Test if we can connect to the target database."""
    try:
        adapter = await adapter_factory.create(connection)
        await adapter.disconnect()
        return CheckResult(
            rule_name="target_connection",
            status=CheckStatus.PASSED,
            message="Successfully connected to target database",
        )
    except Exception as e:
        return CheckResult(
            rule_name="target_connection",
            status=CheckStatus.FAILED,
            message=f"Cannot connect to target database: {e}",
        )


async def check_source_query(
    adapter_factory: AdapterFactory,
    connection: dict,
    query: str,
) -> CheckResult:
    """Test if the source query executes without error (LIMIT 1)."""
    try:
        adapter = await adapter_factory.create(connection)
        try:
            await adapter.execute(f"SELECT * FROM ({query}) AS _q LIMIT 1")
            return CheckResult(
                rule_name="source_query",
                status=CheckStatus.PASSED,
                message="Source query executes successfully",
            )
        except Exception as e:
            return CheckResult(
                rule_name="source_query",
                status=CheckStatus.FAILED,
                message=f"Source query failed: {e}",
            )
        finally:
            await adapter.disconnect()
    except Exception as e:
        return CheckResult(
            rule_name="source_query",
            status=CheckStatus.FAILED,
            message=f"Cannot connect to source database: {e}",
        )


async def check_target_table(
    adapter_factory: AdapterFactory,
    connection: dict,
    table_name: str,
) -> tuple[CheckResult, list[ColumnSchema]]:
    """Test if the target table exists and return its schema."""
    try:
        adapter = await adapter_factory.create(connection)
        try:
            schema = await adapter.fetch_schema(table_name)
            if not schema:
                return (
                    CheckResult(
                        rule_name="target_table",
                        status=CheckStatus.FAILED,
                        message=f"Target table '{table_name}' not found or has no columns",
                    ),
                    [],
                )
            return (
                CheckResult(
                    rule_name="target_table",
                    status=CheckStatus.PASSED,
                    message=f"Target table '{table_name}' exists with {len(schema)} columns",
                ),
                schema,
            )
        except Exception as e:
            return (
                CheckResult(
                    rule_name="target_table",
                    status=CheckStatus.FAILED,
                    message=f"Cannot access target table '{table_name}': {e}",
                ),
                [],
            )
        finally:
            await adapter.disconnect()
    except Exception as e:
        return (
            CheckResult(
                rule_name="target_table",
                status=CheckStatus.FAILED,
                message=f"Cannot connect to target database: {e}",
            ),
            [],
        )
