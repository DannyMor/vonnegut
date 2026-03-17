# backend/src/vonnegut/services/migration_runner.py
import threading

from vonnegut.adapters.base import DatabaseAdapter
from vonnegut.services.transformation_engine import TransformationEngine


class MigrationRunner:
    def __init__(self, engine: TransformationEngine):
        self._engine = engine

    async def run_test(
        self,
        source_adapter: DatabaseAdapter,
        table: str,
        transformations: list[dict],
        rows: int = 10,
    ) -> dict:
        """Run transformations on sample data and return before/after."""
        before = await source_adapter.fetch_sample(table, rows=rows)
        after = self._engine.apply_pipeline(list(before), transformations)
        return {"before": before, "after": after}

    async def run(
        self,
        source_adapter: DatabaseAdapter,
        target_adapter: DatabaseAdapter,
        source_table: str,
        target_table: str,
        transformations: list[dict],
        truncate_target: bool,
        row_limit: int,
        batch_size: int,
        on_progress,
        cancel_flag: threading.Event,
    ) -> dict:
        """Execute the full migration."""
        count_result = await source_adapter.execute(
            f"SELECT COUNT(*) as count FROM {source_table}"
        )
        total_rows = count_result[0]["count"]

        if total_rows > row_limit:
            raise ValueError(
                f"Source table has {total_rows} rows which exceeds the limit of {row_limit}. "
                "Consider filtering or wait for future batching support."
            )

        all_rows = await source_adapter.execute(f"SELECT * FROM {source_table}")
        transformed = self._engine.apply_pipeline(all_rows, transformations)

        if not transformed:
            return {"status": "completed", "rows_processed": 0, "total_rows": 0}

        columns = list(transformed[0].keys())
        rows_processed = 0

        if truncate_target:
            await target_adapter.execute(f"TRUNCATE TABLE {target_table}")

        for i in range(0, len(transformed), batch_size):
            if cancel_flag.is_set():
                return {"status": "cancelled", "rows_processed": rows_processed, "total_rows": total_rows}

            batch = transformed[i : i + batch_size]
            for row in batch:
                placeholders = ", ".join(["%s"] * len(columns))
                col_names = ", ".join(columns)
                values = tuple(row[c] for c in columns)
                await target_adapter.execute(
                    f"INSERT INTO {target_table} ({col_names}) VALUES ({placeholders})",
                    values,
                )
            rows_processed += len(batch)
            await on_progress(rows_processed, total_rows)

        return {"status": "completed", "rows_processed": rows_processed, "total_rows": total_rows}
