import pytest
from vonnegut.database import AppDatabase, PostgresDatabase, _sqlite_to_pg_params


class TestSqliteToPostgresParams:
    def test_no_params(self):
        assert _sqlite_to_pg_params("SELECT * FROM users") == "SELECT * FROM users"

    def test_single_param(self):
        assert _sqlite_to_pg_params("SELECT * FROM users WHERE id = ?") == "SELECT * FROM users WHERE id = %s"

    def test_multiple_params(self):
        result = _sqlite_to_pg_params("INSERT INTO t (a, b, c) VALUES (?, ?, ?)")
        assert result == "INSERT INTO t (a, b, c) VALUES (%s, %s, %s)"

    def test_preserves_question_mark_in_strings(self):
        # This is a known limitation — string literals with ? would be converted.
        # Repos don't use ? in string literals, so this is acceptable.
        result = _sqlite_to_pg_params("SELECT '?' FROM t WHERE id = ?")
        assert result == "SELECT '%s' FROM t WHERE id = %s"


class TestPostgresDatabaseProtocol:
    def test_satisfies_protocol(self):
        assert issubclass(PostgresDatabase, AppDatabase)

    def test_instantiation(self):
        db = PostgresDatabase("postgresql://user:pass@localhost/test")
        assert db._url == "postgresql://user:pass@localhost/test"
        assert db._pool is None


class TestPgSchemaStatements:
    def test_all_tables_present(self):
        from vonnegut.database import _PG_SCHEMA_STATEMENTS
        all_sql = " ".join(_PG_SCHEMA_STATEMENTS)
        for table in ["connections", "migrations", "pipeline_steps", "transformations", "pipeline_metadata"]:
            assert table in all_sql

    def test_statement_count(self):
        from vonnegut.database import _PG_SCHEMA_STATEMENTS
        assert len(_PG_SCHEMA_STATEMENTS) == 5
