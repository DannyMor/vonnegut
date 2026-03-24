import pytest

from vonnegut.adapters.pg_types import pg_type_category


@pytest.mark.parametrize("pg_type,expected", [
    ("int2", "number"),
    ("int4", "number"),
    ("int8", "number"),
    ("float4", "number"),
    ("float8", "number"),
    ("numeric", "number"),
    ("varchar", "text"),
    ("text", "text"),
    ("char", "text"),
    ("bpchar", "text"),
    ("timestamp", "datetime"),
    ("timestamptz", "datetime"),
    ("date", "datetime"),
    ("time", "datetime"),
    ("timetz", "datetime"),
    ("bool", "boolean"),
    ("json", "json"),
    ("jsonb", "json"),
    ("uuid", "uuid"),
    ("_int4", "array"),
    ("_text", "array"),
    ("_uuid", "array"),
    ("bytea", "binary"),
    ("somecustomtype", "unknown"),
])
def test_pg_type_category(pg_type, expected):
    assert pg_type_category(pg_type) == expected
