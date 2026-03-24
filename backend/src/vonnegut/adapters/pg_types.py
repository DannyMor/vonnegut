_NUMBER_TYPES = {"int2", "int4", "int8", "float4", "float8", "numeric", "money", "serial", "bigserial"}
_TEXT_TYPES = {"varchar", "text", "char", "bpchar", "name", "citext"}
_DATETIME_TYPES = {"timestamp", "timestamptz", "date", "time", "timetz", "interval"}
_BOOLEAN_TYPES = {"bool"}
_JSON_TYPES = {"json", "jsonb"}
_UUID_TYPES = {"uuid"}
_BINARY_TYPES = {"bytea"}


def pg_type_category(pg_type: str) -> str:
    """Map a Postgres udt_name to a standard type category."""
    if pg_type.startswith("_"):
        return "array"
    if pg_type in _NUMBER_TYPES:
        return "number"
    if pg_type in _TEXT_TYPES:
        return "text"
    if pg_type in _DATETIME_TYPES:
        return "datetime"
    if pg_type in _BOOLEAN_TYPES:
        return "boolean"
    if pg_type in _JSON_TYPES:
        return "json"
    if pg_type in _UUID_TYPES:
        return "uuid"
    if pg_type in _BINARY_TYPES:
        return "binary"
    return "unknown"
