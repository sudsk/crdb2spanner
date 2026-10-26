"""CockroachDB -> Spanner PostgreSQL-dialect type mapping.

Each mapping names a *value transform* (see ``values.py``) applied identically by the
bulk load, CDC apply and validation code paths, so all three agree on representation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Spanner PG max for varchar(n)
SPANNER_MAX_VARCHAR = 2621440


@dataclass(frozen=True)
class TypeMapping:
    spanner_type: str
    transform: str = "identity"
    severity: str | None = None  # None | info | medium | high | blocker
    note: str | None = None


def _base_and_args(sql_type: str) -> tuple[str, list[str], bool]:
    t = sql_type.strip().upper()
    is_array = t.endswith("[]")
    if is_array:
        t = t[:-2].strip()
    m = re.match(r"^([A-Z0-9_ ]+?)\s*(?:\(([^)]*)\))?$", t)
    if not m:
        return t, [], is_array
    args = [a.strip() for a in (m.group(2) or "").split(",") if a.strip()]
    return m.group(1).strip(), args, is_array


_SCALAR: dict[str, TypeMapping] = {
    # integers: Spanner PG only has 8-byte integers
    "INT8": TypeMapping("bigint", "int"),
    "INT": TypeMapping("bigint", "int"),
    "INTEGER": TypeMapping("bigint", "int"),
    "BIGINT": TypeMapping("bigint", "int"),
    "INT4": TypeMapping("bigint", "int", severity="medium", note="INT4 widened to bigint (app impact)"),
    "INT2": TypeMapping("bigint", "int", severity="medium", note="INT2 widened to bigint (app impact)"),
    "SMALLINT": TypeMapping("bigint", "int", severity="medium", note="SMALLINT widened to bigint"),
    "OID": TypeMapping("bigint", "int", severity="medium"),
    "FLOAT8": TypeMapping("double precision", "float"),
    "FLOAT": TypeMapping("double precision", "float"),
    "DOUBLE PRECISION": TypeMapping("double precision", "float"),
    "FLOAT4": TypeMapping("real", "float"),
    "REAL": TypeMapping("real", "float"),
    "DECIMAL": TypeMapping("numeric", "decimal"),
    "NUMERIC": TypeMapping("numeric", "decimal"),
    "BOOL": TypeMapping("boolean", "bool"),
    "BOOLEAN": TypeMapping("boolean", "bool"),
    "BYTES": TypeMapping("bytea", "bytes"),
    "BYTEA": TypeMapping("bytea", "bytes"),
    "BLOB": TypeMapping("bytea", "bytes"),
    "DATE": TypeMapping("date", "date"),
    "TIMESTAMPTZ": TypeMapping("timestamptz", "timestamp"),
    "TIMESTAMP WITH TIME ZONE": TypeMapping("timestamptz", "timestamp"),
    "TIMESTAMP": TypeMapping(
        "timestamptz", "timestamp_naive_utc", "medium",
        "TIMESTAMP (no tz) stored as timestamptz assuming UTC",
    ),
    "TIMESTAMP WITHOUT TIME ZONE": TypeMapping(
        "timestamptz", "timestamp_naive_utc", "medium",
        "TIMESTAMP (no tz) stored as timestamptz assuming UTC",
    ),
    "JSONB": TypeMapping("jsonb", "json"),
    "JSON": TypeMapping("jsonb", "json"),
    "TIME": TypeMapping("varchar(32)", "to_text", "medium", "TIME stored as ISO text"),
    "TIMETZ": TypeMapping("varchar(40)", "to_text", "medium", "TIMETZ stored as ISO text"),
    "INTERVAL": TypeMapping("varchar(64)", "to_text", "medium", "INTERVAL stored as text"),
    "INET": TypeMapping("varchar(64)", "to_text", "medium", "INET stored as text"),
    "BIT": TypeMapping("varchar", "to_text", "medium", "BIT stored as text"),
    "VARBIT": TypeMapping("varchar", "to_text", "medium", "VARBIT stored as text"),
    "GEOMETRY": TypeMapping("varchar", "to_text", "high", "Geospatial stored as WKT/EWKB text"),
    "GEOGRAPHY": TypeMapping("varchar", "to_text", "high", "Geospatial stored as WKT/EWKB text"),
    "TSVECTOR": TypeMapping("varchar", "to_text", "high", "Use Spanner full-text search instead"),
    "TSQUERY": TypeMapping("varchar", "to_text", "high"),
}

_STRINGS = {"STRING", "VARCHAR", "CHARACTER VARYING", "TEXT", "CHAR", "CHARACTER", "NAME", "CITEXT"}


def map_type(sql_type: str, enums: dict[str, list[str]] | None = None, uuid_native: bool = False) -> TypeMapping:
    base, args, is_array = _base_and_args(sql_type)
    enums = {k.upper(): v for k, v in (enums or {}).items()}

    if base in _STRINGS:
        if args and args[0].isdigit() and int(args[0]) <= SPANNER_MAX_VARCHAR:
            m = TypeMapping(f"varchar({args[0]})", "to_text")
        else:
            m = TypeMapping("varchar", "to_text")
        if base == "CHAR" or base == "CHARACTER":
            m = TypeMapping(m.spanner_type, "to_text", "info", "CHAR padding semantics not kept")
    elif base == "UUID":
        m = TypeMapping("uuid", "uuid") if uuid_native else TypeMapping("varchar(36)", "uuid")
    elif base in ("DECIMAL", "NUMERIC") and args:
        m = TypeMapping("numeric", "decimal", "info", f"precision/scale {','.join(args)} not enforced")
    elif base in _SCALAR:
        m = _SCALAR[base]
    elif base in enums:
        m = TypeMapping("varchar(255)", "to_text", "info", f"ENUM {base.lower()} stored as text")
    else:
        m = TypeMapping("varchar", "to_text", "high", f"Unknown type {sql_type}; stored as text")

    if is_array:
        if m.spanner_type == "jsonb":
            return TypeMapping("jsonb[]", "array:json", m.severity, m.note)
        return TypeMapping(f"{m.spanner_type}[]", f"array:{m.transform}", m.severity, m.note)
    return m
