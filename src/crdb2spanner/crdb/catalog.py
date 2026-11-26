"""CockroachDB catalog model + introspection.

The model is plain dataclasses and round-trips through JSON, so the assessor and the
schema converter can run (and be unit tested) from a saved ``catalog.json`` without a
live cluster.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Column:
    name: str
    sql_type: str  # CockroachDB SQL type as shown by SHOW CREATE / crdb_sql_type, e.g. STRING(50)
    nullable: bool = True
    default: str | None = None
    hidden: bool = False
    generated: str | None = None  # computed column expression
    virtual: bool = False  # VIRTUAL computed column (not stored, not emitted by changefeeds)
    ordinal: int = 0


@dataclass
class Index:
    name: str
    columns: list[str]
    directions: list[str] = field(default_factory=list)  # ASC/DESC per column
    storing: list[str] = field(default_factory=list)
    unique: bool = False
    primary: bool = False
    inverted: bool = False
    partial: str | None = None  # WHERE predicate
    hash_sharded: bool = False
    # per key column: the SQL expression for expression-index columns (crdb_internal_idx_expr*), else None
    expressions: list = field(default_factory=list)


@dataclass
class ForeignKey:
    name: str
    columns: list[str]
    ref_table: str
    ref_columns: list[str]
    on_delete: str | None = None


@dataclass
class Table:
    name: str
    columns: list[Column]
    primary_key: list[str]
    indexes: list[Index] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    checks: list[str] = field(default_factory=list)
    check_names: list[str] = field(default_factory=list)  # parallel to checks
    create_statement: str = ""
    estimated_rows: int | None = None
    # implicit leading PK columns (e.g. crdb_region on REGIONAL BY ROW tables); not part of
    # SHOW INDEXES' explicit key but present in changefeed keys and the physical key
    implicit_key: list[str] = field(default_factory=list)

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"{self.name}.{name}")

    @property
    def has_hidden_rowid_pk(self) -> bool:
        return self.primary_key == ["rowid"] and any(
            c.name == "rowid" and c.hidden for c in self.columns
        )


@dataclass
class Catalog:
    database: str
    version: str = ""
    tables: list[Table] = field(default_factory=list)
    enums: dict[str, list[str]] = field(default_factory=dict)
    sequences: list[str] = field(default_factory=list)
    sequence_values: dict[str, int] = field(default_factory=dict)  # last value per sequence
    sequence_increments: dict[str, int] = field(default_factory=dict)  # INCREMENT BY per sequence
    settings: dict[str, str] = field(default_factory=dict)
    # primary_key_overrides checked against the data: table -> {columns, duplicates, nulls, error}
    key_checks: dict[str, dict] = field(default_factory=dict)

    def table(self, name: str) -> Table:
        for t in self.tables:
            if t.name == name:
                return t
        raise KeyError(name)

    # --- JSON round trip -------------------------------------------------------------
    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Catalog":
        tables = []
        for t in d.get("tables", []):
            tables.append(
                Table(
                    name=t["name"],
                    columns=[Column(**c) for c in t["columns"]],
                    primary_key=t["primary_key"],
                    indexes=[Index(**i) for i in t.get("indexes", [])],
                    foreign_keys=[ForeignKey(**f) for f in t.get("foreign_keys", [])],
                    checks=t.get("checks", []),
                    check_names=t.get("check_names", []),
                    create_statement=t.get("create_statement", ""),
                    estimated_rows=t.get("estimated_rows"),
                    implicit_key=t.get("implicit_key", []),
                )
            )
        return cls(
            database=d["database"],
            version=d.get("version", ""),
            tables=tables,
            enums=d.get("enums", {}),
            sequences=d.get("sequences", []),
            sequence_values=d.get("sequence_values", {}),
            sequence_increments=d.get("sequence_increments", {}),
            settings=d.get("settings", {}),
            key_checks=d.get("key_checks", {}),
        )

    @classmethod
    def load(cls, path: str | Path) -> "Catalog":
        return cls.from_dict(json.loads(Path(path).read_text()))


# ------------------------------------------------------------------------------------
# Live introspection
# ------------------------------------------------------------------------------------

_FK_RE = re.compile(
    r"FOREIGN KEY \((?P<cols>[^)]*)\) REFERENCES (?P<ref>[\w.\"]+)\s*\((?P<refcols>[^)]*)\)"
    r"(?P<rest>.*)",
    re.IGNORECASE,
)


def _split_cols(s: str) -> list[str]:
    return [c.strip().strip('"') for c in s.split(",") if c.strip()]


def parse_index_block(create_statement: str) -> dict[str, dict[str, Any]]:
    """Pull per-index attributes that SHOW INDEXES does not expose uniformly across versions."""
    out: dict[str, dict[str, Any]] = {}
    for line in create_statement.splitlines():
        s = line.strip().rstrip(",")
        m = re.match(r"(UNIQUE\s+)?(INVERTED\s+)?INDEX\s+(\w+)", s, re.IGNORECASE)
        if not m:
            continue
        name = m.group(3)
        where = re.search(r"\bWHERE\s+(.*)$", s, re.IGNORECASE)
        out[name] = {
            "inverted": bool(m.group(2)),
            "hash_sharded": "USING HASH" in s.upper(),
            "partial": where.group(1) if where else None,
            "expressions": _key_expressions(s[m.end():]),
        }
    return out


def _split_top(s: str) -> list[str]:
    parts, buf, depth, in_str = [], [], 0, False
    for ch in s:
        if ch == "'":
            in_str = not in_str
        if not in_str:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                parts.append("".join(buf).strip())
                buf = []
                continue
        buf.append(ch)
    if "".join(buf).strip():
        parts.append("".join(buf).strip())
    return parts


def _key_expressions(rest: str) -> list:
    """Key list of an index line -> [expression or None per key column]."""
    rest = rest.strip()
    if not rest.startswith("("):
        return []
    depth, end = 0, -1
    for i, ch in enumerate(rest):
        depth += ch == "("
        depth -= ch == ")"
        if depth == 0:
            end = i
            break
    out = []
    for part in _split_top(rest[1:end]):
        p = re.sub(r"\s+(ASC|DESC)$", "", part.strip(), flags=re.IGNORECASE)
        out.append(None if re.fullmatch(r'"?[A-Za-z_][\w$]*"?', p) else p)
    return out


def virtual_columns(create_statement: str) -> set[str]:
    """Names of VIRTUAL computed columns in a SHOW CREATE TABLE statement."""
    out = set()
    for line in create_statement.splitlines():
        m = re.match(r'\s*"?([^"\s]+)"?\s+.*\bAS\s*\(.*\)\s*VIRTUAL\b', line, re.IGNORECASE)
        if m:
            out.add(m.group(1))
    return out


def count_families(create_statement: str) -> int:
    return len(re.findall(r"^\s*FAMILY\b", create_statement, re.IGNORECASE | re.MULTILINE))


def introspect(conn, database: str, schema: str = "public", only: list[str] | None = None) -> Catalog:
    """Build a Catalog from a live CockroachDB connection (psycopg3, autocommit)."""
    cur = conn.cursor()
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]

    settings = {}
    for name in ("kv.rangefeed.enabled", "cluster.organization"):
        try:
            cur.execute(f"SHOW CLUSTER SETTING {name}")
            settings[name] = str(cur.fetchone()[0])
        except Exception:  # noqa: BLE001 - permission or unknown setting
            conn.rollback() if not conn.autocommit else None

    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = %s AND table_type = 'BASE TABLE' ORDER BY table_name",
        (schema,),
    )
    names = [r[0] for r in cur.fetchall()]
    if only:
        names = [n for n in names if n in only]

    enums: dict[str, list[str]] = {}
    try:
        cur.execute(
            "SELECT t.typname, e.enumlabel FROM pg_catalog.pg_enum e "
            "JOIN pg_catalog.pg_type t ON e.enumtypid = t.oid ORDER BY t.typname, e.enumsortorder"
        )
        for typ, label in cur.fetchall():
            enums.setdefault(typ, []).append(label)
    except Exception:  # noqa: BLE001
        pass

    cur.execute(
        "SELECT sequence_name, increment FROM information_schema.sequences WHERE sequence_schema = %s",
        (schema,),
    )
    seq_rows = cur.fetchall()
    sequences = [r[0] for r in seq_rows]
    sequence_increments: dict[str, int] = {}
    for sname, inc in seq_rows:
        try:
            sequence_increments[sname] = int(inc)
        except (TypeError, ValueError):
            pass
    sequence_values: dict[str, int] = {}
    for sname in sequences:
        try:
            cur.execute(f'SELECT last_value FROM "{schema}"."{sname}"')
            sequence_values[sname] = int(cur.fetchone()[0])
        except Exception:  # noqa: BLE001 - permission or unsupported; converter uses a safe default
            pass

    tables = [_introspect_table(cur, schema, n) for n in names]
    return Catalog(
        database=database, version=version, tables=tables, enums=enums,
        sequences=sequences, sequence_values=sequence_values, sequence_increments=sequence_increments,
        settings=settings,
    )


def check_key_overrides(conn, schema: str, overrides: dict[str, list[str]]) -> dict[str, dict]:
    """For each table given a natural primary key (primary_key_overrides), count source rows that
    would collide on that key or have a NULL in it. Either would lose rows on Spanner (rows with the
    same key merge into one; key columns must be NOT NULL). Read-only."""
    out: dict[str, dict] = {}
    cur = conn.cursor()
    for table, cols in (overrides or {}).items():
        qcols = ", ".join(f'"{c}"' for c in cols)
        src = f'"{schema}"."{table}"'
        r: dict = {"columns": list(cols)}
        try:
            cur.execute(f"SELECT count(*) FROM (SELECT {qcols} FROM {src} GROUP BY {qcols} HAVING count(*) > 1) AS d")
            r["duplicates"] = int(cur.fetchone()[0])
            nulls = " OR ".join(f'"{c}" IS NULL' for c in cols)
            cur.execute(f"SELECT count(*) FROM {src} WHERE {nulls}")
            r["nulls"] = int(cur.fetchone()[0])
        except Exception as e:  # noqa: BLE001 - reported as a finding, never fatal
            r["error"] = str(e).splitlines()[0][:200]
        out[table] = r
    return out


def _introspect_table(cur, schema: str, name: str) -> Table:
    cur.execute(
        "SELECT column_name, crdb_sql_type, is_nullable, column_default, is_hidden, "
        "generation_expression, ordinal_position FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, name),
    )
    columns = [
        Column(
            name=r[0], sql_type=r[1], nullable=(r[2] == "YES"), default=r[3],
            hidden=(r[4] == "YES"), generated=(r[5] or None), ordinal=r[6],
        )
        for r in cur.fetchall()
    ]

    cur.execute(f'SHOW CREATE TABLE "{schema}"."{name}"')
    create_stmt = cur.fetchone()[1]
    extra = parse_index_block(create_stmt)
    virtual = virtual_columns(create_stmt)
    for c in columns:
        c.virtual = c.name in virtual

    cur.execute(f'SHOW INDEXES FROM "{schema}"."{name}"')
    cols = [d.name for d in cur.description]
    idx: dict[str, Index] = {}
    implicit_key: list[str] = []
    for row in cur.fetchall():
        r = dict(zip(cols, row))
        iname = r["index_name"]
        if r.get("implicit"):
            # implicit columns of the primary index are partitioning prefixes (crdb_region);
            # implicit columns of secondary indexes are the PK suffix - ignore those
            if (iname.endswith("_pkey") or iname == "primary") and not r.get("storing") \
                    and r["column_name"] not in implicit_key:
                implicit_key.append(r["column_name"])
            continue
        i = idx.setdefault(
            iname,
            Index(name=iname, columns=[], unique=not r["non_unique"],
                  primary=(iname.endswith("_pkey") or iname == "primary")),
        )
        if r.get("storing"):
            i.storing.append(r["column_name"])
        else:
            i.columns.append(r["column_name"])
            i.directions.append(r.get("direction") or "ASC")
    for i in idx.values():
        for k, v in extra.get(i.name, {}).items():
            setattr(i, k, v)

    pk = next((i.columns for i in idx.values() if i.primary), [])

    fks: list[ForeignKey] = []
    checks: list[str] = []
    check_names: list[str] = []
    cur.execute(f'SHOW CONSTRAINTS FROM "{schema}"."{name}"')
    ccols = [d.name for d in cur.description]
    for row in cur.fetchall():
        r = dict(zip(ccols, row))
        ctype, details = r["constraint_type"], r["details"]
        if ctype == "FOREIGN KEY":
            m = _FK_RE.search(details)
            if m:
                rest = m.group("rest").upper()
                on_delete = None
                od = re.search(r"ON DELETE (CASCADE|SET NULL|SET DEFAULT|RESTRICT|NO ACTION)", rest)
                if od:
                    on_delete = od.group(1)
                fks.append(ForeignKey(
                    name=r["constraint_name"], columns=_split_cols(m.group("cols")),
                    ref_table=m.group("ref").split(".")[-1].strip('"'),
                    ref_columns=_split_cols(m.group("refcols")), on_delete=on_delete,
                ))
        elif ctype == "CHECK":
            checks.append(details)
            check_names.append(r["constraint_name"])

    est = None
    try:
        cur.execute(
            "SELECT estimated_row_count FROM crdb_internal.table_row_statistics "
            "WHERE table_name = %s", (name,)
        )
        row = cur.fetchone()
        est = int(row[0]) if row else None
    except Exception:  # noqa: BLE001
        cur.connection.rollback() if not cur.connection.autocommit else None

    return Table(
        name=name, columns=columns, primary_key=pk,
        indexes=[i for i in idx.values() if not i.primary],
        foreign_keys=fks, checks=checks, check_names=check_names, create_statement=create_stmt,
        estimated_rows=est,
        implicit_key=implicit_key,
    )
