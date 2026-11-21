"""Built-in schema converter: Catalog -> Spanner PostgreSQL-dialect DDL + MappingSpec.

Output is split into three DDL phases (see ARCHITECTURE.md 4.3):
  * ``tables``   - tables + PKs (+ shadow tables in strict mode). Apply before bulk load.
  * ``indexes``  - secondary indexes. Apply after bulk load (faster).
  * ``post``     - foreign keys / check constraints. Apply after cutover.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from ..crdb.catalog import Catalog, Column, Table
from .mapping import ColumnMap, MappingSpec, TableMap
from .types import map_type

_SAFE_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")
_RESERVED = {
    "all", "and", "any", "array", "as", "asc", "between", "by", "case", "cast", "check",
    "collate", "column", "constraint", "create", "cross", "default", "desc", "distinct",
    "else", "end", "except", "false", "for", "foreign", "from", "full", "group", "having",
    "in", "inner", "interval", "into", "is", "join", "left", "like", "limit", "not", "null",
    "offset", "on", "or", "order", "outer", "primary", "references", "right", "select",
    "table", "then", "to", "true", "union", "unique", "user", "using", "when", "where", "with",
}


def q(ident: str) -> str:
    """Quote an identifier for Spanner PG only when needed."""
    if _SAFE_IDENT.match(ident) and ident not in _RESERVED:
        return ident
    return '"' + ident.replace('"', '""') + '"'


@dataclass
class Issue:
    table: str
    column: str | None
    severity: str
    message: str


@dataclass
class ConversionResult:
    tables_ddl: list[str] = field(default_factory=list)
    index_ddl: list[str] = field(default_factory=list)
    post_ddl: list[str] = field(default_factory=list)
    mapping: MappingSpec | None = None
    issues: list[Issue] = field(default_factory=list)

    def render(self, phase: str) -> str:
        stmts = {"tables": self.tables_ddl, "indexes": self.index_ddl, "post": self.post_ddl}[phase]
        return "".join(s.rstrip(";") + ";\n\n" for s in stmts)


def _default_expr(col: Column) -> str | None:
    d = (col.default or "").strip()
    if not d:
        return None
    low = d.lower()
    if low.startswith("gen_random_uuid()") or low.startswith("uuid_v4()"):
        return "spanner.generate_uuid()"
    if low in ("now()", "current_timestamp()", "current_timestamp", "now():::timestamptz",
               "current_timestamp():::timestamptz"):
        return "CURRENT_TIMESTAMP"
    if re.fullmatch(r"(true|false|-?\d+(\.\d+)?|'[^']*')(:::\w+)?", low):
        return d.split(":::")[0]
    return None


def convert(
    catalog: Catalog,
    *,
    only: list[str] | None = None,
    exclude: list[str] | None = None,
    strict_shadow: bool = True,
    shadow_prefix: str = "c2s_shadow_",
    uuid_native: bool = False,
    target_database: str | None = None,
) -> ConversionResult:
    res = ConversionResult()
    tmaps: list[TableMap] = []
    exclude = exclude or []

    for t in catalog.tables:
        if (only and t.name not in only) or t.name in exclude:
            continue
        tmap, ddl = _convert_table(t, catalog, res, uuid_native)
        res.tables_ddl.append(ddl)
        if strict_shadow:
            tmap.shadow_table = f"{shadow_prefix}{tmap.target}"
            res.tables_ddl.append(_shadow_ddl(tmap))
        idx = _index_ddl(t, tmap, res)
        tmap.index_count = len(idx)
        res.index_ddl.extend(idx)
        res.post_ddl.extend(_post_ddl(t, tmap, catalog, only, exclude))
        tmaps.append(tmap)

    res.mapping = MappingSpec(
        source_database=catalog.database,
        target_database=target_database or catalog.database,
        tables=tmaps,
    )
    return res


def is_shard_column(name: str) -> bool:
    return name.startswith("crdb_internal_") and "_shard_" in name


# Spanner (PostgreSQL dialect) cannot use these types in a primary key or index key
NON_KEY_TYPES = ("jsonb", "numeric", "real")


def non_keyable(spanner_type: str) -> bool:
    t = spanner_type.lower()
    return t.endswith("[]") or t.split("(")[0] in NON_KEY_TYPES


def _convert_table(t: Table, catalog: Catalog, res: ConversionResult, uuid_native: bool):
    if not t.primary_key:
        res.issues.append(Issue(t.name, None, "blocker", "table has no primary key"))
    # hash-sharded PK: drop the shard column, keep the real key columns
    pk = [c for c in t.primary_key if not is_shard_column(c)]
    if pk != list(t.primary_key):
        res.issues.append(Issue(t.name, None, "info",
                                "hash-sharded PK: shard column dropped, Spanner key is " + ", ".join(pk)))

    cols: list[ColumnMap] = []
    lines: list[str] = []
    implicit = [c for c in t.implicit_key if not is_shard_column(c)]
    if implicit:
        res.issues.append(Issue(t.name, None, "high",
                                f"implicit key columns {implicit} (REGIONAL BY ROW / partitioning) kept as "
                                "ordinary columns, not in the Spanner key - review Spanner multi-region design"))
    for c in sorted(t.columns, key=lambda c: c.ordinal):
        if is_shard_column(c.name):
            continue
        if c.hidden and c.name not in pk and c.name not in implicit:
            continue  # hash-shard columns, other hidden internals
        if c.virtual:
            res.issues.append(Issue(t.name, c.name, "medium",
                                    f"VIRTUAL computed column ({c.generated}) not migrated - recreate as a "
                                    "Spanner generated column or compute in the app"))
            continue
        if c.generated:
            res.issues.append(Issue(t.name, c.name, "medium",
                                    f"computed column ({c.generated}) migrated as a plain column"))
        tm = map_type(c.sql_type, catalog.enums, uuid_native)
        transform = tm.transform
        if tm.severity:
            res.issues.append(Issue(t.name, c.name, tm.severity, tm.note or tm.spanner_type))

        if c.name in pk and (c.default or "").lower().startswith("unique_rowid()"):
            if c.hidden:
                transform = "bit_reverse"
                res.issues.append(Issue(
                    t.name, c.name, "high",
                    "hidden rowid PK: made explicit and bit-reversed to avoid Spanner hotspots "
                    "(add an explicit PK at source if apps need stable ids)",
                ))
            else:
                res.issues.append(Issue(
                    t.name, c.name, "high",
                    "PK uses unique_rowid() (monotonic) - hotspot risk in Spanner; consider a "
                    "bit-reversed sequence or UUID",
                ))
        if (c.default or "").lower().startswith("nextval("):
            res.issues.append(Issue(t.name, c.name, "high",
                                    "sequence default - replace with a bit-reversed Spanner sequence"))

        cols.append(ColumnMap(source=c.name, target=c.name, spanner_type=tm.spanner_type,
                              transform=transform, nullable=c.nullable))
        line = f"  {q(c.name)} {tm.spanner_type}"
        if not c.nullable or c.name in pk:
            line += " NOT NULL"
        dflt = _default_expr(c)
        if dflt and c.name not in pk:
            line += f" DEFAULT ({dflt})"
        elif c.default and not c.hidden and not (c.default or "").lower().startswith(
                ("unique_rowid()", "nextval(")) and not dflt:
            res.issues.append(Issue(t.name, c.name, "info", f"default {c.default!r} not migrated"))
        lines.append(line)

    for c in cols:
        if c.source in pk and non_keyable(c.spanner_type):
            res.issues.append(Issue(t.name, c.source, "blocker",
                                    f"PK column type {c.spanner_type} cannot be a Spanner key column"))
    pk_sql = ", ".join(q(p) for p in pk)
    lines.append(f"  PRIMARY KEY ({pk_sql})")
    ddl = f"CREATE TABLE {q(t.name)} (\n" + ",\n".join(lines) + "\n)"

    if "LOCALITY REGIONAL BY ROW" in t.create_statement.upper():
        res.issues.append(Issue(t.name, None, "high",
                                "REGIONAL BY ROW table - choose Spanner multi-region / geo-partitioning"))
    if "ttl" in t.create_statement.lower() and "ttl_expire" in t.create_statement.lower():
        res.issues.append(Issue(t.name, None, "medium",
                                "row-level TTL - recreate as Spanner row deletion policy"))

    full_key = list(t.implicit_key) + list(t.primary_key)
    return TableMap(source=t.name, target=t.name, columns=cols, primary_key=pk,
                    source_key=full_key if full_key != pk else None), ddl


def _shadow_ddl(tmap: TableMap) -> str:
    lines = [f"  {q(tmap.col(s).target)} {tmap.col(s).spanner_type} NOT NULL" for s in tmap.primary_key]
    lines += [
        "  c2s_hlc_wall bigint NOT NULL",
        "  c2s_hlc_logical bigint NOT NULL",
        "  c2s_deleted boolean NOT NULL",
        f"  PRIMARY KEY ({', '.join(q(c) for c in tmap.target_pk)})",
    ]
    return f"CREATE TABLE {q(tmap.shadow_table)} (\n" + ",\n".join(lines) + "\n)"


def _index_ddl(t: Table, tmap: TableMap, res: ConversionResult) -> list[str]:
    out = []
    pk_set = set(t.primary_key)
    for i in t.indexes:
        if i.inverted:
            res.issues.append(Issue(t.name, None, "medium",
                                    f"inverted index {i.name} skipped - use Spanner search index"))
            continue
        if i.partial:
            res.issues.append(Issue(t.name, None, "medium",
                                    f"partial index {i.name} created without predicate ({i.partial})"))
        # implicit PK suffix columns are already excluded at introspection (SHOW INDEXES
        # implicit=true); hash-shard columns are dropped here - Spanner distributes itself.
        dirs = i.directions or ["ASC"] * len(i.columns)
        pairs = [(c, d) for c, d in zip(i.columns, dirs)
                 if not c.startswith("crdb_internal_") and c not in t.implicit_key]
        mapped = {c.source: c for c in tmap.columns}
        missing = [c for c, _ in pairs if c not in mapped]
        if missing:
            res.issues.append(Issue(t.name, None, "medium",
                                    f"index {i.name} skipped - uses columns not migrated ({', '.join(missing)})"))
            continue
        bad = [c for c, _ in pairs if non_keyable(mapped[c].spanner_type)]
        if bad:
            res.issues.append(Issue(t.name, None, "medium",
                                    f"index {i.name} skipped - Spanner cannot index {', '.join(bad)} "
                                    f"({', '.join(mapped[c].spanner_type for c in bad)})"))
            continue
        parts = [f"{q(c)}{' DESC' if d.upper() == 'DESC' else ''}" for c, d in pairs]
        storing = [s for s in i.storing if s not in pk_set and s in mapped]
        stmt = f"CREATE {'UNIQUE ' if i.unique else ''}INDEX {q(i.name)} ON {q(tmap.target)} ({', '.join(parts)})"
        if storing:
            stmt += f" INCLUDE ({', '.join(q(s) for s in storing)})"
        out.append(stmt)
    return out


def _post_ddl(t: Table, tmap: TableMap, catalog: Catalog, only, exclude) -> list[str]:
    out = []
    for fk in t.foreign_keys:
        if (only and fk.ref_table not in only) or fk.ref_table in exclude:
            continue
        stmt = (
            f"ALTER TABLE {q(tmap.target)} ADD CONSTRAINT {q(fk.name)} FOREIGN KEY "
            f"({', '.join(q(c) for c in fk.columns)}) REFERENCES {q(fk.ref_table)} "
            f"({', '.join(q(c) for c in fk.ref_columns)})"
        )
        if fk.on_delete == "CASCADE":
            stmt += " ON DELETE CASCADE"
        out.append(stmt)
    return out
