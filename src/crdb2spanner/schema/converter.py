"""Built-in schema converter: Catalog -> Spanner PostgreSQL-dialect DDL + MappingSpec.

Output is split into three DDL phases (see ARCHITECTURE.md 4.3):
  * ``tables``   - sequences, tables + PKs (+ shadow tables in strict mode). Before bulk load.
  * ``indexes``  - secondary indexes. After bulk load (faster).
  * ``post``     - foreign keys, CHECK constraints, TTL policies. After cutover.

Nothing here requires a change to the source database: every incompatibility is resolved
on the Spanner side or recorded as an issue for review.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from ..crdb.catalog import Catalog, Column, Table
from .mapping import ColumnMap, MappingSpec, TableMap
from .types import map_type

_SAFE_IDENT = re.compile(r"^[a-z][a-z0-9_]*$")
_RESERVED = {
    "all", "and", "any", "array", "as", "asc", "between", "by", "case", "cast", "check",
    "collate", "column", "constraint", "create", "cross", "default", "desc", "distinct",
    "else", "end", "except", "false", "for", "foreign", "from", "full", "group", "having",
    "in", "inner", "interval", "into", "is", "join", "left", "like", "limit", "not", "null",
    "offset", "on", "or", "order", "outer", "primary", "references", "right", "select",
    "table", "then", "to", "true", "union", "unique", "user", "using", "when", "where", "with",
}
MAX_IDENT = 128
# unique_rowid() values are positive and (for decades) below 2^61; generated values skip them
ROWID_SKIP_MAX = (1 << 61) - 1


def q(ident: str) -> str:
    """Quote an identifier for Spanner PG only when needed."""
    if _SAFE_IDENT.match(ident) and ident not in _RESERVED:
        return ident
    return '"' + ident.replace('"', '""') + '"'


def valid_spanner_ident(name: str) -> bool:
    """Spanner identifiers must start with a letter."""
    return bool(name) and name[0].isascii() and name[0].isalpha()


def strip_casts(expr: str) -> str:
    """CockroachDB type annotations (x:::TYPE) -> plain PostgreSQL."""
    return re.sub(r':::"?[\w.]+"?(\[\])?', "", expr)


@dataclass
class Issue:
    table: str
    column: str | None
    severity: str
    message: str


@dataclass
class ConvertOptions:
    uuid_native: bool = True
    strict_shadow: bool = True
    shadow_prefix: str = "c2s_shadow_"
    # "table.column" or "column" -> new name (applied before the default rule)
    rename_columns: dict[str, str] = field(default_factory=dict)
    # table -> natural key columns, replacing CockroachDB's hidden rowid (needs changefeed diff)
    primary_key_overrides: dict[str, list[str]] = field(default_factory=dict)
    # skip range floor for migrated sequences: max(floor, last_value * factor)
    sequence_skip_floor: int = 1_000_000_000
    sequence_skip_factor: int = 100
    ttl_policies: bool = True
    check_constraints: bool = True


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


class _Names:
    """Spanner names (tables, indexes, constraints, sequences) are unique per database."""

    def __init__(self):
        self.used: set[str] = set()

    def claim(self, name: str, prefix: str = "") -> str:
        cand = name[:MAX_IDENT]
        if cand.lower() in self.used and prefix:
            cand = f"{prefix}_{name}"[:MAX_IDENT]
        base, n = cand, 2
        while cand.lower() in self.used:
            cand = f"{base[: MAX_IDENT - 4]}_{n}"
            n += 1
        self.used.add(cand.lower())
        return cand


def default_rename(name: str, taken: set[str]) -> str:
    """Leading non-letters are not allowed: '_id' -> 'id_' (then id_2, ... if taken)."""
    core = re.sub(r"^[^A-Za-z]+", "", name) or "col"
    cand = f"{core}_"
    n = 2
    while cand.lower() in taken:
        cand = f"{core}_{n}"
        n += 1
    return cand


def is_shard_column(name: str) -> bool:
    return name.startswith("crdb_internal_") and "_shard_" in name


def is_expr_column(name: str) -> bool:
    return name.startswith("crdb_internal_idx_expr")


# Spanner (PostgreSQL dialect) cannot use these types in a primary key or index key
NON_KEY_TYPES = ("jsonb", "numeric", "real")


def non_keyable(spanner_type: str) -> bool:
    t = spanner_type.lower()
    return t.endswith("[]") or t.split("(")[0] in NON_KEY_TYPES


def _seq_name_from_default(default: str) -> str | None:
    m = re.search(r"nextval\(\s*'([^']+)'", default or "", re.IGNORECASE)
    return m.group(1).split(".")[-1].strip('"') if m else None


def _literal_default(col: Column, spanner_type: str) -> str | None:
    d = (col.default or "").strip()
    m = re.fullmatch(r"(true|false|-?\d+(\.\d+)?|'(?:[^']|'')*')(:::[\w.\"\[\]]+)?", d, re.IGNORECASE)
    if not m:
        return None
    lit = m.group(1)
    if lit.lower() in ("true", "false"):
        return lit.lower()
    return f"{lit}::{spanner_type}"


def convert(
    catalog: Catalog,
    *,
    only: list[str] | None = None,
    exclude: list[str] | None = None,
    strict_shadow: bool | None = None,
    shadow_prefix: str | None = None,
    uuid_native: bool | None = None,
    target_database: str | None = None,
    options: ConvertOptions | None = None,
) -> ConversionResult:
    opt = options or ConvertOptions()
    if strict_shadow is not None:
        opt.strict_shadow = strict_shadow
    if shadow_prefix is not None:
        opt.shadow_prefix = shadow_prefix
    if uuid_native is not None:
        opt.uuid_native = uuid_native
    exclude = exclude or []
    tables = [t for t in catalog.tables if not ((only and t.name not in only) or t.name in exclude)]

    res = ConversionResult()
    names = _Names()
    # pass 1: table + column names (FKs and indexes need every table's renames)
    tnames: dict[str, str] = {}
    cnames: dict[tuple[str, str], str] = {}
    for t in tables:
        tgt = t.name if valid_spanner_ident(t.name) else default_rename(t.name, names.used)
        if tgt != t.name:
            res.issues.append(Issue(t.name, None, "high", f"table renamed to {tgt} (Spanner names must start "
                                    "with a letter) - application impact"))
        tnames[t.name] = names.claim(tgt)
        taken = {c.name.lower() for c in t.columns}
        for c in t.columns:
            new = opt.rename_columns.get(f"{t.name}.{c.name}") or opt.rename_columns.get(c.name)
            if not new and not valid_spanner_ident(c.name):
                new = default_rename(c.name, taken)
            if new and new != c.name:
                taken.add(new.lower())
                res.issues.append(Issue(t.name, c.name, "high",
                                        f"column renamed to {new} - update application queries / ORM mappings"))
            cnames[(t.name, c.name)] = new or c.name
    if opt.strict_shadow:
        for t in tables:
            names.used.add(f"{opt.shadow_prefix}{tnames[t.name]}".lower())

    # sequences used by the source (explicit CREATE SEQUENCE, e.g. Spring Batch / Hibernate)
    for s in catalog.sequences:
        res.tables_ddl.extend(_sequence_ddl(names.claim(s), catalog.sequence_values.get(s), opt, res))

    tmaps: list[TableMap] = []
    for t in tables:
        tmap, ddl, extra_seq = _convert_table(t, catalog, res, opt, tnames, cnames, names)
        res.tables_ddl.extend(extra_seq)
        res.tables_ddl.append(ddl)
        if opt.strict_shadow:
            tmap.shadow_table = f"{opt.shadow_prefix}{tmap.target}"
            res.tables_ddl.append(_shadow_ddl(tmap))
        idx = _index_ddl(t, tmap, res, names, cnames)
        tmap.index_count = len(idx)
        res.index_ddl.extend(idx)
        res.post_ddl.extend(_post_ddl(t, tmap, res, names, tnames, cnames, opt))
        tmaps.append(tmap)

    res.mapping = MappingSpec(
        source_database=catalog.database,
        target_database=target_database or catalog.database,
        tables=tmaps,
    )
    return res


def _sequence_ddl(name: str, last_value: int | None, opt: ConvertOptions, res: ConversionResult,
                  skip_max: int | None = None) -> list[str]:
    if skip_max is None:
        skip_max = min(ROWID_SKIP_MAX, max(opt.sequence_skip_floor, (last_value or 0) * opt.sequence_skip_factor))
    return [f"CREATE SEQUENCE {q(name)} BIT_REVERSED_POSITIVE",
            f"ALTER SEQUENCE {q(name)} SKIP RANGE 1 {skip_max}"]


def _convert_table(t: Table, catalog: Catalog, res: ConversionResult, opt: ConvertOptions,
                   tnames: dict, cnames: dict, names: _Names):
    target = tnames[t.name]
    cn = lambda c: cnames.get((t.name, c), c)  # noqa: E731
    extra_seq: list[str] = []
    override = opt.primary_key_overrides.get(t.name)

    if not t.primary_key and not override:
        res.issues.append(Issue(t.name, None, "blocker", "table has no primary key"))
    # hash-sharded PK: drop the shard column, keep the real key columns
    pk = [c for c in t.primary_key if not is_shard_column(c)]
    if pk != list(t.primary_key):
        res.issues.append(Issue(t.name, None, "info",
                                "hash-sharded PK: shard column dropped, Spanner key is " + ", ".join(pk)))
    if override:
        missing = [c for c in override if c not in {c.name for c in t.columns}]
        if missing:
            res.issues.append(Issue(t.name, None, "blocker", f"primary_key_overrides: unknown columns {missing}"))
        else:
            res.issues.append(Issue(t.name, None, "high",
                                    f"Spanner key set to natural key ({', '.join(override)}) instead of "
                                    f"{', '.join(pk)} - verify it is unique and non-null at source; changefeed "
                                    "is created WITH diff so deletes carry the key"))
            pk = list(override)

    cols: list[ColumnMap] = []
    lines: list[str] = []
    implicit = [c for c in t.implicit_key if not is_shard_column(c)]
    if implicit:
        res.issues.append(Issue(t.name, None, "high",
                                f"implicit key columns {implicit} (REGIONAL BY ROW / partitioning) kept as "
                                "ordinary columns, not in the Spanner key - review Spanner multi-region design"))
    for c in sorted(t.columns, key=lambda c: c.ordinal):
        if is_shard_column(c.name) or is_expr_column(c.name):
            continue
        if c.hidden and c.name not in pk and c.name not in implicit:
            continue  # rowid replaced by an override, TTL expiration column, other internals
        if c.virtual:
            res.issues.append(Issue(t.name, c.name, "medium",
                                    f"VIRTUAL computed column ({c.generated}) not migrated - recreate as a "
                                    "Spanner generated column or compute in the app"))
            continue
        if c.generated:
            res.issues.append(Issue(t.name, c.name, "medium",
                                    f"computed column ({c.generated}) migrated as a plain column"))
        tm = map_type(c.sql_type, catalog.enums, opt.uuid_native)
        transform = tm.transform
        if tm.severity:
            res.issues.append(Issue(t.name, c.name, tm.severity, tm.note or tm.spanner_type))

        dflt = None
        d = (c.default or "").lower()
        if d.startswith("unique_rowid()"):
            seq = names.claim(f"{target}_{cn(c.name)}_seq")
            if c.hidden:
                transform = "bit_reverse"  # existing rowids spread over the key space
                extra_seq.append(f"CREATE SEQUENCE {q(seq)} BIT_REVERSED_POSITIVE")
                res.issues.append(Issue(t.name, c.name, "high",
                                        f"hidden rowid kept as the Spanner key: existing values bit-reversed, new "
                                        f"rows get nextval('{seq}'). No source change needed; set "
                                        "primary_key_overrides to use a natural key instead"))
            else:
                extra_seq.extend(_sequence_ddl(seq, None, opt, res, skip_max=ROWID_SKIP_MAX))
                res.issues.append(Issue(t.name, c.name, "high",
                                        f"unique_rowid() default -> bit-reversed sequence {seq} skipping the "
                                        "existing id range; new ids are no longer increasing (app impact)"))
            dflt = f"nextval('{seq}')"
        elif d.startswith("nextval("):
            sname = _seq_name_from_default(c.default)
            if sname and sname in catalog.sequences:
                dflt = f"nextval('{sname}')"
                res.issues.append(Issue(t.name, c.name, "medium",
                                        f"sequence default kept as bit-reversed sequence {sname}: new values "
                                        "are no longer increasing (app impact)"))
            else:
                res.issues.append(Issue(t.name, c.name, "high", f"sequence default {c.default} not migrated"))
        elif d.startswith(("gen_random_uuid()", "uuid_v4()")):
            dflt = "gen_random_uuid()" if tm.spanner_type == "uuid" else "spanner.generate_uuid()"
        elif d.startswith(("now()", "current_timestamp", "transaction_timestamp()", "statement_timestamp()")):
            dflt = "CURRENT_TIMESTAMP"
        elif c.default:
            dflt = _literal_default(c, tm.spanner_type)
            if not dflt and not c.hidden:
                res.issues.append(Issue(t.name, c.name, "info", f"default {c.default!r} not migrated"))

        cols.append(ColumnMap(source=c.name, target=cn(c.name), spanner_type=tm.spanner_type,
                              transform=transform, nullable=c.nullable))
        line = f"  {q(cn(c.name))} {tm.spanner_type}"
        if not c.nullable or c.name in pk:
            line += " NOT NULL"
        if dflt:
            line += f" DEFAULT ({dflt})"
        lines.append(line)

    # expression indexes -> stored generated columns the index can use
    for i in t.indexes:
        for pos, col in enumerate(i.columns):
            if not is_expr_column(col):
                continue
            exprs = i.expressions or []
            expr = exprs[pos] if pos < len(exprs) else None
            src_col = next((c for c in t.columns if c.name == col), None)
            expr = expr or (src_col.generated if src_col else None)
            if not expr:
                continue
            gname = names.claim(f"{i.name}_expr{pos + 1}")[:MAX_IDENT]
            gtype = map_type(src_col.sql_type, catalog.enums, opt.uuid_native).spanner_type if src_col else "varchar"
            pg_expr = strip_casts(expr)
            for (tn, sc), tc in cnames.items():
                if tn == t.name and sc != tc:
                    pg_expr = re.sub(rf'(?<![\w"]){re.escape(sc)}(?![\w"])', q(tc), pg_expr)
            lines.append(f"  {q(gname)} {gtype} GENERATED ALWAYS AS ({pg_expr}) STORED")
            cnames[(t.name, col)] = gname
            res.issues.append(Issue(t.name, None, "medium",
                                    f"expression index {i.name} on ({expr}) -> stored generated column "
                                    f"{gname}; verify the expression is valid in Spanner"))

    for c in cols:
        if c.source in pk and non_keyable(c.spanner_type):
            res.issues.append(Issue(t.name, c.source, "blocker",
                                    f"PK column type {c.spanner_type} cannot be a Spanner key column"))
    pk_sql = ", ".join(q(cn(p)) for p in pk)
    lines.append(f"  PRIMARY KEY ({pk_sql})")
    ddl = f"CREATE TABLE {q(target)} (\n" + ",\n".join(lines) + "\n)"

    if "LOCALITY REGIONAL BY ROW" in t.create_statement.upper():
        res.issues.append(Issue(t.name, None, "high",
                                "REGIONAL BY ROW table - choose Spanner multi-region / geo-partitioning"))

    full_key = list(t.implicit_key) + list(t.primary_key)
    tmap = TableMap(source=t.name, target=target, columns=cols, primary_key=pk,
                    source_key=full_key if full_key != pk else None,
                    key_from_row=bool(override))
    return tmap, ddl, extra_seq


def _shadow_ddl(tmap: TableMap) -> str:
    lines = [f"  {q(tmap.col(s).target)} {tmap.col(s).spanner_type} NOT NULL" for s in tmap.primary_key]
    lines += [
        "  c2s_hlc_wall bigint NOT NULL",
        "  c2s_hlc_logical bigint NOT NULL",
        "  c2s_deleted boolean NOT NULL",
        f"  PRIMARY KEY ({', '.join(q(c) for c in tmap.target_pk)})",
    ]
    return f"CREATE TABLE {q(tmap.shadow_table)} (\n" + ",\n".join(lines) + "\n)"


def _index_ddl(t: Table, tmap: TableMap, res: ConversionResult, names: _Names, cnames: dict) -> list[str]:
    out = []
    seen: dict[tuple, str] = {}
    pk_set = set(tmap.primary_key)
    mapped = {c.source: c for c in tmap.columns}
    for i in t.indexes:
        if i.inverted:
            res.issues.append(Issue(t.name, None, "medium",
                                    f"inverted index {i.name} skipped - use Spanner search index"))
            continue
        if i.partial:
            res.issues.append(Issue(t.name, None, "medium",
                                    f"partial index {i.name} created without predicate ({i.partial})"))
        dirs = i.directions or ["ASC"] * len(i.columns)
        pairs = [(c, d) for c, d in zip(i.columns, dirs)
                 if not is_shard_column(c) and c not in t.implicit_key
                 and not (c.startswith("crdb_internal_") and not is_expr_column(c))]
        expr_targets = {c: cnames[(t.name, c)] for c, _ in pairs if is_expr_column(c) and (t.name, c) in cnames}
        missing = [c for c, _ in pairs if c not in mapped and c not in expr_targets]
        if missing:
            res.issues.append(Issue(t.name, None, "medium",
                                    f"index {i.name} skipped - uses columns not migrated ({', '.join(missing)})"))
            continue
        bad = [c for c, _ in pairs if c in mapped and non_keyable(mapped[c].spanner_type)]
        if bad:
            res.issues.append(Issue(t.name, None, "medium",
                                    f"index {i.name} skipped - Spanner cannot index {', '.join(bad)} "
                                    f"({', '.join(mapped[c].spanner_type for c in bad)})"))
            continue
        tcol = lambda c: expr_targets.get(c) or mapped[c].target  # noqa: E731
        storing = [s for s in i.storing if s not in pk_set and s in mapped]
        sig = (i.unique, tuple((tcol(c), d.upper()) for c, d in pairs), tuple(sorted(storing)))
        if sig in seen:
            res.issues.append(Issue(t.name, None, "medium",
                                    f"duplicate index {i.name} (same as {seen[sig]}) skipped - avoids write "
                                    "amplification"))
            continue
        seen[sig] = i.name
        iname = names.claim(i.name, prefix=tmap.target)
        if iname != i.name:
            res.issues.append(Issue(t.name, None, "info",
                                    f"index {i.name} renamed to {iname} (Spanner index names are database-wide)"))
        parts = [f"{q(tcol(c))}{' DESC' if d.upper() == 'DESC' else ''}" for c, d in pairs]
        stmt = f"CREATE {'UNIQUE ' if i.unique else ''}INDEX {q(iname)} ON {q(tmap.target)} ({', '.join(parts)})"
        if storing:
            stmt += f" INCLUDE ({', '.join(q(mapped[s].target) for s in storing)})"
        out.append(stmt)
    return out


def _ttl_policy(t: Table, tmap: TableMap) -> tuple[str | None, str | None]:
    """(column, interval) from a CockroachDB TTL definition, or (None, reason)."""
    ddl = t.create_statement
    m = re.search(r"ttl_expiration_expression\s*=\s*e?'(.*?)'(?:\s*,|\s*\))", ddl, re.IGNORECASE | re.S)
    if m:
        expr = m.group(1).replace("\\'", "'").replace("''", "'")
        days = re.search(r"(\d+)\s*days?", expr, re.IGNORECASE)
        ts_cols = [c for c in tmap.columns if c.spanner_type == "timestamptz"]
        col = next((c for c in ts_cols if re.search(rf'(?<![\w"]){re.escape(c.source)}(?![\w"])', expr)), None)
        if days and col:
            return col.target, f"{days.group(1)} days"
        return None, f"TTL expression not translatable automatically: {expr}"
    m = re.search(r"ttl_expire_after\s*=\s*'([^']+)'", ddl, re.IGNORECASE)
    if m:
        return None, (f"ttl_expire_after = '{m.group(1)}' uses CockroachDB's hidden expiration column; "
                      "Spanner TTL needs a timestamptz column (e.g. a created/updated timestamp)")
    return None, None


def _post_ddl(t: Table, tmap: TableMap, res: ConversionResult, names: _Names,
              tnames: dict, cnames: dict, opt: ConvertOptions) -> list[str]:
    out = []
    cn = lambda tb, c: cnames.get((tb, c), c)  # noqa: E731
    for fk in t.foreign_keys:
        if fk.ref_table not in tnames:
            continue
        fname = names.claim(fk.name, prefix=tmap.target)
        if fname != fk.name:
            res.issues.append(Issue(t.name, None, "info",
                                    f"FK {fk.name} renamed to {fname} (constraint names are database-wide)"))
        stmt = (
            f"ALTER TABLE {q(tmap.target)} ADD CONSTRAINT {q(fname)} FOREIGN KEY "
            f"({', '.join(q(cn(t.name, c)) for c in fk.columns)}) REFERENCES {q(tnames[fk.ref_table])} "
            f"({', '.join(q(cn(fk.ref_table, c)) for c in fk.ref_columns)})"
        )
        if fk.on_delete == "CASCADE":
            stmt += " ON DELETE CASCADE"
        out.append(stmt)

    if opt.check_constraints:
        migrated = {c.source for c in tmap.columns}
        all_cols = {c.name for c in t.columns}
        for idx, details in enumerate(t.checks):
            name = t.check_names[idx] if idx < len(t.check_names) else f"{tmap.target}_check_{idx + 1}"
            m = re.match(r"\s*CHECK\s*\((.*)\)\s*$", details, re.IGNORECASE | re.S)
            if not m:
                continue
            expr = strip_casts(m.group(1))
            used = {c for c in all_cols if re.search(rf'(?<![\w"]){re.escape(c)}(?![\w"])', expr)}
            if used - migrated:
                res.issues.append(Issue(t.name, None, "medium",
                                        f"CHECK {name} skipped - references columns not migrated"))
                continue
            for c in used:
                if cn(t.name, c) != c:
                    expr = re.sub(rf'(?<![\w"]){re.escape(c)}(?![\w"])', q(cn(t.name, c)), expr)
            out.append(f"ALTER TABLE {q(tmap.target)} ADD CONSTRAINT {q(names.claim(name, prefix=tmap.target))} "
                       f"CHECK ({expr})")

    if opt.ttl_policies:
        col, info = _ttl_policy(t, tmap)
        if col:
            out.append(f"ALTER TABLE {q(tmap.target)} ADD TTL INTERVAL '{info}' ON {q(col)}")
            res.issues.append(Issue(t.name, None, "medium",
                                    f"row-level TTL -> Spanner TTL INTERVAL '{info}' ON {col} (post phase: "
                                    "applied after cutover so validation is not affected)"))
        elif info:
            res.issues.append(Issue(t.name, None, "high", info))
    return out
