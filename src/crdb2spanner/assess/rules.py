"""Assessment rules: Catalog -> findings + per-table score + Markdown/JSON report."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass

from ..crdb.catalog import Catalog, Table, count_families
from ..schema.types import map_type

SEVERITY_ORDER = {"blocker": 4, "high": 3, "medium": 2, "info": 1}


@dataclass
class Finding:
    table: str | None
    column: str | None
    rule: str
    severity: str
    message: str
    remedy: str


def assess(catalog: Catalog, only: list[str] | None = None, exclude: list[str] | None = None) -> list[Finding]:
    f: list[Finding] = []
    exclude = exclude or []

    if catalog.settings.get("kv.rangefeed.enabled", "").lower() not in ("true", "t"):
        f.append(Finding(None, None, "CLUSTER_RANGEFEED", "blocker",
                         "kv.rangefeed.enabled is not true (or could not be read)",
                         "SET CLUSTER SETTING kv.rangefeed.enabled = true;"))

    v = crdb_version(catalog.version)
    if v and (23, 1, 0) <= v <= (24, 1, 3):
        f.append(Finding(None, None, "CRDB_OUT_OF_ORDER_ADVISORY", "high",
                         f"CockroachDB v{'.'.join(map(str, v))} is in the range affected by Cockroach Labs "
                         "advisory a122372 (changefeed out-of-order emission)",
                         "upgrade the source cluster before CDC, and keep apply.mode=strict"))

    if catalog.sequences:
        f.append(Finding(None, None, "SEQUENCES", "info",
                         f"{len(catalog.sequences)} sequence(s): {', '.join(catalog.sequences[:10])}"
                         + (" ..." if len(catalog.sequences) > 10 else ""),
                         "generated as bit-reversed sequences with SKIP RANGE past their current values"))

    tables = [t for t in catalog.tables if not ((only and t.name not in only) or t.name in exclude)]
    for t in tables:
        f.extend(_table_rules(t, catalog))

    # index / constraint names must be unique across the whole Spanner database
    seen: dict[str, str] = {}
    for t in tables:
        for name in [i.name for i in t.indexes] + [fk.name for fk in t.foreign_keys] + list(t.check_names):
            if name.lower() in seen and seen[name.lower()] != t.name:
                f.append(Finding(t.name, None, "NAME_COLLISION", "info",
                                 f"{name} is also used on table {seen[name.lower()]}",
                                 "renamed with a table prefix (Spanner names are database-wide)"))
            seen.setdefault(name.lower(), t.name)
    return f


def crdb_version(version: str) -> tuple[int, int, int] | None:
    m = re.search(r"v(\d+)\.(\d+)\.(\d+)", version or "")
    return tuple(int(x) for x in m.groups()) if m else None


def _table_rules(t: Table, catalog: Catalog) -> list[Finding]:
    out: list[Finding] = []
    ddl = t.create_statement.upper()
    add = lambda *a: out.append(Finding(t.name, *a))  # noqa: E731

    if not t.primary_key:
        add(None, "NO_PK", "blocker", "no primary key", "add an explicit primary key")
    elif t.has_hidden_rowid_pk:
        add("rowid", "HIDDEN_ROWID_PK", "high",
            "table relies on hidden rowid (unique_rowid) primary key",
            "no source change: rowid becomes the Spanner key (bit-reversed, sequence default), or set "
            "primary_key_overrides to a natural key")

    for c in t.columns:
        d = (c.default or "").lower()
        if c.name in t.primary_key and not c.hidden and d.startswith("unique_rowid()"):
            add(c.name, "MONOTONIC_PK", "high", "PK default unique_rowid() is monotonic",
                "generated: bit-reversed sequence default skipping the existing id range; new ids are not "
                "increasing (app impact if ids are ordered or exposed)")
        if d.startswith("nextval("):
            add(c.name, "SEQUENCE_DEFAULT", "medium", f"sequence default {c.default}",
                "generated: bit-reversed sequence with SKIP RANGE past current values; new values not increasing")
        if c.name and not c.name[0].isalpha() and not c.hidden:
            add(c.name, "INVALID_IDENTIFIER", "high", f"column name {c.name!r} must start with a letter in Spanner",
                "renamed (default: strip leading characters + '_', or rename_columns in the job config); "
                "application queries / ORM mappings must follow")
        if c.generated and not (c.hidden and "crdb_internal" in c.name and "shard" in c.name):
            add(c.name, "COMPUTED_COLUMN", "medium",
                f"{'VIRTUAL' if c.virtual else 'STORED'} computed column: {c.generated}",
                "VIRTUAL: not migrated, recreate as Spanner generated column; STORED: migrated as a plain column")
        if c.hidden and "crdb_internal" in c.name and "shard" in c.name:
            add(c.name, "HASH_SHARDED", "info", "hash-sharded index/PK shard column",
                "drop - Spanner distributes keys itself (but avoid monotonic keys)")
        tm = map_type(c.sql_type, catalog.enums)
        if tm.severity:
            rule = ("TYPE_COLLATED_STRING" if " COLLATE " in c.sql_type.upper() else
                    "TYPE_ENUM" if (tm.note or "").startswith("ENUM ") else
                    "TYPE_" + c.sql_type.split("(")[0].upper().replace(" ", "_").replace("[]", "_ARRAY"))
            add(c.name, rule,
                tm.severity, tm.note or f"{c.sql_type} -> {tm.spanner_type}",
                f"maps to {tm.spanner_type}")

    if "LOCALITY REGIONAL BY ROW" in ddl:
        add(None, "REGIONAL_BY_ROW", "high", "REGIONAL BY ROW table",
            "design Spanner multi-region config / geo-partitioning")
    elif "LOCALITY REGIONAL BY TABLE IN" in ddl:
        add(None, "REGIONAL_BY_TABLE", "medium", "REGIONAL BY TABLE homed outside the primary region",
            "Spanner multi-region config with a matching default leader / placement")
    elif "LOCALITY GLOBAL" in ddl:
        add(None, "GLOBAL_TABLE", "medium", "GLOBAL table (low-latency reads everywhere)",
            "Spanner multi-region with read replicas")
    if "TTL_EXPIRE_AFTER" in ddl or "TTL_EXPIRATION_EXPRESSION" in ddl:
        add(None, "ROW_TTL", "medium", "row-level TTL (its deletes flow through the changefeed)",
            "Spanner row deletion policy (TTL); set ttl_disable_changefeed_replication at source to "
            "leave expiry to Spanner, otherwise TTL deletes are replicated")
    if count_families(t.create_statement) > 1:
        add(None, "COLUMN_FAMILIES", "high",
            "multiple column families: changefeeds need split_column_families, which CDC apply does not "
            "support yet (roadmap)", "exclude the table for now; split_column_families support is on the roadmap (no source change needed)")
    sigs: dict[tuple, str] = {}
    for i in t.indexes:
        sig = (i.unique, tuple(i.columns), tuple(i.directions), tuple(sorted(i.storing)), i.partial)
        if sig in sigs:
            add(None, "DUPLICATE_INDEX", "medium", f"index {i.name} duplicates {sigs[sig]}",
                "skipped in Spanner (avoids write amplification)")
        sigs.setdefault(sig, i.name)
        if any(c.startswith("crdb_internal_idx_expr") for c in i.columns):
            expr = ", ".join(e for e in (i.expressions or []) if e) or "expression"
            add(None, "EXPRESSION_INDEX", "medium", f"expression index {i.name} on ({expr})",
                "generated: stored generated column + index; verify the expression in Spanner")
        if i.inverted:
            add(None, "INVERTED_INDEX", "medium", f"inverted index {i.name}",
                "Spanner search index or JSON redesign")
        if i.partial:
            add(None, "PARTIAL_INDEX", "medium", f"partial index {i.name} WHERE {i.partial}",
                "full index, or NULL-filtered index pattern")
    if t.checks:
        add(None, "CHECK_CONSTRAINTS", "info", f"{len(t.checks)} CHECK constraint(s)",
            "generated in the post phase (applied after cutover), CockroachDB casts removed")
    if t.foreign_keys:
        add(None, "FOREIGN_KEYS", "info", f"{len(t.foreign_keys)} foreign key(s)",
            "applied after cutover (post DDL); consider interleaving for parent/child")
    return out


def table_scores(findings: list[Finding]) -> dict[str, str]:
    worst: dict[str, int] = {}
    for f in findings:
        if f.table:
            worst[f.table] = max(worst.get(f.table, 0), SEVERITY_ORDER[f.severity])
    return {t: {4: "red", 3: "amber", 2: "amber", 1: "green"}[s] for t, s in worst.items()}


def render_markdown(catalog: Catalog, findings: list[Finding]) -> str:
    scores = table_scores(findings)
    lines = [
        f"# Assessment: `{catalog.database}`",
        "",
        f"- CockroachDB: `{catalog.version.split(' (')[0]}`",
        f"- Tables: {len(catalog.tables)}  ·  Enums: {len(catalog.enums)}  ·  Sequences: {len(catalog.sequences)}",
        "- Findings: " + ", ".join(
            f"{s}: {sum(1 for f in findings if f.severity == s)}" for s in SEVERITY_ORDER),
        "",
        "## Tables",
        "",
        "| Table | Est. rows | Columns | Score |",
        "|---|---:|---:|---|",
    ]
    for t in catalog.tables:
        lines.append(f"| {t.name} | {t.estimated_rows if t.estimated_rows is not None else '?'} | "
                     f"{len([c for c in t.columns if not c.hidden])} | {scores.get(t.name, 'green')} |")
    lines += ["", "## Findings", "", "| Severity | Table | Column | Rule | Finding | Remedy |",
              "|---|---|---|---|---|---|"]
    for f in sorted(findings, key=lambda x: (-SEVERITY_ORDER[x.severity], x.table or "", x.column or "")):
        lines.append(f"| {f.severity} | {f.table or '(cluster)'} | {f.column or ''} | {f.rule} | "
                     f"{f.message} | {f.remedy} |")
    return "\n".join(lines) + "\n"


def render_json(findings: list[Finding]) -> str:
    return json.dumps([asdict(f) for f in findings], indent=2)
