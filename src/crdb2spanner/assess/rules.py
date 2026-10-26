"""Assessment rules: Catalog -> findings + per-table score + Markdown/JSON report."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from ..crdb.catalog import Catalog, Table
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

    for t in catalog.tables:
        if (only and t.name not in only) or t.name in exclude:
            continue
        f.extend(_table_rules(t, catalog))
    return f


def _table_rules(t: Table, catalog: Catalog) -> list[Finding]:
    out: list[Finding] = []
    ddl = t.create_statement.upper()
    add = lambda *a: out.append(Finding(t.name, *a))  # noqa: E731

    if not t.primary_key:
        add(None, "NO_PK", "blocker", "no primary key", "add an explicit primary key")
    elif t.has_hidden_rowid_pk:
        add("rowid", "HIDDEN_ROWID_PK", "high",
            "table relies on hidden rowid (unique_rowid) primary key",
            "add an explicit PK at source, or accept bit-reversed rowid in Spanner")

    for c in t.columns:
        d = (c.default or "").lower()
        if c.name in t.primary_key and not c.hidden and d.startswith("unique_rowid()"):
            add(c.name, "MONOTONIC_PK", "high", "PK default unique_rowid() is monotonic",
                "bit-reversed sequence or UUID in Spanner; app impact if ids are exposed")
        if d.startswith("nextval("):
            add(c.name, "SEQUENCE_DEFAULT", "high", f"sequence default {c.default}",
                "CREATE SEQUENCE ... BIT_REVERSED_POSITIVE in Spanner")
        if c.generated:
            add(c.name, "COMPUTED_COLUMN", "medium", f"computed column: {c.generated}",
                "Spanner generated column, or compute in app")
        if c.hidden and "crdb_internal" in c.name and "shard" in c.name:
            add(c.name, "HASH_SHARDED", "info", "hash-sharded index/PK shard column",
                "drop - Spanner distributes keys itself (but avoid monotonic keys)")
        tm = map_type(c.sql_type, catalog.enums)
        if tm.severity:
            add(c.name, "TYPE_" + c.sql_type.split("(")[0].upper().replace(" ", "_").replace("[]", "_ARRAY"),
                tm.severity, tm.note or f"{c.sql_type} -> {tm.spanner_type}",
                f"maps to {tm.spanner_type}")

    if "LOCALITY REGIONAL BY ROW" in ddl:
        add(None, "REGIONAL_BY_ROW", "high", "REGIONAL BY ROW table",
            "design Spanner multi-region config / geo-partitioning")
    elif "LOCALITY GLOBAL" in ddl:
        add(None, "GLOBAL_TABLE", "medium", "GLOBAL table (low-latency reads everywhere)",
            "Spanner multi-region with read replicas")
    if "TTL_EXPIRE_AFTER" in ddl or "TTL_EXPIRATION_EXPRESSION" in ddl:
        add(None, "ROW_TTL", "medium", "row-level TTL", "Spanner row deletion policy (TTL)")
    if ddl.count("FAMILY ") > 1:
        add(None, "COLUMN_FAMILIES", "info", "multiple column families", "none - dropped")
    for i in t.indexes:
        if i.inverted:
            add(None, "INVERTED_INDEX", "medium", f"inverted index {i.name}",
                "Spanner search index or JSON redesign")
        if i.partial:
            add(None, "PARTIAL_INDEX", "medium", f"partial index {i.name} WHERE {i.partial}",
                "full index, or NULL-filtered index pattern")
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
