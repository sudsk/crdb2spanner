"""SMT bridge: rewrite CockroachDB DDL into a PostgreSQL-compatible schema file that the
Spanner Migration Tool (SMT) can load via its PostgreSQL (pg_dump) path.

Input:  output of ``SHOW CREATE ALL TABLES`` (or concatenated ``SHOW CREATE TABLE``).
Output: plain PostgreSQL DDL - CREATE TABLE with inline PK, then CREATE INDEX statements,
        then ALTER TABLE ... FOREIGN KEY statements.

Best effort by design: anything it cannot translate is emitted as a ``-- c2s:`` comment so
the architect sees it in SMT's review step. Status: M0 spike - validate against SMT.
"""

from __future__ import annotations

import re

_TYPE_REWRITES = [
    (r"\bSTRING\b", "text"),
    (r"\bBYTES\b", "bytea"),
    (r"\bINT8\b", "bigint"),
    (r"\bINT4\b", "integer"),
    (r"\bINT2\b", "smallint"),
    (r"\bFLOAT8\b", "double precision"),
    (r"\bFLOAT4\b", "real"),
    (r"\bBOOL\b", "boolean"),
    (r"\bTIMESTAMPTZ\b", "timestamp with time zone"),
    (r"\bTIMESTAMP\b(?! with)", "timestamp without time zone"),
]


def _rewrite_types(s: str) -> str:
    for pat, rep in _TYPE_REWRITES:
        s = re.sub(pat, rep, s, flags=re.IGNORECASE)
    return s


def _split_statements(ddl: str) -> list[str]:
    stmts, buf, depth, in_str = [], [], 0, False
    for ch in ddl:
        buf.append(ch)
        if ch == "'":
            in_str = not in_str
        elif not in_str:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == ";" and depth == 0:
                stmts.append("".join(buf).strip())
                buf = []
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return [s for s in stmts if s and s != ";"]


def _split_body(body: str) -> list[str]:
    parts, buf, depth, in_str = [], [], 0, False
    for ch in body:
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


def _matching_paren(s: str, open_idx: int) -> int:
    depth, in_str = 0, False
    for i in range(open_idx, len(s)):
        ch = s[i]
        if ch == "'":
            in_str = not in_str
        elif not in_str:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return i
    return -1


def crdb_to_pg_ddl(ddl: str) -> str:
    tables, indexes, fks, notes = [], [], [], []

    for stmt in _split_statements(ddl):
        s = stmt.rstrip(";").strip()
        up = s.upper()
        if up.startswith("ALTER TABLE") and "FOREIGN KEY" in up:
            fks.append(_rewrite_types(re.sub(r"\bNOT VALID\b", "", s, flags=re.I)).strip() + ";")
            continue
        if up.startswith("ALTER TABLE") and "VALIDATE CONSTRAINT" in up:
            continue
        if not up.startswith("CREATE TABLE"):
            notes.append(f"-- c2s: skipped statement: {s.splitlines()[0][:120]}")
            continue

        m = re.match(r"CREATE TABLE\s+([\w.\"]+)\s*\(", s, re.I)
        close = _matching_paren(s, m.end() - 1) if m else -1
        if not m or close < 0:
            notes.append(f"-- c2s: could not parse: {s.splitlines()[0][:120]}")
            continue
        tname = m.group(1).split(".")[-1]
        body, trailer = s[m.end():close], s[close + 1:]
        if trailer.strip():
            notes.append(f"-- c2s: {tname}: dropped table options: {' '.join(trailer.split())[:200]}")

        cols_out = []
        for part in _split_body(body):
            p = part.strip()
            pu = p.upper()
            if pu.startswith("FAMILY "):
                continue
            if pu.startswith("CONSTRAINT") and "PRIMARY KEY" in pu:
                pk = re.search(r"PRIMARY KEY\s*\((.*?)\)", p, re.I).group(1)
                pk = re.sub(r"\s+(ASC|DESC)\b", "", pk, flags=re.I)
                cols_out.append(f"PRIMARY KEY ({pk})")
                continue
            if pu.startswith("CONSTRAINT") and "CHECK" in pu:
                cols_out.append(p)
                continue
            im = re.match(r"(UNIQUE\s+)?(INVERTED\s+)?INDEX\s+(\w+)\s*\((.*?)\)(.*)$", p, re.I | re.S)
            if im:
                uniq, inv, iname, icols, rest = im.groups()
                if inv:
                    notes.append(f"-- c2s: {tname}: inverted index {iname} needs manual mapping")
                    continue
                icols = re.sub(r"\s+ASC\b", "", icols, flags=re.I)
                stmt_i = f"CREATE {'UNIQUE ' if uniq else ''}INDEX {iname} ON {tname} ({icols})"
                st = re.search(r"STORING\s*\((.*?)\)", rest, re.I)
                if st:
                    stmt_i += f" INCLUDE ({st.group(1)})"
                wh = re.search(r"\bWHERE\b(.*)$", rest, re.I | re.S)
                if wh:
                    stmt_i += f" WHERE{wh.group(1).rstrip()}"
                if "USING HASH" in rest.upper():
                    notes.append(f"-- c2s: {tname}: hash-sharded index {iname} -> plain index")
                indexes.append(_rewrite_types(stmt_i) + ";")
                continue
            # column definition
            if "crdb_internal_" in p and "_shard" in p:
                notes.append(f"-- c2s: {tname}: dropped hash-shard column")
                continue
            p = re.sub(r"\bNOT VISIBLE\b", "", p, flags=re.I)
            p = re.sub(r"\s+DEFAULT unique_rowid\(\)", "", p, flags=re.I)
            p = re.sub(r":::[\w\[\]]+", "", p)  # type annotations on defaults
            p = re.sub(r"\bAS \((.*)\) (STORED|VIRTUAL)", r"GENERATED ALWAYS AS (\1) STORED", p, flags=re.I)
            cols_out.append(_rewrite_types(" ".join(p.split())))

        tables.append(f"CREATE TABLE {tname} (\n    " + ",\n    ".join(cols_out) + "\n);")

    out = ["-- Generated by crdb2spanner smt-bridge: PostgreSQL-compatible DDL for SMT", ""]
    out += notes + [""] if notes else []
    out += tables + [""] + indexes + [""] + fks
    return "\n".join(out).strip() + "\n"
