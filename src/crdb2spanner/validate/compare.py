"""Validation v0: exact, timestamp-consistent comparison of source and target.

The source is read ``AS OF SYSTEM TIME t`` (a consistent snapshot); the target is read with
a strong read-only snapshot *after* replication has applied everything up to ``t``. Rows are
pushed through the
same mapping transforms, canonicalised and hashed into B buckets by primary key
(order-independent, so PK transforms such as bit-reversed rowid are fine). Mismatched
buckets are drilled into to list the differing keys.

This is exact when the source receives no writes after ``t`` - i.e. at the cutover write
freeze, or in a test env after traffic stops - and the CDC backlog has drained. Live
validation during replication (excluding keys whose shadow HLC > t) and a Beam version for
very large tables are milestone M2.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import struct
from dataclasses import dataclass, field

from ..config import JobConfig
from ..schema.mapping import MappingSpec, TableMap
from ..schema.values import canonical, from_wire, renders_as_text


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def row_digest(values: list) -> int:
    h = hashlib.blake2b(digest_size=8)
    for v in values:
        h.update(canonical(v).encode())
        h.update(b"\x1f")
    return int.from_bytes(h.digest(), "big")


def _float32(v):
    """Spanner ``real`` stores float32; round the source value the same way before comparing."""
    if isinstance(v, float):
        return struct.unpack("f", struct.pack("f", v))[0]
    if isinstance(v, list):
        return [_float32(x) for x in v]
    return v


def normalisers(tm: TableMap) -> list:
    """Per target column: a function applied to both sides before hashing."""
    out = []
    for c in tm.columns:
        t = c.spanner_type.lower()
        out.append(_float32 if t in ("real", "float4", "real[]", "float4[]") else None)
    return out


def _normalise(values: list, norms: list) -> list:
    return [f(v) if f else v for v, f in zip(values, norms)]


def key_bucket(key: tuple, buckets: int) -> int:
    return row_digest(list(key)) % buckets


@dataclass
class Side:
    rows: int = 0
    bucket_sum: dict[int, int] = field(default_factory=dict)
    keys: dict[int, dict[str, int]] = field(default_factory=dict)  # only for drill-down
    values: dict[str, list[str]] = field(default_factory=dict)  # canonical values, drill-down only

    def add(self, key: tuple, values: list, buckets: int, keep: set[int] | None):
        self.rows += 1
        digest = row_digest(values)
        b = key_bucket(key, buckets)
        self.bucket_sum[b] = (self.bucket_sum.get(b, 0) + digest) % (1 << 64)
        if keep is not None and b in keep:
            k = "|".join(canonical(x) for x in key)
            self.keys.setdefault(b, {})[k] = digest
            self.values[k] = [canonical(v) for v in values]


@dataclass
class TableResult:
    table: str
    source_rows: int
    target_rows: int
    mismatched_buckets: int
    sample_diffs: list[str]

    @property
    def ok(self) -> bool:
        return self.source_rows == self.target_rows and self.mismatched_buckets == 0


def _scan_source(conn, cfg: JobConfig, tm: TableMap, as_of: str, buckets: int, keep=None) -> Side:
    side = Side()
    cols = [c.source for c in tm.columns]
    norms = normalisers(tm)
    # text-mapped types (INTERVAL, TIME, INET, ENUM...) are rendered by CockroachDB itself,
    # matching what the changefeed JSON / EXPORT emit
    exprs = [f"{_q(c.source)}::{_text_cast(c.transform)}" if renders_as_text(c.transform) else _q(c.source)
             for c in tm.columns]
    sql = (f"SELECT {', '.join(exprs)} FROM {_q(cfg.source.database)}."
           f"{_q(cfg.source.schema)}.{_q(tm.source)} AS OF SYSTEM TIME '{as_of}'")
    with conn.cursor(name=None) as cur:
        cur.execute(sql)
        while True:
            batch = cur.fetchmany(5000)
            if not batch:
                break
            for r in batch:
                row = tm.convert_row(dict(zip(cols, r)))
                values = _normalise([row[c] for c in tm.target_columns], norms)
                key = tuple(row[c] for c in tm.target_pk)
                side.add(key, values, buckets, keep)
    return side


def _scan_target(db, tm: TableMap, buckets: int, keep=None) -> Side:
    side = Side()
    cols = tm.target_columns
    pk_idx = [cols.index(c) for c in tm.target_pk]
    norms = normalisers(tm)
    with db.snapshot() as snap:  # strong read
        for r in snap.execute_sql(f"SELECT {', '.join(_q(c) for c in cols)} FROM {_q(tm.target)}"):
            r = _normalise([from_wire(v) for v in r], norms)  # BYTES come back base64-encoded
            side.add(tuple(r[i] for i in pk_idx), r, buckets, keep)
    return side


def hlc_to_datetime(hlc: str) -> dt.datetime:
    wall_ns = int(hlc.split(".")[0])
    return dt.datetime.fromtimestamp(wall_ns / 1e9, tz=dt.timezone.utc)


def compare_table(conn, db, cfg: JobConfig, tm: TableMap, as_of_hlc: str, buckets: int = 1024) -> TableResult:
    src_ts = as_of_hlc
    src = _scan_source(conn, cfg, tm, src_ts, buckets)
    tgt = _scan_target(db, tm, buckets)
    bad = {b for b in set(src.bucket_sum) | set(tgt.bucket_sum)
           if src.bucket_sum.get(b) != tgt.bucket_sum.get(b)}
    diffs: list[str] = []
    if bad:
        keep = set(sorted(bad)[:20])
        s2 = _scan_source(conn, cfg, tm, src_ts, buckets, keep)
        t2 = _scan_target(db, tm, buckets, keep)
        for b in keep:
            sk, tk = s2.keys.get(b, {}), t2.keys.get(b, {})
            for k in set(sk) | set(tk):
                if k not in tk:
                    diffs.append(f"missing in target: {k}")
                elif k not in sk:
                    diffs.append(f"extra in target: {k}")
                elif sk[k] != tk[k]:
                    diffs.append(f"different: {k}" + _column_diff(tm, s2.values.get(k), t2.values.get(k)))
                if len(diffs) >= 50:
                    break
    return TableResult(tm.source, src.rows, tgt.rows, len(bad), diffs)


def _text_cast(transform: str | None) -> str:
    """Arrays of text-rendered values stay arrays (STRING[]), matching the snapshot EXPORT."""
    return "STRING[]" if (transform or "").startswith("array:") else "STRING"


def _short(v: str, n: int = 60) -> str:
    if v == "\x00":
        return "NULL"
    return v if len(v) <= n else v[: n - 3] + "..."


def _column_diff(tm: TableMap, src: list[str] | None, tgt: list[str] | None) -> str:
    if not src or not tgt:
        return ""
    parts = [f"{c}: source={_short(a)} target={_short(b)}"
             for c, a, b in zip(tm.target_columns, src, tgt) if a != b]
    return "  [" + "; ".join(parts[:3]) + "]" if parts else ""


def run(cfg: JobConfig, mapping: MappingSpec, tables: list[str], as_of_hlc: str | None = None,
        buckets: int = 1024, log=print) -> list[TableResult]:
    from ..crdb import connect
    from ..crdb.snapshot import current_hlc

    cfg.target.apply_emulator_env()
    from google.cloud import spanner

    db = spanner.Client(project=cfg.target.project).instance(cfg.target.instance).database(cfg.target.database)
    results = []
    with connect(cfg.source.resolve_dsn()) as conn:
        as_of_hlc = as_of_hlc or current_hlc(conn)
        log(f"validating as of {as_of_hlc} ({hlc_to_datetime(as_of_hlc).isoformat()})")
        for t in tables:
            r = compare_table(conn, db, cfg, mapping.table(t), as_of_hlc, buckets)
            log(f"{'OK  ' if r.ok else 'FAIL'} {t}: source={r.source_rows} target={r.target_rows} "
                f"bad_buckets={r.mismatched_buckets}")
            for d in r.sample_diffs[:10]:
                log(f"      {d}")
            results.append(r)
    return results
