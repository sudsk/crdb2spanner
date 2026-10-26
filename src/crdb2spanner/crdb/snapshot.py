"""Consistent snapshot: pick T0 (an HLC timestamp) and EXPORT every table AS OF SYSTEM TIME T0.

The same T0 is later used as the changefeed ``cursor``, so bulk + CDC have no gap/overlap.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from ..schema.mapping import MappingSpec


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


@dataclass
class SnapshotManifest:
    t0: str
    tables: dict[str, str] = field(default_factory=dict)  # table -> export URI
    rows: dict[str, int] = field(default_factory=dict)

    def save(self, path: Path) -> None:
        path.write_text(json.dumps(asdict(self), indent=2))

    @classmethod
    def load(cls, path: Path) -> "SnapshotManifest":
        return cls(**json.loads(Path(path).read_text()))


def current_hlc(conn) -> str:
    cur = conn.cursor()
    cur.execute("SELECT cluster_logical_timestamp()::STRING")
    return cur.fetchone()[0]


def gc_ttl_seconds(conn, database: str, table: str) -> int | None:
    try:
        cur = conn.cursor()
        cur.execute(f"SHOW ZONE CONFIGURATION FROM TABLE {_q(database)}.public.{_q(table)}")
        raw = cur.fetchone()[1]
        for line in raw.splitlines():
            if "gc.ttlseconds" in line:
                return int(line.split("=")[1].strip().rstrip(","))
    except Exception:  # noqa: BLE001
        return None
    return None


def export_statement(database: str, schema: str, table: str, columns: list[str], uri: str, t0: str) -> str:
    cols = ", ".join(_q(c) for c in columns)  # explicit list: includes hidden rowid when mapped
    return (
        f"EXPORT INTO PARQUET '{uri}' FROM SELECT {cols} FROM "
        f"{_q(database)}.{_q(schema)}.{_q(table)} AS OF SYSTEM TIME '{t0}'"
    )


def take_snapshot(conn, database: str, schema: str, mapping: MappingSpec, export_uri: str,
                  t0: str | None = None, log=print) -> SnapshotManifest:
    t0 = t0 or current_hlc(conn)
    manifest = SnapshotManifest(t0=t0)
    base, _, qs = export_uri.partition("?")
    cur = conn.cursor()
    for tm in mapping.tables:
        uri = f"{base.rstrip('/')}/{tm.source}"
        uri_full = f"{uri}?{qs}" if qs else uri
        stmt = export_statement(database, schema, tm.source, [c.source for c in tm.columns], uri_full, t0)
        log(f"EXPORT {tm.source} AS OF {t0} -> {uri}")
        cur.execute(stmt)
        rows = sum(int(r[1]) for r in cur.fetchall())  # (filename, rows, bytes)
        manifest.tables[tm.source] = uri
        manifest.rows[tm.source] = rows
    return manifest
