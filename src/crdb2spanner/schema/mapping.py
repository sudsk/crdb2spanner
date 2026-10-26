"""Mapping spec: the single contract every pipeline reads (source table/column -> target).

Format ``crdb2spanner.mapping/v1`` (JSON). It is produced by the built-in converter today;
an importer for SMT session files (so architects can make decisions in the SMT UI) is on
the roadmap and will emit this same structure.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

from .values import get_transform

FORMAT = "crdb2spanner.mapping/v1"


@dataclass
class ColumnMap:
    source: str
    target: str
    spanner_type: str
    transform: str = "identity"
    nullable: bool = True


@dataclass
class TableMap:
    source: str
    target: str
    columns: list[ColumnMap]
    primary_key: list[str]  # SOURCE column names, in key order
    shadow_table: str | None = None

    # compiled lazily, not serialised
    _fns: dict[str, Callable[[Any], Any]] = field(default_factory=dict, repr=False, compare=False)

    def col(self, source_name: str) -> ColumnMap:
        for c in self.columns:
            if c.source == source_name:
                return c
        raise KeyError(f"{self.source}.{source_name}")

    def fn(self, source_name: str) -> Callable[[Any], Any]:
        if source_name not in self._fns:
            self._fns[source_name] = get_transform(self.col(source_name).transform)
        return self._fns[source_name]

    @property
    def target_columns(self) -> list[str]:
        return [c.target for c in self.columns]

    @property
    def target_pk(self) -> list[str]:
        return [self.col(s).target for s in self.primary_key]

    def convert_key(self, key: list | tuple) -> tuple:
        """Changefeed key array (source PK order) -> target key tuple."""
        if len(key) != len(self.primary_key):
            raise ValueError(f"{self.source}: key {key!r} does not match PK {self.primary_key}")
        return tuple(self.fn(c)(v) for c, v in zip(self.primary_key, key))

    def convert_row(self, row: dict[str, Any], key: list | tuple | None = None) -> dict[str, Any]:
        """Source row dict -> target row dict (target column names).

        ``key`` (from the changefeed envelope) fills PK columns that are absent from the row,
        e.g. CockroachDB's hidden ``rowid``.
        """
        out: dict[str, Any] = {}
        for c in self.columns:
            if c.source in row:
                out[c.target] = self.fn(c.source)(row[c.source])
        if key is not None:
            for s, v in zip(self.primary_key, key):
                tgt = self.col(s).target
                if tgt not in out:
                    out[tgt] = self.fn(s)(v)
        return out


@dataclass
class MappingSpec:
    source_database: str
    target_database: str
    tables: list[TableMap]
    format: str = FORMAT

    def table(self, source_name: str) -> TableMap:
        for t in self.tables:
            if t.source == source_name:
                return t
        raise KeyError(source_name)

    def to_json(self) -> str:
        d = asdict(self)
        for t in d["tables"]:
            t.pop("_fns", None)
        return json.dumps(d, indent=2)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "MappingSpec":
        if d.get("format") != FORMAT:
            raise ValueError(f"unsupported mapping format {d.get('format')!r}")
        tables = [
            TableMap(
                source=t["source"], target=t["target"],
                columns=[ColumnMap(**c) for c in t["columns"]],
                primary_key=t["primary_key"], shadow_table=t.get("shadow_table"),
            )
            for t in d["tables"]
        ]
        return cls(source_database=d["source_database"], target_database=d["target_database"],
                   tables=tables)

    @classmethod
    def load(cls, path: str | Path) -> "MappingSpec":
        return cls.from_dict(json.loads(Path(path).read_text()))
