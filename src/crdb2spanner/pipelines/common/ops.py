"""Mutation ops: the normalised unit the Spanner sink consumes (target names + values)."""

from __future__ import annotations

import json
import traceback
from dataclasses import dataclass
from typing import Any

import apache_beam as beam
from apache_beam.metrics import Metrics

from ...schema.mapping import MappingSpec
from ...schema.values import canonical
from .envelope import HLC, ChangeEvent

DLQ = "dlq"


@dataclass(frozen=True)
class Op:
    source_table: str
    target_table: str
    kind: str  # "upsert" | "delete"
    key: tuple  # target-typed PK values
    row: dict[str, Any] | None  # target column -> value (upserts)
    hlc: HLC | None  # None for bulk-load rows (snapshot at T0)

    def ident(self) -> str:
        return self.target_table + "|" + "|".join(canonical(k) for k in self.key)


def dlq_record(stage: str, payload: Any, error: BaseException | str) -> str:
    return json.dumps({
        "stage": stage,
        "error": str(error),
        "trace": traceback.format_exc(limit=3) if isinstance(error, BaseException) else None,
        "payload": payload,
    }, default=str)


class EventToOp(beam.DoFn):
    """ChangeEvent (source names, JSON values) -> Op (target names, Spanner values)."""

    def __init__(self, mapping_json: str):
        self.mapping_json = mapping_json
        self.unknown = Metrics.counter("cdc", "unknown_table")
        self.errors = Metrics.counter("cdc", "transform_errors")

    def setup(self):
        self.mapping = MappingSpec.from_dict(json.loads(self.mapping_json))
        self._tables = {t.source: t for t in self.mapping.tables}

    def process(self, ev: ChangeEvent):
        tm = self._tables.get(ev.table)
        if tm is None:
            self.unknown.inc()
            return
        try:
            key = tm.convert_key(ev.key)
            if ev.is_delete:
                yield Op(tm.source, tm.target, "delete", key, None, ev.hlc)
            else:
                row = tm.convert_row(ev.after, key=ev.key)
                for pk_col, v in zip(tm.target_pk, key):
                    row[pk_col] = v  # PK always from the envelope key (handles hidden rowid)
                yield Op(tm.source, tm.target, "upsert", key, row, ev.hlc)
        except Exception as e:  # noqa: BLE001
            self.errors.inc()
            yield beam.pvalue.TaggedOutput(DLQ, dlq_record(
                "transform", {"table": ev.table, "key": list(ev.key), "after": ev.after,
                              "hlc": str(ev.hlc)}, e))


class RowToOp(beam.DoFn):
    """Snapshot row (source names, Parquet values) -> upsert Op."""

    def __init__(self, mapping_json: str, table: str):
        self.mapping_json = mapping_json
        self.table = table
        self.errors = Metrics.counter("bulk", "transform_errors")

    def setup(self):
        self.tm = MappingSpec.from_dict(json.loads(self.mapping_json)).table(self.table)

    def process(self, row: dict):
        try:
            out = self.tm.convert_row(row)
            key = tuple(out[c] for c in self.tm.target_pk)
            yield Op(self.tm.source, self.tm.target, "upsert", key, out, None)
        except Exception as e:  # noqa: BLE001
            self.errors.inc()
            yield beam.pvalue.TaggedOutput(DLQ, dlq_record("transform", {"table": self.table, "row": row}, e))
