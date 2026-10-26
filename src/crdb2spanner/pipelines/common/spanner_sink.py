"""Spanner sink (native Python DoFn, works for PostgreSQL-dialect databases).

Input elements: ``(shard_key, Iterable[Op])`` from GroupIntoBatches.

Modes
  bulk    blind InsertOrUpdate (snapshot rows, no HLC)
  fast    in-batch dedupe by key (max HLC) + blind InsertOrUpdate/Delete
  strict  read-write txn: read shadow table HLCs for the batch keys, apply only newer ops,
          upsert the shadow rows. Correct across job restarts, replays and reordering.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from typing import Iterable

import apache_beam as beam
from apache_beam.metrics import Metrics

from ...schema.mapping import MappingSpec
from ...schema.values import canonical
from .envelope import HLC
from .ops import DLQ, Op, dlq_record

SHADOW_COLS = ["c2s_hlc_wall", "c2s_hlc_logical", "c2s_deleted"]


def dedupe(ops: Iterable[Op]) -> list[Op]:
    """Keep the newest op per key; bulk ops (hlc None) keep the last one seen."""
    best: dict[str, Op] = {}
    for op in ops:
        k = op.ident()
        cur = best.get(k)
        if cur is None or op.hlc is None or (cur.hlc is not None and op.hlc > cur.hlc):
            best[k] = op
    return list(best.values())


def _group_mutations(ops: list[Op]):
    upserts: dict[tuple[str, tuple[str, ...]], list[list]] = defaultdict(list)
    deletes: dict[str, list[list]] = defaultdict(list)
    for op in ops:
        if op.kind == "delete":
            deletes[op.target_table].append(list(op.key))
        else:
            cols = tuple(op.row.keys())
            upserts[(op.target_table, cols)].append([op.row[c] for c in cols])
    return upserts, deletes


class SpannerApply(beam.DoFn):
    def __init__(self, project: str, instance: str, database: str, mapping_json: str,
                 mode: str = "strict", emulator_host: str | None = None):
        assert mode in ("bulk", "fast", "strict")
        self.project, self.instance_id, self.database_id = project, instance, database
        self.mapping_json, self.mode, self.emulator_host = mapping_json, mode, emulator_host
        self.applied = Metrics.counter("spanner", "ops_applied")
        self.skipped = Metrics.counter("spanner", "ops_skipped_stale")
        self.failed = Metrics.counter("spanner", "ops_failed")
        self.batches = Metrics.counter("spanner", "batches")

    def setup(self):
        if self.emulator_host:
            os.environ["SPANNER_EMULATOR_HOST"] = self.emulator_host
        from google.cloud import spanner

        self._spanner = spanner
        self.client = spanner.Client(project=self.project)
        self.db = self.client.instance(self.instance_id).database(self.database_id)
        mapping = MappingSpec.from_dict(json.loads(self.mapping_json))
        self.shadow = {t.target: (t.shadow_table, t.target_pk) for t in mapping.tables}

    def teardown(self):
        try:
            self.client.close()
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------------------
    def process(self, element):
        _, ops = element
        ops = dedupe(ops)
        if not ops:
            return
        self.batches.inc()
        try:
            self._write(ops)
            self.applied.inc(len(ops))
        except Exception:  # noqa: BLE001 - isolate the poison op(s)
            for op in ops:
                try:
                    self._write([op])
                    self.applied.inc()
                except Exception as e:  # noqa: BLE001
                    self.failed.inc()
                    yield beam.pvalue.TaggedOutput(DLQ, dlq_record("spanner_write", {
                        "table": op.target_table, "kind": op.kind, "key": list(op.key),
                        "row": op.row, "hlc": str(op.hlc) if op.hlc else None}, e))

    def _write(self, ops: list[Op]) -> None:
        if self.mode == "strict" and any(o.hlc is not None for o in ops):
            self.db.run_in_transaction(self._strict_txn, ops)
            return
        upserts, deletes = _group_mutations(ops)
        with self.db.batch() as b:
            for (table, cols), values in upserts.items():
                b.insert_or_update(table=table, columns=list(cols), values=values)
            for table, keys in deletes.items():
                b.delete(table=table, keyset=self._spanner.KeySet(keys=keys))

    def _strict_txn(self, txn, ops: list[Op]) -> None:
        by_table: dict[str, list[Op]] = defaultdict(list)
        for op in ops:
            by_table[op.target_table].append(op)

        winners: list[Op] = []
        shadow_rows: dict[str, list[list]] = defaultdict(list)
        for table, tops in by_table.items():
            shadow_table, pk_cols = self.shadow.get(table, (None, None))
            if not shadow_table:
                winners.extend(tops)
                continue
            seen: dict[str, HLC] = {}
            rs = txn.read(table=shadow_table, columns=pk_cols + SHADOW_COLS[:2],
                          keyset=self._spanner.KeySet(keys=[list(o.key) for o in tops]))
            for row in rs:
                k = table + "|" + "|".join(canonical(v) for v in row[: len(pk_cols)])
                seen[k] = HLC(int(row[-2]), int(row[-1]))
            for op in tops:
                prev = seen.get(op.ident())
                if op.hlc is not None and prev is not None and op.hlc <= prev:
                    self.skipped.inc()
                    continue
                winners.append(op)
                if op.hlc is not None:
                    shadow_rows[shadow_table].append(
                        list(op.key) + [op.hlc.wall, op.hlc.logical, op.kind == "delete"])

        upserts, deletes = _group_mutations(winners)
        for (table, cols), values in upserts.items():
            txn.insert_or_update(table=table, columns=list(cols), values=values)
        for table, keys in deletes.items():
            txn.delete(table=table, keyset=self._spanner.KeySet(keys=keys))
        for shadow_table, values in shadow_rows.items():
            pk_cols = next(pk for st, pk in self.shadow.values() if st == shadow_table)
            txn.insert_or_update(table=shadow_table, columns=pk_cols + SHADOW_COLS, values=values)
