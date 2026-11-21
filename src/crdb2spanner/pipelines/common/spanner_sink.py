"""Spanner sink (native Python DoFn, works for PostgreSQL-dialect databases).

Input elements: ``(shard_key, Iterable[Op])`` from GroupIntoBatches.

Modes
  bulk    blind InsertOrUpdate (snapshot rows, no HLC)
  fast    in-batch dedupe by key (max HLC) + blind InsertOrUpdate/Delete
  strict  read-write txn: read shadow table HLCs for the batch keys, apply only newer ops,
          upsert the shadow rows. Correct across job restarts, replays and reordering.

Every batch is split so no commit exceeds ``max_mutations`` (Spanner caps a commit at 80,000
mutations, counting every column written and every secondary-index entry). Commits use
``max_commit_delay`` (throughput vs latency) and a per-commit timeout; aborted transactions
are retried by the client. A failing chunk is retried op by op so one bad row goes to the DLQ
instead of failing its neighbours.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from collections import defaultdict
from typing import Iterable

import apache_beam as beam
from apache_beam.metrics import Metrics

from ...schema.mapping import MappingSpec
from ...schema.values import canonical, from_wire, wire
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


def op_weight(op: Op, index_count: int, strict: bool) -> int:
    """Conservative estimate of the Spanner mutations one op costs."""
    if op.kind == "delete":
        w = 1 + index_count
    else:
        w = len(op.row or {}) * (1 + index_count)
    if strict and op.hlc is not None:
        w += len(op.key) + 3  # shadow-table row
    return max(w, 1)


def chunk_by_mutations(ops: list[Op], weights: dict[str, int], strict: bool, max_mutations: int) -> list[list[Op]]:
    """Greedy split of ops into chunks whose estimated mutation count stays under the limit."""
    chunks, cur, total = [], [], 0
    for op in ops:
        w = op_weight(op, weights.get(op.target_table, 0), strict)
        if cur and total + w > max_mutations:
            chunks.append(cur)
            cur, total = [], 0
        cur.append(op)
        total += w
    if cur:
        chunks.append(cur)
    return chunks


def _wire_row(values) -> list:
    return [wire(v) for v in values]


def _group_mutations(ops: list[Op]):
    upserts: dict[tuple[str, tuple[str, ...]], list[list]] = defaultdict(list)
    deletes: dict[str, list[list]] = defaultdict(list)
    for op in ops:
        if op.kind == "delete":
            deletes[op.target_table].append(_wire_row(op.key))
        else:
            cols = tuple(op.row.keys())
            upserts[(op.target_table, cols)].append(_wire_row(op.row[c] for c in cols))
    return upserts, deletes


class SpannerApply(beam.DoFn):
    def __init__(self, project: str, instance: str, database: str, mapping_json: str,
                 mode: str = "strict", emulator_host: str | None = None, *,
                 max_commit_delay_ms: int = 100, commit_timeout_s: int = 60,
                 max_mutations: int = 60000, transaction_tag: str = "crdb2spanner"):
        assert mode in ("bulk", "fast", "strict")
        assert 0 <= max_commit_delay_ms <= 500, "Spanner allows a max_commit_delay of 0-500 ms"
        self.project, self.instance_id, self.database_id = project, instance, database
        self.mapping_json, self.mode, self.emulator_host = mapping_json, mode, emulator_host
        self.max_commit_delay_ms, self.commit_timeout_s = max_commit_delay_ms, commit_timeout_s
        self.max_mutations, self.transaction_tag = max_mutations, transaction_tag
        self.applied = Metrics.counter("spanner", "ops_applied")
        self.skipped = Metrics.counter("spanner", "ops_skipped_stale")
        self.failed = Metrics.counter("spanner", "ops_failed")
        self.batches = Metrics.counter("spanner", "batches")
        self.commits = Metrics.counter("spanner", "commits")

    def setup(self):
        if self.emulator_host:
            os.environ["SPANNER_EMULATOR_HOST"] = self.emulator_host
        from google.cloud import spanner

        self._spanner = spanner
        self.client = spanner.Client(project=self.project)
        self.db = self.client.instance(self.instance_id).database(self.database_id)
        mapping = MappingSpec.from_dict(json.loads(self.mapping_json))
        self.shadow = {t.target: (t.shadow_table, t.target_pk) for t in mapping.tables}
        self.index_count = {t.target: t.index_count for t in mapping.tables}
        self._commit_kw = {"timeout_secs": self.commit_timeout_s}
        if self.max_commit_delay_ms and not self.emulator_host:  # emulator ignores/rejects it
            self._commit_kw["max_commit_delay"] = dt.timedelta(milliseconds=self.max_commit_delay_ms)

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
        strict = self.mode == "strict"
        for chunk in chunk_by_mutations(ops, self.index_count, strict, self.max_mutations):
            yield from self._write_chunk(chunk)

    def _write_chunk(self, ops: list[Op]):
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
        self.commits.inc()
        if self.mode == "strict" and any(o.hlc is not None for o in ops):
            self.db.run_in_transaction(self._strict_txn, ops, transaction_tag=self.transaction_tag,
                                       **self._commit_kw)
            return
        upserts, deletes = _group_mutations(ops)
        with self.db.batch(**self._commit_kw) as b:
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
                          keyset=self._spanner.KeySet(keys=[_wire_row(o.key) for o in tops]))
            for row in rs:
                k = table + "|" + "|".join(canonical(from_wire(v)) for v in row[: len(pk_cols)])
                seen[k] = HLC(int(row[-2]), int(row[-1]))
            for op in tops:
                prev = seen.get(op.ident())
                if op.hlc is not None and prev is not None and op.hlc <= prev:
                    self.skipped.inc()
                    continue
                winners.append(op)
                if op.hlc is not None:
                    shadow_rows[shadow_table].append(
                        _wire_row(op.key) + [op.hlc.wall, op.hlc.logical, op.kind == "delete"])

        upserts, deletes = _group_mutations(winners)
        for (table, cols), values in upserts.items():
            txn.insert_or_update(table=table, columns=list(cols), values=values)
        for table, keys in deletes.items():
            txn.delete(table=table, keyset=self._spanner.KeySet(keys=keys))
        for shadow_table, values in shadow_rows.items():
            pk_cols = next(pk for st, pk in self.shadow.values() if st == shadow_table)
            txn.insert_or_update(table=shadow_table, columns=pk_cols + SHADOW_COLS, values=values)
