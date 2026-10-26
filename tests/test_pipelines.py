"""Pipeline correctness: duplicates, reordering and deletes must converge to the source's
final state. Spanner is replaced by an in-memory fake that applies ops with the same
semantics, so these run on DirectRunner with no cloud dependencies."""

import json
import random
import threading

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to

from crdb2spanner.config import ApplyConfig, CdcConfig, JobConfig, SourceConfig, TargetConfig
from crdb2spanner.pipelines import cdc_apply
from crdb2spanner.pipelines.common import spanner_sink
from crdb2spanner.pipelines.common.envelope import HLC, ChangeEvent
from crdb2spanner.pipelines.common.lww import LatestWinsDoFn
from crdb2spanner.pipelines.common.ops import Op
from crdb2spanner.schema.converter import convert
from crdb2spanner.validate.compare import Side, row_digest

# ------------------------------------------------------------------------------ fakes
STORE: dict = {}
LOCK = threading.Lock()


class FakeSpannerApply(beam.DoFn):
    """Applies batches to STORE with fast-mode semantics (in-batch dedupe, blind write)."""

    def __init__(self, *a, **kw):
        pass

    def process(self, element):
        _, ops = element
        with LOCK:
            for op in spanner_sink.dedupe(ops):
                k = (op.target_table, op.key)
                if op.kind == "delete":
                    STORE.pop(k, None)
                else:
                    STORE[k] = op.row
        return []


def _line(key, after, hlc):
    return json.dumps({"after": after, "key": key, "updated": str(hlc)})


@pytest.fixture
def job(tmp_path, movr_catalog):
    cfg = JobConfig(
        job_id="t", source=SourceConfig(database="movr", dsn="postgresql://x"),
        target=TargetConfig(project="p", instance="i", database="d"),
        cdc=CdcConfig(sink="files", files_read_path=str(tmp_path / "cdc")),
        apply=ApplyConfig(mode="fast", num_shards=4, batch_size=3, dlq_path=str(tmp_path / "dlq")),
        work_dir=str(tmp_path / "jobs"),
    )
    mapping = convert(movr_catalog, strict_shadow=False).mapping
    return cfg, mapping, tmp_path


def test_cdc_files_replay_converges(job, monkeypatch):
    cfg, mapping, tmp = job
    monkeypatch.setattr(cdc_apply, "SpannerApply", FakeSpannerApply)
    STORE.clear()

    u = lambda i: f"00000000-0000-0000-0000-{i:012d}"  # noqa: E731
    lines, expected = [], {}
    for i in range(30):
        key = ["rome", u(i)]
        versions = [{"id": u(i), "city": "rome", "name": f"v{v}"} for v in range(3)]
        for v, after in enumerate(versions):
            lines.append(_line(key, after, HLC(1000 + v, i)))
        if i % 5 == 0:  # deleted at the end
            lines.append(_line(key, None, HLC(2000, i)))
        else:
            expected[("users", ("rome", u(i)))] = "v2"
    lines += lines[:20]  # duplicates
    random.Random(7).shuffle(lines)  # out of order
    lines.append(json.dumps({"after": {"x": 1}, "key": "oops", "updated": "1.0"}))  # poison

    d = tmp / "cdc" / "users" / "2026-10-25"
    d.mkdir(parents=True)
    (d / "202610251200000000000000000-a-users-1.ndjson").write_text("\n".join(lines[:60]) + "\n")
    (d / "202610251200100000000000000-b-users-1.ndjson").write_text("\n".join(lines[60:]) + "\n")
    (tmp / "cdc" / "users" / "202610251200200000000000000.RESOLVED").write_text('{"resolved":"3000.0000000000"}')

    with TestPipeline() as p:
        cdc_apply.build(p, cfg, mapping, ["users"], source="files", streaming=False)

    got = {k: v["name"] for k, v in STORE.items()}
    assert got == expected
    dlq = list((tmp / "dlq").glob("cdc*.jsonl"))
    assert dlq and "no key array" in dlq[0].read_text()


def test_lww_streaming_drops_stale_and_duplicates():
    def ev(v, hlc):
        return ChangeEvent("users", ("rome", "a"), {"name": v}, HLC(hlc))

    stream = (TestStream()
              .add_elements([("k", ev("new", 5))])
              .add_elements([("k", ev("old", 3)), ("k", ev("new", 5))])  # stale + duplicate
              .add_elements([("k", ev("newer", 9))])
              .advance_watermark_to_infinity())
    with TestPipeline(options=beam.options.pipeline_options.PipelineOptions(streaming=True)) as p:
        out = (p | stream | beam.ParDo(LatestWinsDoFn()) | beam.Map(lambda e: e.after["name"]))
        assert_that(out, equal_to(["new", "newer"]))


def test_dedupe_keeps_newest_and_bulk_last():
    a = Op("t", "t", "upsert", (1,), {"id": 1, "v": "a"}, HLC(5))
    b = Op("t", "t", "upsert", (1,), {"id": 1, "v": "b"}, HLC(3))
    c = Op("t", "t", "delete", (1,), None, HLC(7))
    assert spanner_sink.dedupe([a, b]) == [a]
    assert spanner_sink.dedupe([b, c, a]) == [c]
    x = Op("t", "t", "upsert", (2,), {"id": 2}, None)
    y = Op("t", "t", "upsert", (2,), {"id": 2, "v": 1}, None)
    assert spanner_sink.dedupe([x, y]) == [y]


class FakeTxn:
    def __init__(self, shadow_rows):
        self.shadow_rows, self.writes, self.deletes = shadow_rows, [], []

    def read(self, table, columns, keyset):
        return [r for r in self.shadow_rows if list(r[:-2]) in keyset.keys]

    def insert_or_update(self, table, columns, values):
        self.writes.append((table, columns, values))

    def delete(self, table, keyset):
        self.deletes.append((table, keyset.keys))


def test_strict_mode_skips_ops_older_than_shadow():
    from google.cloud import spanner

    sink = spanner_sink.SpannerApply("p", "i", "d", "{}", mode="strict")
    sink._spanner = spanner
    sink.shadow = {"users": ("c2s_shadow_users", ["city", "id"])}
    sink.skipped = sink.applied = type("C", (), {"inc": lambda *a: None})()
    txn = FakeTxn([["rome", "a", 10, 0]])  # shadow says key (rome,a) last applied at HLC 10
    ops = [
        Op("users", "users", "upsert", ("rome", "a"), {"city": "rome", "id": "a", "name": "stale"}, HLC(8)),
        Op("users", "users", "upsert", ("rome", "b"), {"city": "rome", "id": "b", "name": "fresh"}, HLC(9)),
        Op("users", "users", "delete", ("rome", "c"), None, HLC(11)),
    ]
    sink._strict_txn(txn, ops)
    data = [w for w in txn.writes if w[0] == "users"]
    shadow = [w for w in txn.writes if w[0] == "c2s_shadow_users"][0]
    assert data == [("users", ["city", "id", "name"], [["rome", "b", "fresh"]])]
    assert txn.deletes == [("users", [["rome", "c"]])]
    assert shadow[1] == ["city", "id", "c2s_hlc_wall", "c2s_hlc_logical", "c2s_deleted"]
    assert sorted(shadow[2]) == [["rome", "b", 9, 0, False], ["rome", "c", 11, 0, True]]


def test_validation_buckets_are_order_independent():
    rows = [((i,), [i, f"v{i}"]) for i in range(100)]
    a, b = Side(), Side()
    for k, r in rows:
        a.add(k, row_digest(r), 16, None)
    for k, r in reversed(rows):
        b.add(k, row_digest(r), 16, None)
    assert a.bucket_sum == b.bucket_sum and a.rows == b.rows == 100
    c = Side()
    for k, r in rows[:-1] + [((99,), [99, "changed"])]:
        c.add(k, row_digest(r), 16, None)
    assert sum(1 for x in a.bucket_sum if a.bucket_sum[x] != c.bucket_sum.get(x)) == 1
