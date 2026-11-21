"""Commit sizing/settings, enriched envelopes, multi-region keys, non-keyable index columns,
and the source-version advisory rule."""

import datetime as dt

from crdb2spanner.assess.rules import assess, crdb_version
from crdb2spanner.crdb.catalog import Catalog, Column, Index, Table
from crdb2spanner.pipelines.common import spanner_sink
from crdb2spanner.pipelines.common.envelope import HLC, parse_message
from crdb2spanner.pipelines.common.ops import Op
from crdb2spanner.schema.converter import convert
from crdb2spanner.schema.types import map_type


# --- mutation-aware chunking --------------------------------------------------------
def _op(i, ncols=10, kind="upsert"):
    row = {f"c{j}": j for j in range(ncols)} | {"id": i}
    return Op("t", "t", kind, (i,), None if kind == "delete" else row, HLC(i + 1))


def test_op_weight_counts_columns_indexes_and_shadow():
    up = _op(1, ncols=9)  # 10 columns incl. id
    assert spanner_sink.op_weight(up, index_count=0, strict=False) == 10
    assert spanner_sink.op_weight(up, index_count=2, strict=False) == 30
    assert spanner_sink.op_weight(up, index_count=0, strict=True) == 10 + 1 + 3
    assert spanner_sink.op_weight(_op(2, kind="delete"), index_count=2, strict=False) == 3


def test_chunk_by_mutations_respects_limit():
    ops = [_op(i, ncols=99) for i in range(50)]  # 100 mutations each
    chunks = spanner_sink.chunk_by_mutations(ops, {"t": 0}, strict=False, max_mutations=1000)
    assert [len(c) for c in chunks] == [10, 10, 10, 10, 10]
    assert sum(len(c) for c in chunks) == 50
    # a single op larger than the limit still goes alone (Spanner will reject it -> DLQ)
    big = spanner_sink.chunk_by_mutations([_op(1, ncols=5000)], {}, strict=False, max_mutations=1000)
    assert len(big) == 1


class _FakeBatch:
    def __init__(self, log, kw):
        self.log, self.kw = log, kw

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.log.append(("commit", self.kw))

    def insert_or_update(self, table, columns, values):
        self.log.append(("upsert", table, len(values)))

    def delete(self, table, keyset):
        self.log.append(("delete", table, len(keyset.keys)))


class _FakeDb:
    def __init__(self):
        self.log = []

    def batch(self, **kw):
        return _FakeBatch(self.log, kw)


def test_sink_splits_commits_and_passes_commit_settings():
    from google.cloud import spanner

    sink = spanner_sink.SpannerApply("p", "i", "d", "{}", mode="fast", max_commit_delay_ms=200,
                                     commit_timeout_s=45, max_mutations=500)
    sink._spanner, sink.db, sink.index_count = spanner, _FakeDb(), {"t": 0}
    sink._commit_kw = {"timeout_secs": 45, "max_commit_delay": dt.timedelta(milliseconds=200)}
    out = list(sink.process(("t|0", [_op(i, ncols=99) for i in range(12)])))  # 1,200 mutations
    commits = [e for e in sink.db.log if e[0] == "commit"]
    assert out == [] and len(commits) == 3  # 5 + 5 + 2 rows
    assert commits[0][1] == {"timeout_secs": 45, "max_commit_delay": dt.timedelta(milliseconds=200)}
    assert sum(e[2] for e in sink.db.log if e[0] == "upsert") == 12


def test_commit_delay_must_be_in_spanner_range():
    import pytest

    with pytest.raises(AssertionError):
        spanner_sink.SpannerApply("p", "i", "d", "{}", max_commit_delay_ms=900)


# --- enriched envelope ---------------------------------------------------------------
def test_enriched_envelope_upsert_and_delete():
    up = parse_message('{"after": {"id": 1, "v": "x"}, "key": [1], "op": "u", '
                       '"source": {"ts_hlc": "1795220532601888664.0000000003", "ts_ns": 1}}', table="t")
    assert up.hlc == HLC(1795220532601888664, 3) and up.after == {"id": 1, "v": "x"}
    de = parse_message('{"after": {"id": 1}, "key": [1], "op": "d", "source": {"ts_hlc": "5.0000000001"}}',
                       table="t")
    assert de.is_delete and de.hlc == HLC(5, 1)


# --- multi-region (REGIONAL BY ROW) ---------------------------------------------------
def _rbr_catalog():
    cols = [
        Column("id", "UUID", nullable=False, ordinal=1),
        Column("name", "STRING", ordinal=2),
        Column("crdb_region", "public.crdb_internal_region", nullable=False, hidden=True,
               default="default_to_database_primary_region(gateway_region())", ordinal=3),
    ]
    t = Table("accounts", cols, ["id"], implicit_key=["crdb_region"],
              create_statement="CREATE TABLE accounts (...) LOCALITY REGIONAL BY ROW")
    return Catalog("mr", tables=[t], settings={"kv.rangefeed.enabled": "true"})


def test_regional_by_row_keeps_region_as_column_not_key():
    res = convert(_rbr_catalog())
    ddl = res.render("tables")
    assert "crdb_region varchar(64) NOT NULL" in ddl and "PRIMARY KEY (id)" in ddl
    tm = res.mapping.table("accounts")
    assert tm.primary_key == ["id"] and tm.source_key == ["crdb_region", "id"]
    u = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    assert tm.convert_key(["europe-west2", u]) == (u,)       # changefeed key carries the region
    assert tm.convert_key([u]) == (u,)
    row = tm.convert_row({"name": "x"}, key=["europe-west2", u])
    assert row == {"name": "x", "id": u, "crdb_region": "europe-west2"}
    msgs = " ".join(i.message for i in res.issues)
    assert "REGIONAL BY ROW" in msgs and "implicit key columns" in msgs


def test_crdb_internal_region_type():
    m = map_type("public.crdb_internal_region")
    assert m.spanner_type == "varchar(64)" and m.severity == "info"


# --- Spanner PG cannot index jsonb / numeric / real / arrays ----------------------------
def test_indexes_on_non_keyable_or_unmigrated_columns_are_skipped():
    cols = [
        Column("id", "INT8", nullable=False, ordinal=1),
        Column("doc", "JSONB", ordinal=2),
        Column("amount", "DECIMAL(10,2)", ordinal=3),
        Column("v", "INT8", generated="id * 2", virtual=True, ordinal=4),
        Column("name", "STRING", ordinal=5),
    ]
    t = Table("t", cols, ["id"], indexes=[
        Index("by_amount", ["amount"]), Index("by_doc", ["doc"]), Index("by_v", ["v"]),
        Index("by_name", ["name"], storing=["amount", "v"]),
    ])
    res = convert(Catalog("db", tables=[t]))
    assert res.index_ddl == ["CREATE INDEX by_name ON t (name) INCLUDE (amount)"]
    assert res.mapping.table("t").index_count == 1
    msgs = " ".join(i.message for i in res.issues)
    assert "by_amount skipped" in msgs and "by_doc skipped" in msgs and "by_v skipped" in msgs


def test_non_keyable_pk_is_a_blocker():
    t = Table("t", [Column("k", "DECIMAL", nullable=False, ordinal=1)], ["k"])
    res = convert(Catalog("db", tables=[t]))
    assert any(i.severity == "blocker" and "cannot be a Spanner key" in i.message for i in res.issues)


# --- source version advisory -----------------------------------------------------------
def test_crdb_version_parsing_and_advisory_rule():
    assert crdb_version("CockroachDB CCL v24.3.37 (x86_64-pc-linux-gnu)") == (24, 3, 37)
    affected = Catalog("db", version="CockroachDB CCL v23.2.5", settings={"kv.rangefeed.enabled": "true"})
    fixed = Catalog("db", version="CockroachDB CCL v24.1.4", settings={"kv.rangefeed.enabled": "true"})
    assert any(f.rule == "CRDB_OUT_OF_ORDER_ADVISORY" for f in assess(affected))
    assert not any(f.rule == "CRDB_OUT_OF_ORDER_ADVISORY" for f in assess(fixed))


def test_bytes_are_base64_encoded_for_the_spanner_client():
    import base64

    from crdb2spanner.pipelines.common.spanner_sink import _group_mutations, wire

    raw = bytes(range(256))
    assert base64.b64decode(wire(raw)) == raw
    assert wire(b"") == b""
    assert wire([b"ab", None]) == [base64.b64encode(b"ab"), None]
    assert wire("text") == "text" and wire(5) == 5
    ops = [Op("t", "t", "upsert", (b"\x00k",), {"id": b"\x00k", "payload": b"\xff\xfe"}, None),
           Op("t", "t", "delete", (b"\x01",), None, None)]
    ups, dels = _group_mutations(ops)
    assert ups[("t", ("id", "payload"))] == [[base64.b64encode(b"\x00k"), base64.b64encode(b"\xff\xfe")]]
    assert dels["t"] == [[base64.b64encode(b"\x01")]]


def test_local_batch_runs_pin_fnapi_runner(monkeypatch):
    from apache_beam.options.pipeline_options import PipelineOptions

    from crdb2spanner.pipelines.common.io import pick_local_runner

    monkeypatch.delenv("C2S_LOCAL_RUNNER", raising=False)
    assert pick_local_runner(PipelineOptions([]), streaming=False) == "FnApiRunner"
    assert pick_local_runner(PipelineOptions([]), streaming=True) is None
    assert pick_local_runner(PipelineOptions(["--runner", "DataflowRunner"]), streaming=False) == "DataflowRunner"
    monkeypatch.setenv("C2S_LOCAL_RUNNER", "PrismRunner")
    assert pick_local_runner(PipelineOptions([]), streaming=False) == "PrismRunner"


def test_from_wire_decodes_bytes_read_back_from_spanner():
    from crdb2spanner.schema.values import from_wire, wire

    raw = b"\x00\x01\xff"
    assert from_wire(wire(raw)) == raw
    assert from_wire([wire(raw), None]) == [raw, None]
    assert from_wire("s") == "s" and from_wire(1) == 1


def test_decimals_go_to_spanner_as_exact_text():
    from decimal import Decimal

    from crdb2spanner.schema.values import wire

    assert wire(Decimal("12345678901234567890.123456789012345")) == "12345678901234567890.123456789012345"
    assert wire(Decimal("NaN")) == "NaN"
    assert wire(Decimal("1E-30")) == "0." + "0" * 29 + "1"
    assert wire([Decimal("1.5"), None]) == ["1.5", None]


def test_validation_rounds_real_columns_to_float32_and_shows_column_diffs():
    from crdb2spanner.schema.mapping import ColumnMap, TableMap
    from crdb2spanner.validate.compare import _column_diff, _normalise, normalisers

    tm = TableMap(source="t", target="t", columns=[ColumnMap("id", "id", "bigint"), ColumnMap("r", "r", "real")],
                  primary_key=["id"])
    norms = normalisers(tm)
    assert _normalise([1, 0.1], norms) == [1, _normalise([1, 0.10000000149011612], norms)[1]]
    assert "r: source=1.0 target=2.0" in _column_diff(tm, ["1", "1.0"], ["1", "2.0"])


def test_region_move_same_hlc_upsert_beats_delete():
    """REGIONAL BY ROW: moving a row's region deletes (old_region, id) and inserts (new_region, id)
    at one HLC; both map to Spanner key (id,). The row must survive in any processing order."""
    from crdb2spanner.pipelines.common.envelope import HLC, supersedes
    from crdb2spanner.pipelines.common.spanner_sink import dedupe

    t = HLC(10, 0)
    assert supersedes(t, False, t, True) and not supersedes(t, True, t, False)
    assert not supersedes(t, False, t, False) and not supersedes(t, True, t, True)  # replays are no-ops
    assert supersedes(HLC(11, 0), True, t, False) and not supersedes(HLC(9, 0), False, t, True)
    up = Op("accounts", "accounts", "upsert", ("a1",), {"id": "a1", "crdb_region": "asia-south1"}, t)
    rm = Op("accounts", "accounts", "delete", ("a1",), None, t)
    assert [o.kind for o in dedupe([up, rm])] == ["upsert"]
    assert [o.kind for o in dedupe([rm, up])] == ["upsert"]
