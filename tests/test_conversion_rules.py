"""Identifier renames, database-wide names, sequences, defaults, expression/duplicate indexes,
CHECK constraints, TTL policies and natural-key overrides."""

import json
import re

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from crdb2spanner.config import CdcConfig
from crdb2spanner.crdb.catalog import Catalog, Column, ForeignKey, Index, Table
from crdb2spanner.crdb.changefeed import create_statement
from crdb2spanner.pipelines.common.envelope import HLC, ChangeEvent, parse_message
from crdb2spanner.pipelines.common.ops import DLQ, EventToOp
from crdb2spanner.schema.converter import ConvertOptions, convert, strip_casts


def col(name, typ, i, **kw):
    return Column(name=name, sql_type=typ, ordinal=i, **kw)


def test_leading_underscore_column_renamed_everywhere():
    t = Table("doc_messages", [col("_id", "STRING", 1, nullable=False), col("body", "JSONB", 2),
                                col("sent", "TIMESTAMPTZ", 3)],
              ["_id"], indexes=[Index("om_sent_idx", ["sent"], storing=["_id"])])
    child = Table("acks", [col("id", "INT8", 1, nullable=False), col("msg_id", "STRING", 2)], ["id"],
                  foreign_keys=[ForeignKey("acks_msg_fk", ["msg_id"], "doc_messages", ["_id"])])
    res = convert(Catalog("db", tables=[t, child]))
    ddl = res.render("tables")
    assert "id_ varchar NOT NULL" in ddl and "PRIMARY KEY (id_)" in ddl
    assert not re.search(r"(?<![\w])_id\b", ddl)
    assert "REFERENCES doc_messages (id_)" in res.render("post")
    tm = res.mapping.table("doc_messages")
    assert tm.col("_id").target == "id_" and tm.target_pk == ["id_"]
    assert any("renamed to id_" in i.message for i in res.issues)


def test_rename_columns_config_wins():
    t = Table("orders", [col("_id", "STRING", 1, nullable=False)], ["_id"])
    res = convert(Catalog("db", tables=[t]),
                  options=ConvertOptions(rename_columns={"orders._id": "order_doc_id"}))
    assert "order_doc_id varchar NOT NULL" in res.render("tables")


def test_index_and_fk_names_unique_across_database():
    a = Table("a", [col("id", "INT8", 1, nullable=False), col("created", "TIMESTAMPTZ", 2)], ["id"],
              indexes=[Index("by_created", ["created"])])
    b = Table("b", [col("id", "INT8", 1, nullable=False), col("created", "TIMESTAMPTZ", 2),
                    col("a_id", "INT8", 3)], ["id"],
              indexes=[Index("by_created", ["created"])],
              foreign_keys=[ForeignKey("by_created", ["a_id"], "a", ["id"])])
    res = convert(Catalog("db", tables=[a, b]))
    assert res.index_ddl == ["CREATE INDEX by_created ON a (created)", "CREATE INDEX b_by_created ON b (created)"]
    assert "ADD CONSTRAINT b_by_created_2 FOREIGN KEY" in res.render("post")


def test_sequences_skip_range_and_nextval_default():
    t = Table("batch_job", [col("id", "INT8", 1, nullable=False, default="nextval('public.batch_job_seq'::REGCLASS)"),
                            col("name", "STRING", 2)], ["id"])
    cat = Catalog("db", tables=[t], sequences=["batch_job_seq", "hibernate_sequence"],
                  sequence_values={"batch_job_seq": 12345})
    ddl = convert(cat).render("tables")
    assert "CREATE SEQUENCE batch_job_seq BIT_REVERSED_POSITIVE;" in ddl
    assert "ALTER SEQUENCE batch_job_seq SKIP RANGE 1 1000000000;" in ddl  # floor wins over 12345*100
    assert "CREATE SEQUENCE hibernate_sequence BIT_REVERSED_POSITIVE;" in ddl
    assert "id bigint NOT NULL DEFAULT (nextval('batch_job_seq'))" in ddl
    big = Catalog("db", tables=[], sequences=["s"], sequence_values={"s": 50_000_000})
    assert "SKIP RANGE 1 5000000000" in convert(big).render("tables")


def test_unique_rowid_pk_gets_sequence_skipping_existing_ids():
    t = Table("hits", [col("id", "INT8", 1, nullable=False, default="unique_rowid()"), col("v", "STRING", 2)], ["id"])
    ddl = convert(Catalog("db", tables=[t])).render("tables")
    assert "CREATE SEQUENCE hits_id_seq BIT_REVERSED_POSITIVE;" in ddl
    assert f"ALTER SEQUENCE hits_id_seq SKIP RANGE 1 {(1 << 61) - 1};" in ddl
    assert "id bigint NOT NULL DEFAULT (nextval('hits_id_seq'))" in ddl


def test_defaults_uuid_timestamp_and_typed_literals():
    t = Table("t", [col("id", "UUID", 1, nullable=False, default="gen_random_uuid()"),
                    col("at", "TIMESTAMPTZ", 2, default="now():::TIMESTAMPTZ"),
                    col("status", "STRING", 3, default="'PENDING':::STRING"),
                    col("tags", "JSONB", 4, default="'[]':::JSONB"),
                    col("n", "INT8", 5, default="0:::INT8"),
                    col("ok", "BOOL", 6, default="true")], ["id"])
    ddl = convert(Catalog("db", tables=[t])).render("tables")
    assert "id uuid NOT NULL DEFAULT (gen_random_uuid())" in ddl
    assert "at timestamptz DEFAULT (CURRENT_TIMESTAMP)" in ddl
    assert "status varchar DEFAULT ('PENDING'::varchar)" in ddl
    assert "tags jsonb DEFAULT ('[]'::jsonb)" in ddl
    assert "n bigint DEFAULT (0::bigint)" in ddl and "ok boolean DEFAULT (true)" in ddl
    legacy = convert(Catalog("db", tables=[t]), uuid_native=False).render("tables")
    assert "id varchar(36) NOT NULL DEFAULT (spanner.generate_uuid())" in legacy


def test_expression_index_becomes_generated_column():
    t = Table("product_request", [
        col("id", "INT8", 1, nullable=False), col("developer_email", "STRING", 2),
        col("crdb_internal_idx_expr", "STRING", 3, hidden=True, virtual=True, generated="lower(developer_email)"),
    ], ["id"], indexes=[Index("dev_email_lower_idx", ["crdb_internal_idx_expr"],
                              expressions=["lower(developer_email)"])])
    res = convert(Catalog("db", tables=[t]))
    ddl = res.render("tables")
    assert "dev_email_lower_idx_expr1 varchar GENERATED ALWAYS AS (lower(developer_email)) STORED" in ddl
    assert res.index_ddl == ["CREATE INDEX dev_email_lower_idx ON product_request (dev_email_lower_idx_expr1)"]
    # generated column is not written by the pipelines
    assert [c.source for c in res.mapping.table("product_request").columns] == ["id", "developer_email"]


def test_duplicate_index_skipped():
    t = Table("email_record", [col("id", "INT8", 1, nullable=False), col("email", "STRING", 2)], ["id"],
              indexes=[Index("email_idx", ["email"]), Index("email_id_idx", ["email"])])
    res = convert(Catalog("db", tables=[t]))
    assert res.index_ddl == ["CREATE INDEX email_idx ON email_record (email)"]
    assert any("duplicate index email_id_idx" in i.message for i in res.issues)


def test_check_constraints_in_post_phase():
    t = Table("billing", [col("id", "INT8", 1, nullable=False), col("request_status", "STRING", 2),
                          col("_amount", "DECIMAL", 3)], ["id"],
              checks=["CHECK ((request_status IN ('VALID':::STRING, 'REJECTED':::STRING)))",
                      "CHECK ((_amount >= 0:::DECIMAL))"],
              check_names=["check_request_status", "check_amount"])
    post = convert(Catalog("db", tables=[t])).render("post")
    assert "ADD CONSTRAINT check_request_status CHECK ((request_status IN ('VALID', 'REJECTED')))" in post
    assert "ADD CONSTRAINT check_amount CHECK ((amount_ >= 0)))" not in post  # sanity on parens
    assert "CHECK ((amount_ >= 0))" in post
    assert strip_casts("x = 'a':::public.order_status") == "x = 'a'"


def test_ttl_expression_becomes_spanner_ttl_and_expire_after_is_flagged():
    ddl = ("CREATE TABLE public.health_check_messages (...) WITH (ttl = 'on', ttl_expiration_expression = "
           "e'(received_at + \\'90 days\\':::INTERVAL)', ttl_job_cron = '@hourly')")
    t = Table("health_check_messages", [col("id", "UUID", 1, nullable=False),
                                        col("received_at", "TIMESTAMPTZ", 2)], ["id"], create_statement=ddl)
    res = convert(Catalog("db", tables=[t]))
    assert "ALTER TABLE health_check_messages ADD TTL INTERVAL '90 days' ON received_at" in res.render("post")
    t2 = Table("events", [col("id", "INT8", 1, nullable=False)], ["id"],
               create_statement="CREATE TABLE events (...) WITH (ttl = 'on', ttl_expire_after = '3650 days')")
    res2 = convert(Catalog("db", tables=[t2]))
    assert "ADD TTL" not in res2.render("post")
    assert any("ttl_expire_after" in i.message and i.severity == "high" for i in res2.issues)


# --- natural-key override for keyless tables ------------------------------------------------
def _ops_catalog():
    t = Table("link_target_ops", [
        col("rowid", "INT8", 1, nullable=False, hidden=True, default="unique_rowid()"),
        col("target_operation_id", "INT8", 2, nullable=False), col("api_operation_id", "INT8", 3, nullable=False),
        col("note", "STRING", 4)], ["rowid"])
    return Catalog("db", tables=[t])


def test_primary_key_override_replaces_rowid():
    opts = ConvertOptions(primary_key_overrides={"link_target_ops": ["target_operation_id", "api_operation_id"]})
    res = convert(_ops_catalog(), options=opts)
    ddl = res.render("tables")
    assert "PRIMARY KEY (target_operation_id, api_operation_id)" in ddl and "rowid" not in ddl.split("c2s_shadow")[0]
    tm = res.mapping.table("link_target_ops")
    assert tm.key_from_row and tm.primary_key == ["target_operation_id", "api_operation_id"]
    sql = create_statement("db", "public", "link_target_ops", CdcConfig(sink="files", files_sink_uri="nodelocal://1/c"),
                           "1.0", diff=tm.key_from_row)
    assert sql.endswith(", diff")


def test_envelope_carries_before_image():
    ev = parse_message('{"after": {"a": 2}, "before": {"a": 1}, "key": [7], "updated": "3.0"}', table="t")
    assert ev.before == {"a": 1} and ev.after == {"a": 2}


def test_natural_key_ops_update_key_change_and_delete():
    opts = ConvertOptions(primary_key_overrides={"link_target_ops": ["target_operation_id", "api_operation_id"]})
    mapping_json = convert(_ops_catalog(), options=opts).mapping.to_json()
    t = "link_target_ops"
    events = [
        ChangeEvent(t, (111,), {"target_operation_id": 1, "api_operation_id": 2, "note": "x"}, HLC(5), None),
        ChangeEvent(t, (111,), {"target_operation_id": 1, "api_operation_id": 3, "note": "x"}, HLC(6),
                    {"target_operation_id": 1, "api_operation_id": 2, "note": "x"}),
        ChangeEvent(t, (111,), None, HLC(7), {"target_operation_id": 1, "api_operation_id": 3, "note": "x"}),
        ChangeEvent(t, (222,), None, HLC(8), None),  # delete without before image -> DLQ
    ]
    with TestPipeline() as p:
        out = (p | beam.Create(events) | beam.ParDo(EventToOp(mapping_json)).with_outputs(DLQ, main="ops"))
        ops = out.ops | beam.Map(lambda o: (o.kind, o.key, o.hlc.wall))
        assert_that(ops, equal_to([("upsert", (1, 2), 5), ("delete", (1, 2), 6), ("upsert", (1, 3), 6),
                                   ("delete", (1, 3), 7)]), label="ops")
        dlq = out[DLQ] | beam.Map(lambda s: "WITH diff" in json.loads(s)["error"])
        assert_that(dlq, equal_to([True]), label="dlq")


def test_wave_renders_one_config_per_database(tmp_path):
    from crdb2spanner.control.wave import render_configs

    tpl = tmp_path / "t.yaml"
    tpl.write_text("job_id: '{db}'\nsource: {database: '{db}', dsn: 'postgresql://h/{db}'}\n"
                   "target: {project: p, instance: i, database: '{db}'}\n")
    paths = render_configs(tpl, ["billing", "inventory"], tmp_path / "out")
    assert sorted(paths) == ["billing", "inventory"] and "database: 'billing'" in paths["billing"].read_text()
    with pytest.raises(ValueError):
        (tmp_path / "bad.yaml").write_text("job_id: x\n")
        render_configs(tmp_path / "bad.yaml", ["a"], tmp_path / "o2")


def test_check_expressions_use_postgres_type_names():
    from crdb2spanner.schema.converter import strip_casts

    assert strip_casts("status IN ('A':::STRING, 'B':::STRING)") == "status IN ('A', 'B')"
    assert strip_casts("status::STRING = 'x'") == "status::text = 'x'"
    assert strip_casts("(qty::INT8 > 0) AND (x::FLOAT8[] IS NULL)") == "(qty::bigint > 0) AND (x::double precision[] IS NULL)"
    assert strip_casts("ts::TIMESTAMPTZ") == "ts::timestamptz"


def test_redundant_indexes_dropped_pk_copy_and_unique_cover():
    t = Table("product_history",
              [col("track_code", "UUID", 1, nullable=False), col("product_id", "UUID", 2),
               col("name", "STRING", 3), col("ts", "TIMESTAMPTZ", 4)], ["track_code"],
              indexes=[Index("history_track_code_uq", ["track_code"], unique=True),          # same as the PK
                       Index("history_name_key", ["name"], unique=True),
                       Index("history_name_idx", ["name"]),                              # covered by the unique one
                       Index("history_name_ts_idx", ["name", "ts"]),                     # different key: kept
                       Index("history_name_desc_idx", ["name"], directions=["DESC"]),    # different order: kept
                       Index("history_name_store_idx", ["name"], storing=["ts"])])       # stores more: kept
    res = convert(Catalog("db", tables=[t]))
    names = " ".join(res.index_ddl)
    assert "history_track_code_uq" not in names and "history_name_idx " not in names + " "
    for kept in ("history_name_key", "history_name_ts_idx", "history_name_desc_idx", "history_name_store_idx"):
        assert kept in names
    msgs = " ".join(i.message for i in res.issues)
    assert "same columns as the primary key" in msgs and "unique index history_name_key has the same key" in msgs


def test_key_override_data_check_blocks_conversion_and_assessment():
    from crdb2spanner.assess.rules import assess

    t = Table("link_target_ops", [col("target_id", "STRING", 1), col("op_id", "STRING", 2),
                                        col("rowid", "INT8", 3, nullable=False, hidden=True)], ["rowid"])
    cat = Catalog("db", tables=[t], settings={"kv.rangefeed.enabled": "true"},
                  key_checks={"link_target_ops": {"columns": ["target_id", "op_id"],
                                                        "duplicates": 3, "nulls": 1}})
    opts = ConvertOptions(primary_key_overrides={"link_target_ops": ["target_id", "op_id"]})
    res = convert(cat, options=opts)
    assert any(i.severity == "blocker" and "3 duplicate" in i.message for i in res.issues)
    rules = {f.rule: f.severity for f in assess(cat)}
    assert rules["KEY_OVERRIDE_NOT_UNIQUE"] == "blocker" and rules["KEY_OVERRIDE_NULLS"] == "blocker"
    clean = Catalog("db", tables=[t], key_checks={"link_target_ops": {"columns": ["target_id", "op_id"],
                                                                            "duplicates": 0, "nulls": 0}})
    assert not [i for i in convert(clean, options=opts).issues if i.severity == "blocker"]


def test_sequence_increment_flagged_for_orm_allocation():
    from crdb2spanner.assess.rules import assess

    cat = Catalog("db", sequences=["orders_seq", "job_seq"],
                  sequence_increments={"orders_seq": 50, "job_seq": 1})
    found = [f for f in assess(cat) if f.rule == "SEQUENCE_INCREMENT"]
    assert len(found) == 1 and "orders_seq" in found[0].message and "allocationSize=1" in found[0].remedy
    assert Catalog.from_dict(json.loads(cat.to_json())).sequence_increments == {"orders_seq": 50, "job_seq": 1}


def test_assess_reports_pk_copy_and_unique_covered_indexes():
    from crdb2spanner.assess.rules import assess

    t = Table("user", [col("id", "STRING", 1, nullable=False), col("email", "STRING", 2)], ["id"],
              indexes=[Index("user_id_uq", ["id"], unique=True), Index("user_email_key", ["email"], unique=True),
                       Index("user_email_idx", ["email"])])
    msgs = [f.message for f in assess(Catalog("db", tables=[t])) if f.rule == "DUPLICATE_INDEX"]
    assert any("same columns as the primary key" in m for m in msgs)
    assert any("user_email_idx has the same key as unique index user_email_key" in m for m in msgs)
