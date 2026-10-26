from crdb2spanner.assess.rules import assess, render_markdown, table_scores
from crdb2spanner.crdb.catalog import Catalog
from crdb2spanner.schema.converter import convert
from crdb2spanner.schema.mapping import MappingSpec
from crdb2spanner.schema.smt_bridge import crdb_to_pg_ddl


def test_catalog_json_roundtrip(movr_catalog):
    import json

    again = Catalog.from_dict(json.loads(movr_catalog.to_json()))
    assert again == movr_catalog


def test_convert_tables_and_mapping(movr_catalog):
    res = convert(movr_catalog, target_database="movr")
    ddl = res.render("tables")
    assert "CREATE TABLE users (" in ddl
    assert "id varchar(36) NOT NULL" in ddl
    assert "PRIMARY KEY (city, id)" in ddl
    assert "revenue numeric" in ddl
    assert "creation_time timestamptz" in ddl
    assert "CREATE TABLE c2s_shadow_users (" in ddl and "c2s_hlc_wall bigint NOT NULL" in ddl
    # hidden rowid becomes an explicit, bit-reversed bigint PK
    assert "rowid bigint NOT NULL" in ddl
    ev = res.mapping.table("events")
    assert ev.col("rowid").transform == "bit_reverse"
    assert ev.col("took").spanner_type == "varchar(64)"
    assert ev.col("tags").spanner_type == "varchar[]"
    assert "seen_at timestamptz DEFAULT (CURRENT_TIMESTAMP)" in ddl
    # mapping round-trips
    again = MappingSpec.from_dict(__import__("json").loads(res.mapping.to_json()))
    assert [t.source for t in again.tables] == ["users", "vehicles", "rides", "events"]


def test_convert_deferred_ddl(movr_catalog):
    res = convert(movr_catalog)
    assert res.index_ddl == ["CREATE INDEX vehicles_auto_index_fk_city_ref_users ON vehicles (city, owner_id)"]
    assert any("FOREIGN KEY (city, owner_id) REFERENCES users (city, id)" in s for s in res.post_ddl)
    msgs = " ".join(i.message for i in res.issues)
    assert "inverted index events_payload_idx skipped" in msgs
    assert "bit-reversed" in msgs


def test_convert_fast_mode_has_no_shadow(movr_catalog):
    res = convert(movr_catalog, strict_shadow=False)
    assert "c2s_shadow_" not in res.render("tables")
    assert res.mapping.table("users").shadow_table is None


def test_mapping_converts_rows_and_keys(movr_catalog):
    tm = convert(movr_catalog).mapping.table("events")
    key = tm.convert_key([1])
    row = tm.convert_row({"kind": "created", "attempts": 3, "took": "00:01:00", "tags": ["a"]}, key=[1])
    assert row["rowid"] == key[0] != 1
    assert row["attempts"] == 3 and row["tags"] == ["a"]


def test_assess(movr_catalog):
    findings = assess(movr_catalog)
    rules = {(f.table, f.rule) for f in findings}
    assert ("events", "HIDDEN_ROWID_PK") in rules
    assert ("events", "INVERTED_INDEX") in rules
    assert ("events", "ROW_TTL") in rules
    assert ("events", "TYPE_INT4") in rules
    assert ("vehicles", "FOREIGN_KEYS") in rules
    assert table_scores(findings)["events"] == "amber"
    assert "| events |" in render_markdown(movr_catalog, findings)


def test_assess_rangefeed_blocker(movr_catalog):
    movr_catalog.settings = {}
    assert any(f.rule == "CLUSTER_RANGEFEED" and f.severity == "blocker" for f in assess(movr_catalog))


SAMPLE_CRDB_DDL = """
CREATE TABLE public.vehicles (
  id UUID NOT NULL,
  city VARCHAR NOT NULL,
  owner_id UUID NULL,
  creation_time TIMESTAMP NULL,
  ext JSONB NULL,
  bump INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
  CONSTRAINT vehicles_pkey PRIMARY KEY (city ASC, id ASC),
  INDEX vehicles_auto_index_fk_city_ref_users (city ASC, owner_id ASC) STORING (ext),
  INVERTED INDEX vehicles_ext_idx (ext),
  FAMILY "primary" (id, city, owner_id, creation_time, ext, bump)
) WITH (ttl_expire_after = '30 days');
ALTER TABLE public.vehicles ADD CONSTRAINT vehicles_city_owner_id_fkey FOREIGN KEY (city, owner_id) REFERENCES public.users(city, id);
ALTER TABLE public.vehicles VALIDATE CONSTRAINT vehicles_city_owner_id_fkey;
"""


def test_smt_bridge():
    out = crdb_to_pg_ddl(SAMPLE_CRDB_DDL)
    assert "CREATE TABLE vehicles (" in out
    assert "PRIMARY KEY (city, id)" in out
    assert "creation_time timestamp without time zone NULL" in out
    assert "bump bigint NOT NULL" in out and "unique_rowid" not in out and "NOT VISIBLE" not in out
    assert "FAMILY" not in out.split("--")[-1] or "FAMILY \"primary\"" not in out
    assert "CREATE INDEX vehicles_auto_index_fk_city_ref_users ON vehicles (city, owner_id) INCLUDE (ext);" in out
    assert "inverted index vehicles_ext_idx needs manual mapping" in out
    assert "dropped table options" in out
    assert "FOREIGN KEY (city, owner_id) REFERENCES public.users(city, id);" in out
    assert "VALIDATE CONSTRAINT" not in out
