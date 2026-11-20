"""Unit coverage for incompatibilities exercised by tests/e2e (virtual computed columns,
collated strings, column families)."""

from crdb2spanner.assess.rules import assess
from crdb2spanner.crdb.catalog import Catalog, Column, Table, count_families, virtual_columns
from crdb2spanner.schema.converter import convert
from crdb2spanner.schema.types import map_type

ORDER_DDL = """CREATE TABLE public."Order" (
  customer_id UUID NOT NULL,
  amount DECIMAL(18,4) NOT NULL,
  tax DECIMAL(18,4) NULL AS (amount * 0.2:::DECIMAL) STORED,
  amount_cents INT8 NULL AS ((amount * 100:::DECIMAL)::INT8) VIRTUAL,
  CONSTRAINT "Order_pkey" PRIMARY KEY (customer_id ASC)
)"""

FAMILIES_DDL = """CREATE TABLE public.wide_families (
  id INT8 NOT NULL,
  a STRING NULL,
  b STRING NULL,
  CONSTRAINT wide_families_pkey PRIMARY KEY (id ASC),
  FAMILY fa (id, a),
  FAMILY fb (b)
)"""


def test_virtual_and_family_parsing():
    assert virtual_columns(ORDER_DDL) == {"amount_cents"}
    assert count_families(FAMILIES_DDL) == 2
    assert count_families(ORDER_DDL) == 0


def _order_catalog():
    cols = [
        Column("customer_id", "UUID", nullable=False, ordinal=1),
        Column("amount", "DECIMAL(18,4)", nullable=False, ordinal=2),
        Column("tax", "DECIMAL(18,4)", generated="amount * 0.2", ordinal=3),
        Column("amount_cents", "INT8", generated="(amount * 100)::INT8", virtual=True, ordinal=4),
        Column("name_ci", "STRING COLLATE en_u_ks_level2", ordinal=5),
    ]
    fam = Table("wide_families", [Column("id", "INT8", nullable=False, ordinal=1),
                                  Column("a", "STRING", ordinal=2)], ["id"], create_statement=FAMILIES_DDL)
    return Catalog("torture", tables=[Table("Order", cols, ["customer_id"], create_statement=ORDER_DDL), fam],
                   settings={"kv.rangefeed.enabled": "true"})


def test_virtual_column_excluded_and_stored_kept():
    res = convert(_order_catalog(), only=["Order"])
    tm = res.mapping.table("Order")
    assert [c.source for c in tm.columns] == ["customer_id", "amount", "tax", "name_ci"]
    ddl = res.render("tables")
    assert 'CREATE TABLE "Order" (' in ddl and "amount_cents" not in ddl
    assert any("VIRTUAL computed column" in i.message for i in res.issues)


def test_collated_string():
    m = map_type("STRING COLLATE en_u_ks_level2")
    assert m.spanner_type == "varchar" and m.transform == "to_text" and m.severity == "medium"
    assert "en_u_ks_level2" in m.note
    assert map_type("VARCHAR(20) COLLATE de").spanner_type == "varchar(20)"


def test_assess_flags_families_virtual_and_collation():
    rules = {(f.table, f.rule, f.severity) for f in assess(_order_catalog())}
    assert ("wide_families", "COLUMN_FAMILIES", "high") in rules
    assert ("Order", "TYPE_COLLATED_STRING", "medium") in rules
    assert ("Order", "COMPUTED_COLUMN", "medium") in rules
