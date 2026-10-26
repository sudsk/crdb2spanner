"""Shared fixtures: a catalog modelled on CockroachDB's `movr` sample plus an `events` table
that exercises the awkward cases (hidden rowid, INT4, INTERVAL, enum, arrays, inverted index)."""

import pytest

from crdb2spanner.crdb.catalog import Catalog, Column, ForeignKey, Index, Table


def _cols(*specs):
    out = []
    for i, s in enumerate(specs, 1):
        name, typ, *rest = s
        kw = rest[0] if rest else {}
        out.append(Column(name=name, sql_type=typ, ordinal=i, **kw))
    return out


@pytest.fixture
def movr_catalog() -> Catalog:
    users = Table(
        name="users",
        columns=_cols(("id", "UUID", {"nullable": False}), ("city", "VARCHAR", {"nullable": False}),
                      ("name", "VARCHAR"), ("address", "VARCHAR"), ("credit_card", "VARCHAR")),
        primary_key=["city", "id"],
        create_statement="CREATE TABLE public.users (\n id UUID NOT NULL,\n city VARCHAR NOT NULL,\n"
                         " name VARCHAR NULL,\n CONSTRAINT users_pkey PRIMARY KEY (city ASC, id ASC)\n)",
        estimated_rows=50,
    )
    vehicles = Table(
        name="vehicles",
        columns=_cols(("id", "UUID", {"nullable": False}), ("city", "VARCHAR", {"nullable": False}),
                      ("type", "VARCHAR"), ("owner_id", "UUID"), ("creation_time", "TIMESTAMP"),
                      ("status", "VARCHAR"), ("current_location", "VARCHAR"), ("ext", "JSONB")),
        primary_key=["city", "id"],
        indexes=[Index(name="vehicles_auto_index_fk_city_ref_users", columns=["city", "owner_id"],
                       directions=["ASC", "ASC"])],
        foreign_keys=[ForeignKey("vehicles_city_owner_id_fkey", ["city", "owner_id"], "users", ["city", "id"])],
        create_statement="CREATE TABLE public.vehicles (...)",
        estimated_rows=15,
    )
    rides = Table(
        name="rides",
        columns=_cols(("id", "UUID", {"nullable": False}), ("city", "VARCHAR", {"nullable": False}),
                      ("vehicle_city", "VARCHAR"), ("rider_id", "UUID"), ("vehicle_id", "UUID"),
                      ("start_time", "TIMESTAMP"), ("end_time", "TIMESTAMP"), ("revenue", "DECIMAL(10,2)")),
        primary_key=["city", "id"],
        foreign_keys=[ForeignKey("rides_city_rider_id_fkey", ["city", "rider_id"], "users", ["city", "id"])],
        checks=["CHECK ((vehicle_city = city))"],
        create_statement="CREATE TABLE public.rides (...)",
    )
    events = Table(
        name="events",
        columns=_cols(("rowid", "INT8", {"nullable": False, "hidden": True, "default": "unique_rowid()"}),
                      ("kind", "event_kind"), ("attempts", "INT4"), ("took", "INTERVAL"),
                      ("tags", "STRING[]"), ("payload", "JSONB"), ("seen_at", "TIMESTAMPTZ",
                                                                     {"default": "now():::TIMESTAMPTZ"})),
        primary_key=["rowid"],
        indexes=[Index(name="events_payload_idx", columns=["payload"], inverted=True)],
        create_statement="CREATE TABLE public.events (\n rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),\n"
                         " payload JSONB NULL,\n INVERTED INDEX events_payload_idx (payload)\n) WITH (ttl_expire_after = '30 days')",
    )
    return Catalog(database="movr", version="CockroachDB CCL v24.3.1 (x86_64-pc-linux-gnu)",
                   tables=[users, vehicles, rides, events],
                   enums={"event_kind": ["created", "updated"]},
                   settings={"kv.rangefeed.enabled": "true"})
